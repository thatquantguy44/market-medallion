"""Immutable, typed threshold history snapshots for downstream Delta publication."""

import hashlib
import json
import os
import shutil
import uuid
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.episodes import derive_episodes
from market_data_pipeline.pipelines.reg_sho_thresholds.research_export import _episode_table
from market_data_pipeline.pipelines.reg_sho_thresholds.storage import storage_schemas
from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import warehouse_for
from market_data_pipeline.shared.files import atomic_json, utc_timestamp

SNAPSHOT_VERSION = "reg-sho-delta-snapshot/1"
CALENDAR_BASIS = "configured-and-observed-list-dates/1"
KEYS = {
    "membership": ("version_id",),
    "coverage": ("version_id",),
    "audit": ("run_id",),
    "episodes": ("provider", "market", "raw_symbol", "rule_family", "start_date"),
}


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def read_json(path: Path, maximum=1024 * 1024) -> dict:
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON field")
            value[key] = item
        return value

    try:
        if path.is_symlink() or not path.is_file():
            raise ValueError("JSON artifact is unavailable")
        with path.open("rb") as stream:
            payload = stream.read(maximum + 1)
        if len(payload) > maximum:
            raise ValueError("JSON artifact exceeds its byte limit")
        value = json.loads(payload, object_pairs_hook=unique)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise ValueError("invalid JSON artifact") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON artifact requires an object")
    return value


def snapshot_schemas():
    return {
        **{
            name: schema.arrow_schema()
            for name, schema in storage_schemas().items()
            if name in ("membership", "coverage", "audit")
        },
        "episodes": _episode_table([]).schema,
    }


def _latest(rows, keys):
    selected = {}
    for row in sorted(rows, key=lambda row: (row["first_seen_at"], row["capture_sequence"])):
        selected[tuple(row[key] for key in keys)] = row
    return list(selected.values())


def episode_rows(membership, coverage, expected_dates):
    members = _latest(membership, ("provider", "market", "list_date", "raw_symbol", "rule_family"))
    covered = _latest(coverage, ("partition_id",))
    return derive_episodes(members, covered, expected_dates)


def _snapshot_id(manifest):
    content = {key: value for key, value in manifest.items() if key != "snapshot_id"}
    return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()[:32]


def stage_delta_snapshot(config_path: Path, *, max_rows=1_000_000) -> Path:
    """Freeze terminal SQLite/PostgreSQL history under its source writer lock; no HTTP."""
    if type(max_rows) is not int or not 1 <= max_rows <= 10_000_000:
        raise ValueError("max_rows requires an integer in [1,10000000]")
    config = load_config(Path(config_path))
    if not (config.root / ".warehouse.json").is_file():
        raise ValueError("threshold source warehouse does not exist")
    with warehouse_for(config, read_only=True) as warehouse:
        rows = {
            name: sorted(
                warehouse.select_all(name),
                key=lambda row, name=name: (
                    row["capture_sequence"],
                    *(row[key] for key in KEYS[name]),
                ),
            )
            for name in ("membership", "coverage", "audit")
        }
        if not rows["audit"] or any(row["status"] == "STARTED" for row in rows["audit"]):
            raise ValueError("threshold snapshot requires a nonempty terminal capture history")
        dates = sorted(
            {part.list_date for part in config.partitions.values()}
            | {row["list_date"] for row in rows["coverage"]}
        )
        rows["episodes"] = episode_rows(rows["membership"], rows["coverage"], dates)
        if any(len(values) > max_rows for values in rows.values()):
            raise ValueError("threshold snapshot exceeds max_rows")
        base = config.root / "delta-staging"
        if base.is_symlink():
            raise ValueError("threshold staging root cannot be a symbolic link")
        base.mkdir(exist_ok=True)
        staging = base / f".{uuid.uuid4().hex}.tmp"
        staging.mkdir()
        try:
            schemas = storage_schemas()
            for name, values in rows.items():
                table = (
                    _episode_table(values)
                    if name == "episodes"
                    else schemas[name].arrow_table(values)
                )
                path = staging / f"{name}.parquet"
                pq.write_table(table, path)
                with path.open("rb") as stream:
                    os.fsync(stream.fileno())
            manifest = {
                "snapshot_version": SNAPSHOT_VERSION,
                "pipeline": "reg-sho-thresholds",
                "warehouse_id": warehouse.identity["warehouse_id"],
                "storage_contract_id": schemas["membership"].contract_id,
                "capture_sequence": max(row["capture_sequence"] for row in rows["audit"]),
                "generated_at": utc_timestamp(max(row["completed_at"] for row in rows["audit"])),
                "calendar_basis": CALENDAR_BASIS,
                "expected_dates": [value.isoformat() for value in dates],
                "row_counts": {name: len(values) for name, values in rows.items()},
                "files": {name: digest(staging / f"{name}.parquet") for name in rows},
            }
            manifest["snapshot_id"] = _snapshot_id(manifest)
            final = base / manifest["snapshot_id"]
            atomic_json(staging / "manifest.json", manifest)
            if final.exists():
                validate_snapshot(config, final)
            else:
                os.replace(staging, final)
            atomic_json(
                base / "current.json",
                {"snapshot_id": final.name, "manifest_sha256": digest(final / "manifest.json")},
            )
            return final
        finally:
            if staging.exists():
                shutil.rmtree(staging)


def current_snapshot(root: Path) -> Path:
    base = Path(root) / "delta-staging"
    pointer = read_json(base / "current.json", 65536)
    selected = pointer.get("snapshot_id")
    if (
        not isinstance(selected, str)
        or len(selected) != 32
        or any(c not in "0123456789abcdef" for c in selected)
    ):
        raise ValueError("invalid threshold snapshot pointer")
    path = base / selected
    if digest(path / "manifest.json") != pointer.get("manifest_sha256"):
        raise ValueError("threshold snapshot manifest checksum mismatch")
    return path


def validate_snapshot(config, snapshot: Path):
    snapshot = Path(snapshot)
    if snapshot.is_symlink() or snapshot.parent.resolve() != config.root / "delta-staging":
        raise ValueError("threshold snapshot differs from the configured source root")
    manifest_path = snapshot / "manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("threshold snapshot manifest cannot be a symbolic link")
    manifest = read_json(manifest_path)
    identity = read_json(config.root / ".warehouse.json", 65536)
    schemas = storage_schemas()
    if (
        manifest.get("snapshot_version") != SNAPSHOT_VERSION
        or manifest.get("pipeline") != "reg-sho-thresholds"
        or identity.get("dataset") != "reg-sho-thresholds"
        or identity.get("storage_contract_id") != schemas["membership"].contract_id
        or manifest.get("warehouse_id") != identity["warehouse_id"]
        or manifest.get("storage_contract_id") != schemas["membership"].contract_id
        or manifest.get("calendar_basis") != CALENDAR_BASIS
        or manifest.get("snapshot_id") != snapshot.name
        or _snapshot_id(manifest) != snapshot.name
        or set(manifest.get("files", {})) != set(KEYS)
        or set(manifest.get("row_counts", {})) != set(KEYS)
    ):
        raise ValueError("threshold snapshot manifest differs from its source contract")
    tables = {}
    for name, schema in snapshot_schemas().items():
        path = snapshot / f"{name}.parquet"
        if path.is_symlink() or digest(path) != manifest["files"][name]:
            raise ValueError("threshold snapshot checksum differs from its manifest")
        table = pq.read_table(path)
        if (
            not table.schema.equals(schema, check_metadata=True)
            or type(manifest["row_counts"][name]) is not int
            or table.num_rows != manifest["row_counts"][name]
        ):
            raise ValueError("threshold snapshot schema or row count differs")
        values = table.to_pylist() if name == "episodes" else schemas[name].read_arrow(table)
        if len({tuple(row[key] for key in KEYS[name]) for row in values}) != len(values):
            raise ValueError("threshold snapshot has duplicate keys")
        tables[name] = table
    audits = tables["audit"].to_pylist()
    if (
        not audits
        or any(row["status"] == "STARTED" for row in audits)
        or type(manifest["capture_sequence"]) is not int
        or manifest["capture_sequence"] != max(row["capture_sequence"] for row in audits)
    ):
        raise ValueError("threshold snapshot requires terminal ordered audit history")
    by_run = {row["run_id"]: row for row in audits}
    sequences = sorted(row["capture_sequence"] for row in audits)
    if (
        sequences != list(range(1, len(audits) + 1))
        or manifest["generated_at"] != utc_timestamp(max(row["completed_at"] for row in audits))
        or tables["coverage"].num_rows != len(audits)
    ):
        raise ValueError("threshold snapshot has inconsistent capture ordering or coverage")
    for name in ("membership", "coverage"):
        for row in tables[name].to_pylist():
            audit = by_run.get(row["run_id"])
            if (
                audit is None
                or any(
                    row[key] != audit[key]
                    for key in ("provider", "market", "capture_sequence", "raw_sha256")
                )
                or row["first_seen_at"] != audit["captured_at"]
                or (name == "membership" and audit["status"] != "SUCCEEDED")
            ):
                raise ValueError("threshold snapshot revision lineage differs from its audit")
    dates = [date.fromisoformat(value) for value in manifest["expected_dates"]]
    expected = episode_rows(tables["membership"].to_pylist(), tables["coverage"].to_pylist(), dates)
    if tables["episodes"].to_pylist() != expected:
        raise ValueError("threshold snapshot episodes differ from membership/coverage")
    return manifest, tables
