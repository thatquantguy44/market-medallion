"""Coherent, recoverable SEC FTD Parquet snapshot publication."""

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import IDENTITY
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import warehouse_for
from market_data_pipeline.shared.files import atomic_json

PUBLICATION_VERSION = "sec-ftd-parquet/1"
POINTER_VERSION = "sec-ftd-parquet-pointer/1"
TABLES = {"facts": "facts", "gold": "facts", "coverage": "coverage", "audit": "audit"}


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate publication JSON field")
        value[key] = item
    return value


def _json_file(path: Path, maximum: int) -> dict:
    try:
        with path.open("rb") as stream:
            payload = stream.read(maximum + 1)
        if len(payload) > maximum:
            raise ValueError("publication JSON exceeds its byte limit")
        value = json.loads(payload, object_pairs_hook=_unique)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise ValueError("invalid publication JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("publication JSON requires an object")
    return value


def _sync(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _clock() -> datetime:
    return datetime.now(UTC)


def _snapshot_path(root: Path, snapshot_id: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{32}", snapshot_id):
        raise ValueError("invalid SEC FTD snapshot identifier")
    return root / "parquet" / "snapshots" / snapshot_id


def _validate_snapshot(path: Path, warehouse, expected_hash: str, expected=None) -> dict:
    if path.is_symlink() or not path.is_dir():
        raise ValueError("publication snapshot directory is unavailable")
    manifest_path = path / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("publication manifest is unavailable")
    if _digest(manifest_path) != expected_hash:
        raise ValueError("publication manifest checksum differs from the database")
    manifest = _json_file(manifest_path, 1024 * 1024)
    required = {
        "publication_version",
        "pipeline",
        "snapshot_id",
        "run_id",
        "capture_sequence",
        "generated_at",
        "warehouse_id",
        "storage_contract_id",
        "record_schemas",
        "row_counts",
        "files",
    }
    if (
        not isinstance(manifest, dict)
        or set(manifest) != required
        or manifest["publication_version"] != PUBLICATION_VERSION
        or manifest["pipeline"] != IDENTITY["dataset"]
        or manifest["snapshot_id"] != path.name
        or manifest["warehouse_id"] != warehouse.identity["warehouse_id"]
        or manifest["storage_contract_id"] != warehouse.expected["storage_contract_id"]
        or manifest["record_schemas"]
        != {name: schema.version for name, schema in warehouse.schemas.items()}
        or not isinstance(manifest["row_counts"], dict)
        or set(manifest["row_counts"]) != set(TABLES)
        or any(type(value) is not int or value < 0 for value in manifest["row_counts"].values())
        or not isinstance(manifest["files"], dict)
        or set(manifest["files"]) != set(TABLES)
        or any(
            not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
            for value in manifest["files"].values()
        )
        or not isinstance(manifest["run_id"], str)
        or type(manifest["capture_sequence"]) is not int
        or manifest["capture_sequence"] < 1
    ):
        raise ValueError("publication manifest differs from the warehouse contract")
    try:
        generated_at = datetime.fromisoformat(manifest["generated_at"])
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid publication generation clock") from exc
    if generated_at.utcoffset() is None:
        raise ValueError("publication generation clock requires a timezone")
    if expected is not None:
        audit = next(
            (row for row in warehouse.select_runs() if row["run_id"] == expected["run_id"]),
            None,
        )
        if (
            manifest["run_id"] != expected["run_id"]
            or expected["snapshot_path"] != f"parquet/snapshots/{path.name}"
            or audit is None
            or manifest["capture_sequence"] != audit["capture_sequence"]
            or generated_at != expected["generated_at"]
        ):
            raise ValueError("publication manifest disagrees with its database journal")
    for name, schema_name in TABLES.items():
        file = path / f"{name}.parquet"
        if file.is_symlink() or not file.is_file() or _digest(file) != manifest["files"][name]:
            raise ValueError("publication Parquet checksum mismatch")
        table = pq.read_table(file)
        warehouse.schemas[schema_name].read_arrow(table)
        if table.num_rows != manifest["row_counts"][name]:
            raise ValueError("publication Parquet row count mismatch")
    return manifest


def _publish_pointer(root: Path, row: dict) -> None:
    atomic_json(
        root / "parquet" / "current.json",
        {
            "pointer_version": POINTER_VERSION,
            "snapshot": row["snapshot_path"],
            "snapshot_id": row["snapshot_id"],
            "manifest_sha256": row["manifest_sha256"],
        },
    )


def current_snapshot(root: Path) -> Path:
    root = Path(root).resolve()
    pointer_path = root / "parquet" / "current.json"
    try:
        pointer = _json_file(pointer_path, 65536)
    except ValueError as exc:
        raise ValueError("SEC FTD current publication is unavailable") from exc
    if (
        not isinstance(pointer, dict)
        or set(pointer)
        != {
            "pointer_version",
            "snapshot",
            "snapshot_id",
            "manifest_sha256",
        }
        or pointer["pointer_version"] != POINTER_VERSION
        or pointer["snapshot"] != f"parquet/snapshots/{pointer['snapshot_id']}"
        or not re.fullmatch(r"[0-9a-f]{32}", pointer["snapshot_id"])
        or not re.fullmatch(r"[0-9a-f]{64}", pointer["manifest_sha256"])
    ):
        raise ValueError("invalid SEC FTD current publication pointer")
    snapshot = (root / pointer["snapshot"]).resolve()
    if not snapshot.is_relative_to((root / "parquet" / "snapshots").resolve()):
        raise ValueError("invalid SEC FTD snapshot path")
    try:
        matches = _digest(snapshot / "manifest.json") == pointer["manifest_sha256"]
    except OSError as exc:
        raise ValueError("current publication snapshot is unavailable") from exc
    if not matches:
        raise ValueError("current publication manifest checksum mismatch")
    return snapshot


def _recover(root: Path, warehouse) -> list[dict]:
    parquet = root / "parquet"
    staging = parquet / ".staging"
    snapshots = parquet / "snapshots"
    staging.mkdir(parents=True, exist_ok=True)
    snapshots.mkdir(parents=True, exist_ok=True)
    pending = {row["snapshot_id"]: row for row in warehouse.pending_publications()}
    for path in staging.iterdir():
        if path.name not in pending:
            if path.is_symlink() or not path.is_dir():
                raise ValueError("unsafe artifact in SEC FTD publication staging")
            shutil.rmtree(path)
    recovered = []
    for snapshot_id, row in sorted(pending.items(), key=lambda item: item[1]["generated_at"]):
        stage = staging / snapshot_id
        final = _snapshot_path(root, snapshot_id)
        if row["status"] == "STAGED":
            candidate = final if final.is_dir() and not final.is_symlink() else stage
            try:
                _validate_snapshot(candidate, warehouse, row["manifest_sha256"], row)
            except ValueError:
                warehouse.transition_publication(snapshot_id, "ABANDONED")
                if stage.is_dir() and not stage.is_symlink():
                    shutil.rmtree(stage)
                continue
            if candidate == stage:
                os.replace(stage, final)
                _sync(snapshots)
            warehouse.transition_publication(snapshot_id, "COMMITTED")
            row = {**row, "status": "COMMITTED"}
        try:
            _validate_snapshot(final, warehouse, row["manifest_sha256"], row)
        except ValueError:
            warehouse.transition_publication(snapshot_id, "ABANDONED")
            continue
        _publish_pointer(root, row)
        published_at = _clock()
        warehouse.transition_publication(snapshot_id, "PUBLISHED", published_at)
        recovered.append({**row, "status": "PUBLISHED", "published_at": published_at})
    return recovered


def recover_publications(root: Path, warehouse) -> list[dict]:
    """Finish or abandon every pending generation under the root writer lock."""
    return _recover(Path(root).resolve(), warehouse)


def publish_new(root: Path, warehouse) -> dict:
    """Publish one new coherent generation while the root writer lock is held."""
    root = Path(root).resolve()
    with warehouse.transaction():
        facts = warehouse.select_all("facts")
        coverage = warehouse.select_all("coverage")
        audits = warehouse.select_all("audit")
        gold = warehouse.select_facts()
    if not audits or audits[-1]["status"] == "STARTED":
        raise ValueError("SEC FTD publication requires terminal warehouse state")
    run = audits[-1]
    snapshot_id = uuid.uuid4().hex
    generated_at = _clock()
    parquet = root / "parquet"
    staging_parent = parquet / ".staging"
    snapshots = parquet / "snapshots"
    staging_parent.mkdir(parents=True, exist_ok=True)
    snapshots.mkdir(parents=True, exist_ok=True)
    stage = staging_parent / snapshot_id
    stage.mkdir()
    rows = {"facts": facts, "gold": gold, "coverage": coverage, "audit": audits}
    files = {}
    registered = False
    try:
        for name, schema_name in TABLES.items():
            path = stage / f"{name}.parquet"
            pq.write_table(
                warehouse.schemas[schema_name].arrow_table(rows[name]), path, compression="zstd"
            )
            _sync(path)
            files[name] = _digest(path)
        manifest = {
            "publication_version": PUBLICATION_VERSION,
            "pipeline": IDENTITY["dataset"],
            "snapshot_id": snapshot_id,
            "run_id": run["run_id"],
            "capture_sequence": run["capture_sequence"],
            "generated_at": generated_at.isoformat(timespec="microseconds"),
            "warehouse_id": warehouse.identity["warehouse_id"],
            "storage_contract_id": warehouse.expected["storage_contract_id"],
            "record_schemas": {name: schema.version for name, schema in warehouse.schemas.items()},
            "row_counts": {name: len(value) for name, value in rows.items()},
            "files": files,
        }
        atomic_json(stage / "manifest.json", manifest)
        manifest_sha256 = _digest(stage / "manifest.json")
        _validate_snapshot(stage, warehouse, manifest_sha256)
        row = {
            **IDENTITY,
            "snapshot_id": snapshot_id,
            "run_id": run["run_id"],
            "status": "STAGED",
            "generated_at": generated_at,
            "published_at": None,
            "manifest_sha256": manifest_sha256,
            "snapshot_path": f"parquet/snapshots/{snapshot_id}",
        }
        warehouse.record_publication(row)
        registered = True
        final = _snapshot_path(root, snapshot_id)
        os.replace(stage, final)
        _sync(snapshots)
        warehouse.transition_publication(snapshot_id, "COMMITTED")
        row["status"] = "COMMITTED"
        _publish_pointer(root, row)
        warehouse.transition_publication(snapshot_id, "PUBLISHED", _clock())
        return {
            "status": "PUBLISHED",
            "snapshot": str(final),
            "snapshot_id": snapshot_id,
            "recovered": False,
        }
    except BaseException:
        if not registered:
            shutil.rmtree(stage, ignore_errors=True)
        # Once STAGED is durable, deterministic recovery owns finalization.
        raise


def publish(root: Path, warehouse) -> dict:
    """Recover pending work, or publish current state when nothing was pending."""
    recovered = recover_publications(root, warehouse)
    if recovered:
        row = recovered[-1]
        return {
            "status": "PUBLISHED",
            "snapshot": str(_snapshot_path(Path(root).resolve(), row["snapshot_id"])),
            "snapshot_id": row["snapshot_id"],
            "recovered": True,
        }
    return publish_new(root, warehouse)


def publish_config(config_path: Path) -> dict:
    config = load_config(Path(config_path))
    with warehouse_for(config) as warehouse:
        return publish(config.root, warehouse)
