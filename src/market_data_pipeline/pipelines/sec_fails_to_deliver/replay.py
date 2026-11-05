"""Deterministic offline reconstruction from original captures and admission evidence."""

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from contextlib import ExitStack
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.bronze import (
    _read_bounded,
    _sync,
    _write_once,
    load_capture,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.completeness import parse_evidence
from market_data_pipeline.pipelines.sec_fails_to_deliver.data import parse_archive
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import parse_manifest
from market_data_pipeline.pipelines.sec_fails_to_deliver.outcomes import (
    _canonical,
    _unique,
    load_outcome,
    make_outcome,
    validate_lineage,
    write_outcome,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import (
    _fact,
    _runtime,
    coverage_record,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import publish_new
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import (
    PostgresWarehouse,
    SQLiteWarehouse,
    warehouse_for,
)
from market_data_pipeline.shared.files import atomic_json, utc_timestamp


def _inventory(source, maximum):
    bronze = source / "bronze"
    if bronze.is_symlink() or not bronze.is_dir():
        raise ValueError("source requires an original Bronze capture inventory")
    paths = list(islice(bronze.iterdir(), maximum + 1))
    if not paths or len(paths) > maximum:
        raise ValueError("capture inventory is empty or exceeds max-captures")
    for directory in paths:
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError("Bronze inventory must contain only original capture directories")
        for path in directory.iterdir():
            if (
                path.is_symlink()
                or not path.is_file()
                or path.name
                not in {
                    "receipt.json",
                    "manifest.yml",
                    "storage.json",
                    "payload.bin",
                    "coverage-evidence.json",
                    "outcome.json",
                }
            ):
                raise ValueError("unexpected or unsafe artifact in capture inventory")
    return paths


def _ledger(warehouse, maximum):
    result = {}
    for name in ("audit", "coverage"):
        schema = warehouse.schemas[name]
        rows = list(
            warehouse.connection.execute(
                f"SELECT * FROM sec_ftd_{name} ORDER BY capture_sequence LIMIT ?", (maximum + 1,)
            )
        )
        if len(rows) > maximum:
            raise ValueError("source ledger exceeds max-captures")
        result[name] = {}
        for row in rows:
            decoded = schema.decode(dict(row), warehouse.backend)
            if decoded["run_id"] in result[name]:
                raise ValueError("ambiguous original outcome ledger")
            result[name][decoded["run_id"]] = decoded
    if not result["audit"] or result["audit"].keys() != result["coverage"].keys():
        raise ValueError("source requires a terminal audit and coverage for every capture")
    sequence = warehouse.connection.execute(
        "SELECT value FROM sec_ftd_sequence WHERE id=1"
    ).fetchone()
    if sequence is None or sequence["value"] != max(
        row["capture_sequence"] for row in result["audit"].values()
    ):
        raise ValueError("source capture allocator disagrees with its outcome ledger")
    return result


def _capture_index(captures):
    return [
        {
            "run_id": item[4]["run_id"],
            "capture_sequence": item[0],
            "outcome_sha256": item[3]["sha256"],
        }
        for item in captures
    ]


def _portable_inventory(source, maximum, identity):
    try:
        value = json.loads(
            _read_bounded(source / "replay.json", 4 * 1024 * 1024), object_pairs_hook=_unique
        )
        entries = value["capture_inventory"]
        if (
            value["replay_version"] != "sec-ftd-replay/1"
            or value["status"] != "SUCCEEDED"
            or not isinstance(entries, list)
            or not 1 <= len(entries) <= maximum
            or type(value["replayed"]) is not int
            or value["replayed"] != len(entries)
            or type(value["fact_version_count"]) is not int
            or value["fact_version_count"] < 0
            or not isinstance(value["warehouse_id"], str)
            or not re.fullmatch(r"[0-9a-f]{32}", value["warehouse_id"])
            or value["capture_inventory_sha256"] != hashlib.sha256(_canonical(entries)).hexdigest()
            or (identity is not None and value["warehouse_id"] != identity["warehouse_id"])
        ):
            raise ValueError("invalid finalized capture inventory")
        return value
    except (KeyError, TypeError, UnicodeError, OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            "database-free replay requires a complete finalized capture inventory"
        ) from exc


def _preflight(paths, settings, warehouse, ledger, portable=None):
    captures, seen_runs, seen_sequences = [], set(), set()
    reconciled = 0
    for directory in paths:
        receipt, _, _, profile = load_capture(directory)
        if profile != settings:
            raise ValueError("source capture has a different destination storage contract")
        run_id, sequence = receipt["run_id"], receipt["capture_sequence"]
        if run_id in seen_runs or sequence in seen_sequences:
            raise ValueError("duplicate/ambiguous capture ordering")
        seen_runs.add(run_id)
        seen_sequences.add(sequence)
        expected = None
        if ledger is not None:
            if run_id not in ledger["audit"]:
                raise ValueError("capture has no original terminal outcome")
            audit, coverage = ledger["audit"][run_id], ledger["coverage"][run_id]
            validate_lineage(receipt, audit, coverage)
            expected = make_outcome(
                directory, settings, audit, coverage, warehouse.fact_fingerprint(run_id)
            )
        if (directory / "outcome.json").exists():
            value, audit, coverage = load_outcome(directory, receipt, settings)
            if expected is not None and value != expected:
                raise ValueError("sealed outcome disagrees with the source warehouse ledger")
        elif expected is not None:
            value = expected
            reconciled += 1
        else:
            raise ValueError("missing original outcome; reconcile the source ledger before replay")
        captures.append((sequence, directory, receipt, value, audit, coverage))
    if ledger is not None and seen_runs != ledger["audit"].keys():
        raise ValueError("source ledger has missing or unresolved Bronze captures")
    captures.sort(key=lambda item: item[0])
    if ledger is None and (
        portable is None
        or _capture_index(captures) != portable["capture_inventory"]
        or sum(item[3]["facts"]["count"] for item in captures) != portable["fact_version_count"]
    ):
        raise ValueError("archived captures disagree with the finalized capture inventory")
    last = None
    for _, _, _receipt, _, audit, _ in captures:
        if last is not None and audit["captured_at"] < last:
            raise ValueError("capture knowledge clock regressed in sequence order")
        last = audit["captured_at"]
    if warehouse is not None:
        total = warehouse.connection.execute(
            "SELECT count(*) AS count FROM sec_ftd_facts"
        ).fetchone()["count"]
        if total != sum(item[3]["facts"]["count"] for item in captures):
            raise ValueError("source facts contain unresolved capture lineage")
    return captures, reconciled


def _copy_capture(source, target):
    receipt, payload, manifest, profile = load_capture(source)
    target.mkdir(parents=True, exist_ok=False)
    _write_once(target / "receipt.json", _read_bounded(source / "receipt.json", 65536))
    _write_once(target / "manifest.yml", manifest)
    _write_once(target / "storage.json", _read_bounded(source / "storage.json", 4096))
    if payload is not None:
        _write_once(target / "payload.bin", payload)
    proof = None
    if receipt.get("coverage_evidence_sha256") is not None:
        proof = _read_bounded(source / "coverage-evidence.json", 65536)
        _write_once(target / "coverage-evidence.json", proof)
    verified, copied, copied_manifest, copied_profile = load_capture(target)
    if (
        verified != receipt
        or copied != payload
        or copied_manifest != manifest
        or copied_profile != profile
    ):
        raise ValueError("copied capture differs from its original artifacts")
    return verified, copied, copied_manifest, proof


def _rebuild(warehouse, capture):
    _, original, expected_receipt, outcome, audit, coverage = capture
    directory = warehouse.root / "bronze" / audit["run_id"]
    receipt, payload, manifest_payload, proof = _copy_capture(original, directory)
    if receipt != expected_receipt:
        raise ValueError("source receipt changed after preflight")
    facts, evidence, context = [], None, None
    if audit["status"] == "SUCCEEDED":
        if receipt["source_state"] != "AVAILABLE":
            raise ValueError("accepted capture has no complete payload")
        manifest = parse_manifest(manifest_payload)
        parsed = parse_archive(payload, manifest)
        if proof is not None:
            evidence = parse_evidence(proof, manifest)
            evidence.validate(raw_sha256=parsed.raw_sha256, observed_dates=parsed.observed_dates)
        expected = coverage_record(
            manifest, evidence, parsed, audit, "COMPLETE" if evidence is not None else "PARTIAL"
        )
        if warehouse.schemas["coverage"].normalize(expected) != coverage:
            raise ValueError("reparsed coverage differs from original admission")
        receipt["member_sha256"] = parsed.member_sha256
        facts = [_fact(row, receipt, audit["captured_at"]) for row in parsed.observations]
        if len(facts) != audit["row_count"]:
            raise ValueError("reparsed observation count differs from original admission")
        context = {
            "raw_member": receipt["member_name"],
            "member_sha256": parsed.member_sha256,
            "parser_version": receipt["parser_version"],
        }
    started = {
        **audit,
        "status": "STARTED",
        "completed_at": None,
        "row_count": 0,
        "raw_sha256": None,
        "error_code": None,
    }
    warehouse.start_replay_run(started)
    warehouse.promote_partition(facts, coverage, audit, evidence=evidence, capture=context)
    if warehouse.fact_fingerprint(audit["run_id"]) != outcome["facts"]:
        raise ValueError("replayed fact versions differ from the original outcome")
    write_outcome(directory, outcome)
    _sync(directory)
    _sync(directory.parent)


def _verify_final(warehouse, captures):
    ledger = _ledger(warehouse, len(captures))
    if set(ledger["audit"]) != {item[4]["run_id"] for item in captures}:
        raise ValueError("final replay state has unexpected captures")
    for _, _, _, outcome, audit, coverage in captures:
        run_id = audit["run_id"]
        if (
            ledger["audit"][run_id] != audit
            or ledger["coverage"][run_id] != coverage
            or warehouse.fact_fingerprint(run_id) != outcome["facts"]
        ):
            raise ValueError("final replay state differs from the original admission ledger")
    total = warehouse.connection.execute("SELECT count(*) AS count FROM sec_ftd_facts").fetchone()[
        "count"
    ]
    if total != sum(item[3]["facts"]["count"] for item in captures):
        raise ValueError("final replay state has unexpected fact versions")


def replay(source_root: Path, config_path: Path, *, max_captures=1000) -> dict:
    """Rebuild a separate empty SQLite root without transport or live inputs."""
    config = _runtime(config_path)
    if config.databricks.enabled:
        raise ValueError("SEC FTD Delta publication is not implemented yet (T21)")
    if type(max_captures) is not int or not 1 <= max_captures <= 10000:
        raise ValueError("max-captures requires an integer in [1,10000]")
    source, destination = Path(source_root).resolve(), config.root
    if (
        source == destination
        or destination.is_relative_to(source)
        or source.is_relative_to(destination)
    ):
        raise ValueError("replay requires separate nonoverlapping source and empty destination")
    if destination.is_symlink() or (
        destination.exists() and (not destination.is_dir() or any(destination.iterdir()))
    ):
        raise ValueError("replay requires an empty destination")
    with ExitStack() as stack:
        warehouse = None
        portable = None
        reader = SQLiteWarehouse(source, config.storage, read_only=True)
        if (source / "warehouse.sqlite3").exists() or (source / "warehouse.sqlite3").is_symlink():
            warehouse = stack.enter_context(reader)
        else:
            if (source / ".writer.lock").exists() or (source / ".writer.lock").is_symlink():
                stack.enter_context(reader._reader_lock())
            marker = source / ".warehouse.json"
            identity = None
            if marker.exists() or marker.is_symlink():
                identity = json.loads(_read_bounded(marker, 65536))
                reader._check_identity(identity)
            portable = _portable_inventory(source, max_captures, identity)
        paths = _inventory(source, max_captures)
        ledger = _ledger(warehouse, max_captures) if warehouse is not None else None
        captures, reconciled = _preflight(paths, config.storage, warehouse, ledger, portable)
        destination.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(
            tempfile.mkdtemp(prefix=f".{destination.name}.replay-", dir=destination.parent)
        )
        target = None
        try:
            target = warehouse_for(config, root=stage)
            with target:
                for capture in captures:
                    _rebuild(target, capture)
                _verify_final(target, captures)
                publication = publish_new(stage, target)
                counts = Counter(item[4]["status"] for item in captures)
                index = _capture_index(captures)
                result = {
                    "pipeline": "sec-fails-to-deliver",
                    "replay_version": "sec-ftd-replay/1",
                    "status": "SUCCEEDED",
                    "replayed": len(captures),
                    "succeeded": counts["SUCCEEDED"],
                    "rejected": counts["REJECTED"],
                    "failed": counts["FAILED"],
                    "legacy_outcomes_reconciled": reconciled,
                    "fact_version_count": sum(item[3]["facts"]["count"] for item in captures),
                    "capture_inventory": index,
                    "capture_inventory_sha256": hashlib.sha256(_canonical(index)).hexdigest(),
                    "source_warehouse_id": warehouse.identity["warehouse_id"]
                    if warehouse is not None
                    else portable["warehouse_id"],
                    "warehouse_id": target.identity["warehouse_id"],
                    "source_coverage": "NOT_CERTIFIED",
                    "replayed_at": utc_timestamp(datetime.now(UTC)),
                    "snapshot": str(destination / Path(publication["snapshot"]).relative_to(stage)),
                    "snapshot_id": publication["snapshot_id"],
                }
                atomic_json(stage / "replay.json", result)
            # rename refuses a concurrently populated destination; never delete it.
            os.replace(stage, destination)
            _sync(destination.parent)
            return result
        finally:
            if stage.exists():
                if isinstance(target, PostgresWarehouse):
                    target.cleanup_created_store()
                shutil.rmtree(stage)
