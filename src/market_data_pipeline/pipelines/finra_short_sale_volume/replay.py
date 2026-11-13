"""SSV-AC11: exact local reconstruction from Bronze plus its terminal ledger.

Only a fully verified fresh rebuild is installed. Existing warehouses cannot be
moved through this API. Published generations remain explicit T10 operations.
"""

import os
import shutil
import sqlite3
import tempfile
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume import pipeline
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import (
    digest,
    encoded,
    settings,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    DEFAULT_BYTES,
    DEFAULT_ROWS,
    limits,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.versions import TRANSFORM
from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze, replay_archive
from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse as store
from market_data_pipeline.shared.files import writer_lock

CONTRACT = "finra-ssv-replay/1"
TABLES = {
    "attempts": "rowid",
    "captures": "capture_sequence",
    "versions": "version_id",
    "capture_rows": "capture_id,symbol_raw",
    "partitions": "trade_date,facility_id",
    "gold": "trade_date,facility_id,symbol_raw",
}


def _state(db, max_rows, max_bytes):
    count = size = 0
    for name in TABLES:
        expression = (
            "length(CAST(body AS BLOB))+512"
            if name in {"versions", "captures", "gold"}
            else ("length(CAST(profile AS BLOB))+512" if name == "attempts" else "512")
        )
        row = db.execute(f"SELECT COUNT(*),COALESCE(SUM({expression}),0) FROM {name}").fetchone()
        count += row[0]
        size += row[1]
    if count > max_rows or size > max_bytes:
        raise ValueError("FINRA replay state row/byte budget exceeded")
    result = {
        name: [dict(r) for r in db.execute(f"SELECT * FROM {name} ORDER BY {order}")]
        for name, order in TABLES.items()
    }
    size = len(encoded(result))
    if size > max_bytes:
        raise ValueError("FINRA replay state byte budget exceeded")
    return result, size


def _ledger(db, original, archive, max_bytes, max_rows):
    captures = {
        r["capture_id"]: store.capture_result(db, r["capture_id"]) for r in original["captures"]
    }
    receipts = {r["receipt"]["capture_id"]: r for r in archive["captures"]}
    if captures.keys() != receipts.keys():
        raise ValueError("FINRA replay archive and terminal ledger disagree")
    for row in original["versions"]:
        store.version_document(row)
    attempts = {key: [] for key in captures}
    for row in original["attempts"]:
        capture_id = row["capture_id"]
        if capture_id not in captures or row["status"] not in {
            "APPLIED",
            "PARTIAL",
            "REJECTED",
            "FAILED",
            "INTERRUPTED",
        }:
            raise ValueError("FINRA replay requires resolved terminal source attempts")
        bronze._identity(row["attempt_id"])
        saved = receipts[capture_id]["receipt"]
        if any(
            row[k] != saved[k] for k in ("capture_id", "run_id", "archived_at", "capture_sequence")
        ):
            raise ValueError("FINRA replay attempt lineage mismatch")
        bronze._clock_text(row["started_at"])
        bronze._clock_text(row["completed_at"])
        if row["started_at"] < saved["archived_at"] or row["completed_at"] < row["started_at"]:
            raise ValueError("FINRA replay attempt clock mismatch")
        profile = store._document(row["profile"])
        if set(profile) != {"parser", "transform"} or profile["transform"] != TRANSFORM:
            raise ValueError("FINRA replay attempt transform mismatch")
        replay_archive._profile(profile["parser"], max_bytes, max_rows)
        attempts[capture_id].append(row)
    for capture_id, result in captures.items():
        saved = receipts[capture_id]["receipt"]
        profile = replay_archive._profile(result["profile"], max_bytes, max_rows)
        if (
            result["transform"] != TRANSFORM
            or any(
                result[k] != saved[k]
                for k in (
                    "capture_id",
                    "run_id",
                    "archived_at",
                    "capture_sequence",
                    "receipt_sha256",
                    "payload_sha256",
                )
            )
            or digest(profile) not in receipts[capture_id]["outcomes"]
        ):
            raise ValueError("FINRA replay capture provenance or parser outcome mismatch")
        terminal = [
            r for r in attempts[capture_id] if r["status"] in {"APPLIED", "PARTIAL", "REJECTED"}
        ]
        if len(terminal) != 1 or attempts[capture_id][-1] != terminal[0]:
            raise ValueError("FINRA replay requires one final terminal attempt per capture")
        last = terminal[0]
        if any(
            last[k] != result[k]
            for k in ("status", "completed_at", "trade_date", "facility_id", "error_code")
        ) or store._document(last["profile"]) != dict(parser=profile, transform=TRANSFORM):
            raise ValueError("FINRA replay terminal audit mismatch")
        store.rows(db, capture_id)
    return captures


def _destination(source, destination):
    source, destination = Path(source), Path(destination)
    if source.is_symlink() or destination.is_symlink():
        raise ValueError("FINRA replay roots cannot be symlinks")
    source, destination = source.resolve(), destination.resolve()
    if (
        source == destination
        or source.is_relative_to(destination)
        or destination.is_relative_to(source)
    ):
        raise ValueError("FINRA replay requires disjoint source and destination roots")
    _empty(destination)
    if not source.is_dir():
        raise ValueError("FINRA replay source root is missing")
    for name in (".ingest.lock", ".writer.lock"):
        if (source / name).is_symlink() or not (source / name).is_file():
            raise ValueError("FINRA replay requires a complete safe source root")
    return source, destination


def _empty(destination):
    if destination.is_symlink() or (
        destination.exists()
        and (not destination.is_dir() or next(destination.iterdir(), None) is not None)
    ):
        raise ValueError("FINRA replay requires an empty destination")


def _replace(path, body):
    temporary = path.parent / (".replay-write-" + uuid4().hex)
    try:
        bronze._write(temporary, body)
        os.replace(temporary, path)
        bronze._sync(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _seal(stage, destination, manifest, shared, warehouse_id):
    """Bind only this fresh private rebuild to its final path before exposure."""
    with store.connect(stage, manifest, **shared) as db:
        identity = bronze._json(stage / "warehouse/identity.json")
        if (
            identity["warehouse_id"] != warehouse_id
            or db.execute("SELECT COUNT(*) FROM identity").fetchone()[0] != 1
        ):
            raise ValueError("FINRA replay private warehouse identity mismatch")
    identity["root_sha256"] = digest(str(destination))
    body = encoded(identity)
    with closing(sqlite3.connect(stage / "warehouse/ssv.sqlite")) as db:
        db.execute("PRAGMA synchronous=FULL")
        db.execute("UPDATE identity SET body=?", (body.decode(),))
        db.commit()
        if db.execute("SELECT body FROM identity").fetchone()[0] != body.decode():
            raise ValueError("FINRA replay identity seal failed")
    _replace(stage / "warehouse/identity.json", body)
    _replace(stage / "warehouse.json", body)
    if (
        bronze._json(stage / "warehouse.json") != identity
        or bronze._json(stage / "warehouse/identity.json") != identity
    ):
        raise ValueError("FINRA replay identity seal readback mismatch")
    bronze._sync(stage / "warehouse")
    bronze._sync(stage)


def replay(
    source_root,
    destination_root,
    manifest,
    *,
    approved_use,
    operation,
    max_captures=1000,
    max_bytes=20 * 1024**2,
    max_rows=5000,
    max_total_bytes=DEFAULT_BYTES,
    max_state_rows=DEFAULT_ROWS,
    clock=lambda: datetime.now(UTC),
    _fault=lambda at: None,
):
    bronze._admit(manifest, approved_use, operation, {"fixture", "transform"})
    limits(max_state_rows, max_total_bytes)
    settings(max_bytes, max_rows)
    if type(max_captures) is not int or not 1 <= max_captures <= 10000:
        raise ValueError("FINRA replay capture budget must be in 1..10000")
    source, destination = _destination(source_root, destination_root)
    shared = dict(approved_use=approved_use, operation=operation)
    with (
        store.ingestion_lock(source),
        writer_lock(source),
        store.connect(source, manifest, **shared) as db,
    ):
        db.execute("BEGIN")
        original, state_bytes = _state(db, max_state_rows, max_total_bytes)
        archive = replay_archive.inventory(
            source,
            manifest,
            **shared,
            max_captures=max_captures,
            max_bytes=max_bytes,
            max_rows=max_rows,
            max_total_bytes=max_total_bytes - state_bytes,
        )
        terminal = _ledger(db, original, archive, max_bytes, max_rows)
        source_identity = bronze._json(source / "warehouse/identity.json")
        _empty(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(
            tempfile.mkdtemp(prefix="." + destination.name + ".replay-", dir=destination.parent)
        )
        try:
            replay_archive.copy(source, stage, manifest, archive, **shared, max_bytes=max_bytes)
            _fault("copied")
            for entry in archive["captures"]:
                saved = entry["receipt"]
                expected = terminal[saved["capture_id"]]
                for outcome in entry["outcomes"].values():
                    _, parsed = bronze.parse_capture(
                        stage,
                        saved["capture_id"],
                        manifest,
                        **shared,
                        max_bytes=outcome["profile"]["max_bytes"],
                        max_rows=outcome["profile"]["max_rows"],
                    )
                    if parsed != outcome:
                        raise ValueError("FINRA replay parse outcome differs from original")
                result = pipeline.ingest_capture(
                    stage,
                    saved["capture_id"],
                    manifest,
                    **shared,
                    max_bytes=expected["profile"]["max_bytes"],
                    max_rows=expected["profile"]["max_rows"],
                    clock=lambda expected=expected: datetime.fromisoformat(
                        expected["completed_at"].replace("Z", "+00:00")
                    ),
                )
                if result != expected:
                    raise ValueError("FINRA replay terminal result differs from original")
                _fault("ingested")
            with store.connect(stage, manifest, **shared, create=True) as target:
                target.execute("BEGIN IMMEDIATE")
                try:
                    target.execute("DELETE FROM attempts")
                    for row in original["attempts"]:
                        target.execute(
                            "INSERT INTO attempts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                            tuple(row.values()),
                        )
                    target.commit()
                except BaseException:
                    target.rollback()
                    raise
                rebuilt, _ = _state(target, max_state_rows, max_total_bytes)
                if rebuilt != original:
                    raise ValueError("FINRA replay logical state differs from original")
            _fault("verified")
            warehouse_id = bronze._json(stage / "warehouse/identity.json")["warehouse_id"]
            index = [
                {
                    k: entry["receipt"][k]
                    for k in (
                        "capture_id",
                        "capture_sequence",
                        "archived_at",
                        "receipt_sha256",
                        "payload_sha256",
                    )
                }
                for entry in archive["captures"]
            ]
            result = dict(
                schema_id=CONTRACT,
                status="SUCCEEDED",
                replayed=len(index),
                source_warehouse_id=source_identity["warehouse_id"],
                warehouse_id=warehouse_id,
                replayed_at=bronze._clock(clock()),
                knowledge_basis="CURRENT_VINTAGE",
                clock_basis="LOCAL_CAPTURE",
                capture_inventory=index,
                capture_inventory_sha256=digest(index),
                logical_state_sha256=digest(original),
                archive_bytes=archive["bytes"],
                state_bytes=state_bytes,
                row_counts={n: len(r) for n, r in original.items()},
                capture_statuses=dict(Counter(r["status"] for r in terminal.values())),
                publication="EXPLICIT_PUBLISH_REQUIRED",
            )
            bronze._write(stage / "replay.json", encoded(result))
            _seal(stage, destination, manifest, shared, warehouse_id)
            _fault("sealed")
            _fault("before_install")
            _empty(destination)
            os.rename(stage, destination)
            bronze._sync(destination.parent)
            return result
        finally:
            if stage.exists():
                shutil.rmtree(stage)
                bronze._sync(stage.parent)
