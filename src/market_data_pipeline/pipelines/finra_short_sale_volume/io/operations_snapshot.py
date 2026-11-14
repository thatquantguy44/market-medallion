"""SSV-AC13: offline health and PRESERVE_ALL retention accounting.

These readers take nonblocking shared locks on existing files only. They never
initialize stores, reparse into Bronze, repair pointers, or delete artifacts.
"""

import fcntl
import hashlib
import json
import os
from contextlib import ExitStack, contextmanager
from pathlib import Path

from market_data_pipeline.pipelines.finra_short_sale_volume import publication, replay
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest
from market_data_pipeline.pipelines.finra_short_sale_volume.io import (
    bronze,
    catalog,
    operations_journal,
    replay_archive,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse as store


def _bounded(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("FINRA operations budget is invalid")


@contextmanager
def _lock(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError(
                "FINRA store is busy; retry health after the writer finishes"
            ) from None
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _files(root, max_files, max_total_bytes):
    files, size, entries = [], 0, 0
    pending = [root] if root.exists() else []
    while pending:
        directory = pending.pop()
        for path in directory.iterdir():
            entries += 1
            if entries > max_files:
                raise ValueError("FINRA operations file/directory budget exceeded")
            if path.is_symlink():
                raise ValueError("FINRA operations inventory contains a symlink")
            if path.is_dir():
                pending.append(path)
                continue
            if not path.is_file():
                raise ValueError("FINRA operations inventory contains a special file")
            length = path.stat().st_size
            size += length
            if size > max_total_bytes:
                raise ValueError("FINRA operations total byte budget exceeded")
            checksum = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    checksum.update(block)
            files.append(
                dict(
                    path=path.relative_to(root).as_posix(),
                    bytes=length,
                    sha256=checksum.hexdigest(),
                )
            )
    return sorted(files, key=lambda f: f["path"])


def _archive(root, manifest, shared, max_captures, max_bytes, max_rows):
    if not (root / "bronze").exists():
        return {}, {}
    base = bronze._base(root)
    captures, collections = {}, {}
    for directory in replay_archive._paths(base / "captures", max_captures):
        capture_id = bronze._identity(directory.name)
        if {p.name for p in replay_archive._paths(directory, 2)} != {"payload.bin", "receipt.json"}:
            raise ValueError("FINRA health capture inventory mismatch")
        saved, payload = bronze.read_capture(
            root, capture_id, manifest, **shared, max_bytes=max_bytes
        )
        captures[capture_id] = saved
        if saved["schema_id"] == bronze.COLLECTION:
            collections[capture_id] = [
                child for group in json.loads(payload)["passes"] for child in group
            ]
    for directory in replay_archive._paths(base / "outcomes", max_captures):
        capture_id = bronze._identity(directory.name)
        if capture_id not in captures:
            raise ValueError("FINRA health found orphan parse outcomes")
        for path in replay_archive._paths(directory, replay_archive.MAX_PROFILES):
            outcome = bronze._json(path)
            body = dict(outcome)
            checksum = body.pop("outcome_sha256", None)
            profile = replay_archive._profile(body.get("profile"), max_bytes, max_rows)
            if (
                checksum != digest(body)
                or body.get("schema_id") != "finra-ssv-parse-outcome/1"
                or body.get("profile_sha256") != digest(profile)
                or path.name != digest(profile) + ".json"
                or any(
                    body.get(k) != captures[capture_id][k]
                    for k in ("capture_id", "run_id", "receipt_sha256", "payload_sha256")
                )
            ):
                raise ValueError("FINRA health parse outcome mismatch")
    # Verify abandoned reservations as well as slots used by read_capture.
    for path in replay_archive._paths(base / "sequences", bronze.MAX_RESERVATIONS):
        saved = bronze._json(path)
        sequence = saved.get("capture_sequence")
        if (
            type(sequence) is not int
            or not 1 <= sequence < 2**63
            or path.name != f"{sequence:020d}.json"
            or saved
            != dict(
                schema_id=bronze.CONTRACT,
                capture_sequence=sequence,
                capture_id=bronze._identity(saved.get("capture_id")),
            )
        ):
            raise ValueError("FINRA health allocator evidence mismatch")
    return captures, collections


def _warehouse(root, manifest, shared, receipts, max_rows, max_total_bytes):
    empty = {name: [] for name in replay.TABLES}
    if not (root / "warehouse").exists() and not (root / "warehouse.json").exists():
        return empty, {}
    with store.connect(root, manifest, **shared) as db:
        db.execute("BEGIN")
        state, _ = replay._state(db, max_rows, max_total_bytes)
        results = {}
        for row in state["captures"]:
            capture_id = row["capture_id"]
            result = store.capture_result(db, capture_id)
            receipt = receipts.get(capture_id)
            if receipt is None or any(
                result[k] != receipt[k]
                for k in (
                    "capture_id",
                    "run_id",
                    "archived_at",
                    "capture_sequence",
                    "receipt_sha256",
                    "payload_sha256",
                )
            ):
                raise ValueError("FINRA health warehouse capture lineage mismatch")
            outcome = bronze._json(
                root / "bronze/outcomes" / capture_id / (digest(result["profile"]) + ".json")
            )
            if outcome["profile"] != result["profile"]:
                raise ValueError("FINRA health terminal parse evidence mismatch")
            store.rows(db, capture_id)
            results[capture_id] = result
        for row in state["versions"]:
            store.version_document(row)
        for row in state["attempts"]:
            bronze._identity(row["attempt_id"])
            receipt = receipts.get(row["capture_id"])
            if receipt is None or any(
                row[k] != receipt[k] for k in ("run_id", "archived_at", "capture_sequence")
            ):
                raise ValueError("FINRA health attempt lineage mismatch")
            started = bronze._clock_text(row["started_at"])
            if started < receipt["archived_at"] or row["status"] not in {
                "STARTED",
                "APPLIED",
                "PARTIAL",
                "REJECTED",
                "FAILED",
                "INTERRUPTED",
            }:
                raise ValueError("FINRA health attempt status/clock mismatch")
            if (
                row["completed_at"] is not None
                and bronze._clock_text(row["completed_at"]) < started
            ):
                raise ValueError("FINRA health attempt completion regressed")
        for capture_id, result in results.items():
            terminal = [
                r
                for r in state["attempts"]
                if r["capture_id"] == capture_id
                and r["status"] in {"APPLIED", "PARTIAL", "REJECTED"}
            ]
            if len(terminal) != 1 or any(
                terminal[0][k] != result[k]
                for k in ("status", "completed_at", "trade_date", "facility_id")
            ):
                raise ValueError("FINRA health terminal audit mismatch")
        for row in state["partitions"]:
            store.partition_snapshot(db, row["trade_date"], row["facility_id"])
        return state, results


def _publications(root, manifest, shared, max_rows, max_total_bytes):
    status = dict(
        status="NOT_PUBLISHED",
        current_generation_id=None,
        pending_generation_ids=[],
        generations=[],
    )
    if not (root / "parquet").exists() and not (root / "parquet.json").exists():
        return status, {}, {}
    capture_refs, version_refs = {}, {}
    current_partitions = set()
    with catalog.connect(root, manifest, **shared) as db:
        db.execute("BEGIN")
        head = publication._head(root, db)
        count = db.execute("SELECT COUNT(*) FROM generations").fetchone()[0]
        if count > 1000:
            raise ValueError("FINRA operations generation budget exceeded")
        total_rows = db.execute("SELECT COUNT(*) FROM snapshot_rows").fetchone()[0]
        if total_rows > max_rows:
            raise ValueError("FINRA operations frozen row budget exceeded")
        previous = None
        for item in db.execute("SELECT generation_id FROM generations ORDER BY ordinal"):
            record = catalog.get(db, item[0])
            if record["previous_id"] != previous:
                raise ValueError("FINRA health publication chain mismatch")
            previous = record["generation_id"]
            final = root / "parquet/snapshots" / previous
            stage = root / "parquet/.staging" / previous
            tables = publication._verify(
                root, db, record, final if final.exists() else stage, max_rows, max_total_bytes
            )
            if head and record["generation_id"] == head["generation_id"]:
                current_partitions = {
                    r["capture_id"] for r in tables["coverage"] if r.get("capture_id")
                }
            for rows in tables.values():
                for row in rows:
                    for field, target in (
                        ("capture_id", capture_refs),
                        ("observed_capture_id", capture_refs),
                        ("latest_attempt_capture_id", capture_refs),
                        ("version_id", version_refs),
                    ):
                        if row.get(field) is not None:
                            target.setdefault(row[field], set()).add(previous)
            status["generations"].append(
                dict(
                    generation_id=previous,
                    state=record["state"],
                    ordinal=record["ordinal"],
                    published_at=record["published_at"],
                )
            )
            if record["state"] == "COMMITTED":
                status["pending_generation_ids"].append(previous)
        status["current_generation_id"] = head["generation_id"] if head else None
        status["current_partition_capture_ids"] = sorted(current_partitions)
        status["status"] = (
            "RECOVERY_REQUIRED"
            if status["pending_generation_ids"]
            else ("PUBLISHED" if head else "NOT_PUBLISHED")
        )
    return status, capture_refs, version_refs


@contextmanager
def read_snapshot(
    root,
    manifest,
    *,
    approved_use,
    operation,
    max_captures=1000,
    max_rows=250000,
    max_bytes=20 * 1024**2,
    max_total_bytes=128 * 1024**2,
    max_files=50000,
    max_runs=1000,
    max_events=10000,
):
    kind = bronze._admit(manifest, approved_use, operation, {"fixture", "transform"})
    for value, high in (
        (max_captures, 10000),
        (max_rows, 1000000),
        (max_bytes, 1024**3),
        (max_total_bytes, 1024**3),
        (max_files, 100000),
        (max_runs, 10000),
        (max_events, 50000),
    ):
        _bounded(value, 1, high)
    root = Path(root)
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise ValueError("FINRA operations requires a safe root")
    shared = dict(approved_use=approved_use, operation=operation)
    with ExitStack() as stack:
        for name, required in (
            (".acquire.lock", ("acquisition", "acquisition.json")),
            (".writer.lock", ("bronze",)),
            (".ingest.lock", ("warehouse", "warehouse.json", "parquet", "parquet.json")),
        ):
            path = root / name
            if path.exists() or path.is_symlink() or any((root / p).exists() for p in required):
                stack.enter_context(_lock(path))
        files = _files(root, max_files, max_total_bytes)
        receipts, collections = _archive(root, manifest, shared, max_captures, max_bytes, max_rows)
        state, results = _warehouse(root, manifest, shared, receipts, max_rows, max_total_bytes)
        runs = operations_journal.read(
            root, manifest, kind, max_runs=max_runs, max_events=max_events
        )
        published, capture_refs, version_refs = _publications(
            root, manifest, shared, max_rows, max_total_bytes
        )
        if set(capture_refs) - set(receipts) or set(version_refs) - {
            r["version_id"] for r in state["versions"]
        }:
            raise ValueError("FINRA publication references unavailable source lineage")
        for run in runs:
            if set(run["capture_ids"]) - set(receipts):
                raise ValueError("FINRA acquisition references missing raw evidence")
            for row in run["partitions"]:
                if row["status"] == "APPLIED":
                    result = results.get(row.get("capture_id"))
                    if (
                        result is None
                        or result["status"] != "APPLIED"
                        or any(result[k] != row[k] for k in ("trade_date", "facility_id"))
                        or result["run_id"] != run["run_id"]
                    ):
                        raise ValueError(
                            "FINRA acquisition success has no matching committed outcome"
                        )
        yield dict(
            files=files,
            receipts=receipts,
            collections=collections,
            state=state,
            results=results,
            runs=runs,
            publication=published,
            published_captures=capture_refs,
            published_versions=version_refs,
        )
