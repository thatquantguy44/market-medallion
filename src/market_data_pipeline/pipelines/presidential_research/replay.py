"""Offline selected-snapshot replay into a separate, initially absent root."""

import json
import os
import re
import shutil
import tempfile
from csv import Error as CSVError
from datetime import datetime
from pathlib import Path
from zipfile import BadZipFile

from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

from .frozen import evidence_inventory
from .parsers import ParseError, parse_payload
from .publication import _sync_dir, _write, publish_baseline, publish_full_bundle, read_current
from .review import load_review_version
from .review_workbook import MAX_WORKBOOK_BYTES
from .storage import MAX_CONFIG_BYTES, _read_bounded, _safe_child, load_capture
from .warehouse import _database, ingest_capture

_SOURCE = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")
_ID = re.compile(r"^[0-9a-f]{32}$")
_MAX_INVENTORY_BYTES = 512 * 1024 * 1024
_MAX_CAPTURES = 20_000


def _capture_inventory(root):
    bronze = _safe_child(root, "bronze")
    captures = []
    total_bytes = 0
    if not bronze.is_dir():
        raise ValueError("Source capture inventory is unavailable.")
    for source_path in bronze.iterdir():
        source_id = source_path.name
        if not _SOURCE.fullmatch(source_id) or source_path.is_symlink() or not source_path.is_dir():
            raise ValueError("Invalid source capture inventory.")
        for directory in source_path.iterdir():
            capture_id = directory.name
            if not _ID.fullmatch(capture_id) or directory.is_symlink() or not directory.is_dir():
                raise ValueError("Invalid source capture inventory.")
            receipt, raw, config = load_capture(root, source_id, capture_id)
            receipt_bytes = _read_bounded(_safe_child(directory, "receipt.json"), 65536)
            parse_path = _safe_child(directory, "parse.json")
            parse_bytes = _read_bounded(parse_path, 65536) if parse_path.exists() else None
            if parse_bytes is not None:
                parse = json.loads(parse_bytes)
                if (
                    set(parse) != {"status", "row_count", "error_code"}
                    or parse["status"] not in ("PARSED", "REJECTED")
                    or type(parse["row_count"]) is not int
                    or parse["row_count"] < 0
                ):
                    raise ValueError("Invalid source parse receipt.")
                try:
                    parsed = parse_payload(receipt["parser_id"], raw, source_id)
                    expected = {"status": "PARSED", "row_count": len(parsed), "error_code": None}
                except (BadZipFile, CSVError, EOFError, ParseError, ValueError, UnicodeError):
                    expected = {
                        "status": "REJECTED",
                        "row_count": 0,
                        "error_code": "PARSE_REJECTED",
                    }
                if parse != expected:
                    raise ValueError("Source parse receipt does not match archived bytes.")
            total_bytes += len(raw) + len(config) + len(receipt_bytes) + len(parse_bytes or b"")
            if total_bytes > _MAX_INVENTORY_BYTES or len(captures) >= _MAX_CAPTURES:
                raise ValueError("Replay capture inventory exceeds size limits.")
            captures.append((receipt, raw, config, receipt_bytes, parse_bytes))
    captures.sort(key=lambda item: item[0]["capture_sequence"])
    if len({item[0]["capture_sequence"] for item in captures}) != len(captures):
        raise ValueError("Duplicate source capture sequence.")
    return captures


def _review_inventory(root):
    review = _safe_child(root, "review")
    if not review.is_dir():
        raise ValueError("Review inventory is unavailable.")
    result = []
    for directory in review.iterdir():
        review_id = directory.name
        if not _ID.fullmatch(review_id) or directory.is_symlink() or not directory.is_dir():
            raise ValueError("Invalid review inventory.")
        receipt, _, _ = load_review_version(root, review_id)
        limits = {
            "review.xlsx": MAX_WORKBOOK_BYTES,
            "sources.toml": MAX_CONFIG_BYTES,
            "controls.csv": 5 * 1024 * 1024,
            "controls.manifest.json": 5 * 1024 * 1024,
            "receipt.json": 65536,
        }
        result.append(
            (
                receipt,
                {
                    name: _read_bounded(_safe_child(directory, name), limit)
                    for name, limit in limits.items()
                },
            )
        )
    return sorted(result, key=lambda item: item[0]["review_id"])


def replay_selected_snapshot(source_root, destination, *, replayed_at: datetime):
    """Rebuild a verified current generation; never fetch and never overwrite a root."""
    source = Path(source_root).resolve(strict=True)
    requested = Path(destination)
    parent = requested.parent.resolve(strict=True)
    target = parent / requested.name
    if (
        target.exists()
        or target.is_symlink()
        or target == source
        or source in target.parents
        or target in source.parents
    ):
        raise ValueError("Replay destination must be a separate absent directory.")
    admitted_at = utc_timestamp(replayed_at)
    with writer_lock(source):
        current = read_current(source)
        captures = _capture_inventory(source)
        reviews = _review_inventory(source)
        manual_evidence = evidence_inventory(source)
        connection = _database(source, create=False)
        try:
            audit = connection.execute(
                "SELECT capture_id, source_id, capture_sequence, status, row_count, "
                "missing_count, new_versions FROM captures ORDER BY capture_sequence"
            ).fetchall()
            original_facts = connection.execute(
                "SELECT * FROM facts ORDER BY version_id"
            ).fetchall()
            original_captures = connection.execute(
                "SELECT * FROM captures ORDER BY capture_sequence"
            ).fetchall()
            original_mappings = connection.execute(
                "SELECT * FROM capture_facts ORDER BY capture_id, fact_type, fact_key"
            ).fetchall()
        finally:
            connection.close()
        seq_path = _safe_child(_safe_child(source, "audit"), "capture-sequence.json")
        sequence_bytes = _read_bounded(seq_path, 4096)
        sequence = json.loads(sequence_bytes)
        if (
            set(sequence) != {"sequence"}
            or type(sequence["sequence"]) is not int
            or sequence["sequence"]
            < max((item[0]["capture_sequence"] for item in captures), default=0)
        ):
            raise ValueError("Invalid capture ordering inventory.")
    by_id = {item[0]["capture_id"]: item for item in captures}
    if len(by_id) != len(captures) or any(
        row[0] not in by_id
        or row[1] != by_id[row[0]][0]["source_id"]
        or row[2] != by_id[row[0]][0]["capture_sequence"]
        or by_id[row[0]][4] is None
        for row in audit
    ):
        raise ValueError("Warehouse and capture inventories do not match.")
    clocks = [item[0]["first_seen_at"] for item in captures] + [
        item[0]["admitted_at"] for item in reviews
    ]
    if any(admitted_at < value for value in clocks):
        raise ValueError("Replay admission cannot precede the source evidence.")
    staging = Path(tempfile.mkdtemp(prefix=".pres-replay-", dir=parent))
    try:
        for receipt, raw, config, receipt_bytes, parse_bytes in captures:
            directory = staging / "bronze" / receipt["source_id"] / receipt["capture_id"]
            directory.mkdir(parents=True, mode=0o700)
            for name, value in (
                ("payload", raw),
                ("config.toml", config),
                ("receipt.json", receipt_bytes),
            ):
                _write(directory / name, value)
            if parse_bytes is not None:
                _write(directory / "parse.json", parse_bytes)
        if manual_evidence:
            directory = staging / "manual-evidence"
            directory.mkdir(mode=0o700)
            for identity, value in manual_evidence.items():
                _write(directory / identity, value)
        for receipt, files in reviews:
            directory = staging / "review" / receipt["review_id"]
            directory.mkdir(parents=True, mode=0o700)
            for name, value in files.items():
                _write(directory / name, value)
            load_review_version(staging, receipt["review_id"])
        (staging / "audit").mkdir(mode=0o700)
        _write(staging / "audit" / "capture-sequence.json", sequence_bytes)
        for capture_id, source_id, _, status, count, missing, new_versions in audit:
            restored = ingest_capture(staging, source_id, capture_id)
            if (
                restored["status"],
                restored["row_count"],
                restored["missing_count"],
                restored["new_versions"],
            ) != (status, count, missing, new_versions):
                raise ValueError("Replay capture outcome does not match source.")
        connection = _database(staging, create=False)
        try:
            if (
                connection.execute("SELECT * FROM facts ORDER BY version_id").fetchall()
                != original_facts
                or connection.execute("SELECT * FROM captures ORDER BY capture_sequence").fetchall()
                != original_captures
                or connection.execute(
                    "SELECT * FROM capture_facts ORDER BY capture_id, fact_type, fact_key"
                ).fetchall()
                != original_mappings
            ):
                raise ValueError("Replay fact versions or audit records do not match source.")
        finally:
            connection.close()
        catalog = current["catalog"]
        source_inputs = catalog["inputInventory"]
        created_at = datetime.fromisoformat(catalog["createdAt"].replace("Z", "+00:00"))
        if len(catalog["datasets"]) == 1:
            published = publish_baseline(
                staging,
                source_inputs["captureId"],
                source_inputs["reviewId"],
                profile_id=catalog["profileId"],
                created_at=created_at,
            )
        else:
            selected = {item["sourceId"]: item["captureId"] for item in source_inputs["captures"]}
            published = publish_full_bundle(
                staging,
                selected,
                source_inputs["reviewId"],
                profile_id=catalog["profileId"],
                created_at=created_at,
            )
        if (
            published["snapshotId"] != catalog["snapshotId"]
            or published["catalogSha256"] != current["catalogSha256"]
        ):
            raise ValueError("Replay publication does not match the selected generation.")
        atomic_json(
            staging / "audit" / "replay-receipt.json",
            {
                "schemaVersion": "presidential-replay/1",
                "snapshotId": published["snapshotId"],
                "catalogSha256": published["catalogSha256"],
                "destinationAdmittedAt": admitted_at,
                "originalCaptureClocksPreserved": True,
                "captureCount": len(captures),
                "reviewCount": len(reviews),
            },
        )
        try:
            target.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise ValueError("Replay destination already exists.") from exc
        # The pointer-bearing Gold directory moves last. A partial move never
        # exposes a selected generation, and an existing directory is not replaced.
        for child in sorted(staging.iterdir(), key=lambda path: (path.name == "gold", path.name)):
            os.rename(child, target / child.name)
        _sync_dir(target)
        _sync_dir(parent)
        return {
            "snapshotId": published["snapshotId"],
            "catalogSha256": published["catalogSha256"],
            "destination": str(target),
        }
    finally:
        if staging.exists():
            shutil.rmtree(staging)
