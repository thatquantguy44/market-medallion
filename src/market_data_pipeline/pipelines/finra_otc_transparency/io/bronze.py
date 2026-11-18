"""OTC-AC05 immutable local raw captures and linked validation outcomes; no HTTP."""

import fcntl
import json
import os
import re
import shutil
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import (
    legacy_manifest,
    select_partitions,
)
from market_data_pipeline.pipelines.finra_otc_transparency.data.weekly import (
    FIXTURE_HEADERS,
    ParsedWeekly,
    parse_weekly,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import (
    CONTRACT,
    DEFAULT_BYTES,
    MAX_METADATA,
    MAX_PARTS,
    MAX_SEQUENCES,
    OUTCOME_VERSION,
    Part,
    clock,
    clock_text,
    digest,
    encoded,
    freeze_part,
    identity,
    limits,
    make_receipt,
    manifest_snapshot,
    part_id,
    schema_snapshot,
)
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import (
    manifest_from_dict,
    require_approved_use,
)

MARKER = encoded({"format": CONTRACT, "dataset": "finra-otc-transparency"})


@dataclass(frozen=True)
class ArchivedCapture:
    receipt: dict
    parts: dict[str, bytes]
    discovery: bytes | None


@dataclass(frozen=True)
class ValidationResult:
    receipt: dict
    outcome: dict
    parsed: ParsedWeekly | None


def _admit(approved_use, operation):
    route = approved_use.get("route") if isinstance(approved_use, dict) else None
    operation = "fixture" if route == "invented_fixture" else operation
    require_approved_use(approved_use, operation=operation)
    return {"route": route, "operation": operation, "profile_sha256": digest(approved_use)}


def _directory(path):
    if path.is_symlink() or not path.is_dir():
        raise ValueError("OTC archive directory is missing or unsafe")
    return path


def _root(root, *, create=False):
    root = Path(root)
    if ".." in root.parts or root.is_symlink():
        raise ValueError("OTC archive root cannot be a symlink or traversal path")
    root = root.absolute()
    if create:
        if root.exists():
            _directory(root)
            if not (root / "bronze").exists() and not (root / "bronze").is_symlink():
                if any(
                    p.name != ".writer.lock"
                    and not re.fullmatch(r"\.otc-stage-[0-9a-f]{32}", p.name)
                    for p in root.iterdir()
                ):
                    raise ValueError("OTC refuses an unmarked nonempty pipeline root")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
    return _directory(root)


@contextmanager
def _locked(root):
    try:
        fd = os.open(
            root / ".writer.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600
        )
    except OSError:
        raise ValueError("OTC writer lock is missing or unsafe") from None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("OTC writer lock must be a regular file")
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(path, body):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())


def _read(path, limit):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise ValueError
    except (OSError, ValueError):
        raise ValueError(
            "OTC archive file is missing, nonregular or exceeds its byte bound"
        ) from None
    return raw


def _json(path):
    raw = _read(path, MAX_METADATA)
    try:
        value = json.loads(raw)
        if not isinstance(value, dict) or encoded(value) != raw:
            raise ValueError
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("OTC archive JSON is corrupt or noncanonical") from None
    return value


def _publish_directory(parent, name, files):
    stage = parent / (".otc-stage-" + uuid4().hex)
    stage.mkdir(mode=0o700)
    directories = []
    try:
        for relative, body in files.items():
            path = stage / relative
            if body is None:
                path.mkdir(mode=0o700)
                directories.append(path)
            else:
                _write(path, body)
        for directory in reversed(directories):
            _sync(directory)
        _sync(stage)
        os.rename(stage, parent / name)
        _sync(parent)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def _put(path, body):
    if path.exists() or path.is_symlink():
        if _read(path, MAX_METADATA) != body:
            raise ValueError("OTC immutable artifact conflict")
        return
    stage = path.parent / (".otc-write-" + uuid4().hex + ".tmp")
    try:
        _write(stage, body)
        os.link(stage, path)
        _sync(path.parent)
    finally:
        stage.unlink(missing_ok=True)


def _base(root, *, create=False):
    base = root / "bronze"
    if create and not base.exists() and not base.is_symlink():
        _publish_directory(
            root,
            "bronze",
            {"format.json": MARKER, "captures": None, "sequences": None, "outcomes": None},
        )
    _directory(base)
    if _read(base / "format.json", 1024) != MARKER:
        raise ValueError("OTC archive storage identity mismatch")
    for name in ("captures", "sequences", "outcomes"):
        _directory(base / name)
    return base


def _slot(sequence, capture_id, run_id, captured_at):
    return {
        "format": CONTRACT,
        "capture_sequence": sequence,
        "capture_id": capture_id,
        "run_id": run_id,
        "captured_at": captured_at,
    }


def _reserve(base, capture_id, run_id, captured_at):
    numbers = []
    for scanned, entry in enumerate((base / "sequences").iterdir(), start=1):
        if scanned > MAX_SEQUENCES:
            raise ValueError("OTC capture reservation budget exhausted")
        if re.fullmatch(r"\.otc-write-[0-9a-f]{32}\.tmp", entry.name):
            continue
        if (
            entry.is_symlink()
            or not stat.S_ISREG(entry.stat().st_mode)
            or not re.fullmatch(r"[0-9]{20}\.json", entry.name)
        ):
            raise ValueError("OTC capture reservation inventory is corrupt")
        number = int(entry.stem)
        if not 1 <= number <= MAX_SEQUENCES:
            raise ValueError("OTC capture reservation is out of bounds")
        numbers.append(number)
    last = max(numbers, default=0)
    if last:
        previous = _json(base / "sequences" / f"{last:020d}.json")
        if set(previous) != {"format", "capture_sequence", "capture_id", "run_id", "captured_at"}:
            raise ValueError("OTC capture reservation is corrupt")
        expected = _slot(
            last,
            identity(previous["capture_id"]),
            identity(previous["run_id"]),
            clock_text(previous["captured_at"]),
        )
        if previous != expected:
            raise ValueError("OTC capture reservation is corrupt")
        if captured_at < previous["captured_at"]:
            raise ValueError("OTC capture clock regression")
    sequence = last + 1
    if sequence > MAX_SEQUENCES:
        raise ValueError("OTC capture sequence exhausted")
    _put(
        base / "sequences" / f"{sequence:020d}.json",
        encoded(_slot(sequence, capture_id, run_id, captured_at)),
    )
    return sequence


def archive_capture(
    root,
    *,
    partition,
    manifest,
    approved_use,
    run_id,
    expected_parts,
    parts,
    discovery=None,
    capture_id=None,
    captured_at=None,
    max_bytes=DEFAULT_BYTES,
    max_rows=10000,
):
    """Commit exact bytes before parsing; this performs no provider request."""
    admission = _admit(approved_use, "retain")
    if captured_at is not None and admission["route"] != "invented_fixture":
        raise ValueError("OTC real retention must use the actual archive clock")
    if not isinstance(parts, (list, tuple)) or len(parts) > MAX_PARTS:
        raise ValueError("OTC received parts require a finite explicit list")
    if not isinstance(expected_parts, (list, tuple)) or not 1 <= len(expected_parts) <= MAX_PARTS:
        raise ValueError("OTC expected parts require a finite explicit list")
    parts = tuple(freeze_part(p) for p in tuple(parts))
    expected_parts = tuple(expected_parts)
    discovery = freeze_part(discovery) if discovery is not None else None
    snapshot = manifest_snapshot(manifest)
    capture_id = identity(capture_id) if capture_id is not None else str(uuid4())
    run_id = identity(run_id)
    capture_limits = limits(max_bytes, max_rows)
    stamp = clock(captured_at if captured_at is not None else datetime.now(UTC))
    values = dict(
        partition=partition,
        manifest=snapshot,
        parts=parts,
        discovery=discovery,
        expected_parts=expected_parts,
        run_id=run_id,
        capture_id=capture_id,
        captured_at=stamp,
        sequence=1,
        admission=admission,
        capture_limits=capture_limits,
    )
    make_receipt(**values)  # Validate all metadata before filesystem access.
    root = _root(root, create=True)
    if (root / "bronze").exists() or (root / "bronze").is_symlink():
        _base(root)  # Refuse a foreign store before creating its lock file.
    with _locked(root):
        base = _base(root, create=True)
        target = base / "captures" / capture_id
        if target.exists() or target.is_symlink():
            stored = read_capture(root, capture_id, approved_use=approved_use, max_bytes=max_bytes)
            values["sequence"] = stored.receipt["capture_sequence"]
            values["captured_at"] = stored.receipt["captured_at"] if captured_at is None else stamp
            value = make_receipt(**values)
            if (
                value != stored.receipt
                or stored.parts != {p.part_id: p.payload for p in parts}
                or stored.discovery != (discovery.payload if discovery is not None else None)
            ):
                raise ValueError("OTC immutable capture identity conflict")
            return value
        if captured_at is None:
            values["captured_at"] = clock(datetime.now(UTC))
        make_receipt(**values)
        values["sequence"] = _reserve(base, capture_id, run_id, values["captured_at"])
        value = make_receipt(**values)
        files = {"parts": None, "receipt.json": encoded(value)}
        files.update({"parts/" + p.part_id + ".bin": p.payload for p in parts})
        if discovery is not None:
            files["discovery.bin"] = discovery.payload
        _publish_directory(base / "captures", capture_id, files)
        if (
            read_capture(root, capture_id, approved_use=approved_use, max_bytes=max_bytes).receipt
            != value
        ):
            raise ValueError("OTC capture readback mismatch")
        return value


def _names(path, maximum):
    names = set()
    for item in path.iterdir():
        names.add(item.name)
        if len(names) > maximum:
            raise ValueError("OTC archive directory exceeds its inventory bound")
    return names


def _restore_part(record, path, budget):
    if not isinstance(record, dict) or set(record) != {
        "part_id",
        "sha256",
        "byte_count",
        "source_url",
        "http_status",
        "retrieved_at",
        "headers",
        "body_complete",
    }:
        raise ValueError("invalid OTC stored part receipt")
    part_id(record["part_id"])
    count = record["byte_count"]
    if type(count) is not int or not 0 <= count <= budget:
        raise ValueError("OTC stored bytes exceed the read budget")
    raw = _read(path, count)
    retrieved = (
        datetime.fromisoformat(clock_text(record["retrieved_at"]).replace("Z", "+00:00"))
        if record["retrieved_at"] is not None
        else None
    )
    return Part(
        record["part_id"],
        raw,
        record["source_url"],
        record["http_status"],
        retrieved,
        record["headers"],
        record["body_complete"],
    )


def read_capture(root, capture_id, *, approved_use, max_bytes=DEFAULT_BYTES):
    """Verify an immutable archive, never creating directories or acquiring data.

    Stored snapshots are evidence, not current parser admission; validation below
    separately requires the reviewed current manifest and declared schema.
    """
    admission = _admit(approved_use, "retain")
    limits(max_bytes, 1)
    capture_id = identity(capture_id)
    base = _base(_root(root))
    target = _directory(base / "captures" / capture_id)
    value = _json(target / "receipt.json")
    try:
        if value["capture_id"] != capture_id or value["admission"]["route"] != admission["route"]:
            raise ValueError("OTC capture identity/route mismatch")
        (partition,) = select_partitions([value["partition"]])
        if not isinstance(value["parts"], list) or len(value["parts"]) > MAX_PARTS:
            raise ValueError("OTC stored parts exceed their finite ledger")
        _directory(target / "parts")
        parts = []
        total = 0
        for record in value["parts"]:
            name = part_id(record["part_id"])
            part = _restore_part(record, target / "parts" / (name + ".bin"), max_bytes - total)
            parts.append(part)
            total += len(part.payload)
        discovery = (
            _restore_part(value["discovery"], target / "discovery.bin", max_bytes - total)
            if value["discovery"] is not None
            else None
        )
        expected = make_receipt(
            partition=partition,
            manifest=value["manifest"],
            schema=value["schema"],
            parts=parts,
            discovery=discovery,
            expected_parts=value["expected_parts"],
            run_id=value["run_id"],
            capture_id=capture_id,
            sequence=value["capture_sequence"],
            captured_at=value["captured_at"],
            admission=value["admission"],
            capture_limits=value["limits"],
        )
        if expected != value:
            raise ValueError("OTC receipt or raw byte/hash mismatch")
        if _names(target, 3) != (
            {"receipt.json", "parts", "discovery.bin"} if discovery else {"receipt.json", "parts"}
        ):
            raise ValueError("unexpected OTC capture members")
        if _names(target / "parts", MAX_PARTS) != {p.part_id + ".bin" for p in parts}:
            raise ValueError("unexpected OTC raw part members")
        slot = _json(base / "sequences" / f"{value['capture_sequence']:020d}.json")
        if slot != _slot(
            value["capture_sequence"], capture_id, value["run_id"], value["captured_at"]
        ):
            raise ValueError("OTC capture/reservation mismatch")
    except (KeyError, TypeError, AttributeError, UnicodeError):
        raise ValueError("malformed OTC archive receipt") from None
    return ArchivedCapture(
        value, {p.part_id: p.payload for p in parts}, discovery.payload if discovery else None
    )


def validate_capture(root, capture_id, *, approved_use, max_bytes=DEFAULT_BYTES):
    """Persist a sanitized immutable parse outcome after verifying raw evidence."""
    _admit(approved_use, "transform")
    capture = read_capture(root, capture_id, approved_use=approved_use, max_bytes=max_bytes)
    receipt, parsed, reason = capture.receipt, None, None
    status = receipt["coverage"]
    if status == "RECEIVED_ALL":
        records = receipt["parts"] + ([receipt["discovery"]] if receipt["discovery"] else [])
        if any(p["http_status"] not in (None, 200) for p in records):
            status, reason = "REJECTED", "HTTP_STATUS"
        elif receipt["admission"]["route"] != "invented_fixture":
            status, reason = "REJECTED", "UNSUPPORTED_ROUTE"
        elif len(capture.parts) != 1:
            status, reason = "REJECTED", "MULTIPART_UNSUPPORTED"
        elif receipt["schema"]["schema_profile"] not in FIXTURE_HEADERS:
            status, reason = "REJECTED", "UNSUPPORTED_PROFILE"
        else:
            try:
                if receipt["manifest"] != legacy_manifest():
                    manifest_from_dict(receipt["manifest"])
                (partition,) = select_partitions([receipt["partition"]])
                if receipt["schema"] != schema_snapshot(partition, receipt["manifest"]):
                    raise ValueError
            except ValueError:
                status, reason = "REJECTED", "UNSUPPORTED_CONTRACT"
            else:
                try:
                    parsed = parse_weekly(
                        next(iter(capture.parts.values())),
                        partition=partition,
                        schema_profile=receipt["schema"]["schema_profile"],
                        max_bytes=receipt["limits"]["max_bytes"],
                        max_rows=receipt["limits"]["max_rows"],
                    )
                except ValueError:
                    status, reason = "REJECTED", "PARSER_REJECTED"
                else:
                    status = parsed.outcome
    outcome = {
        "format": OUTCOME_VERSION,
        "capture_id": capture_id,
        "capture_sequence": receipt["capture_sequence"],
        "receipt_sha256": receipt["receipt_sha256"],
        "manifest_sha256": receipt["manifest_sha256"],
        "schema_sha256": receipt["schema_sha256"],
        "status": status,
        "reason": reason,
        "row_count": len(parsed.rows) if parsed is not None else None,
        "membership": "UNVERIFIED",
    }
    outcome["outcome_sha256"] = digest(outcome)
    outcome_id = digest({"format": OUTCOME_VERSION, "receipt_sha256": receipt["receipt_sha256"]})
    root = _root(root)
    with _locked(root):
        base = _base(root)
        target = base / "outcomes" / capture_id
        if not target.exists() and not target.is_symlink():
            target.mkdir(mode=0o700)
            _sync(target.parent)
        _directory(target)
        _put(target / (outcome_id + ".json"), encoded(outcome))
    return ValidationResult(receipt, outcome, parsed)
