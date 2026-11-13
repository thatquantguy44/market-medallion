"""SSV-AC05: local immutable API captures and sanitized parser outcomes; no HTTP."""

import hashlib
import json
import os
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import parse_page
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import (
    digest,
    encoded,
    public_receipt,
    reject_credentials,
    settings,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import (
    manifest_from_dict,
    require_approved_use,
)
from market_data_pipeline.shared.files import utc_timestamp, writer_lock

CONTRACT = "finra-ssv-bronze/1"
MARKER = encoded({"schema_id": CONTRACT})
MAX_RESERVATIONS = 100000
MAX_RECEIPT_BYTES = 65536
_CAPTURE_KEYS = frozenset(
    {
        "schema_id",
        "source_schema_id",
        "manifest",
        "manifest_sha256",
        "capture_id",
        "run_id",
        "capture_sequence",
        "archived_at",
        "retrieved_at",
        "provider_published_at",
        "input_kind",
        "admission",
        "capture_limits",
        "byte_count",
        "payload_sha256",
        "api_receipt",
        "receipt_issues",
        "receipt_sha256",
    }
)
_OPERATIONS = {"fixture", "local_import", "acquire", "transform"}
_ISSUES = {
    "invalid_response_headers",
    "duplicate_response_headers",
    "payload_receipt_mismatch",
    "invalid_http_status",
    "incomplete_body",
}


def _admit(manifest, grant, operation, allowed=_OPERATIONS):
    require_approved_use(grant, operation=operation)
    if operation not in allowed:
        raise PermissionError("FINRA operation does not admit this storage action")
    if grant["route"] not in {"invented_fixture", "finra_query_api", "approved_local_file"}:
        raise PermissionError("FINRA Bronze admits only the selected API-response profile")
    if manifest_from_dict(manifest.to_dict()) != manifest:
        raise ValueError("FINRA Bronze requires the verified manifest")
    return "invented_fixture" if grant["route"] == "invented_fixture" else "provider_api"


def _identity(value):
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise ValueError("FINRA lineage requires canonical UUID text") from None
    return value


def _clock(value):
    if not isinstance(value, datetime):
        raise ValueError("FINRA archive time requires an aware datetime")
    return utc_timestamp(value)


def _clock_text(value):
    try:
        if _clock(datetime.fromisoformat(value.replace("Z", "+00:00"))) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise ValueError("FINRA archive clock is invalid") from None
    return value


def _sync(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(path, body):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())


def _read(path, limit):
    try:
        if path.is_symlink() or not path.is_file():
            raise ValueError
        with path.open("rb") as stream:
            body = stream.read(limit + 1)
        if len(body) > limit:
            raise ValueError
    except (OSError, ValueError):
        raise ValueError("FINRA archive file is missing, unsafe or exceeds its bound") from None
    return body


def _json(path):
    raw = _read(path, MAX_RECEIPT_BYTES)
    try:
        value = json.loads(raw)
        if not isinstance(value, dict) or encoded(value) != raw:
            raise ValueError
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("FINRA archive JSON is corrupt or noncanonical") from None
    return value


def _publish_directory(parent, name, files):
    stage = parent / (".stage-" + uuid4().hex)
    stage.mkdir(mode=0o700)
    try:
        for key, body in files.items():
            if body is None:
                (stage / key).mkdir(mode=0o700)
                _sync(stage / key)
            else:
                _write(stage / key, body)
        _sync(stage)
        os.rename(stage, parent / name)
        _sync(parent)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def _put(path, body):
    if path.exists() or path.is_symlink():
        if _read(path, MAX_RECEIPT_BYTES) != body:
            raise ValueError("FINRA immutable artifact conflict")
        return
    stage = path.parent / (".write-" + uuid4().hex + ".tmp")
    try:
        _write(stage, body)
        os.link(stage, path)
        _sync(path.parent)
    finally:
        stage.unlink(missing_ok=True)


def _base(root, *, create=False):
    root = Path(root)
    base = root / "bronze"
    if create and not base.exists() and not base.is_symlink():
        _publish_directory(
            root,
            "bronze",
            {"format.json": MARKER, "captures": None, "sequences": None, "outcomes": None},
        )
    if base.is_symlink() or _read(base / "format.json", 1024) != MARKER:
        raise ValueError("FINRA Bronze storage identity mismatch")
    for name in ("captures", "sequences", "outcomes"):
        if (base / name).is_symlink() or not (base / name).is_dir():
            raise ValueError("FINRA Bronze directory is missing or unsafe")
    return base


def _reserve(base, capture_id):
    names = list((base / "sequences").iterdir())
    if len(names) >= MAX_RESERVATIONS:
        raise ValueError("FINRA capture reservation limit reached")
    numbers = []
    for path in names:
        if re.fullmatch(r"\.write-[0-9a-f]{32}\.tmp", path.name):
            continue
        if (
            path.is_symlink()
            or not path.is_file()
            or not re.fullmatch(r"[0-9]{20}\.json", path.name)
        ):
            raise ValueError("FINRA sequence inventory is corrupt")
        number = int(path.stem)
        if not 1 <= number < 2**63:
            raise ValueError("FINRA sequence is invalid")
        numbers.append(number)
    sequence = max(numbers, default=0) + 1
    if sequence >= 2**63:
        raise ValueError("FINRA capture sequence exhausted")
    slot = {"schema_id": CONTRACT, "capture_sequence": sequence, "capture_id": capture_id}
    _put(base / "sequences" / f"{sequence:020d}.json", encoded(slot))
    return sequence


def _receipt(
    manifest,
    public,
    issues,
    *,
    capture_id,
    run_id,
    sequence,
    clock,
    retrieved,
    kind,
    grant,
    operation,
    limits,
    payload,
):
    if retrieved is not None and retrieved > clock:
        raise ValueError("FINRA retrieval completion follows archive time")
    value = {
        "schema_id": CONTRACT,
        "source_schema_id": manifest.schema_id,
        "manifest": manifest.to_dict(),
        "manifest_sha256": digest(manifest.to_dict()),
        "capture_id": capture_id,
        "run_id": run_id,
        "capture_sequence": sequence,
        "archived_at": clock,
        "retrieved_at": retrieved,
        "provider_published_at": None,
        "input_kind": kind,
        "admission": {
            "profile_sha256": digest(grant),
            "route": grant["route"],
            "operation": operation,
        },
        "capture_limits": limits,
        "byte_count": len(payload),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "api_receipt": public,
        "receipt_issues": issues,
    }
    value["receipt_sha256"] = digest(value)
    if len(encoded(value)) > MAX_RECEIPT_BYTES:
        raise ValueError("FINRA receipt exceeds metadata budget")
    return value


def archive_input(
    root,
    payload,
    manifest,
    api_receipt,
    *,
    approved_use,
    operation,
    run_id,
    capture_id=None,
    archived_at=None,
    retrieved_at=None,
    max_bytes=20 * 1024 * 1024,
    max_rows=5000,
):
    """Archive admitted response bytes before dataset parsing. No provider request."""
    kind = _admit(manifest, approved_use, operation, {"fixture", "local_import", "acquire"})
    limits = {k: v for k, v in settings(max_bytes, max_rows).items() if k != "algorithm"}
    run_id = _identity(run_id)
    capture_id = _identity(capture_id) if capture_id is not None else str(uuid4())
    if not isinstance(payload, bytes) or len(payload) > max_bytes:
        raise ValueError("FINRA input exceeds byte/type boundary")
    reject_credentials(payload)
    public, issues = public_receipt(api_receipt, payload, manifest, max_rows=max_rows)
    clock = _clock(archived_at if archived_at is not None else datetime.now(UTC))
    retrieved = _clock(retrieved_at) if retrieved_at is not None else None
    # Validate all candidate metadata, including clocks, before creating the root.
    _receipt(
        manifest,
        public,
        issues,
        capture_id=capture_id,
        run_id=run_id,
        sequence=1,
        clock=clock,
        retrieved=retrieved,
        kind=kind,
        grant=approved_use,
        operation=operation,
        limits=limits,
        payload=payload,
    )
    root = Path(root)
    if (root / ".writer.lock").is_symlink():
        raise ValueError("FINRA writer lock is unsafe")
    with writer_lock(root):
        base = _base(root, create=True)
        target = base / "captures" / capture_id
        if target.exists() or target.is_symlink():
            stored, raw = read_capture(
                root,
                capture_id,
                manifest,
                approved_use=approved_use,
                operation=operation,
                max_bytes=max_bytes,
            )
            sequence = stored["capture_sequence"]
            if archived_at is None:
                clock = stored["archived_at"]
            if retrieved_at is None:
                retrieved = stored["retrieved_at"]
        else:
            stored = None
            raw = None
            if archived_at is None:
                clock = _clock(datetime.now(UTC))
            sequence = _reserve(base, capture_id)
        value = _receipt(
            manifest,
            public,
            issues,
            capture_id=capture_id,
            run_id=run_id,
            sequence=sequence,
            clock=clock,
            retrieved=retrieved,
            kind=kind,
            grant=approved_use,
            operation=operation,
            limits=limits,
            payload=payload,
        )
        if stored is not None:
            if stored != value or raw != payload:
                raise ValueError("FINRA capture identity conflict")
        else:
            _publish_directory(
                base / "captures",
                capture_id,
                {"payload.bin": payload, "receipt.json": encoded(value)},
            )
        verified, raw = read_capture(
            root,
            capture_id,
            manifest,
            approved_use=approved_use,
            operation=operation,
            max_bytes=max_bytes,
        )
        if verified != value or raw != payload:
            raise ValueError("FINRA capture readback mismatch")
        return verified


def read_capture(
    root, capture_id, manifest, *, approved_use, operation, max_bytes=20 * 1024 * 1024
):
    kind = _admit(manifest, approved_use, operation)
    settings(max_bytes, 1)
    capture_id = _identity(capture_id)
    base = _base(root)
    target = base / "captures" / capture_id
    if target.is_symlink() or not target.is_dir():
        raise ValueError("FINRA capture directory is missing or unsafe")
    value = _json(target / "receipt.json")
    try:
        body = dict(value)
        identity = body.pop("receipt_sha256")
        if (
            set(value) != _CAPTURE_KEYS
            or identity != digest(body)
            or value["schema_id"] != CONTRACT
            or value["source_schema_id"] != manifest.schema_id
            or value["manifest"] != manifest.to_dict()
            or value["manifest_sha256"] != digest(manifest.to_dict())
            or value["capture_id"] != capture_id
        ):
            raise ValueError
        admission = value["admission"]
        if (
            set(admission) != {"profile_sha256", "route", "operation"}
            or not isinstance(admission["profile_sha256"], str)
            or re.fullmatch("[0-9a-f]{64}", admission["profile_sha256"]) is None
            or admission["route"]
            not in {"invented_fixture", "finra_query_api", "approved_local_file"}
            or admission["operation"] not in {"fixture", "local_import", "acquire"}
            or (admission["route"] == "invented_fixture") != (admission["operation"] == "fixture")
            or value["input_kind"]
            != ("invented_fixture" if admission["route"] == "invented_fixture" else "provider_api")
        ):
            raise ValueError
        if value["input_kind"] != kind:
            raise PermissionError("FINRA fixture/provider capture kind cannot be relabelled")
        payload = _read(target / "payload.bin", max_bytes)
        if (
            value["payload_sha256"] != hashlib.sha256(payload).hexdigest()
            or type(value["byte_count"]) is not int
            or value["byte_count"] != len(payload)
            or value["provider_published_at"] is not None
            or value["receipt_issues"] != sorted(set(value["receipt_issues"]))
            or set(value["receipt_issues"]) - _ISSUES
        ):
            raise ValueError
        _identity(value["run_id"])
        _clock_text(value["archived_at"])
        if value["retrieved_at"] is not None:
            _clock_text(value["retrieved_at"])
            if value["retrieved_at"] > value["archived_at"]:
                raise ValueError
        limits = value["capture_limits"]
        settings(**limits)
        if len(payload) > limits["max_bytes"]:
            raise ValueError
        public, issues = public_receipt(
            value["api_receipt"], payload, manifest, max_rows=limits["max_rows"]
        )
        if public != value["api_receipt"] or set(issues) - set(value["receipt_issues"]):
            raise ValueError
        sequence = value["capture_sequence"]
        if type(sequence) is not int or not 1 <= sequence < 2**63:
            raise ValueError
        expected = {"schema_id": CONTRACT, "capture_sequence": sequence, "capture_id": capture_id}
        if _json(base / "sequences" / f"{sequence:020d}.json") != expected:
            raise ValueError
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("FINRA capture integrity or lineage mismatch") from None
    reject_credentials(payload)
    return value, payload


def parse_capture(
    root,
    capture_id,
    manifest,
    *,
    approved_use,
    operation,
    max_bytes=20 * 1024 * 1024,
    max_rows=5000,
):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    profile = settings(max_bytes, max_rows)
    saved, payload = read_capture(
        root,
        capture_id,
        manifest,
        approved_use=approved_use,
        operation=operation,
        max_bytes=max_bytes,
    )
    page = None
    error = "receipt_rejected" if saved["receipt_issues"] else None
    if error is None:
        try:
            page = parse_page(
                payload, manifest, saved["api_receipt"], max_bytes=max_bytes, max_rows=max_rows
            )
        except ValueError:
            error = "parse_rejected"
        except Exception:
            raise RuntimeError("FINRA parse execution failed; original capture retained") from None
    value = {
        "schema_id": "finra-ssv-parse-outcome/1",
        "capture_id": capture_id,
        "run_id": saved["run_id"],
        "receipt_sha256": saved["receipt_sha256"],
        "payload_sha256": saved["payload_sha256"],
        "profile": profile,
        "profile_sha256": digest(profile),
        "status": "parsed" if page is not None else "rejected",
        "error_code": error,
        "published": False,
        "page": None
        if page is None
        else {
            "row_count": len(page.rows),
            "trade_date": page.trade_date.isoformat(),
            "facility_id": page.facility_id,
            "offset": page.offset,
            "limit": page.limit,
            "record_total": page.record_total,
            "page_partition_complete": page.partition_complete,
        },
    }
    value["outcome_sha256"] = digest(value)
    root = Path(root)
    if (root / ".writer.lock").is_symlink():
        raise ValueError("FINRA writer lock is unsafe")
    with writer_lock(root):
        base = _base(root)
        directory = base / "outcomes" / capture_id
        if directory.is_symlink():
            raise ValueError("FINRA outcome directory is unsafe")
        directory.mkdir(mode=0o700, exist_ok=True)
        _sync(directory.parent)
        _put(directory / (value["profile_sha256"] + ".json"), encoded(value))
    return page, value
