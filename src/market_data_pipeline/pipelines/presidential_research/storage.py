"""Offline immutable raw captures for presidential research (PR016-R02/N02)."""

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

MAX_RAW_BYTES = 50 * 1024 * 1024
MAX_CONFIG_BYTES = 1024 * 1024
_ID = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")
_CAPTURE = re.compile(r"^[0-9a-f]{32}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_child(parent: Path, name: str) -> Path:
    path = parent / name
    if path.is_symlink():
        raise ValueError("Symlinked capture paths are not permitted.")
    return path


def _read_bounded(path: Path, limit: int) -> bytes:
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError("Capture file cannot be safely opened.") from exc
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("Capture must be a regular file.")
        payload = handle.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("Capture exceeds the size limit.")
    return payload


def archive_capture(
    root: Path,
    source_id: str,
    payload: bytes,
    config_bytes: bytes,
    *,
    admitted_at: datetime,
    input_kind: str,
    retrieved_at: datetime | None = None,
    expected_sha256: str | None = None,
    parser_id: str,
    acquisition: dict | None = None,
) -> dict:
    """Archive bytes before parsing; caller must select an admitted source catalog entry."""
    if not isinstance(source_id, str) or not _ID.fullmatch(source_id):
        raise ValueError("Invalid source ID.")
    if not isinstance(payload, bytes) or len(payload) > MAX_RAW_BYTES:
        raise ValueError("Raw payload is invalid or exceeds the size limit.")
    if not isinstance(config_bytes, bytes) or len(config_bytes) > MAX_CONFIG_BYTES:
        raise ValueError("Source configuration is invalid or exceeds the size limit.")
    if not isinstance(parser_id, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", parser_id):
        raise ValueError("Invalid parser ID.")
    if acquisition is not None:
        _validate_acquisition(acquisition, input_kind)
    if input_kind not in ("CAPTURED_SOURCE", "SYNTHETIC_FIXTURE"):
        raise ValueError("An explicit input kind is required.")
    digest = _digest(payload)
    if expected_sha256 is not None and (
        not isinstance(expected_sha256, str)
        or not _DIGEST.fullmatch(expected_sha256)
        or expected_sha256 != digest
    ):
        raise ValueError("Raw payload digest mismatch.")
    admitted = utc_timestamp(admitted_at)
    retrieved = utc_timestamp(retrieved_at) if retrieved_at is not None else None
    if retrieved is not None and retrieved > admitted:
        raise ValueError("Source retrieval time cannot be after admission.")
    root = Path(root)
    with writer_lock(root):
        bronze = _safe_child(root, "bronze")
        bronze.mkdir(mode=0o700, exist_ok=True)
        source = _safe_child(bronze, source_id)
        source.mkdir(mode=0o700, exist_ok=True)
        audit = _safe_child(root, "audit")
        audit.mkdir(mode=0o700, exist_ok=True)
        sequence_path = _safe_child(audit, "capture-sequence.json")
        sequence = 0
        if sequence_path.exists():
            state = json.loads(_read_bounded(sequence_path, 4096))
            if (
                set(state) != {"sequence"}
                or type(state["sequence"]) is not int
                or state["sequence"] < 0
            ):
                raise ValueError("Invalid capture sequence state.")
            sequence = state["sequence"]
        sequence += 1
        atomic_json(sequence_path, {"sequence": sequence})
        capture_id = uuid.uuid4().hex
        final = _safe_child(source, capture_id)
        if final.exists():
            raise ValueError("Capture identity already exists.")
        staging = Path(tempfile.mkdtemp(prefix=".capture-", dir=source))
        receipt = {
            "schema": "presidential-raw-capture/2" if acquisition else "presidential-raw-capture/1",
            "capture_id": capture_id,
            "capture_sequence": sequence,
            "source_id": source_id,
            "parser_id": parser_id,
            "method_version": "presidential-donor/0.1.0",
            "sha256": digest,
            "bytes": len(payload),
            "config_sha256": _digest(config_bytes),
            "config_bytes": len(config_bytes),
            "retrieved_at": retrieved,
            "admitted_at": admitted,
            "first_seen_at": admitted,
            "provider_published_at": None,
            "transport": acquisition["transport"] if acquisition else "local_import",
            "input_kind": input_kind,
        }
        if acquisition is not None:
            receipt["acquisition"] = acquisition
        try:
            for name, data in (("payload", payload), ("config.toml", config_bytes)):
                with (staging / name).open("xb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
            atomic_json(staging / "receipt.json", receipt)
            os.rename(staging, final)
            directory_fd = os.open(source, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    return receipt


def _validate_acquisition(value, input_kind):
    keys = {
        "transport",
        "http_status",
        "route",
        "startup_fallback_used",
        "content_type",
        "etag",
        "last_modified",
    }
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("Invalid acquisition receipt.")
    if (
        value["http_status"] != 200
        or type(value["startup_fallback_used"]) is not bool
        or value["route"] not in ("direct", "proxy", "injected")
        or (
            input_kind == "SYNTHETIC_FIXTURE"
            and (value["transport"] != "injected_fixture" or value["route"] != "injected")
        )
        or (
            input_kind == "CAPTURED_SOURCE"
            and (value["transport"] != "https_fetch" or value["route"] not in ("direct", "proxy"))
        )
    ):
        raise ValueError("Invalid acquisition receipt.")
    for field in ("content_type", "etag", "last_modified"):
        item = value[field]
        if item is not None and (
            not isinstance(item, str)
            or len(item) > 512
            or any(ord(char) < 32 or ord(char) == 127 for char in item)
        ):
            raise ValueError("Invalid acquisition metadata.")


def load_capture(root: Path, source_id: str, capture_id: str) -> tuple[dict, bytes, bytes]:
    if not isinstance(source_id, str) or not _ID.fullmatch(source_id):
        raise ValueError("Invalid source ID.")
    if not isinstance(capture_id, str) or not _CAPTURE.fullmatch(capture_id):
        raise ValueError("Invalid capture ID.")
    directory = _safe_child(_safe_child(_safe_child(Path(root), "bronze"), source_id), capture_id)
    receipt_path = _safe_child(directory, "receipt.json")
    payload_path = _safe_child(directory, "payload")
    config_path = _safe_child(directory, "config.toml")
    receipt = json.loads(_read_bounded(receipt_path, 65536))
    payload = _read_bounded(payload_path, MAX_RAW_BYTES)
    config_bytes = _read_bounded(config_path, MAX_CONFIG_BYTES)
    expected_keys = {
        "schema",
        "capture_id",
        "capture_sequence",
        "source_id",
        "parser_id",
        "method_version",
        "sha256",
        "bytes",
        "config_sha256",
        "config_bytes",
        "retrieved_at",
        "admitted_at",
        "first_seen_at",
        "provider_published_at",
        "transport",
        "input_kind",
    }
    try:
        if not isinstance(receipt, dict):
            raise ValueError("Invalid capture receipt.")
        if receipt.get("schema") == "presidential-raw-capture/2":
            expected_keys.add("acquisition")
            _validate_acquisition(receipt.get("acquisition"), receipt.get("input_kind"))
            if receipt.get("transport") != receipt["acquisition"]["transport"]:
                raise ValueError("Invalid capture transport.")
        elif (
            receipt.get("schema") != "presidential-raw-capture/1"
            or receipt.get("transport") != "local_import"
        ):
            raise ValueError("Invalid capture receipt.")
        if set(receipt) != expected_keys:
            raise ValueError("Invalid capture receipt.")
        admitted = receipt["admitted_at"]
        retrieved = receipt["retrieved_at"]
        if (
            not isinstance(admitted, str)
            or utc_timestamp(datetime.fromisoformat(admitted.replace("Z", "+00:00"))) != admitted
            or receipt["first_seen_at"] != admitted
            or receipt["provider_published_at"] is not None
            or receipt["input_kind"] not in ("CAPTURED_SOURCE", "SYNTHETIC_FIXTURE")
            or receipt["method_version"] != "presidential-donor/0.1.0"
            or not isinstance(receipt["parser_id"], str)
            or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", receipt["parser_id"])
            or type(receipt["capture_sequence"]) is not int
            or receipt["capture_sequence"] < 1
        ):
            raise ValueError("Invalid capture receipt.")
        if retrieved is not None and (
            not isinstance(retrieved, str)
            or utc_timestamp(datetime.fromisoformat(retrieved.replace("Z", "+00:00"))) != retrieved
            or retrieved > admitted
        ):
            raise ValueError("Invalid capture receipt.")
    except (TypeError, AttributeError, ValueError) as exc:
        raise ValueError("Invalid capture receipt.") from exc
    if (
        receipt.get("source_id") != source_id
        or receipt.get("capture_id") != capture_id
        or receipt.get("bytes") != len(payload)
        or receipt.get("sha256") != _digest(payload)
        or receipt.get("config_bytes") != len(config_bytes)
        or receipt.get("config_sha256") != _digest(config_bytes)
    ):
        raise ValueError("Capture digest or identity mismatch.")
    return receipt, payload, config_bytes
