"""Immutable bounded local SEC FTD captures, including rejected payloads."""

import hashlib
import json
import os
import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import StorageConfig
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import (
    SCHEMA_VERSION,
    parse_manifest,
)
from market_data_pipeline.shared.files import utc_timestamp


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write_once(path: Path, payload: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _sync(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_bounded(path: Path, maximum: int) -> bytes:
    if path.is_symlink():
        raise ValueError("capture artifacts must not be symbolic links")
    with path.open("rb") as stream:
        payload = stream.read(maximum + 1)
    if len(payload) > maximum:
        raise ValueError("capture artifact exceeds its size budget")
    return payload


def archive_capture(
    root: Path,
    *,
    run_id: str,
    sequence: int,
    captured_at: datetime,
    manifest_payload: bytes,
    settings: StorageConfig,
    input_path: Path,
    clock=None,
    coverage_evidence: bytes | None = None,
) -> tuple[dict, bytes | None]:
    if coverage_evidence is not None and (
        not isinstance(coverage_evidence, bytes) or len(coverage_evidence) > 65536
    ):
        raise ValueError("completeness evidence must fit its 64 KiB capture budget")
    manifest = parse_manifest(manifest_payload)
    if not re.fullmatch(r"[0-9a-f]{32}", run_id) or type(sequence) is not int or sequence < 1:
        raise ValueError("capture requires a safe run ID and positive sequence")
    started_clock = utc_timestamp(captured_at)
    directory = Path(root) / "bronze" / run_id
    if directory.parent.is_symlink():
        raise ValueError("Bronze parent must not be a symbolic link")
    directory.mkdir(parents=True, exist_ok=False)
    _write_once(directory / "manifest.yml", manifest_payload)
    profile = (json.dumps(asdict(settings), sort_keys=True, indent=2) + "\n").encode()
    _write_once(directory / "storage.json", profile)
    if coverage_evidence is not None:
        _write_once(directory / "coverage-evidence.json", coverage_evidence)
    try:
        if not Path(input_path).is_file():
            raise OSError("source is not a regular local file")
        with Path(input_path).open("rb") as stream:
            payload = stream.read(manifest.max_archive_bytes + 1)
        complete = len(payload) <= manifest.max_archive_bytes
        state = "AVAILABLE" if complete else "OVERSIZED"
    except OSError:
        payload, complete, state = None, False, "UNAVAILABLE"
    captured_clock = utc_timestamp(clock()) if clock is not None else started_clock
    if payload is not None:
        _write_once(directory / "payload.bin", payload)
    receipt = {
        "receipt_version": "sec-ftd-capture/2",
        "coverage_evidence_sha256": _digest(coverage_evidence)
        if coverage_evidence is not None
        else None,
        "run_id": run_id,
        "capture_sequence": sequence,
        "captured_at": captured_clock,
        "capture_started_at": started_clock,
        "transport": "local-file",
        "provider_published_at": None,
        "origin_retrieved_at": None,
        "input_name": Path(input_path).name,
        "source_state": state,
        "payload_complete": complete,
        "captured_bytes": len(payload) if payload is not None else 0,
        "captured_sha256": _digest(payload) if payload is not None else None,
        "raw_sha256": _digest(payload) if complete else None,
        "manifest_sha256": _digest(manifest_payload),
        "storage_sha256": _digest(profile),
        "storage_contract_id": settings.contract_id,
        "parser_version": SCHEMA_VERSION,
        "partition_id": f"{manifest.period_start}_{manifest.period_end}",
        "member_name": manifest.member_name,
    }
    _write_once(
        directory / "receipt.json", (json.dumps(receipt, sort_keys=True, indent=2) + "\n").encode()
    )
    _sync(directory)
    _sync(directory.parent)
    _sync(Path(root))
    return receipt, payload


def load_capture(directory: Path) -> tuple[dict, bytes | None, bytes, StorageConfig]:
    """Validate complete or explicitly incomplete capture artifacts, without replay."""
    try:
        receipt = json.loads(_read_bounded(directory / "receipt.json", 65536))
        if receipt["receipt_version"] not in ("sec-ftd-capture/1", "sec-ftd-capture/2"):
            raise ValueError("unsupported receipt")
        proof = directory / "coverage-evidence.json"
        proof_hash = (
            receipt["coverage_evidence_sha256"]
            if receipt["receipt_version"] == "sec-ftd-capture/2"
            else None
        )
        if proof_hash is not None:
            if _digest(_read_bounded(proof, 65536)) != proof_hash:
                raise ValueError("completeness evidence digest mismatch")
        elif proof.exists():
            raise ValueError("unexpected completeness evidence in capture")
        if (
            not re.fullmatch(r"[0-9a-f]{32}", receipt["run_id"])
            or directory.name != receipt["run_id"]
        ):
            raise ValueError("capture run identity mismatch")
        if (
            type(receipt["capture_sequence"]) is not int
            or receipt["capture_sequence"] < 1
            or type(receipt["payload_complete"]) is not bool
            or type(receipt["captured_bytes"]) is not int
            or receipt["captured_bytes"] < 0
            or receipt["parser_version"] != SCHEMA_VERSION
        ):
            raise ValueError("invalid capture sequence, completeness or parser version")
        try:
            start = datetime.fromisoformat(receipt["capture_started_at"])
            captured = datetime.fromisoformat(receipt["captured_at"])
            if (
                utc_timestamp(start) != receipt["capture_started_at"]
                or utc_timestamp(captured) != receipt["captured_at"]
                or captured < start
            ):
                raise ValueError("invalid capture clocks")
        except ValueError as exc:
            raise ValueError("invalid capture clocks") from exc
        manifest = _read_bounded(directory / "manifest.yml", 65536)
        profile = _read_bounded(directory / "storage.json", 4096)
        if (
            _digest(manifest) != receipt["manifest_sha256"]
            or _digest(profile) != receipt["storage_sha256"]
        ):
            raise ValueError("capture manifest/profile digest mismatch")
        selected = parse_manifest(manifest)
        if (
            receipt["member_name"] != selected.member_name
            or receipt["partition_id"] != f"{selected.period_start}_{selected.period_end}"
        ):
            raise ValueError("capture partition differs from its manifest")
        settings = StorageConfig(**json.loads(profile))
        if settings.contract_id != receipt["storage_contract_id"]:
            raise ValueError("capture storage contract mismatch")
        payload_path = directory / "payload.bin"
        if receipt["source_state"] == "UNAVAILABLE":
            if (
                payload_path.exists()
                or receipt["captured_sha256"] is not None
                or receipt["raw_sha256"] is not None
                or receipt["payload_complete"]
                or receipt["captured_bytes"] != 0
            ):
                raise ValueError("unavailable capture cannot have a payload")
            payload = None
        else:
            payload = _read_bounded(payload_path, selected.max_archive_bytes + 1)
            if (
                len(payload) > selected.max_archive_bytes + 1
                or len(payload) != receipt["captured_bytes"]
                or _digest(payload) != receipt["captured_sha256"]
            ):
                raise ValueError("capture payload digest/size mismatch")
            if receipt["payload_complete"]:
                if (
                    receipt["source_state"] != "AVAILABLE"
                    or _digest(payload) != receipt["raw_sha256"]
                    or len(payload) > selected.max_archive_bytes
                ):
                    raise ValueError("invalid complete capture")
            elif receipt["source_state"] != "OVERSIZED" or receipt["raw_sha256"] is not None:
                raise ValueError("invalid incomplete capture")
        return receipt, payload, manifest, settings
    except (KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid capture receipt") from exc
