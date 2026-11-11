"""Durable acquisition intents and explicit offline recovery; no provider requests."""

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import manifest_from_dict


def begin_attempt(root, *, manifest, source_url, kind, max_bytes, started_at, phase):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import _json, _write

    manifest_from_dict(manifest.to_dict())
    manifest.capture_url(source_url, kind)
    if (
        phase not in ("probe", "get", "local_import")
        or type(max_bytes) is not int
        or max_bytes < 1
        or not isinstance(started_at, datetime)
        or started_at.utcoffset() is None
    ):
        raise ValueError("invalid FOMC acquisition intent")
    identity = str(uuid.uuid4())
    value = {
        "schema_id": "fomc-acquisition-attempt/1",
        "capture_id": identity,
        "source_url": source_url,
        "kind": kind,
        "phase": phase,
        "started_at": started_at.astimezone(UTC).isoformat(),
        "max_bytes": max_bytes,
        "manifest": manifest.to_dict(),
        "manifest_sha256": hashlib.sha256(_json(manifest.to_dict())).hexdigest(),
    }
    directory = Path(root) / "attempts"
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import make_directory

    make_directory(directory)
    _write(directory / (identity + ".json"), _json(value))
    return identity


def read_attempt(root, identity):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import _identity, _json

    try:
        value = json.loads((Path(root) / "attempts" / (_identity(identity) + ".json")).read_bytes())
        manifest = manifest_from_dict(value["manifest"])
        if (
            value["schema_id"] != "fomc-acquisition-attempt/1"
            or value["capture_id"] != identity
            or value["manifest_sha256"] != hashlib.sha256(_json(manifest.to_dict())).hexdigest()
        ):
            raise ValueError
        manifest.capture_url(value["source_url"], value["kind"])
        if (
            value["phase"] not in ("probe", "get", "local_import")
            or datetime.fromisoformat(value["started_at"]).utcoffset() is None
            or type(value["max_bytes"]) is not int
            or value["max_bytes"] < 1
        ):
            raise ValueError
        return value
    except (OSError, ValueError, UnicodeError, KeyError, TypeError):
        raise ValueError("FOMC acquisition intent is missing or invalid") from None


def finish_attempt(root, identity, result):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        _identity,
        _json,
        _write,
    )

    if not (Path(root) / "attempts" / (_identity(identity) + ".json")).is_file():
        return
    read_attempt(root, identity)
    directory = Path(root) / "attempt_outcomes"
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import make_directory

    make_directory(directory)
    path = directory / (identity + ".json")
    data = _json({"schema_id": "fomc-acquisition-outcome/1", "capture_id": identity, **result})
    try:
        _write(path, data)
    except FileExistsError:
        if path.read_bytes() != data:
            raise ValueError("FOMC attempt has conflicting terminal evidence") from None


def recover_attempt(root, identity, store):
    """Retry durable bytes only. Missing pre-receipt bodies remain interrupted evidence.

    Invoke after the owning writer has stopped; no live request is resumed.
    Existing corrupt files/markers are never silently replaced.
    """
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        _identity,
        commit_capture,
        read_capture,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import _sync_directory

    identity = _identity(identity)
    directory = Path(root) / "captures" / identity
    intent_path = Path(root) / "attempts" / (identity + ".json")
    intent = read_attempt(root, identity) if intent_path.exists() else None
    if intent is None and not directory.is_dir():
        raise ValueError("FOMC recovery requires a recorded attempt or capture")
    if not (directory / "receipt.json").is_file():
        outcome = Path(root) / "attempt_outcomes" / (identity + ".json")
        if outcome.is_file():
            value = json.loads(outcome.read_bytes())
            if value.get("result") == "probe_selected" and value.get("capture_id") == identity:
                return {"capture_id": identity, "status": "probe_selected"}
        return {"capture_id": identity, "status": "interrupted_before_receipt"}
    receipt = json.loads((directory / "receipt.json").read_bytes())
    if not isinstance(receipt, dict):
        raise ValueError("invalid FOMC recovery receipt")
    if intent and receipt.get("attempt_phase") != intent["phase"]:
        raise ValueError("FOMC capture conflicts with its acquisition phase")
    if intent and any(
        receipt.get(k) != intent[k] for k in ("source_url", "kind", "max_bytes", "manifest_sha256")
    ):
        raise ValueError("FOMC capture conflicts with its acquisition intent")
    if not (directory / "payload.bin").exists():
        digest = receipt.get("sha256", "")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)
        ):
            raise ValueError("FOMC recovery receipt hash is invalid")
        obj = Path(root) / "objects" / "sha256" / (digest + ".bin")
        if not obj.is_file():
            return {"capture_id": identity, "status": "missing_original_bytes"}
        payload = obj.read_bytes()
        if hashlib.sha256(payload).hexdigest() != digest or len(payload) != receipt.get(
            "size_bytes"
        ):
            raise ValueError("FOMC recovery object hash mismatch")
        os.link(obj, directory / "payload.bin")
        _sync_directory(directory)
    read_capture(root, identity)
    commit_capture(root, identity, store)
    return {"capture_id": identity, "status": "committed", "accepted": receipt["accepted"]}
