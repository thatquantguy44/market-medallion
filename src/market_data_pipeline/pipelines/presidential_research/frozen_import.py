"""Explicit offline admission of the previously archived frozen research study."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from .config import load_config
from .frozen import (
    CAPTURE_MANIFEST_SHA,
    CAPTURES,
    CONFIG_SHA,
    CONTROLS_MANIFEST_SHA,
    CONTROLS_SHA,
    EVIDENCE,
    PROFILE,
    RETRIEVED_AT,
    WORKBOOK_SHA,
    import_manual_evidence,
)
from .pipeline import import_local_capture
from .publication import publish_full_bundle
from .review import promote_review_workbook
from .storage import MAX_CONFIG_BYTES, MAX_RAW_BYTES, _read_bounded
from .warehouse import ingest_capture

_PATHS = {
    "FF_MKT_001": "raw/FF_MKT_001/20261031T043311Z_b03d39e4bb25.zip",
    "FEDFUNDS_001": "raw/FEDFUNDS_001/20261031T043317Z_74680b2d09bb.csv",
    "SHILLER_001": "raw/SHILLER_001/20261031T043404Z_0df9392b7dac.xls",
}
_EVIDENCE_PATHS = {
    "d716bed3e2d8bff37dc3585307d7e0990eae094ca5c521d9a7e50502351a1b4d": (
        "ctrl_074_house_profile.html"
    ),
    "dd79cf46562091e216dccc7dfbf1ffb4fecf6561a94e8c930bd4d04c0b42dce3": (
        "ctrl_074_house_speakers.html"
    ),
    "65400bbe9f162a2b1b7d05df39b928f3ba4c4b6a0af1d7bbe71496232edfe17f": (
        "house_party_government.html"
    ),
    "7712424eb2ac8a7ca83a02e494d14a795253bccc0c6f9275c73e31822d692b55": "nara_president_terms.html",
    "2be84a8b5cc5c71ba08ee3bf499712e6b5131a9ef8a215ac83c9c3b5625cf9f0": (
        "senate_party_division.html"
    ),
}


def _pinned(path, digest, limit):
    payload = _read_bounded(path, limit)
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("Frozen input digest mismatch.")
    return payload


def import_frozen_study(donor_pipeline, workbook, workspace, *, now=None):
    """Admit only the named, pinned donor files into a new local workspace.

    No provider or sibling runtime import is used. Existing roots are never modified.
    Original capture and control manifests are retained as content-addressed evidence.
    """
    donor, workbook, root = Path(donor_pipeline), Path(workbook), Path(workspace)
    if root.exists() or root.is_symlink():
        raise ValueError("Frozen import destination must be absent.")
    local = donor / ".local"
    _pinned(donor / "config/sources.toml", CONFIG_SHA, MAX_CONFIG_BYTES)
    raw_manifest = _pinned(local / "raw/manifest.json", CAPTURE_MANIFEST_SHA, MAX_CONFIG_BYTES)
    _pinned(local / "inputs/controls.manifest.json", CONTROLS_MANIFEST_SHA, 5 * 1024 * 1024)
    _pinned(local / "inputs/controls.csv", CONTROLS_SHA, 5 * 1024 * 1024)
    _pinned(workbook, WORKBOOK_SHA, MAX_RAW_BYTES)
    manifest = json.loads(raw_manifest)
    config = load_config(donor / "config/sources.toml")
    raw = {}
    for source, relative_path in _PATHS.items():
        rows = [
            row
            for row in manifest["artifacts"]
            if row.get("source_id") == source
            and row.get("relative_path") == relative_path
            and row.get("status") == "FETCHED"
        ]
        if len(rows) != 1 or rows[0].get("sha256") != CAPTURES[source]:
            raise ValueError("Frozen original capture manifest is inconsistent.")
        retrieved = datetime.fromisoformat(rows[0]["retrieved_at"].replace("Z", "+00:00"))
        if (
            retrieved.isoformat(timespec="microseconds").replace("+00:00", "Z")
            != RETRIEVED_AT[source]
        ):
            raise ValueError("Frozen retrieval clock does not match.")
        raw[source] = (_pinned(local / relative_path, CAPTURES[source], MAX_RAW_BYTES), retrieved)
    if set(_EVIDENCE_PATHS) != EVIDENCE:
        raise ValueError("Frozen manual evidence inventory is inconsistent.")
    for digest, filename in _EVIDENCE_PATHS.items():
        _pinned(local / "review_evidence" / filename, digest, MAX_RAW_BYTES)
    admitted_at = now or datetime.now(UTC)
    root.mkdir(mode=0o700, parents=False)
    captures = {}
    for source, (payload, retrieved) in raw.items():
        receipt, outcome = import_local_capture(
            root,
            config,
            source,
            payload,
            admitted_at=admitted_at,
            retrieved_at=retrieved,
            expected_sha256=CAPTURES[source],
            input_kind="CAPTURED_SOURCE",
        )
        if outcome["status"] != "PARSED":
            raise ValueError("Archived source failed parsing; raw capture retained.")
        if ingest_capture(root, source, receipt["capture_id"])["status"] != "PARSED":
            raise ValueError("Archived source failed warehouse admission; raw capture retained.")
        captures[source] = receipt["capture_id"]
    for digest, filename in _EVIDENCE_PATHS.items():
        import_manual_evidence(root, local / "review_evidence" / filename, digest)
    import_manual_evidence(root, local / "raw/manifest.json", CAPTURE_MANIFEST_SHA)
    import_manual_evidence(root, local / "inputs/controls.manifest.json", CONTROLS_MANIFEST_SHA)
    review = promote_review_workbook(
        root, config, workbook, admitted_at=admitted_at, evidence_kind="REVIEWED_HISTORY"
    )
    publication = publish_full_bundle(
        root, captures, review["review_id"], profile_id=PROFILE, created_at=admitted_at
    )
    return {
        **publication,
        "captureIds": captures,
        "reviewId": review["review_id"],
        "profileId": PROFILE,
    }
