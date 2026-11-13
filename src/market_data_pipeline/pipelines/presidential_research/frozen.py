"""Explicit frozen research profile and content-pinned manual evidence."""

import hashlib
import re
from pathlib import Path

from .storage import _read_bounded, _safe_child

PROFILE = "public-reconstruction-2026-11-03"
CONFIG_SHA = "da1803275ce3d1d6f26d823a51e29e34a5c45dfd2e09b5576c2aa53ef96376e8"
WORKBOOK_SHA = "56781c0f530ba66655069af4291c938d9bb7d3c60cf0516ee44097dd0e27c912"
CONTROLS_SHA = "210ea006674d7b9ad4bf7687c70a5be02290e3f7243ef203e574b20112d09749"
CAPTURES = {
    "FF_MKT_001": "b03d39e4bb253dda0f14893db3b78e64b40b8d6549a0fc0fd4111dae84c98618",
    "FEDFUNDS_001": "74680b2d09bb28f90fb30c1c09e93ac2d443de1b7345ba5a9112b3eca9be0a25",
    "SHILLER_001": "0df9392b7dacf91f756e92c8db508ad903c4588b43f3253680eec3dd8b40db68",
}
RETRIEVED_AT = {
    "FF_MKT_001": "2026-10-31T04:33:11.000000Z",
    "FEDFUNDS_001": "2026-10-31T04:33:17.000000Z",
    "SHILLER_001": "2026-10-31T04:34:04.000000Z",
}
CAPTURE_MANIFEST_SHA = "dde50f8e458bf06518e279a6deebadab745dfb472f817cbb296e49e70947be81"
CONTROLS_MANIFEST_SHA = "d59f61c1415e25a23e116255acc2b3a8623548d367560fb41ce19ae40ce5dcb6"
EVIDENCE = frozenset(
    {
        "d716bed3e2d8bff37dc3585307d7e0990eae094ca5c521d9a7e50502351a1b4d",
        "dd79cf46562091e216dccc7dfbf1ffb4fecf6561a94e8c930bd4d04c0b42dce3",
        "65400bbe9f162a2b1b7d05df39b928f3ba4c4b6a0af1d7bbe71496232edfe17f",
        "7712424eb2ac8a7ca83a02e494d14a795253bccc0c6f9275c73e31822d692b55",
        "2be84a8b5cc5c71ba08ee3bf499712e6b5131a9ef8a215ac83c9c3b5625cf9f0",
    }
)
MAX_EVIDENCE_BYTES = 10 * 1024 * 1024


def import_manual_evidence(root, path, expected_sha256):
    """Admit one explicitly selected file under its independently expected digest."""
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256 or ""):
        raise ValueError("A SHA-256 digest is required.")
    raw = _read_bounded(Path(path), MAX_EVIDENCE_BYTES)
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("Manual evidence digest mismatch.")
    directory = _safe_child(Path(root), "manual-evidence")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = _safe_child(directory, expected_sha256)
    try:
        with target.open("xb") as handle:
            handle.write(raw)
    except FileExistsError:
        if _read_bounded(target, MAX_EVIDENCE_BYTES) != raw:
            raise ValueError("Manual evidence digest collision or corruption.") from None
    return expected_sha256


def evidence_inventory(root):
    directory = _safe_child(Path(root), "manual-evidence")
    if not directory.exists():
        return {}
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("Invalid manual evidence directory.")
    result = {}
    for path in directory.iterdir():
        if not re.fullmatch(r"[0-9a-f]{64}", path.name):
            raise ValueError("Invalid manual evidence identity.")
        raw = _read_bounded(path, MAX_EVIDENCE_BYTES)
        if hashlib.sha256(raw).hexdigest() != path.name:
            raise ValueError("Manual evidence digest mismatch.")
        result[path.name] = raw
        if len(result) > 100 or sum(map(len, result.values())) > 50 * 1024 * 1024:
            raise ValueError("Manual evidence inventory exceeds limit.")
    return result


def validate_frozen_inputs(root, receipts, review):
    if (
        review["evidence_kind"] != "REVIEWED_HISTORY"
        or review["workbook_sha256"] != WORKBOOK_SHA
        or review["controls_sha256"] != CONTROLS_SHA
        or review["config_sha256"] != CONFIG_SHA
    ):
        raise ValueError("Frozen review or configuration digest mismatch.")
    if set(receipts) != set(CAPTURES) or any(
        item["sha256"] != CAPTURES[key]
        or item["config_sha256"] != CONFIG_SHA
        or item["input_kind"] != "CAPTURED_SOURCE"
        or item["retrieved_at"] != RETRIEVED_AT[key]
        for key, item in receipts.items()
    ):
        raise ValueError("Frozen source capture digest or retrieval time mismatch.")
    if not (EVIDENCE | {CAPTURE_MANIFEST_SHA, CONTROLS_MANIFEST_SHA}).issubset(
        evidence_inventory(root)
    ):
        raise ValueError("Frozen manual evidence is incomplete.")
