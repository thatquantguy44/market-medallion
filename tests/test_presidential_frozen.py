"""PR016-R02/R05: frozen inputs require independently pinned evidence."""

import hashlib

import pytest

from market_data_pipeline.pipelines.presidential_research.frozen import (
    evidence_inventory,
    import_manual_evidence,
    validate_frozen_inputs,
)


def test_manual_evidence_requires_hash_and_detects_corruption(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"fictional evidence")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    root = tmp_path / "store"
    with pytest.raises(ValueError, match="digest"):
        import_manual_evidence(root, source, "0" * 64)
    assert import_manual_evidence(root, source, digest) == digest
    assert import_manual_evidence(root, source, digest) == digest
    assert evidence_inventory(root)[digest] == b"fictional evidence"
    (root / "manual-evidence" / digest).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="digest"):
        evidence_inventory(root)


def test_frozen_profile_cannot_accept_unverified_review(tmp_path):
    with pytest.raises(ValueError, match="Frozen review"):
        validate_frozen_inputs(tmp_path, {}, {"evidence_kind": "SYNTHETIC_FIXTURE"})


def test_profile_binds_capture_clocks_review_and_all_evidence(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.presidential_research import frozen

    source = tmp_path / "evidence"
    source.write_bytes(b"fictional verified document")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    root = tmp_path / "root"
    import_manual_evidence(root, source, digest)
    monkeypatch.setattr(frozen, "CAPTURES", {"FF_MKT_001": "a" * 64})
    monkeypatch.setattr(frozen, "RETRIEVED_AT", {"FF_MKT_001": "2026-01-01T00:00:00.000000Z"})
    monkeypatch.setattr(frozen, "EVIDENCE", frozenset({digest}))
    monkeypatch.setattr(frozen, "CAPTURE_MANIFEST_SHA", digest)
    monkeypatch.setattr(frozen, "CONTROLS_MANIFEST_SHA", digest)
    review = {
        "evidence_kind": "REVIEWED_HISTORY",
        "workbook_sha256": frozen.WORKBOOK_SHA,
        "controls_sha256": frozen.CONTROLS_SHA,
        "config_sha256": frozen.CONFIG_SHA,
    }
    receipts = {
        "FF_MKT_001": {
            "sha256": "a" * 64,
            "config_sha256": frozen.CONFIG_SHA,
            "input_kind": "CAPTURED_SOURCE",
            "retrieved_at": "2026-01-01T00:00:00.000000Z",
        }
    }
    frozen.validate_frozen_inputs(root, receipts, review)
    receipts["FF_MKT_001"]["retrieved_at"] = "2026-01-02T00:00:00.000000Z"
    with pytest.raises(ValueError, match="capture"):
        frozen.validate_frozen_inputs(root, receipts, review)
    receipts["FF_MKT_001"]["retrieved_at"] = "2026-01-01T00:00:00.000000Z"
    (root / "manual-evidence" / digest).unlink()
    with pytest.raises(ValueError, match="evidence"):
        frozen.validate_frozen_inputs(root, receipts, review)
