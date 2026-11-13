"""PR016-R13: health signals are separate and never request a source."""

from datetime import timedelta
from unittest.mock import patch

import test_presidential_bundle_assembly as fixtures

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.operations import research_health
from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture
from market_data_pipeline.pipelines.presidential_research.publication import publish_full_bundle
from market_data_pipeline.pipelines.presidential_research.warehouse import ingest_capture

NOW = fixtures.NOW


def test_health_separates_capture_freshness_coverage_and_review(tmp_path, monkeypatch):
    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)
    publish_full_bundle(
        root, ids, review["review_id"], profile_id="fixture-complete", created_at=NOW
    )
    with patch("socket.socket", side_effect=AssertionError("network is forbidden")):
        status = research_health(
            root,
            now=NOW + timedelta(days=60),
            stale_after=timedelta(days=30),
            expected_end_year=1940,
        )
    assert status["snapshot"]["status"] == "VALID"
    assert status["captures"]["FF_MKT_001"]["freshness"] == "STALE"
    assert status["coverage"]["status"] == "INSUFFICIENT"
    assert status["review"]["status"] == "VALID"
    bad, _ = import_local_capture(
        root,
        load_config(tmp_path / "sources.toml"),
        "FEDFUNDS_001",
        b"observation_date,FEDFUNDS\n1939-01-01,NaN\n",
        admitted_at=NOW + timedelta(days=61),
        input_kind="SYNTHETIC_FIXTURE",
    )
    assert ingest_capture(root, "FEDFUNDS_001", bad["capture_id"])["status"] == "REJECTED"
    status = research_health(
        root, now=NOW + timedelta(days=61), stale_after=timedelta(days=30), expected_end_year=1940
    )
    assert status["captures"]["FEDFUNDS_001"]["latestOutcome"] == "REJECTED"
    assert status["snapshot"]["status"] == "VALID"
    (root / "review" / review["review_id"] / "controls.csv").write_bytes(b"changed")
    status = research_health(root, now=NOW + timedelta(days=61), stale_after=timedelta(days=30))
    assert status["review"]["status"] == "INTEGRITY_FAILURE"
    assert status["snapshot"]["status"] == "VALID"


def test_missing_root_reports_unavailable_without_creating_files(tmp_path):
    root = tmp_path / "missing"
    status = research_health(root, now=NOW, stale_after=timedelta(days=1))
    assert status["snapshot"]["status"] == "UNAVAILABLE"
    assert not root.exists()


def test_unprocessed_capture_and_invalid_snapshot_are_explicit(tmp_path, monkeypatch):
    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)
    result = publish_full_bundle(
        root, ids, review["review_id"], profile_id="fixture-complete", created_at=NOW
    )
    pending, _ = import_local_capture(
        root,
        load_config(tmp_path / "sources.toml"),
        "FEDFUNDS_001",
        b"observation_date,FEDFUNDS\n1939-01-01,4.00\n",
        admitted_at=NOW + timedelta(days=1),
        input_kind="SYNTHETIC_FIXTURE",
    )
    status = research_health(root, now=NOW, stale_after=timedelta(days=30))
    assert status["captures"]["FEDFUNDS_001"]["freshness"] == "CLOCK_AHEAD"
    assert status["captures"]["FEDFUNDS_001"]["latestOutcome"] == "NOT_PROCESSED"
    assert status["snapshot"]["status"] == "VALID"
    assert pending["capture_id"]
    artifact = (
        root
        / "gold"
        / "snapshots"
        / result["snapshotId"]
        / "evidence"
        / "presidential-cycle-ledger.csv"
    )
    artifact.write_bytes(b"changed")
    status = research_health(root, now=NOW + timedelta(days=1), stale_after=timedelta(days=30))
    assert status["snapshot"]["status"] == "INTEGRITY_FAILURE"
    assert status["captureInventoryStatus"] == "VALID"
