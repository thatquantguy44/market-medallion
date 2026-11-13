"""PR016-R02/R03: review promotion, immutable evidence and input bounds."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import test_presidential_review_workbook as fixtures

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.review import promote_review_workbook
from market_data_pipeline.pipelines.presidential_research.review_workbook import (
    ReviewWorkbookError,
    export_approved_controls,
)

CATALOG = Path("config/presidential_research/sources.toml")
NOW = datetime(2026, 11, 13, tzinfo=UTC)


def test_approved_review_preserves_immutable_source_and_hashes(tmp_path):
    workbook = tmp_path / "source.xlsx"
    fixtures._workbook_fixture(workbook, "APPROVED")
    source_hash = hashlib.sha256(workbook.read_bytes()).hexdigest()
    result = promote_review_workbook(
        tmp_path / "store", load_config(CATALOG), workbook, admitted_at=NOW
    )
    archive = tmp_path / "store" / "review" / result["review_id"]
    manifest = json.loads((archive / "controls.manifest.json").read_text())
    assert hashlib.sha256((archive / "review.xlsx").read_bytes()).hexdigest() == source_hash
    assert manifest["review_workbook_sha256"] == source_hash
    assert manifest["review_workbook"] == "review.xlsx"
    assert (
        manifest["controls_sha256"]
        == hashlib.sha256((archive / "controls.csv").read_bytes()).hexdigest()
    )
    assert manifest["reviewed_records"][0]["reviewer"] == "Research reviewer"
    assert workbook.exists()


def test_unapproved_or_evidenceless_rows_never_promote(tmp_path):
    config = load_config(CATALOG)
    for status, evidence in [("IN REVIEW", "evidence"), ("APPROVED", "")]:
        workbook = tmp_path / f"{status.replace(' ', '-')}.xlsx"
        fixtures._workbook_fixture(workbook, status, evidence)
        with pytest.raises(ReviewWorkbookError):
            promote_review_workbook(tmp_path / "store", config, workbook, admitted_at=NOW)
    assert not list((tmp_path / "store" / "review").glob("[0-9a-f]*"))


def test_rejects_candidate_source_and_invalid_reviewed_date(tmp_path):
    config = load_config(CATALOG)
    row = {
        "Review status": "APPROVED",
        "Candidate ID": "candidate",
        "Approved interval ID": "i1",
        "Start date": "1935-01-03",
        "End date": "1937-01-03",
        "President ID": "p1",
        "President name": "Example",
        "President party": "D",
        "House control": "D",
        "Senate control": "D",
        "Controlling source ID": "HOUSE_GOV_001",
        "Primary source locator": "https://example.test",
        "Evidence note": "reviewed",
        "Reviewer": "researcher",
        "Reviewed at": "2026-11-01",
    }
    workbook = tmp_path / "book.xlsx"
    workbook.write_bytes(b"test")
    with patch(
        "market_data_pipeline.pipelines.presidential_research.review_workbook._review_rows",
        return_value=[row],
    ):
        with pytest.raises(ReviewWorkbookError, match="manual reviewed"):
            export_approved_controls(config, workbook, tmp_path / "controls.csv")
    row["Controlling source ID"] = "CTRL_074_A"
    row["Reviewed at"] = "not-a-date"
    with patch(
        "market_data_pipeline.pipelines.presidential_research.review_workbook._review_rows",
        return_value=[row],
    ):
        with pytest.raises(ReviewWorkbookError, match="reviewed"):
            export_approved_controls(config, workbook, tmp_path / "controls.csv")


def test_rejects_oversized_workbook_member_before_parsing(tmp_path):
    workbook = tmp_path / "large.xlsx"
    with ZipFile(workbook, "w", ZIP_DEFLATED) as archive:
        archive.writestr("xl/workbook.xml", b"x" * 200)
    with patch(
        "market_data_pipeline.pipelines.presidential_research.review_workbook.MAX_WORKBOOK_MEMBER_BYTES",
        100,
    ):
        with pytest.raises(ReviewWorkbookError, match="size"):
            export_approved_controls(load_config(CATALOG), workbook, tmp_path / "controls.csv")


def test_review_version_rejects_tampered_controls_and_invalid_id(tmp_path):
    from market_data_pipeline.pipelines.presidential_research.review import load_review_version

    workbook = tmp_path / "source.xlsx"
    fixtures._workbook_fixture(workbook, "APPROVED")
    root = tmp_path / "store"
    result = promote_review_workbook(root, load_config(CATALOG), workbook, admitted_at=NOW)
    receipt, controls, manifest = load_review_version(root, result["review_id"])
    assert receipt["approved_count"] == 1
    assert b"CTRL_074_A" in controls
    assert manifest["artifact_kind"] == "APPROVED_POLITICAL_CONTROLS"
    with pytest.raises(ValueError, match="ID"):
        load_review_version(root, "../../escape")
    (root / "review" / result["review_id"] / "controls.csv").write_bytes(b"modified")
    with pytest.raises(ValueError, match="integrity"):
        load_review_version(root, result["review_id"])


def test_promotion_rejects_overlap_and_invalid_source_locator(tmp_path):
    config = load_config(CATALOG)
    base = {
        "Review status": "APPROVED",
        "Candidate ID": "candidate",
        "Approved interval ID": "i1",
        "Start date": "1935-01-03",
        "End date": "1937-01-03",
        "President ID": "p1",
        "President name": "Example",
        "President party": "D",
        "House control": "D",
        "Senate control": "D",
        "Controlling source ID": "CTRL_074_A",
        "Primary source locator": "https://example.test",
        "Evidence note": "reviewed",
        "Reviewer": "researcher",
        "Reviewed at": "2026-11-01",
    }
    workbook = tmp_path / "book.xlsx"
    workbook.write_bytes(b"test")
    overlap = {**base, "Approved interval ID": "i2", "Start date": "1936-01-01"}
    with patch(
        "market_data_pipeline.pipelines.presidential_research.review_workbook._review_rows",
        return_value=[base, overlap],
    ):
        with pytest.raises(ReviewWorkbookError, match="overlap"):
            export_approved_controls(config, workbook, tmp_path / "controls.csv")
    invalid = {**base, "Primary source locator": "unverifiable text"}
    with patch(
        "market_data_pipeline.pipelines.presidential_research.review_workbook._review_rows",
        return_value=[invalid],
    ):
        with pytest.raises(ReviewWorkbookError, match="locator"):
            export_approved_controls(config, workbook, tmp_path / "controls.csv")


def test_promotion_never_overwrites_existing_controls(tmp_path):
    workbook = tmp_path / "source.xlsx"
    fixtures._workbook_fixture(workbook, "APPROVED")
    output = tmp_path / "controls.csv"
    output.write_text("existing controls")
    with pytest.raises(ReviewWorkbookError, match="already exists"):
        export_approved_controls(load_config(CATALOG), workbook, output)
    assert output.read_text() == "existing controls"
