"""PR016-R05/R06/N02: one immutable, review-bound fixture publication."""

import io
from datetime import UTC, datetime
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import test_presidential_review_workbook as fixtures

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture
from market_data_pipeline.pipelines.presidential_research.publication import (
    publish_baseline,
    read_current,
    recover_publications,
)
from market_data_pipeline.pipelines.presidential_research.review import promote_review_workbook

NOW = datetime(2026, 11, 13, tzinfo=UTC)


def setup_fixture(tmp_path, *, factor="0.10"):
    catalog = tmp_path / "sources.toml"
    template = open("config/presidential_research/sources.toml", encoding="utf-8").read()
    catalog.write_text(template.replace("annual_end_year = 2023", "annual_end_year = 1936"))
    config = load_config(catalog)
    lines = [",Mkt-RF,SMB,HML,RF"]
    lines.extend(f"1935{month:02},{factor},0,0,0" for month in range(1, 13))
    lines.extend(f"1936{month:02},1.00,0,0,0" for month in range(1, 13))
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        archive.writestr("factors.csv", "\n".join(lines))
    root = tmp_path / "store"
    capture, outcome = import_local_capture(
        root,
        config,
        "FF_MKT_001",
        buffer.getvalue(),
        admitted_at=NOW,
        input_kind="SYNTHETIC_FIXTURE",
    )
    assert outcome["status"] == "PARSED"
    workbook = tmp_path / "review.xlsx"
    fixtures._workbook_fixture(workbook, "APPROVED")
    review = promote_review_workbook(
        root, config, workbook, admitted_at=NOW, evidence_kind="SYNTHETIC_FIXTURE"
    )
    return root, capture, review


def test_publishes_coherent_fictional_baseline_and_reuses_identity(tmp_path):
    root, capture, review = setup_fixture(tmp_path)
    out = publish_baseline(
        root, capture["capture_id"], review["review_id"], profile_id="fixture-v1", created_at=NOW
    )
    snapshot = read_current(root)
    assert snapshot["catalog"]["snapshotId"] == out["snapshotId"]
    assert snapshot["catalog"]["inputKind"] == "SYNTHETIC_FIXTURE"
    assert snapshot["dataset"]["title"].startswith("Fictional fixture")
    assert len(snapshot["dataset"]["windows"]) == 1
    assert all(item["locator"] is None for item in snapshot["dataset"]["sources"])
    same = publish_baseline(
        root,
        capture["capture_id"],
        review["review_id"],
        profile_id="fixture-v1",
        created_at=datetime(2026, 11, 14, tzinfo=UTC),
    )
    assert same["snapshotId"] == out["snapshotId"]
    assert read_current(root)["catalog"]["snapshotId"] == out["snapshotId"]


def test_tampered_dataset_or_raw_source_is_never_returned(tmp_path):
    root, capture, review = setup_fixture(tmp_path)
    out = publish_baseline(
        root, capture["capture_id"], review["review_id"], profile_id="fixture-v1", created_at=NOW
    )
    path = (
        root
        / "gold"
        / "snapshots"
        / out["snapshotId"]
        / "datasets"
        / "crsp-market-proxy-annual.json"
    )
    path.write_bytes(b"{}")
    with pytest.raises(ValueError, match="integrity"):
        read_current(root)
    (root / "bronze" / "FF_MKT_001" / capture["capture_id"] / "payload").write_bytes(b"changed")
    with pytest.raises(ValueError, match="digest"):
        publish_baseline(
            root,
            capture["capture_id"],
            review["review_id"],
            profile_id="fixture-v1",
            created_at=NOW,
        )


def test_rejects_fixture_inputs_under_historical_profile(tmp_path):
    root, capture, review = setup_fixture(tmp_path)
    with pytest.raises(ValueError, match="profile"):
        publish_baseline(
            root,
            capture["capture_id"],
            review["review_id"],
            profile_id="public-reconstruction-2026-11-03",
            created_at=NOW,
        )


@pytest.mark.parametrize("point", ["after_stage", "after_journal", "after_rename", "after_pointer"])
def test_interrupted_publication_retains_a_coherent_generation(tmp_path, point):
    root, capture, review = setup_fixture(tmp_path)
    first = publish_baseline(
        root, capture["capture_id"], review["review_id"], profile_id="fixture-v1", created_at=NOW
    )

    def fail(stage):
        if stage == point:
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError):
        publish_baseline(
            root,
            capture["capture_id"],
            review["review_id"],
            profile_id="fixture-v2",
            created_at=NOW,
            fault=fail,
        )
    current = read_current(root)
    assert len(current["dataset"]["windows"]) == 1
    if point != "after_pointer":
        assert current["catalog"]["snapshotId"] == first["snapshotId"]
    recover_publications(root)
    assert read_current(root)["catalog"]["snapshotId"] == current["catalog"]["snapshotId"]


def test_pointer_replace_failure_keeps_previous_snapshot(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.presidential_research import publication

    root, capture, review = setup_fixture(tmp_path)
    first = publish_baseline(
        root, capture["capture_id"], review["review_id"], profile_id="fixture-v1", created_at=NOW
    )
    original = publication.atomic_json

    def reject_pointer(path, value):
        if path.name == "current.json":
            raise OSError("injected pointer replacement failure")
        return original(path, value)

    monkeypatch.setattr(publication, "atomic_json", reject_pointer)
    with pytest.raises(OSError, match="injected"):
        publish_baseline(
            root,
            capture["capture_id"],
            review["review_id"],
            profile_id="fixture-v2",
            created_at=NOW,
        )
    assert read_current(root)["catalog"]["snapshotId"] == first["snapshotId"]


def test_corrupted_review_cannot_replace_previous_snapshot(tmp_path):
    root, capture, review = setup_fixture(tmp_path)
    first = publish_baseline(
        root, capture["capture_id"], review["review_id"], profile_id="fixture-v1", created_at=NOW
    )
    (root / "review" / review["review_id"] / "controls.csv").write_text("changed")
    with pytest.raises(ValueError, match="integrity"):
        publish_baseline(
            root,
            capture["capture_id"],
            review["review_id"],
            profile_id="fixture-v2",
            created_at=NOW,
        )
    assert read_current(root)["catalog"]["snapshotId"] == first["snapshotId"]


@pytest.mark.parametrize(
    "filename",
    [
        _
        for _ in (
            "crsp-market-proxy-annual.json",
            "crsp-market-proxy-annual.manifest.json",
            "catalog.json",
        )
    ],
)
def test_staged_file_write_failure_keeps_previous_snapshot(tmp_path, monkeypatch, filename):
    from market_data_pipeline.pipelines.presidential_research import publication

    root, capture, review = setup_fixture(tmp_path)
    first = publish_baseline(
        root, capture["capture_id"], review["review_id"], profile_id="fixture-v1", created_at=NOW
    )
    original = publication._write

    def interrupted(path, data):
        if path.name == filename:
            raise OSError("injected staging failure")
        return original(path, data)

    monkeypatch.setattr(publication, "_write", interrupted)
    with pytest.raises(OSError, match="injected"):
        publish_baseline(
            root,
            capture["capture_id"],
            review["review_id"],
            profile_id="fixture-v2",
            created_at=NOW,
        )
    assert read_current(root)["catalog"]["snapshotId"] == first["snapshotId"]


def test_unrepresentable_source_preserves_previous_snapshot(tmp_path):
    root, capture, review = setup_fixture(tmp_path)
    first = publish_baseline(
        root, capture["capture_id"], review["review_id"], profile_id="fixture-v1", created_at=NOW
    )
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        archive.writestr("factors.csv", ",Mkt-RF,SMB,HML,RF\n193601,0.1234567890123456789,0,0,0\n")
    bad, outcome = import_local_capture(
        root,
        load_config(tmp_path / "sources.toml"),
        "FF_MKT_001",
        buffer.getvalue(),
        admitted_at=NOW,
        input_kind="SYNTHETIC_FIXTURE",
    )
    assert outcome["status"] == "PARSED"
    with pytest.raises(ValueError, match="decimal128"):
        publish_baseline(
            root, bad["capture_id"], review["review_id"], profile_id="fixture-v2", created_at=NOW
        )
    assert read_current(root)["catalog"]["snapshotId"] == first["snapshotId"]
