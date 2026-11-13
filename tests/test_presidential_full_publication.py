"""PR016-R05/N02: one complete immutable generation or the previous one."""

import pytest
import test_presidential_bundle_assembly as fixtures

from market_data_pipeline.pipelines.presidential_research.publication import (
    publish_baseline,
    publish_full_bundle,
    read_current,
    recover_publications,
)

NOW = fixtures.NOW


def test_complete_bundle_roundtrip_and_immutable_identity(tmp_path, monkeypatch):
    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)
    result = publish_full_bundle(
        root, ids, review["review_id"], profile_id="fixture-complete", created_at=NOW
    )
    selection = read_current(root)
    assert selection["catalog"]["snapshotId"] == result["snapshotId"]
    assert len(selection["datasets"]) == 5
    assert len(selection["artifacts"]) == 7
    assert [len(item["dataset"]["windows"]) for item in selection["datasets"]] == [3, 2, 2, 2, 2]
    assert selection["catalog"]["inputKind"] == "SYNTHETIC_FIXTURE"
    assert (
        publish_full_bundle(
            root, ids, review["review_id"], profile_id="fixture-complete", created_at=NOW
        )["snapshotId"]
        == result["snapshotId"]
    )
    evidence = (
        root
        / "gold"
        / "snapshots"
        / result["snapshotId"]
        / "evidence"
        / "presidential-cycle-ledger.csv"
    )
    evidence.write_bytes(b"changed")
    with pytest.raises(ValueError, match="integrity"):
        read_current(root)


@pytest.mark.parametrize("point", ["after_stage", "after_journal", "after_rename", "after_pointer"])
def test_interrupted_complete_publish_preserves_a_coherent_selection(tmp_path, monkeypatch, point):
    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)
    first = publish_baseline(
        root, ids["FF_MKT_001"], review["review_id"], profile_id="fixture-baseline", created_at=NOW
    )

    def fail(stage):
        if stage == point:
            raise RuntimeError("injected")

    with pytest.raises(RuntimeError, match="injected"):
        publish_full_bundle(
            root,
            ids,
            review["review_id"],
            profile_id="fixture-complete",
            created_at=NOW,
            fault=fail,
        )
    current = read_current(root)
    if point != "after_pointer":
        assert current["catalog"]["snapshotId"] == first["snapshotId"]
        assert len(current["datasets"]) == 1
    else:
        assert len(current["datasets"]) == 5
    recover_publications(root)
    assert read_current(root)["catalog"]["snapshotId"] == current["catalog"]["snapshotId"]


def test_missing_required_artifact_cannot_replace_previous_snapshot(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.presidential_research import publication

    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)
    first = publish_baseline(
        root, ids["FF_MKT_001"], review["review_id"], profile_id="fixture-baseline", created_at=NOW
    )
    original = publication.build_full_bundle

    def incomplete(*args, **kwargs):
        bundle = original(*args, **kwargs)
        del bundle["artifacts"]["presidential-cycle-summary.json"]
        return bundle

    monkeypatch.setattr(publication, "build_full_bundle", incomplete)
    with pytest.raises(ValueError, match="Required artifact"):
        publish_full_bundle(
            root, ids, review["review_id"], profile_id="fixture-complete", created_at=NOW
        )
    assert read_current(root)["catalog"]["snapshotId"] == first["snapshotId"]


def test_recovery_finishes_sqlite_publication_journal(tmp_path, monkeypatch):
    import sqlite3

    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)

    def fail(stage):
        if stage == "after_pointer":
            raise RuntimeError("injected")

    with pytest.raises(RuntimeError, match="injected"):
        publish_full_bundle(
            root,
            ids,
            review["review_id"],
            profile_id="fixture-complete",
            created_at=NOW,
            fault=fail,
        )
    identity = read_current(root)["catalog"]["snapshotId"]
    with sqlite3.connect(root / "silver" / "presidential.sqlite3") as db:
        assert db.execute(
            "SELECT status FROM publication_journal WHERE snapshot_id=?", (identity,)
        ).fetchone() == ("PENDING",)
    recover_publications(root)
    with sqlite3.connect(root / "silver" / "presidential.sqlite3") as db:
        assert db.execute(
            "SELECT status FROM publication_journal WHERE snapshot_id=?", (identity,)
        ).fetchone() == ("PUBLISHED",)
