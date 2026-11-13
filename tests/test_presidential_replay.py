"""PR016-R04/R13: isolated replay preserves selected identity and terminal outcomes."""

import json
import sqlite3
from datetime import timedelta
from unittest.mock import patch

import pytest
import test_presidential_bundle_assembly as fixtures

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture
from market_data_pipeline.pipelines.presidential_research.publication import (
    publish_full_bundle,
    read_current,
)
from market_data_pipeline.pipelines.presidential_research.replay import replay_selected_snapshot
from market_data_pipeline.pipelines.presidential_research.warehouse import (
    ingest_capture,
    observed_as_of,
)

NOW = fixtures.NOW


def source(tmp_path, monkeypatch):
    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)
    published = publish_full_bundle(
        root, ids, review["review_id"], profile_id="fixture-complete", created_at=NOW
    )
    bad, outcome = import_local_capture(
        root,
        load_config(tmp_path / "sources.toml"),
        "FEDFUNDS_001",
        b"observation_date,FEDFUNDS\n1939-01-01,NaN\n",
        admitted_at=NOW + timedelta(seconds=1),
        input_kind="SYNTHETIC_FIXTURE",
    )
    assert outcome["status"] == "REJECTED"
    assert ingest_capture(root, "FEDFUNDS_001", bad["capture_id"])["status"] == "REJECTED"
    corrected, _ = import_local_capture(
        root,
        load_config(tmp_path / "sources.toml"),
        "FEDFUNDS_001",
        b"observation_date,FEDFUNDS\n1935-10-01,8.00\n",
        admitted_at=NOW + timedelta(seconds=2),
        input_kind="SYNTHETIC_FIXTURE",
    )
    assert ingest_capture(root, "FEDFUNDS_001", corrected["capture_id"])["status"] == "PARSED"
    return root, published, bad


def test_replays_to_fresh_root_without_network_and_preserves_clocks(tmp_path, monkeypatch):
    root, published, bad = source(tmp_path, monkeypatch)
    destination = tmp_path / "restored"
    with patch("socket.socket", side_effect=AssertionError("network is forbidden")):
        result = replay_selected_snapshot(root, destination, replayed_at=NOW + timedelta(hours=1))
    assert result["snapshotId"] == published["snapshotId"]
    assert read_current(destination)["catalogSha256"] == read_current(root)["catalogSha256"]
    assert observed_as_of(destination, NOW) == observed_as_of(root, NOW)
    assert observed_as_of(destination, NOW + timedelta(seconds=2)) == observed_as_of(
        root, NOW + timedelta(seconds=2)
    )
    with sqlite3.connect(destination / "silver" / "presidential.sqlite3") as db:
        assert db.execute(
            "SELECT status FROM captures WHERE capture_id=?", (bad["capture_id"],)
        ).fetchone() == ("REJECTED",)
    receipt = json.loads((destination / "audit" / "replay-receipt.json").read_text())
    assert receipt["destinationAdmittedAt"] == "2026-11-13T01:00:00.000000Z"
    assert receipt["snapshotId"] == published["snapshotId"]


def test_corrupt_input_and_existing_destination_fail_without_replacing_data(tmp_path, monkeypatch):
    root, _, bad = source(tmp_path, monkeypatch)
    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "keep").write_text("keep")
    with pytest.raises(ValueError, match="destination"):
        replay_selected_snapshot(root, existing, replayed_at=NOW + timedelta(hours=1))
    assert (existing / "keep").read_text() == "keep"
    (root / "bronze" / "FEDFUNDS_001" / bad["capture_id"] / "payload").write_bytes(b"changed")
    destination = tmp_path / "restored"
    with pytest.raises(ValueError, match="digest"):
        replay_selected_snapshot(root, destination, replayed_at=NOW + timedelta(hours=1))
    assert not destination.exists()


def test_parse_outcome_tampering_and_backdated_replay_are_blocked(tmp_path, monkeypatch):
    root, _, bad = source(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="admission"):
        replay_selected_snapshot(root, tmp_path / "too-early", replayed_at=NOW)
    parse_path = root / "bronze" / "FEDFUNDS_001" / bad["capture_id"] / "parse.json"
    parse = json.loads(parse_path.read_text())
    parse["status"] = "PARSED"
    parse_path.write_text(json.dumps(parse))
    with pytest.raises(ValueError, match="parse receipt"):
        replay_selected_snapshot(root, tmp_path / "tampered", replayed_at=NOW + timedelta(hours=1))
    assert not (tmp_path / "tampered").exists()


def test_changed_nonselected_audit_mapping_is_not_silently_repaired(tmp_path, monkeypatch):
    root, _, _ = source(tmp_path, monkeypatch)
    with sqlite3.connect(root / "silver" / "presidential.sqlite3") as db:
        latest = db.execute(
            "SELECT capture_id FROM captures WHERE status='PARSED' "
            "ORDER BY capture_sequence DESC LIMIT 1"
        ).fetchone()[0]
        db.execute("DELETE FROM capture_facts WHERE capture_id=?", (latest,))
    destination = tmp_path / "invalid-audit"
    with pytest.raises(ValueError, match="audit records"):
        replay_selected_snapshot(root, destination, replayed_at=NOW + timedelta(hours=1))
    assert not destination.exists()
