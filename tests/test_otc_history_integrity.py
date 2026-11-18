"""OTC-AC07 independent history/lineage corruption and fail-closed read checks."""

import json
import sqlite3
from copy import deepcopy

import pytest
from test_otc_bronze import USE
from test_otc_migration import restore_legacy, selection
from test_otc_pipeline import PROFILES, partition, payload, run

from market_data_pipeline.pipelines.finra_otc_transparency import pipeline
from market_data_pipeline.pipelines.finra_otc_transparency.data.versions import (
    VERSION,
    verify_version,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest


def inspect(root):
    return pipeline.inspect_panel(root, partitions=[partition(PROFILES[0])], approved_use=USE)


def test_membership_cannot_reference_a_version_whose_origin_attempt_is_not_published(tmp_path):
    root = tmp_path / "panel"
    first = run(root)
    run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        db.execute(
            "UPDATE otc2_attempts SET status='FAILED' WHERE capture_id=?", (first["capture_id"],)
        )
    with pytest.raises(ValueError):
        inspect(root)


def test_v2_attempt_index_clock_must_match_its_immutable_capture_context(tmp_path):
    root = tmp_path / "panel"
    run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        db.execute("UPDATE otc2_attempts SET captured_at='2099-01-01T00:00:00.000000Z'")
    with pytest.raises(ValueError):
        inspect(root)


def test_missing_unreferenced_removal_occurrence_is_detected_by_history_read(tmp_path):
    root = tmp_path / "panel"
    run(root)
    raw = payload(PROFILES[0])
    run(root, payload=raw.splitlines()[0] + b"\n", expected_rows=0)
    run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        for version_id, record in db.execute(
            "SELECT version_id,record_json FROM otc2_versions"
        ).fetchall():
            if json.loads(record)["state"] == "REMOVED":
                db.execute("DELETE FROM otc2_versions WHERE version_id=?", (version_id,))
    with pytest.raises(ValueError):
        pipeline.observation_history(root, partitions=[partition(PROFILES[0])], approved_use=USE)


def test_migrated_legacy_attempts_remain_integrity_checked_after_v2_processing(tmp_path):
    root = tmp_path / "legacy"
    restore_legacy(root)
    pipeline.migrate_warehouse(root, approved_use=USE)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        db.execute("UPDATE otc_attempts SET status='PUBLISHED' WHERE status='REJECTED'")
    with pytest.raises(ValueError):
        pipeline.inspect_panel(root, partitions=selection(), approved_use=USE)


@pytest.mark.parametrize("mutation", ["state", "content_hash", "first_seen"])
def test_resealed_semantically_inconsistent_versions_are_refused(tmp_path, mutation):
    root = tmp_path / "panel"
    run(root)
    version = deepcopy(
        pipeline.observation_history(root, partitions=[partition(PROFILES[0])], approved_use=USE)[
            "versions"
        ][0]
    )
    version.pop("version_sha256")
    version.pop("version_id")
    if mutation == "state":
        version["state"] = "REMOVED"
    elif mutation == "content_hash":
        version["content_sha256"] = "0" * 64
    else:
        version["first_seen_at"] = "1990-01-01T00:00:00.000000Z"
    version["version_id"] = VERSION + ":" + digest(version)
    version["version_sha256"] = digest(version)
    with pytest.raises(ValueError):
        verify_version(version)


def test_resealed_snapshot_missing_required_coverage_fails_as_contract_error(tmp_path):
    root = tmp_path / "panel"
    run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        capture, raw = db.execute("SELECT capture_id,record_json FROM otc2_snapshots").fetchone()
        record = json.loads(raw)
        record.pop("coverage")
        from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import encoded

        db.execute(
            "UPDATE otc2_snapshots SET record_json=?,record_sha256=? WHERE capture_id=?",
            (encoded(record).decode(), digest(record), capture),
        )
    with pytest.raises(ValueError):
        inspect(root)


def test_resealed_version_chain_cannot_skip_an_intermediate_correction(tmp_path):
    from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import encoded

    root = tmp_path / "panel"
    run(root)
    run(root, payload=payload(PROFILES[0]).replace(b"12345678901234567890", b"7"))
    run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        candidates = [
            (identity, json.loads(record))
            for identity, record in db.execute(
                "SELECT version_id,record_json FROM otc2_versions ORDER BY capture_sequence"
            )
        ]
        final_id, record = candidates[-1]
        original = next(
            v
            for _, v in candidates
            if v["symbol_raw"] == record["symbol_raw"]
            and v["participant_key"] == record["participant_key"]
        )
        record.pop("version_id")
        record.pop("version_sha256")
        record["previous_version_id"] = original["version_id"]
        record["version_id"] = VERSION + ":" + digest(record)
        record["version_sha256"] = digest(record)
        db.execute(
            "UPDATE otc2_versions SET version_id=?,record_json=? WHERE version_id=?",
            (record["version_id"], encoded(record).decode(), final_id),
        )
        db.execute(
            "UPDATE otc2_members SET version_id=? WHERE version_id=?",
            (record["version_id"], final_id),
        )
    with pytest.raises(ValueError):
        pipeline.observation_history(root, partitions=[partition(PROFILES[0])], approved_use=USE)
