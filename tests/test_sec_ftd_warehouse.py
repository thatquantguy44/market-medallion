"""T04 root binding, allocation and durable source-owned SQLite state."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import SQLiteWarehouse

SETTINGS = load_config(Path("config/sec_fails_to_deliver/pipeline.example.yml")).storage
CLOCK = datetime(2026, 11, 3, 9, tzinfo=UTC)


def started(sequence=1, run_id="a" * 32):
    return dict(
        source="sec",
        dataset="sec-fails-to-deliver",
        reporting_regime="semi-monthly-2009+",
        run_id=run_id,
        capture_sequence=sequence,
        captured_at=CLOCK,
        completed_at=None,
        status="STARTED",
        row_count=0,
        raw_sha256=None,
        error_code=None,
    )


def test_root_and_database_identity_survive_reopen(tmp_path):
    root = tmp_path / "warehouse"
    with SQLiteWarehouse(root, SETTINGS) as warehouse:
        identity = warehouse.identity
        assert warehouse.select_facts() == []
        assert warehouse.select_coverage() == []
    with SQLiteWarehouse(root, SETTINGS) as reopened:
        assert reopened.identity == identity
        assert reopened.select_runs() == []
    assert json.loads((root / ".warehouse.json").read_text()) == identity


def test_incompatible_contract_and_namespace_are_rejected_before_writes(tmp_path):
    root = tmp_path / "warehouse"
    with SQLiteWarehouse(root, SETTINGS):
        pass
    before = (root / "warehouse.sqlite3").read_bytes()
    with pytest.raises(ValueError, match="contract"):
        with SQLiteWarehouse(root, replace(SETTINGS, price_scale=6)):
            pass
    identity = json.loads((root / ".warehouse.json").read_text())
    identity["dataset"] = "aaii-investor-sentiment"
    (root / ".warehouse.json").write_text(json.dumps(identity))
    with pytest.raises(ValueError):
        with SQLiteWarehouse(root, SETTINGS):
            pass
    assert (root / "warehouse.sqlite3").read_bytes() == before


def test_nonempty_unbound_root_and_missing_database_are_not_silently_recreated(tmp_path):
    root = tmp_path / "unbound"
    root.mkdir()
    (root / "user.txt").write_text("keep")
    with pytest.raises(ValueError):
        with SQLiteWarehouse(root, SETTINGS):
            pass
    assert not (root / "warehouse.sqlite3").exists()
    second = tmp_path / "bound"
    with SQLiteWarehouse(second, SETTINGS):
        pass
    (second / "warehouse.sqlite3").unlink()
    with pytest.raises(ValueError):
        with SQLiteWarehouse(second, SETTINGS):
            pass
    assert not (second / "warehouse.sqlite3").exists()


def test_sequence_and_started_audit_commit_atomically_and_rollback(tmp_path):
    root = tmp_path / "warehouse"
    with SQLiteWarehouse(root, SETTINGS) as warehouse:
        with warehouse.transaction():
            assert warehouse.next_capture_sequence() == 1
            warehouse.record_run(started())
        with pytest.raises(RuntimeError):
            with warehouse.transaction():
                assert warehouse.next_capture_sequence() == 2
                warehouse.record_run(started(2, "b" * 32))
                raise RuntimeError("injected transaction failure")
        assert [row["capture_sequence"] for row in warehouse.select_runs()] == [1]
        with warehouse.transaction():
            assert warehouse.next_capture_sequence() == 2
            warehouse.record_run(started(2, "c" * 32))
    with SQLiteWarehouse(root, SETTINGS) as warehouse:
        assert [row["capture_sequence"] for row in warehouse.select_runs()] == [1, 2]


def test_binding_marker_cannot_disagree_with_database_identity(tmp_path):
    with SQLiteWarehouse(tmp_path, SETTINGS):
        pass
    marker = tmp_path / ".warehouse.json"
    identity = json.loads(marker.read_text())
    identity["warehouse_id"] = "different"
    marker.write_text(json.dumps(identity))
    with pytest.raises(ValueError):
        with SQLiteWarehouse(tmp_path, SETTINGS):
            pass
