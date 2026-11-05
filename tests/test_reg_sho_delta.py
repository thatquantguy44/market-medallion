"""S11-T22/A15 threshold-owned snapshots and coherent Delta generations."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from test_reg_sho_pipeline import _config

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.delta import (
    DeltaPathTarget,
    publish_configured_delta,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.delta_snapshot import (
    stage_delta_snapshot,
    validate_snapshot,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import run

CLOCK = datetime(2026, 11, 5, 12, 0, 0, 123456, tzinfo=UTC)


def test_staging_preserves_history_and_reuses_identical_terminal_state(tmp_path):
    config, source = _config(tmp_path)
    run(config, "fixture", clock=lambda: CLOCK)
    first = stage_delta_snapshot(config)
    assert stage_delta_snapshot(config) == first
    original = source.read_bytes()
    source.write_bytes(original.replace(b"ALFA INVENTED CORPORATION", b"ALFA REVISED"))
    run(config, "fixture", clock=lambda: CLOCK)
    second = stage_delta_snapshot(config)
    assert second != first
    manifest, tables = validate_snapshot(load_config(config), second)
    assert manifest["capture_sequence"] == 2
    assert manifest["calendar_basis"] == "configured-and-observed-list-dates/1"
    assert {name: table.num_rows for name, table in tables.items()} == {
        "membership": 3,
        "coverage": 2,
        "episodes": 2,
        "audit": 2,
    }
    assert {row["capture_sequence"] for row in tables["membership"].to_pylist()} == {1, 2}
    assert all(row["first_seen_at"] == CLOCK for row in tables["membership"].to_pylist())
    assert pq.read_table(first / "membership.parquet").num_rows == 2


def test_snapshot_checksum_and_source_identity_are_enforced(tmp_path):
    config, _source = _config(tmp_path)
    run(config, "fixture", clock=lambda: CLOCK)
    snapshot = stage_delta_snapshot(config)
    payload = snapshot / "membership.parquet"
    payload.write_bytes(payload.read_bytes() + b"corruption")
    with pytest.raises(ValueError, match="checksum"):
        validate_snapshot(load_config(config), snapshot)
    other = tmp_path / "other"
    other.mkdir()
    other_config, _ = _config(other)
    with pytest.raises(ValueError, match="source|snapshot"):
        validate_snapshot(load_config(other_config), snapshot)


def test_target_and_config_require_explicit_safe_binding(tmp_path):
    config, _ = _config(tmp_path)
    assert not load_config(config).databricks.enabled
    with pytest.raises(ValueError, match="disabled"):
        publish_configured_delta(None, config)
    with pytest.raises(ValueError, match="prefix"):
        DeltaPathTarget(tmp_path / "delta", "bad-prefix", "0" * 64)
    target = tmp_path / "occupied"
    target.mkdir()
    (target / "unrelated.txt").write_text("keep")
    with pytest.raises(ValueError, match="nonempty"):
        DeltaPathTarget(target, "reg_sho", "0" * 64).bind()


def test_empty_and_failed_snapshots_retain_explicit_schemas(tmp_path):
    config, source = _config(tmp_path, "nasdaq-empty.txt")
    source.write_bytes(source.read_bytes().replace(b"20261104", b"20261103"))
    run(config, "fixture", clock=lambda: CLOCK)
    first = stage_delta_snapshot(config)
    _manifest, tables = validate_snapshot(load_config(config), first)
    assert tables["membership"].num_rows == tables["episodes"].num_rows == 0
    assert tables["coverage"].to_pylist()[0]["coverage_state"] == "COMPLETE_EMPTY"
    source.unlink()
    run(config, "fixture", clock=lambda: CLOCK)
    second = stage_delta_snapshot(config)
    _manifest, tables = validate_snapshot(load_config(config), second)
    assert tables["coverage"].num_rows == 2
    assert tables["coverage"].to_pylist()[-1]["coverage_state"] == "UNAVAILABLE"
    assert tables["membership"].num_rows == tables["episodes"].num_rows == 0
    with pytest.raises(ValueError, match="max_rows"):
        stage_delta_snapshot(config, max_rows=1)


def test_staging_rejects_missing_and_unfinished_warehouses(tmp_path):
    from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import warehouse_for

    config, _ = _config(tmp_path)
    with pytest.raises(ValueError, match="does not exist"):
        stage_delta_snapshot(config)
    with warehouse_for(load_config(config)) as warehouse:
        with warehouse.transaction():
            sequence = warehouse.next_capture_sequence()
            warehouse.record_run(
                {
                    "dataset": "reg-sho-thresholds",
                    "provider": "nasdaq",
                    "market": "NASDAQ",
                    "run_id": "unfinished",
                    "capture_sequence": sequence,
                    "captured_at": CLOCK,
                    "completed_at": None,
                    "status": "STARTED",
                    "row_count": 0,
                    "raw_sha256": None,
                    "error_code": None,
                }
            )
    with pytest.raises(ValueError, match="terminal"):
        stage_delta_snapshot(config)


def test_staging_cli_and_pointer_are_no_request(tmp_path, capsys):
    from market_data_pipeline.pipelines.reg_sho_thresholds.cli import main
    from market_data_pipeline.pipelines.reg_sho_thresholds.delta_snapshot import current_snapshot

    config, _ = _config(tmp_path)
    run(config, "fixture", clock=lambda: CLOCK)
    assert main(["stage-delta", "--config", str(config)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["request_performed"] is False
    assert Path(result["snapshot"]) == current_snapshot(load_config(config).root)
    pointer = load_config(config).root / "delta-staging" / "current.json"
    value = json.loads(pointer.read_text())
    value["manifest_sha256"] = "0" * 64
    pointer.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="checksum"):
        current_snapshot(load_config(config).root)


def test_snapshot_identity_is_independent_of_database_tie_order(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import SQLiteWarehouse

    config, _ = _config(tmp_path)
    run(config, "fixture", clock=lambda: CLOCK)
    first = stage_delta_snapshot(config)
    select_all = SQLiteWarehouse.select_all
    monkeypatch.setattr(
        SQLiteWarehouse, "select_all", lambda self, name: list(reversed(select_all(self, name)))
    )
    assert stage_delta_snapshot(config) == first


def test_configured_publication_requires_snapshot_inside_volume(tmp_path):
    import yaml

    config, _ = _config(tmp_path)
    run(config, "fixture", clock=lambda: CLOCK)
    snapshot = stage_delta_snapshot(config)
    value = yaml.safe_load(config.read_text())
    value["databricks"] = {
        "enabled": True,
        "catalog": "research",
        "schema": "thresholds",
        "staging_volume": "/Volumes/research/thresholds/data",
        "table_prefix": "reg_sho",
    }
    config.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError, match="inside.*volume"):
        publish_configured_delta(None, config, snapshot)
