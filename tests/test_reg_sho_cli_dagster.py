import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.reg_sho_thresholds.cli import main


def _config(tmp_path):
    value = yaml.safe_load(Path("config/reg_sho_thresholds/pipeline.example.yml").read_text())
    value["root"] = str(tmp_path / "root")
    value["partitions"] = {
        "fixture": {
            "provider": "nasdaq",
            "market": "NASDAQ",
            "list_date": "2026-11-03",
            "manifest": str(Path("manifests/reg-sho-nasdaq.yml").resolve()),
            "input": str(Path("tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt").resolve()),
            "source_url": (
                "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth20261103.txt"
            ),
        }
    }
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_threshold_cli_validate_health_plan_run_gold_and_export(tmp_path, capsys):
    path = _config(tmp_path)
    assert main(["validate-config", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["request_performed"] is False
    assert main(["health", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["request_performed"] is False
    assert main(["plan", "--config", str(path), "--provider", "nasdaq"]) == 0
    assert json.loads(capsys.readouterr().out)["partition_count"] == 1
    assert main(["run", "--config", str(path), "--partition", "fixture"]) == 0
    capsys.readouterr()
    assert main(["gold", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["row_count"] == 2
    destination = tmp_path / "exchange"
    assert (
        main(
            [
                "export-research",
                "--config",
                str(path),
                "--destination",
                str(destination),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["membership_count"] == 2


def test_threshold_dagster_definitions_are_stopped(monkeypatch):
    dg = pytest.importorskip("dagster")
    from market_data_pipeline.pipelines.reg_sho_thresholds.dagster_defs import defs, schedule

    dg.Definitions.validate_loadable(defs)
    assert defs.schedules[0].default_status == dg.DefaultScheduleStatus.STOPPED
    monkeypatch.delenv("REG_SHO_SCHEDULED_LIVE_ACCESS", raising=False)
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 4, 23, 30, tzinfo=UTC)
    )
    assert isinstance(schedule(context), dg.SkipReason)
