"""FOMC-AC06: source entrypoints, local gates and stopped orchestration."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.fomc_meeting_minutes.cli import main


def test_validation_and_closed_live_gate_require_no_service_or_probe(monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        raise AssertionError("service or HTTP accessed")

    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.cli.PostgresInventoryStore", forbidden
    )
    monkeypatch.setattr("market_data_pipeline.shared.http.prepare_http", forbidden)
    args = [
        "--config",
        "config/fomc_meeting_minutes/pipeline.example.yml",
        "--operations",
        "config/fomc_meeting_minutes/operations.example.yml",
    ]
    assert main(["validate-config", *args]) == 0
    assert json.loads(capsys.readouterr().out)["request_performed"] is False
    assert main(["run", *args, "--start-year", "1990", "--end-year", "1990"]) == 2
    assert json.loads(capsys.readouterr().out)["request_performed"] is False
    from market_data_pipeline.cli import main as root_main

    assert root_main(["fomc", "validate-config", *args]) == 0
    capsys.readouterr()


def test_dagster_definition_is_loadable_and_schedule_requires_explicit_access(
    tmp_path, monkeypatch
):
    dg = pytest.importorskip("dagster")
    from market_data_pipeline.pipelines.fomc_meeting_minutes.dagster_defs import defs, schedule

    dg.Definitions.validate_loadable(defs)
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    for name in ["FOMC_CONFIG", "FOMC_OPERATIONS_CONFIG"]:
        monkeypatch.delenv(name, raising=False)
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 12, 22, tzinfo=UTC)
    )
    assert isinstance(schedule(context), dg.SkipReason)
    path = tmp_path / "operations.yml"
    value = yaml.safe_load(Path("config/fomc_meeting_minutes/operations.example.yml").read_text())
    path.write_text(yaml.safe_dump(value))
    monkeypatch.setenv(
        "FOMC_CONFIG", str(Path("config/fomc_meeting_minutes/pipeline.example.yml").resolve())
    )
    monkeypatch.setenv("FOMC_OPERATIONS_CONFIG", str(path))
    assert isinstance(schedule(context), dg.SkipReason)
    value["scheduled_live_access"] = True
    path.write_text(yaml.safe_dump(value))
    request = schedule(context)
    assert isinstance(request, dg.RunRequest)
    config = request.run_config["ops"]["fomc_meeting_minutes_refresh"]["config"]
    assert (config["start_year"], config["end_year"], config["mode"]) == (2025, 2026, "refresh")
    assert config["scheduled"] is True and config["live"] is True


@pytest.mark.parametrize("live,scheduled", [(False, False), (True, True)])
def test_manual_job_rechecks_access_before_service_or_probe(monkeypatch, live, scheduled):
    pytest.importorskip("dagster")
    from market_data_pipeline.pipelines.fomc_meeting_minutes.dagster_defs import defs

    def forbidden(*args, **kwargs):
        raise AssertionError("service or HTTP accessed")

    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.dagster_defs.PostgresInventoryStore",
        forbidden,
    )
    monkeypatch.setattr("market_data_pipeline.shared.http.prepare_http", forbidden)
    result = defs.resolve_job_def("fomc_meeting_minutes_job").execute_in_process(
        run_config={
            "ops": {
                "fomc_meeting_minutes_refresh": {
                    "config": {
                        "config_path": "config/fomc_meeting_minutes/pipeline.example.yml",
                        "operations_path": "config/fomc_meeting_minutes/operations.example.yml",
                        "live": live,
                        "scheduled": scheduled,
                    }
                }
            }
        },
        raise_on_error=False,
    )
    assert not result.success
    assert not result.asset_materializations_for_node("fomc_meeting_minutes_refresh")
    failure = result.failure_data_for_node("fomc_meeting_minutes_refresh")
    assert "live/scheduled access gate is closed" in failure.error.cause.message
