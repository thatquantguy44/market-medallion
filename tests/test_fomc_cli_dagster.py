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
    assert failure.error.cause is None
    assert failure.user_failure_data.metadata["error_code"].value == "preflight_failed"


@pytest.mark.parametrize("name", ["dagster-fomc-refresh.yml", "dagster-fomc-backfill.yml"])
def test_launch_examples_validate_and_require_explicit_live_access(name, monkeypatch):
    dg = pytest.importorskip("dagster")
    from market_data_pipeline.pipelines.fomc_meeting_minutes import dagster_defs as module

    job = module.defs.resolve_job_def("fomc_meeting_minutes_job")
    value = yaml.safe_load((Path("resources") / name).read_text())
    dg.validate_run_config(job, value)
    config = value["ops"]["fomc_meeting_minutes_refresh"]["config"]
    assert config["live"] is False and config["scheduled"] is False
    if "refresh" in name:
        assert config["recent_years"] is True and config["mode"] == "refresh"
        assert "start_year" not in config and "end_year" not in config
    else:
        assert (config["start_year"], config["end_year"], config["mode"]) == (
            1995,
            1996,
            "backfill",
        )
    config.update(
        config_path="config/fomc_meeting_minutes/pipeline.example.yml",
        operations_path="config/fomc_meeting_minutes/operations.example.yml",
    )
    calls = []

    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError("database or provider accessed")

    monkeypatch.setattr(module, "PostgresInventoryStore", forbidden)
    monkeypatch.setattr("market_data_pipeline.shared.http.prepare_http", forbidden)
    result = job.execute_in_process(run_config=value, raise_on_error=False)
    assert not result.success and calls == []
    assert not result.asset_materializations_for_node("fomc_meeting_minutes_refresh")


@pytest.mark.parametrize(
    "tick,expected",
    [
        (datetime(2027, 1, 1, 0, 30, tzinfo=UTC), (2025, 2026)),
        (datetime(2027, 1, 1, 5, tzinfo=UTC), (2026, 2027)),
    ],
)
def test_schedule_and_manual_refresh_share_new_york_year_policy(
    tmp_path, monkeypatch, tick, expected
):
    dg = pytest.importorskip("dagster")
    from market_data_pipeline.pipelines.fomc_meeting_minutes import dagster_defs as module

    operations = yaml.safe_load(
        Path("config/fomc_meeting_minutes/operations.example.yml").read_text()
    )
    operations["scheduled_live_access"] = True
    path = tmp_path / "operations.yml"
    path.write_text(yaml.safe_dump(operations))
    monkeypatch.setenv("FOMC_OPERATIONS_CONFIG", str(path))
    monkeypatch.setenv(
        "FOMC_CONFIG", str(Path("config/fomc_meeting_minutes/pipeline.example.yml").resolve())
    )
    context = dg.build_schedule_context(scheduled_execution_time=tick)
    request = module.schedule(context)
    assert isinstance(request, dg.RunRequest)
    cfg = request.run_config["ops"]["fomc_meeting_minutes_refresh"]["config"]
    assert (cfg["start_year"], cfg["end_year"]) == expected
    assert cfg["live"] is cfg["scheduled"] is True
    assert module.schedule.default_status == dg.DefaultScheduleStatus.STOPPED


@pytest.mark.parametrize("invalid", ["future_floor", "missing_tick", "bad_yaml"])
def test_schedule_invalid_state_skips_without_service_access(tmp_path, monkeypatch, invalid):
    dg = pytest.importorskip("dagster")
    from market_data_pipeline.pipelines.fomc_meeting_minutes import dagster_defs as module

    operations = yaml.safe_load(
        Path("config/fomc_meeting_minutes/operations.example.yml").read_text()
    )
    operations["scheduled_live_access"] = True
    path = tmp_path / "operations.yml"
    path.write_text(yaml.safe_dump(operations))
    pipeline = yaml.safe_load(Path("config/fomc_meeting_minutes/pipeline.example.yml").read_text())
    pipeline["manifest"] = str(Path("manifests/fomc-meeting-minutes.yml").resolve())
    if invalid == "future_floor":
        pipeline["start_year"] = 2028
    cfg_path = tmp_path / "pipeline.yml"
    cfg_path.write_text(yaml.safe_dump(pipeline))
    if invalid == "bad_yaml":
        path.write_text("secret: [malformed")
    monkeypatch.setenv("FOMC_CONFIG", str(cfg_path))
    monkeypatch.setenv("FOMC_OPERATIONS_CONFIG", str(path))
    calls = []

    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError("service accessed")

    monkeypatch.setattr(module, "PostgresInventoryStore", forbidden)
    monkeypatch.setattr("market_data_pipeline.shared.http.prepare_http", forbidden)
    tick = None if invalid == "missing_tick" else datetime(2026, 7, 1, tzinfo=UTC)
    assert isinstance(
        module.schedule(dg.build_schedule_context(scheduled_execution_time=tick)), dg.SkipReason
    )
    assert calls == []
