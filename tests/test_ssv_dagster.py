"""SSV-AC15: controllable stopped orchestration, no implicit provider requests."""

from datetime import UTC, datetime

import pytest
import yaml
from test_ssv_parser import manifest as manifest
from test_ssv_source import ENV, Script, config, data_reply

pytest.importorskip("dagster")
import dagster as dg

from market_data_pipeline.pipelines.finra_short_sale_volume import dagster_defs as module
from market_data_pipeline.pipelines.finra_short_sale_volume import research_export

__all__ = ["manifest"]
ASSET = "finra_short_sale_volume"


def execute(**changes):
    settings = dict(
        config_path="invented.yml",
        start_date="2026-11-09",
        end_date="2026-11-09",
        scopes=["nyse_trf"],
        overlap_days=0,
        live=True,
    )
    return module.defs.resolve_job_def("finra_short_sale_volume_job").execute_in_process(
        run_config={"ops": {ASSET: {"config": settings | changes}}}, raise_on_error=False
    )


def test_ssv_ac15_definitions_composition_and_stopped_defaults(monkeypatch):
    from market_data_pipeline.orchestration.dagster_defs import defs

    dg.Definitions.validate_loadable(defs)
    assert defs.resolve_job_def("finra_short_sale_volume_job")
    assert module.schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert module.schedule.execution_timezone == "America/New_York"
    assert all(s.default_status == dg.DefaultSensorStatus.STOPPED for s in module.defs.sensors)
    monkeypatch.delenv("FINRA_SSV_SCHEDULED_LIVE_ACCESS", raising=False)
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 10, 14, 15, tzinfo=UTC)
    )
    assert isinstance(module.schedule(context), dg.SkipReason)


@pytest.mark.parametrize(
    "changes,environment",
    [({"live": False}, "true"), ({}, None), ({}, "false"), ({"scheduled": True}, "true")],
)
def test_ssv_ac15_gates_refuse_before_configuration_or_http(monkeypatch, changes, environment):
    monkeypatch.delenv("FINRA_SSV_SCHEDULED_LIVE_ACCESS", raising=False)
    if environment is None:
        monkeypatch.delenv("FINRA_SSV_LIVE_ACCESS", raising=False)
    else:
        monkeypatch.setenv("FINRA_SSV_LIVE_ACCESS", environment)
    monkeypatch.setattr(module, "load_config", lambda *a: pytest.fail("config read"))
    result = execute(**changes)
    assert not result.success
    assert not any(e.event_type_value == "STEP_UP_FOR_RETRY" for e in result.all_events)
    assert sum(e.event_type_value == "STEP_START" for e in result.all_events) == 1


@pytest.mark.parametrize(
    "tick,expected",
    [
        ("2026-11-10T14:15:00+00:00", "2026-11-09"),
        ("2026-11-16T14:15:00+00:00", "2026-11-13"),
        ("2026-11-27T14:15:00+00:00", "2026-11-25"),
    ],
)
def test_ssv_ac15_schedule_bounds_previous_reporting_day(monkeypatch, tick, expected):
    for key, value in dict(
        FINRA_SSV_LIVE_ACCESS="true",
        FINRA_SSV_SCHEDULED_LIVE_ACCESS="true",
        FINRA_SSV_CONFIG="invented.yml",
        FINRA_SSV_SCOPES="nyse_trf",
    ).items():
        monkeypatch.setenv(key, value)
    context = dg.build_schedule_context(scheduled_execution_time=datetime.fromisoformat(tick))
    request = module.schedule(context)
    settings = request.run_config["ops"][ASSET]["config"]
    assert settings["start_date"] == settings["end_date"] == expected
    assert settings["live"] and settings["scheduled"] and settings["overlap_days"] == 2
    assert settings["scopes"] == ["nyse_trf"]
    assert request.run_key.endswith(datetime.fromisoformat(tick).isoformat())


@pytest.mark.parametrize(
    "missing", ["FINRA_SSV_CONFIG", "FINRA_SSV_SCOPES", "FINRA_SSV_LIVE_ACCESS"]
)
def test_ssv_ac15_schedule_missing_activation_config_skips(monkeypatch, missing):
    for key, value in dict(
        FINRA_SSV_LIVE_ACCESS="true",
        FINRA_SSV_SCHEDULED_LIVE_ACCESS="true",
        FINRA_SSV_CONFIG="invented.yml",
        FINRA_SSV_SCOPES="nyse_trf",
    ).items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv(missing)
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 10, 14, 15, tzinfo=UTC)
    )
    assert isinstance(module.schedule(context), dg.SkipReason)


def test_ssv_ac15_schedule_unknown_calendar_refuses(monkeypatch):
    for key, value in dict(
        FINRA_SSV_LIVE_ACCESS="true",
        FINRA_SSV_SCHEDULED_LIVE_ACCESS="true",
        FINRA_SSV_CONFIG="invented.yml",
        FINRA_SSV_SCOPES="nyse_trf",
    ).items():
        monkeypatch.setenv(key, value)
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2027, 2, 1, 14, 15, tzinfo=UTC)
    )
    assert isinstance(module.schedule(context), dg.SkipReason)


@pytest.fixture
def offline_job(tmp_path, manifest, monkeypatch):
    cfg = config(tmp_path / "source", manifest)
    monkeypatch.setenv("FINRA_SSV_LIVE_ACCESS", "true")
    monkeypatch.setattr(module, "load_config", lambda p: cfg)
    script = Script(manifest, [data_reply()])
    from market_data_pipeline.pipelines.finra_short_sale_volume import source

    acquire = source.acquire

    def injected(cfg, **kwargs):
        return acquire(
            cfg,
            **kwargs,
            transport=script,
            environ=ENV,
            clock=lambda: datetime(2026, 11, 13, 15, 0, tzinfo=UTC),
            sleep=lambda _: None,
        )

    monkeypatch.setattr(module, "acquire", injected)
    return cfg, script


def test_ssv_ac15_actual_job_acquires_publishes_exports_with_injected_http(offline_job, tmp_path):
    cfg, script = offline_job
    destination = tmp_path / "research.json"
    result = execute(export_destination=str(destination), output_use="invented_fixture")
    assert result.success
    value = research_export.read(
        destination, expected_scopes=["nyse_trf"], output_use="invented_fixture"
    )
    assert len(value["rows"]) == 2
    assert value["rows"][0]["symbol_raw"] == "000SYN.A"
    assert len(script.calls) == 3
    events = result.asset_materializations_for_node(ASSET)
    metadata = {k: v.value for k, v in events[0].metadata.items()}
    assert metadata["acquisition_status"] == "COMPLETE"
    assert metadata["publication_state"] == "PUBLISHED"
    assert metadata["generation_id"] == value["publication"]["generation_id"]


def test_ssv_ac15_incomplete_acquisition_never_publishes(offline_job, monkeypatch):
    monkeypatch.setattr(
        module, "acquire", lambda *a, **k: dict(status="INCOMPLETE", error_code="invented")
    )
    monkeypatch.setattr(module.publication, "publish", lambda *a, **k: pytest.fail("publication"))
    result = execute()
    assert not result.success and result.asset_materializations_for_node(ASSET) == []


def test_ssv_ac15_kill_switch_revoked_during_acquisition_stops_publication(
    offline_job, monkeypatch
):
    def revoke(*a, **k):
        monkeypatch.delenv("FINRA_SSV_LIVE_ACCESS")
        return dict(status="COMPLETE")

    monkeypatch.setattr(module, "acquire", revoke)
    monkeypatch.setattr(module.publication, "publish", lambda *a, **k: pytest.fail("publication"))
    assert not execute().success


def test_ssv_ac15_export_rights_preflight_before_acquisition(offline_job, monkeypatch, tmp_path):
    monkeypatch.setattr(module, "acquire", lambda *a, **k: pytest.fail("acquisition"))
    assert not execute(export_destination=str(tmp_path / "out.json")).success


def test_ssv_ac15_launch_example_defaults_live_false():
    from pathlib import Path

    settings = yaml.safe_load(Path("resources/dagster-ssv.yml").read_text())["ops"][ASSET]["config"]
    assert settings["live"] is False
    assert dg.validate_run_config(
        module.defs.resolve_job_def("finra_short_sale_volume_job"),
        {"ops": {ASSET: {"config": settings}}},
    )


def test_ssv_ac15_postpublication_export_failure_retains_pointer(offline_job, tmp_path):
    cfg, _ = offline_job
    path = tmp_path / "research.json"
    path.write_bytes(b"existing artifact")
    result = execute(export_destination=str(path), output_use="invented_fixture")
    assert not result.success and result.asset_materializations_for_node(ASSET) == []
    from test_ssv_bronze import fixture_grant

    from market_data_pipeline.pipelines.finra_short_sale_volume import publication

    selected = publication.read(
        cfg.root, cfg.manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert selected["state"] == "PUBLISHED" and len(selected["tables"]["gold"]) == 2
    assert path.read_bytes() == b"existing artifact"


def test_ssv_ac15_private_exceptions_are_not_serialized(offline_job, monkeypatch):
    def fail(*a, **k):
        raise OSError("invented-private-password-and-path")

    monkeypatch.setattr(module, "acquire", fail)
    result = execute()
    failure = result.failure_data_for_node(ASSET)
    assert "invented-private-password-and-path" not in str(failure)
    assert failure.user_failure_data.metadata["phase"].value == "acquisition"


def test_ssv_ac15_schedule_scope_overlap_skips(monkeypatch):
    for key, value in dict(
        FINRA_SSV_LIVE_ACCESS="true",
        FINRA_SSV_SCHEDULED_LIVE_ACCESS="true",
        FINRA_SSV_CONFIG="invented.yml",
        FINRA_SSV_SCOPES="listed_trfs,nyse_trf",
    ).items():
        monkeypatch.setenv(key, value)
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 10, 14, 15, tzinfo=UTC)
    )
    assert isinstance(module.schedule(context), dg.SkipReason)


def test_ssv_ac15_yaml_live_false_blocks_the_actual_source_runner(offline_job, monkeypatch):
    from dataclasses import replace

    cfg, script = offline_job
    monkeypatch.setattr(module, "load_config", lambda p: replace(cfg, live=False))
    assert not execute().success
    assert script.calls == [] and not cfg.root.exists()


def test_ssv_ac15_other_publication_cannot_change_the_job_export_generation(
    offline_job, tmp_path, monkeypatch
):
    original = module.publication.publish
    generations = []

    def interleaved(*args, **kwargs):
        first = original(*args, **kwargs)
        later = original(*args, **kwargs, as_of=datetime(2026, 11, 14, tzinfo=UTC))
        generations.extend([first["generation_id"], later["generation_id"]])
        return first

    monkeypatch.setattr(module.publication, "publish", interleaved)
    path = tmp_path / "research.json"
    result = execute(export_destination=str(path), output_use="invented_fixture")
    assert len(generations) == 2 and generations[0] != generations[1]
    assert not result.success and result.asset_materializations_for_node(ASSET) == []
    assert not path.exists()
