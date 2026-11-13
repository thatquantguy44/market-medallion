"""Actual Dagster failures, optional configs, safe sensor errors and registration."""

from pathlib import Path

import pytest
import yaml

pytest.importorskip("dagster")
import dagster as dg

from market_data_pipeline.orchestration import slack_alerts as module


@dg.op
def broken_op():
    raise RuntimeError("invented pipeline failure")


@dg.job
def example_alert_job():
    broken_op()


@pytest.fixture
def failed_context(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with dg.DagsterInstance.ephemeral() as instance:
        result = example_alert_job.execute_in_process(
            instance=instance, raise_on_error=False, tags={"private": "NEVER_FORWARD_THIS"}
        )
        run = instance.get_run_by_id(result.run_id)
        context = dg.build_run_status_sensor_context(
            sensor_name="example_slack_failure",
            dagster_event=result.get_run_failure_event(),
            dagster_instance=instance,
            dagster_run=run,
        )
        yield context


def sensor():
    return module.slack_failure_sensor(pipeline="example", job=example_alert_job)


def test_missing_default_and_disabled_config_do_not_send_or_change_run(failed_context, monkeypatch):
    calls = []
    monkeypatch.setattr(module, "send_failure", lambda *a, **k: calls.append(True))
    assert isinstance(sensor()(failed_context), dg.SkipReason)
    path = Path("config/example/alerts.local.yml")
    path.parent.mkdir(parents=True)
    path.write_text("slack: {enabled: false}")
    assert isinstance(sensor()(failed_context), dg.SkipReason)
    assert calls == [] and failed_context.dagster_run.status == dg.DagsterRunStatus.FAILURE


def test_enabled_pipeline_config_passes_only_safe_identifiers(failed_context, monkeypatch):
    path = Path("custom.yml")
    path.write_text(
        yaml.safe_dump({"slack": {"enabled": True, "webhook_url_env": "EXAMPLE_WEBHOOK"}})
    )
    monkeypatch.setenv("EXAMPLE_ALERTS_CONFIG", str(path))
    calls = []
    monkeypatch.setattr(module, "send_failure", lambda *a, **kw: calls.append((a, kw)))
    assert sensor()(failed_context) is None
    args, kwargs = calls[0]
    assert args[0].webhook_url_env == "EXAMPLE_WEBHOOK"
    assert kwargs == {
        "pipeline": "example",
        "job": "example_alert_job",
        "run_id": failed_context.dagster_run.run_id,
    }
    assert "NEVER_FORWARD_THIS" not in repr(calls)


@pytest.mark.parametrize("stage", ["load_alerts", "send_failure"])
def test_unexpected_config_and_delivery_errors_are_redacted(failed_context, monkeypatch, stage):
    import sys

    from dagster._utils.error import serializable_error_info_from_exc_info

    from market_data_pipeline.shared.slack_alerts import SlackAlerts

    monkeypatch.setattr(module, "load_alerts", lambda *a, **kw: SlackAlerts(enabled=True))

    def fail(*args, **kwargs):
        raise RuntimeError("https://hooks.slack.com/services/DO_NOT_LOG_SECRET")

    monkeypatch.setattr(module, stage, fail)
    try:
        sensor()(failed_context)
    except RuntimeError:
        error = serializable_error_info_from_exc_info(sys.exc_info())
    else:
        pytest.fail("sensor should fail")
    assert "DO_NOT_LOG_SECRET" not in repr(error)
    assert error.cause is None and error.context is None
    assert failed_context.dagster_run.status == dg.DagsterRunStatus.FAILURE


def test_explicit_missing_alert_path_is_a_visible_error(failed_context, monkeypatch):
    monkeypatch.setenv("EXAMPLE_ALERTS_CONFIG", "missing.yml")
    with pytest.raises(RuntimeError, match="configuration"):
        sensor()(failed_context)


def test_root_exposes_both_stopped_status_sensors_per_implemented_pipeline():
    from market_data_pipeline.orchestration.dagster_defs import defs

    expected = {
        "aaii",
        "naaim_exposure_public_delayed",
        "sec_fails_to_deliver",
        "reg_sho_thresholds",
        "fomc_meeting_minutes",
    }
    sensors = defs.get_repository_def().sensor_defs
    assert {s.name for s in sensors} == {
        p + "_slack_" + status for p in expected for status in ("failure", "success")
    }
    assert all(s.default_status == dg.DefaultSensorStatus.STOPPED for s in sensors)
    for pipeline in expected:
        settings = Path("config") / pipeline / "alerts.example.yml"
        assert settings.exists()
        value = yaml.safe_load(settings.read_text())
        assert value["slack"]["enabled"] is False
        assert value["slack"]["webhook_url_env"] == pipeline.upper() + "_SLACK_WEBHOOK_URL"


@dg.job
def other_job():
    broken_op()


@pytest.mark.parametrize("delivery", ["delivered", "error", "disabled"])
def test_real_ticks_filter_jobs_handle_pre_step_failure_and_advance_cursor(
    tmp_path, monkeypatch, delivery
):
    from market_data_pipeline.shared.slack_alerts import SlackAlerts

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        module, "load_alerts", lambda *a, **kw: SlackAlerts(enabled=delivery != "disabled")
    )
    calls = []

    def send(settings, **kwargs):
        calls.append(kwargs["run_id"])
        if delivery == "error":
            raise RuntimeError("SECRET_CONNECTION")

    monkeypatch.setattr(module, "send_failure", send)
    definition = sensor()
    with dg.DagsterInstance.ephemeral() as instance:
        initial = definition.evaluate_tick(dg.build_sensor_context(instance=instance))
        assert initial.cursor and calls == []
        failed = example_alert_job.execute_in_process(instance=instance, raise_on_error=False)
        other_job.execute_in_process(instance=instance, raise_on_error=False)
        early = instance.create_run_for_job(example_alert_job)
        instance.report_run_failed(early)

        @dg.job(name="example_alert_job")
        def successful_alert_job():
            pass

        assert successful_alert_job.execute_in_process(instance=instance).success
        result = definition.evaluate_tick(
            dg.build_sensor_context(instance=instance, cursor=initial.cursor)
        )
        # A disabled callback returns SkipReason and may process one matching run per tick.
        next_result = definition.evaluate_tick(
            dg.build_sensor_context(instance=instance, cursor=result.cursor)
        )
        if delivery == "disabled":
            assert calls == []
        else:
            assert calls == [failed.run_id, early.run_id]
        assert "SECRET_CONNECTION" not in repr(result)
        assert instance.get_run_by_id(early.run_id).status == dg.DagsterRunStatus.FAILURE
        final = definition.evaluate_tick(
            dg.build_sensor_context(instance=instance, cursor=next_result.cursor)
        )
        assert final.cursor == next_result.cursor
        assert len(calls) == (0 if delivery == "disabled" else 2)
        if delivery == "error":
            assert len(result.dagster_run_reactions) == 2
            assert all(reaction.error is not None for reaction in result.dagster_run_reactions)


def test_sensor_to_webhook_flow_does_not_forward_run_tags(failed_context, monkeypatch):
    import json

    from market_data_pipeline.shared import slack_alerts

    path = Path("config/example/alerts.local.yml")
    path.parent.mkdir(parents=True)
    path.write_text("slack: {enabled: true, webhook_url_env: EXAMPLE_SLACK_WEBHOOK_URL}")
    monkeypatch.setenv("EXAMPLE_SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/PRIVATE")
    calls = []

    class Response:
        code = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self, limit):
            return b"ok"

    def prepare(*args, **kwargs):
        def send(request):
            calls.append(request)
            return Response()

        return send

    monkeypatch.setattr(slack_alerts, "prepare_http", prepare)
    assert sensor()(failed_context) is None
    assert len(calls) == 1
    payload = json.loads(calls[0].data)
    assert failed_context.dagster_run.run_id in payload["text"]
    assert "NEVER_FORWARD_THIS" not in repr(payload)
    assert "PRIVATE" not in repr(payload)


@pytest.mark.parametrize("on_success", [True, False])
def test_success_sensor_uses_config_and_does_not_replay(tmp_path, monkeypatch, on_success):
    from market_data_pipeline.shared.slack_alerts import SlackAlerts

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        module, "load_alerts", lambda *a, **k: SlackAlerts(enabled=True, on_success=on_success)
    )
    calls = []
    monkeypatch.setattr(module, "send_success", lambda *a, **k: calls.append(k))
    definition = module.slack_success_sensor(pipeline="example", job=example_alert_job)
    with dg.DagsterInstance.ephemeral() as instance:
        initial = definition.evaluate_tick(dg.build_sensor_context(instance=instance))

        @dg.job(name="example_alert_job")
        def successful_job():
            pass

        success = successful_job.execute_in_process(instance=instance)
        example_alert_job.execute_in_process(instance=instance, raise_on_error=False)
        tick = definition.evaluate_tick(
            dg.build_sensor_context(instance=instance, cursor=initial.cursor)
        )
        assert calls == (
            [{"pipeline": "example", "job": "example_alert_job", "run_id": success.run_id}]
            if on_success
            else []
        )
        again = definition.evaluate_tick(
            dg.build_sensor_context(instance=instance, cursor=tick.cursor)
        )
        assert again.cursor == tick.cursor
        assert len(calls) == int(on_success)
