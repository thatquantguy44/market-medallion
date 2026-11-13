"""Pipeline-owned stopped run-status sensors; no configuration I/O on import."""

import os
from pathlib import Path

import dagster as dg

from market_data_pipeline.shared.slack_alerts import load_alerts, send_failure, send_success


def slack_failure_sensor(*, pipeline, job):
    return _slack_sensor(pipeline=pipeline, job=job, status="failure")


def slack_success_sensor(*, pipeline, job):
    return _slack_sensor(pipeline=pipeline, job=job, status="success")


def slack_alert_sensors(*, pipeline, job):
    return [
        slack_failure_sensor(pipeline=pipeline, job=job),
        slack_success_sensor(pipeline=pipeline, job=job),
    ]


def _slack_sensor(*, pipeline, job, status):
    @dg.run_status_sensor(
        name=pipeline + "_slack_" + status,
        run_status=dg.DagsterRunStatus.SUCCESS
        if status == "success"
        else dg.DagsterRunStatus.FAILURE,
        monitored_jobs=[job],
        default_status=dg.DefaultSensorStatus.STOPPED,
        minimum_interval_seconds=30,
        description="Optional pipeline-owned Slack "
        + status
        + " alert; requires enabled alert YAML",
    )
    def sensor(context):
        phase, failed = "configuration", False
        try:
            explicit = os.environ.get(pipeline.upper() + "_ALERTS_CONFIG")
            path = Path(explicit) if explicit else Path("config") / pipeline / "alerts.local.yml"
            settings = load_alerts(path, missing_ok=explicit is None)
            if not settings.enabled or not getattr(settings, "on_" + status):
                return dg.SkipReason("Slack " + status + " alerts are disabled for " + pipeline)
            phase = "delivery"
            sender = send_success if status == "success" else send_failure
            sender(
                settings,
                pipeline=pipeline,
                job=context.dagster_run.job_name,
                run_id=context.dagster_run.run_id,
            )
        except Exception:
            failed = True
        if failed:
            # Raise outside except so Dagster cannot serialize sensitive context.
            raise RuntimeError(
                "Slack alert " + phase + " failed; check alert settings and endpoint"
            )
        context.log.info(
            "slack_alert_delivered pipeline=%s job=%s run_id=%s status=%s",
            pipeline,
            context.dagster_run.job_name,
            context.dagster_run.run_id,
            status,
        )

    return sensor
