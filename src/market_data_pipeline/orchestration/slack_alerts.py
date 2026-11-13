"""Pipeline-owned stopped run-failure sensors; no configuration I/O on import."""

import os
from pathlib import Path

import dagster as dg

from market_data_pipeline.shared.slack_alerts import load_alerts, send_failure


def slack_failure_sensor(*, pipeline, job):
    @dg.run_failure_sensor(
        name=pipeline + "_slack_failure",
        monitored_jobs=[job],
        default_status=dg.DefaultSensorStatus.STOPPED,
        minimum_interval_seconds=30,
        description="Optional pipeline-owned Slack failure alert; requires enabled alert YAML",
    )
    def sensor(context):
        phase, failed = "configuration", False
        try:
            explicit = os.environ.get(pipeline.upper() + "_ALERTS_CONFIG")
            path = Path(explicit) if explicit else Path("config") / pipeline / "alerts.local.yml"
            settings = load_alerts(path, missing_ok=explicit is None)
            if not settings.enabled:
                return dg.SkipReason("Slack alerts are disabled for " + pipeline)
            phase = "delivery"
            send_failure(
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
            "slack_alert_delivered pipeline=%s job=%s run_id=%s",
            pipeline,
            context.dagster_run.job_name,
            context.dagster_run.run_id,
        )

    return sensor
