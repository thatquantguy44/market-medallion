"""Stopped-by-default orchestration for exact-partition SEC FTD acquisition."""

import os
from pathlib import Path

import dagster as dg

from market_data_pipeline.orchestration.slack_alerts import slack_alert_sensors
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import acquire_and_run


class SecFtdConfig(dg.Config):
    config_path: str
    partition: str


@dg.asset(
    group_name="sec_ftd",
    description="Acquire, validate, archive and publish one configured SEC FTD partition",
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def sec_fails_to_deliver(
    context: dg.AssetExecutionContext, config: SecFtdConfig
) -> dg.MaterializeResult:
    try:
        result = acquire_and_run(Path(config.config_path), config.partition)
    except ValueError as exc:
        raise dg.Failure(str(exc), allow_retries=False) from exc
    if result["status"] != "SUCCEEDED":
        raise dg.Failure(
            f"SEC FTD capture ended {result['status']}: {result['error_code']}",
            allow_retries=result["status"] == "FAILED",
        )
    context.log.info("SEC FTD capture %s published %s rows", result["run_id"], result["row_count"])
    return dg.MaterializeResult(
        metadata={
            "run_id": result["run_id"],
            "capture_sequence": result["capture_sequence"],
            "rows": result["row_count"],
            "new_versions": result["inserted_versions"],
            "coverage_state": result["coverage_state"],
            "snapshot_id": result["snapshot_id"],
            "transport": result["transport"],
        }
    )


job = dg.define_asset_job(
    "sec_fails_to_deliver_job",
    selection=[sec_fails_to_deliver],
    tags={"dataset": "sec-fails-to-deliver"},
)


@dg.schedule(
    name="sec_fails_to_deliver_daily_live",
    job=job,
    cron_schedule="30 8 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def schedule(context):
    if os.environ.get("SEC_FTD_SCHEDULED_LIVE_ACCESS") != "true":
        return dg.SkipReason(
            "Set SEC_FTD_SCHEDULED_LIVE_ACCESS=true after reviewing SEC access policy"
        )
    config_path = os.environ.get("SEC_FTD_CONFIG")
    partition = os.environ.get("SEC_FTD_PARTITION")
    if not config_path or not partition:
        return dg.SkipReason("Set SEC_FTD_CONFIG and SEC_FTD_PARTITION")
    return dg.RunRequest(
        run_key=f"sec-ftd-{partition}-{context.scheduled_execution_time.isoformat()}",
        run_config={
            "ops": {
                "sec_fails_to_deliver": {
                    "config": {"config_path": config_path, "partition": partition}
                }
            }
        },
    )


defs = dg.Definitions(
    assets=[sec_fails_to_deliver],
    jobs=[job],
    schedules=[schedule],
    sensors=slack_alert_sensors(pipeline="sec_fails_to_deliver", job=job),
)
