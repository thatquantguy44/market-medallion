"""Stopped-by-default threshold-list orchestration."""

import os
from pathlib import Path

import dagster as dg

from market_data_pipeline.orchestration.slack_alerts import slack_alert_sensors
from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import acquire_and_run


class ThresholdConfig(dg.Config):
    config_path: str
    partition: str


@dg.asset(
    group_name="reg_sho_thresholds",
    description="Acquire and version one configured Nasdaq or NYSE threshold list",
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def reg_sho_threshold_list(
    context: dg.AssetExecutionContext, config: ThresholdConfig
) -> dg.MaterializeResult:
    try:
        result = acquire_and_run(Path(config.config_path), config.partition)
    except ValueError as exc:
        raise dg.Failure(str(exc), allow_retries=False) from exc
    if result["status"] != "SUCCEEDED":
        raise dg.Failure(
            f"threshold capture ended {result['status']}: {result['error_code']}",
            allow_retries=result["status"] == "FAILED",
        )
    context.log.info(
        "%s %s threshold list captured %s rows",
        result["provider"],
        result["market"],
        result["row_count"],
    )
    return dg.MaterializeResult(
        metadata={
            "provider": result["provider"],
            "market": result["market"],
            "list_date": result["list_date"],
            "run_id": result["run_id"],
            "rows": result["row_count"],
            "new_versions": result["inserted_versions"],
            "coverage_state": result["coverage_state"],
        }
    )


job = dg.define_asset_job(
    "reg_sho_threshold_list_job",
    selection=[reg_sho_threshold_list],
    tags={"dataset": "reg-sho-thresholds"},
)


@dg.schedule(
    name="reg_sho_threshold_lists_daily_live",
    job=job,
    cron_schedule="30 23 * * 1-5",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def schedule(context):
    if os.environ.get("REG_SHO_SCHEDULED_LIVE_ACCESS") != "true":
        return dg.SkipReason(
            "Set REG_SHO_SCHEDULED_LIVE_ACCESS=true after reviewing both provider policies"
        )
    config_path = os.environ.get("REG_SHO_CONFIG")
    partition = os.environ.get("REG_SHO_PARTITION")
    if not config_path or not partition:
        return dg.SkipReason("Set REG_SHO_CONFIG and REG_SHO_PARTITION")
    return dg.RunRequest(
        run_key=f"reg-sho-{partition}-{context.scheduled_execution_time.isoformat()}",
        run_config={
            "ops": {
                "reg_sho_threshold_list": {
                    "config": {"config_path": config_path, "partition": partition}
                }
            }
        },
    )


defs = dg.Definitions(
    assets=[reg_sho_threshold_list],
    jobs=[job],
    schedules=[schedule],
    sensors=slack_alert_sensors(pipeline="reg_sho_thresholds", job=job),
)
