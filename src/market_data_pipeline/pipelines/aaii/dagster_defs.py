"""Dagster owns execution; CSV semantics and persistence stay in the core.

https://docs.dagster.io/api/dagster/assets
https://docs.dagster.io/guides/automate/schedules
"""

import os
from pathlib import Path

import dagster as dg

from market_data_pipeline.orchestration.slack_alerts import slack_failure_sensor
from market_data_pipeline.pipelines.aaii.config import load_config, resolve_runtime
from market_data_pipeline.pipelines.aaii.operations import weekly_window
from market_data_pipeline.pipelines.aaii.pipeline import run


class AaiiConfig(dg.Config):
    input_path: str = "data/inbox/aaii.csv"
    manifest_path: str = "manifests/aaii-investor-sentiment.yml"
    root: str | None = None
    warehouse: str | None = None
    config_path: str | None = None
    live: bool = False
    start: str | None = None
    end: str | None = None


@dg.asset(
    group_name="aaii",
    description="Authorized CSV → immutable Bronze → versioned Silver → Gold + Parquet snapshot",
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def aaii_sentiment(context: dg.AssetExecutionContext, config: AaiiConfig) -> dg.MaterializeResult:
    try:
        runtime = load_config(Path(config.config_path)) if config.config_path else None
        root, _, dsn = resolve_runtime(
            config=runtime,
            root=Path(config.root) if config.root is not None else None,
            warehouse=config.warehouse,
        )
        if config.live:
            from market_data_pipeline.pipelines.aaii.source import run_live

            result = run_live(
                Path(config.manifest_path),
                root,
                start=config.start,
                end=config.end,
                dsn=dsn,
                max_attempts=runtime.operations.http_attempts if runtime else 3,
                network=runtime.network if runtime else None,
            )
        else:
            result = run(
                Path(config.input_path),
                Path(config.manifest_path),
                root,
                dsn=dsn,
                start=config.start,
                end=config.end,
            )
    except ValueError as exc:
        raise dg.Failure(str(exc), allow_retries=False) from exc
    context.log.info(
        "AAII capture %s committed %s versions", result["run_id"], result["inserted_versions"]
    )
    return dg.MaterializeResult(
        metadata={
            "run_id": result["run_id"],
            "rows": result["row_count"],
            "new_versions": result["inserted_versions"],
            "parquet_snapshot": dg.MetadataValue.path(result["snapshot"]),
        }
    )


job = dg.define_asset_job(
    "aaii_sentiment_job",
    selection=[aaii_sentiment],
    tags={"dataset": "aaii-investor-sentiment"},
)
schedule = dg.ScheduleDefinition(
    name="aaii_thursday_local_import",
    job=job,
    cron_schedule="0 10 * * 4",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.schedule(
    name="aaii_weekly_live",
    job=job,
    cron_schedule="0 10 * * 4",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def live_schedule(context):
    path = os.environ.get("MARKET_MEDALLION_CONFIG")
    if not path:
        return dg.SkipReason("Set MARKET_MEDALLION_CONFIG for the runtime YAML")
    runtime = load_config(Path(path))
    if not runtime.operations.scheduled_live_access:
        return dg.SkipReason(
            "Scheduled live acquisition requires an explicit access acknowledgement"
        )
    start, end = weekly_window(context.scheduled_execution_time, runtime.operations.overlap_days)
    return dg.RunRequest(
        run_key=f"aaii-{context.scheduled_execution_time.isoformat()}",
        run_config={
            "ops": {
                "aaii_sentiment": {
                    "config": {
                        "live": True,
                        "config_path": path,
                        "manifest_path": "manifests/aaii-investor-sentiment-xls.yml",
                        "start": start,
                        "end": end,
                    }
                }
            }
        },
    )


defs = dg.Definitions(
    assets=[aaii_sentiment],
    jobs=[job],
    schedules=[schedule, live_schedule],
    sensors=[slack_failure_sensor(pipeline="aaii", job=job)],
)
