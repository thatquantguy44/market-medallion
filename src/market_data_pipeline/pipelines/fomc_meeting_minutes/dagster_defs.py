"""FOMC-owned manual job and explicitly gated, stopped refresh schedule."""

import os
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import load_operations
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.operations import (
    run_operation,
    validate_run,
)


class FomcConfig(dg.Config):
    config_path: str
    operations_path: str
    start_year: int = 1990
    end_year: int = 1990
    mode: str = "backfill"
    live: bool = False
    scheduled: bool = False


@dg.asset(
    group_name="fomc_meeting_minutes",
    description="Bounded FOMC archive/text/chunk refresh with explicit source outcomes",
)
def fomc_meeting_minutes_refresh(
    context: dg.AssetExecutionContext, config: FomcConfig
) -> dg.MaterializeResult:
    try:
        cfg = load_config(Path(config.config_path))
        settings = load_operations(Path(config.operations_path))
        options = {
            "start_year": config.start_year,
            "end_year": config.end_year,
            "mode": config.mode,
            "live": config.live,
            "scheduled": config.scheduled,
        }
        validate_run(cfg, settings, **options)
        with PostgresInventoryStore(cfg.postgres) as store:
            result = run_operation(cfg, settings, inventory_store=store, **options)
    except Exception as exc:
        raise dg.Failure(
            "FOMC configuration or local state is unavailable", allow_retries=False
        ) from exc
    if result["status"] != "completed":
        raise dg.Failure(
            "FOMC bounded run incomplete: " + result["error_code"],
            metadata={"operation_run_id": result["run_id"]},
            allow_retries=False,
        )
    return dg.MaterializeResult(
        metadata={
            "operation_run_id": result["run_id"],
            "start_year": config.start_year,
            "end_year": config.end_year,
            "mode": config.mode,
            "data_requests": result["requests"],
            "received_bytes": result["received_bytes"],
            "selected_documents": result["selected_documents"],
        }
    )


job = dg.define_asset_job(
    "fomc_meeting_minutes_job",
    selection=[fomc_meeting_minutes_refresh],
    tags={"dataset": "fomc-meeting-minutes"},
)


@dg.schedule(
    name="fomc_meeting_minutes_weekday_refresh",
    job=job,
    cron_schedule="10 17 * * 1-5",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def schedule(context):
    config_path = os.environ.get("FOMC_CONFIG")
    operations_path = os.environ.get("FOMC_OPERATIONS_CONFIG")
    if not config_path or not operations_path:
        return dg.SkipReason("Set FOMC_CONFIG and FOMC_OPERATIONS_CONFIG")
    try:
        settings = load_operations(Path(operations_path))
        if not settings.scheduled_live_access:
            return dg.SkipReason("FOMC scheduled live access is disabled")
        cfg = load_config(Path(config_path))
        if context.scheduled_execution_time is None:
            return dg.SkipReason("FOMC schedule requires an explicit tick time")
        year = context.scheduled_execution_time.astimezone(ZoneInfo("America/New_York")).year
        start = max(cfg.start_year, year - settings.overlap_years)
        if start > year:
            return dg.SkipReason("FOMC operating scope has not started")
    except (ValueError, OSError):
        return dg.SkipReason("FOMC schedule configuration is unavailable")
    return dg.RunRequest(
        run_key="fomc-" + context.scheduled_execution_time.isoformat(),
        run_config={
            "ops": {
                "fomc_meeting_minutes_refresh": {
                    "config": {
                        "config_path": config_path,
                        "operations_path": operations_path,
                        "start_year": start,
                        "end_year": year,
                        "mode": "refresh",
                        "live": True,
                        "scheduled": True,
                    }
                }
            }
        },
    )


defs = dg.Definitions(assets=[fomc_meeting_minutes_refresh], jobs=[job], schedules=[schedule])
