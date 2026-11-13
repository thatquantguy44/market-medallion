"""FOMC-owned manual job and explicitly gated, stopped refresh schedule."""

import os
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg

from market_data_pipeline.orchestration.slack_alerts import slack_failure_sensor
from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import (
    load_operations,
    recent_refresh_years,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.operations import (
    health,
    run_operation,
    validate_run,
)


class FomcConfig(dg.Config):
    config_path: str
    operations_path: str
    start_year: int | None = None
    end_year: int | None = None
    recent_years: bool = False
    mode: str = "backfill"
    live: bool = False
    scheduled: bool = False


def _now_utc():
    return datetime.now(UTC)


def _run_years(config, cfg, settings):
    if config.recent_years:
        if config.mode != "refresh" or config.start_year is not None or config.end_year is not None:
            raise ValueError("recent refresh cannot include explicit bounds or backfill mode")
        years = recent_refresh_years(cfg.start_year, settings, _now_utc())
        return years[0], years[-1]
    if config.start_year is None and config.end_year is None:
        return 1990, 1990  # Keep the original manual job default.
    return config.start_year, config.end_year


def _operation_metadata(result):
    metadata = {
        "operation_run_id": result["run_id"],
        "operation_committed": result["status"] == "completed",
        "data_requests": result["requests"],
        "probe_requests": result["probe_requests"],
        "received_bytes": result["received_bytes"],
        "selected_documents": result["selected_documents"],
        "catalog_pointers_before": dg.MetadataValue.json(result["pointers_before"]),
        "document_outcomes": dg.MetadataValue.json(result["document_outcomes"]),
        "source_outcomes": dg.MetadataValue.json(result["source_outcomes"]),
        "full_history_complete": False,
        "text_or_ocr_accuracy_certified": False,
    }
    for key, value in result["pointers_after"].items():
        metadata[key + "_catalog_id"] = value
    return metadata


@dg.asset(
    group_name="fomc_meeting_minutes",
    description="Bounded FOMC acquisition and verified archive/text/chunk publication",
)
def fomc_meeting_minutes_refresh(
    context: dg.AssetExecutionContext, config: FomcConfig
) -> dg.MaterializeResult:
    phase, failed, result, report = "preflight", False, None, None
    try:
        cfg = load_config(Path(config.config_path))
        settings = load_operations(Path(config.operations_path))
        start, end = _run_years(config, cfg, settings)
        options = {
            "start_year": start,
            "end_year": end,
            "mode": config.mode,
            "live": config.live,
            "scheduled": config.scheduled,
        }
        validate_run(cfg, settings, **options)
        phase = "operation"
        with PostgresInventoryStore(cfg.postgres) as store:
            result = run_operation(cfg, settings, inventory_store=store, **options)
            if result["status"] == "completed":
                phase = "health"
                report = health(cfg, settings, inventory_store=store)
    except Exception:
        failed = True
    # Raise outside the except block so serialized Dagster events cannot retain
    # the original exception/context (which can include connection credentials).
    metadata = _operation_metadata(result) if result else {}
    if failed:
        code = {
            "preflight": "preflight_failed",
            "operation": "operation_failed",
            "health": "post_run_health_failed",
        }[phase]
        raise dg.Failure(
            "FOMC " + phase + " failed; inspect safe metadata and retained operation evidence",
            metadata={**metadata, "phase": phase, "error_code": code},
            allow_retries=False,
        )
    if result["status"] != "completed":
        raise dg.Failure(
            "FOMC bounded acquisition did not complete",
            metadata={**metadata, "phase": "operation", "error_code": result["error_code"]},
            allow_retries=False,
        )
    metadata.update(
        start_year=start,
        end_year=end,
        mode=config.mode,
        archive_health=report["status"],
        chunk_coverage=dg.MetadataValue.json(report["chunk_coverage"]),
        source_gap_outcomes=dg.MetadataValue.json(report["source_gap_outcomes"]),
        health_issues=dg.MetadataValue.json(report["issues"]),
    )
    if report["status"] != "healthy" or report["pointers"] != result["pointers_after"]:
        code = (
            "post_run_health_failed"
            if report["status"] != "healthy"
            else "post_run_catalog_changed"
        )
        raise dg.Failure(
            "FOMC committed operation requires archive health review",
            metadata={**metadata, "phase": "health", "error_code": code},
            allow_retries=False,
        )
    return dg.MaterializeResult(metadata=metadata)


job = dg.define_asset_job(
    "fomc_meeting_minutes_job",
    selection=[fomc_meeting_minutes_refresh],
    tags={"dataset": "fomc-meeting-minutes", "dagster/max_retries": "0"},
    op_retry_policy=dg.RetryPolicy(max_retries=0),
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
        years = recent_refresh_years(cfg.start_year, settings, context.scheduled_execution_time)
        start, year = years[0], years[-1]
    except Exception:
        # A preview has no service effects; fail closed for an unavailable tick
        # or configuration without serializing credential-bearing exceptions.
        return dg.SkipReason("FOMC schedule configuration or tick is unavailable")
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


defs = dg.Definitions(
    assets=[fomc_meeting_minutes_refresh],
    jobs=[job],
    schedules=[schedule],
    sensors=[slack_failure_sensor(pipeline="fomc_meeting_minutes", job=job)],
)
