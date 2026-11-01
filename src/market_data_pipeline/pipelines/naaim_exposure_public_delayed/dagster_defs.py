"""Stopped-by-default daily orchestration for the permitted NAAIM research slice."""

import os
from pathlib import Path

import dagster as dg

from market_data_pipeline.pipelines.naaim_exposure_public_delayed.operations import (
    retain_captures,
)
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.pipeline import run_live

RETENTION_DAYS = 365


class NaaimDelayedConfig(dg.Config):
    root: str
    permission_reference: str


@dg.asset(
    group_name="naaim",
    description=(
        "NAAIM Exposure Index — 3-month-delayed public data; internal research, 365-day retention"
    ),
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def naaim_delayed_capture(
    context: dg.AssetExecutionContext, config: NaaimDelayedConfig
) -> dg.MaterializeResult:
    try:
        result = run_live(Path(config.root), permission_reference=config.permission_reference)
        retention = retain_captures(Path(config.root), retention_days=RETENTION_DAYS, apply=True)
    except ValueError as exc:
        raise dg.Failure(str(exc), allow_retries=False) from exc
    context.log.info(
        "NAAIM capture %s retained %s expired capture payload(s)",
        result["run_id"],
        len(retention["remove_capture_ids"]),
    )
    return dg.MaterializeResult(
        metadata={
            "display_name": result["display_name"],
            "delayed_public_display": result["delayed_public_display"],
            "provider_delay_months": result["provider_delay_months"],
            "delay_notice": result["delay_notice"],
            "latest_as_of_date": result["latest_as_of_date"],
            "run_id": result["run_id"],
            "rows": result["row_count"],
            "new_versions": result["inserted_versions"],
            "retention_days": RETENTION_DAYS,
            "expired_capture_payloads": len(retention["remove_capture_ids"]),
        }
    )


job = dg.define_asset_job(
    "naaim_delayed_capture_job",
    selection=[naaim_delayed_capture],
    tags={"dataset": "naaim-exposure-index-public-delayed"},
)


@dg.schedule(
    name="naaim_delayed_daily_live",
    job=job,
    cron_schedule="0 10 * * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def daily_schedule(_context):
    if os.environ.get("NAAIM_SCHEDULED_LIVE_ACCESS") != "true":
        return dg.SkipReason("Set NAAIM_SCHEDULED_LIVE_ACCESS=true after confirming provider terms")
    root = os.environ.get("NAAIM_DELAYED_ROOT")
    if not root:
        return dg.SkipReason("Set NAAIM_DELAYED_ROOT to the pipeline-owned data directory")
    permission_reference = os.environ.get("NAAIM_PERMISSION_REFERENCE")
    if not permission_reference:
        return dg.SkipReason("Set NAAIM_PERMISSION_REFERENCE to a nonsecret permission reference")
    return dg.RunRequest(
        run_key=f"naaim-delayed-{_context.scheduled_execution_time.isoformat()}",
        run_config={
            "ops": {
                "naaim_delayed_capture": {
                    "config": {
                        "permission_reference": permission_reference,
                        "root": root,
                    }
                }
            }
        },
    )


defs = dg.Definitions(assets=[naaim_delayed_capture], jobs=[job], schedules=[daily_schedule])
