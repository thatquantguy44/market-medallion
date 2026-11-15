"""SSV-AC15: bounded source-owned job; execution gates and default-stopped automation."""

import os
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg

from market_data_pipeline.orchestration.slack_alerts import slack_alert_sensors
from market_data_pipeline.pipelines.finra_short_sale_volume import publication, research_export
from market_data_pipeline.pipelines.finra_short_sale_volume.acquisition_config import load_config
from market_data_pipeline.pipelines.finra_short_sale_volume.data.release import (
    day,
    default_policy,
    load_policy,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.registry import select_scopes
from market_data_pipeline.pipelines.finra_short_sale_volume.source import acquire


class SsvConfig(dg.Config):
    config_path: str
    start_date: str
    end_date: str
    scopes: list[str]
    overlap_days: int = 2
    live: bool = False
    scheduled: bool = False
    export_destination: str = ""
    output_use: str = research_export.PURPOSE


def _enabled(config):
    if config.live is not True or os.environ.get("FINRA_SSV_LIVE_ACCESS") != "true":
        raise PermissionError("FINRA execution requires both live gates")
    if config.scheduled and os.environ.get("FINRA_SSV_SCHEDULED_LIVE_ACCESS") != "true":
        raise PermissionError("FINRA scheduled execution has been disabled")


@dg.asset(
    group_name="finra_short_sale_volume",
    description=("Bounded admitted acquisition, verified publication and optional research export"),
    retry_policy=dg.RetryPolicy(max_retries=0),
)
def finra_short_sale_volume(
    context: dg.AssetExecutionContext, config: SsvConfig
) -> dg.MaterializeResult:
    failed, phase = False, "preflight"
    try:
        _enabled(config)
        settings = load_config(Path(config.config_path))
        operation = "fixture" if settings.operation == "fixture" else "transform"
        shared = dict(approved_use=settings.approved_use, operation=operation)
        if config.export_destination:
            research_export._gate(
                settings.manifest, settings.approved_use, operation, config.output_use
            )
        phase = "acquisition"
        result = acquire(
            settings,
            start=config.start_date,
            end=config.end_date,
            scopes=config.scopes,
            overlap_days=config.overlap_days,
            live=config.live,
        )
        if result["status"] != "COMPLETE":
            raise ValueError("incomplete FINRA acquisition")
        _enabled(config)
        phase = "publication"
        published = publication.publish(
            settings.root,
            settings.manifest,
            **shared,
            trade_date=config.end_date,
            scopes=config.scopes,
        )
        if published["state"] != "PUBLISHED" or published["recovery_required"]:
            raise ValueError("FINRA publication requires recovery")
        exported = None
        if config.export_destination:
            _enabled(config)
            phase = "export"
            exported = research_export.export(
                settings.root,
                settings.manifest,
                **shared,
                destination=Path(config.export_destination),
                output_use=config.output_use,
                expected_generation_id=published["generation_id"],
                scopes=config.scopes,
            )
    except (OSError, ValueError, PermissionError, RuntimeError, RecursionError):
        failed = True
    # Raise outside except so Dagster cannot serialize private exceptions/credentials.
    if failed:
        raise dg.Failure(
            "FINRA job failed during " + phase + "; inspect retained source evidence",
            allow_retries=False,
            metadata={"phase": phase},
        )
    return dg.MaterializeResult(
        metadata={
            "acquisition_status": result["status"],
            "complete_partitions": result["complete_partitions"],
            "request_budget_used": result["request_budget_used"],
            "response_bytes": result["response_bytes"],
            "publication_state": published["state"],
            "generation_id": published["generation_id"],
            "row_counts": published["row_counts"],
            "scope": config.scopes,
            "trade_date": config.end_date,
            "api_release_timing": "UNKNOWN_UNLESS_SEPARATELY_CONFIGURED",
            "research_exported": exported is not None,
            "research_rows": exported["rows"] if exported else 0,
        }
    )


job = dg.define_asset_job(
    "finra_short_sale_volume_job",
    selection=[finra_short_sale_volume],
    tags={"dataset": "finra-short-sale-volume", "dagster/max_retries": "0"},
)


@dg.schedule(
    name="finra_short_sale_volume_weekday_refresh",
    job=job,
    cron_schedule="15 9 * * 1-5",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def schedule(context):
    if (
        os.environ.get("FINRA_SSV_SCHEDULED_LIVE_ACCESS") != "true"
        or os.environ.get("FINRA_SSV_LIVE_ACCESS") != "true"
    ):
        return dg.SkipReason("FINRA scheduled and execution live gates are disabled")
    config_path, scopes = os.environ.get("FINRA_SSV_CONFIG"), os.environ.get("FINRA_SSV_SCOPES")
    if not config_path or not scopes:
        return dg.SkipReason("Set FINRA_SSV_CONFIG and explicit FINRA_SSV_SCOPES")
    try:
        selected = [f.id for f in select_scopes(scopes.split(","))]
        policy_path = os.environ.get("FINRA_SSV_OPERATIONS_CONFIG")
        policy = load_policy(Path(policy_path)) if policy_path else default_policy()
        tick = context.scheduled_execution_time
        if tick is None:
            raise ValueError("missing scheduled execution time")
        local = tick.astimezone(ZoneInfo("America/New_York")).date()
        if not day(policy["calendar_start"]) <= local <= day(policy["calendar_end"]):
            raise ValueError("unreviewed calendar")
        target = local - timedelta(days=1)
        for _ in range(10):
            if target < day(policy["calendar_start"]):
                raise ValueError("unreviewed prior calendar")
            if target.weekday() < 5 and target.isoformat() not in policy["closed_dates"]:
                break
            target -= timedelta(days=1)
        else:
            raise ValueError("no bounded reporting day")
    except (OSError, ValueError, TypeError, RuntimeError, RecursionError):
        return dg.SkipReason("Review FINRA scope/calendar settings before scheduling")
    return dg.RunRequest(
        run_key="finra-ssv-" + target.isoformat() + "-" + tick.isoformat(),
        run_config={
            "ops": {
                "finra_short_sale_volume": {
                    "config": dict(
                        config_path=config_path,
                        start_date=target.isoformat(),
                        end_date=target.isoformat(),
                        scopes=selected,
                        overlap_days=2,
                        live=True,
                        scheduled=True,
                    )
                }
            }
        },
    )


defs = dg.Definitions(
    assets=[finra_short_sale_volume],
    jobs=[job],
    schedules=[schedule],
    sensors=slack_alert_sensors(pipeline="finra_short_sale_volume", job=job),
)
