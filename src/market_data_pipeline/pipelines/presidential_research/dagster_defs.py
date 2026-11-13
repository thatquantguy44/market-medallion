"""Explicit presidential rebuild and blocked-by-default live operations."""

import os
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg

from .config import load_config
from .publication import publish_full_bundle
from .source import fetch_selected


class PresidentialRebuildConfig(dg.Config):
    workspace: str
    ff_capture_id: str
    shiller_capture_id: str
    fedfunds_capture_id: str
    review_id: str
    profile_id: str


class PresidentialFetchConfig(dg.Config):
    workspace: str
    source_config: str
    source_ids: list[str]


@dg.op
def presidential_offline_rebuild(config: PresidentialRebuildConfig):
    """Rebuild solely from selected captures and reviewed controls; no network."""
    return publish_full_bundle(
        Path(config.workspace),
        {
            "FF_MKT_001": config.ff_capture_id,
            "SHILLER_001": config.shiller_capture_id,
            "FEDFUNDS_001": config.fedfunds_capture_id,
        },
        config.review_id,
        profile_id=config.profile_id,
        created_at=datetime.now(UTC),
    )


@dg.op
def presidential_live_fetch(config: PresidentialFetchConfig):
    result = fetch_selected(
        Path(config.workspace), load_config(config.source_config), config.source_ids
    )
    if result["status"] != "SUCCEEDED":
        raise dg.Failure("Presidential source acquisition did not complete.", allow_retries=False)
    return result


@dg.job
def presidential_rebuild_job():
    presidential_offline_rebuild()


@dg.job
def presidential_live_fetch_job():
    presidential_live_fetch()


@dg.schedule(
    name="presidential_live_fetch_schedule",
    job=presidential_live_fetch_job,
    cron_schedule="0 9 * * 1",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def live_schedule(context):
    if os.environ.get("PRES_SCHEDULED_LIVE_ACCESS") != "true":
        return dg.SkipReason("Live source review and explicit schedule configuration are required.")
    workspace = os.environ.get("PRES_WORKSPACE")
    config = os.environ.get("PRES_SOURCE_CONFIG")
    sources = os.environ.get("PRES_SOURCE_IDS")
    if not workspace or not config or not sources:
        return dg.SkipReason("PRES_WORKSPACE, PRES_SOURCE_CONFIG and PRES_SOURCE_IDS are required.")
    return dg.RunRequest(
        run_key=f"presidential-live-{context.scheduled_execution_time.isoformat()}",
        run_config={
            "ops": {
                "presidential_live_fetch": {
                    "config": {
                        "workspace": workspace,
                        "source_config": config,
                        "source_ids": [
                            source.strip() for source in sources.split(",") if source.strip()
                        ],
                    }
                }
            }
        },
    )


defs = dg.Definitions(
    jobs=[presidential_rebuild_job, presidential_live_fetch_job], schedules=[live_schedule]
)
