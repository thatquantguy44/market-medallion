"""Explicit presidential rebuild and blocked-by-default live operations."""

import os
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg

from market_data_pipeline.shared.http import load_network_config

from .config import load_config
from .publication import publish_full_bundle
from .review import load_review_version
from .source import fetch_selected
from .warehouse import ingest_capture


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
    review_id: str
    profile_id: str
    network_config: str | None = None


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
    required = {"FF_MKT_001", "FEDFUNDS_001", "SHILLER_001"}
    if set(config.source_ids) != required or len(config.source_ids) != len(required):
        raise dg.Failure("Select exactly the three required market sources.", allow_retries=False)
    if not config.profile_id.startswith("local-"):
        raise dg.Failure("Live refresh requires an explicit local profile.", allow_retries=False)
    review, _, _ = load_review_version(Path(config.workspace), config.review_id)
    if review["evidence_kind"] != "REVIEWED_HISTORY":
        raise dg.Failure("Live refresh requires reviewed historical controls.", allow_retries=False)
    network = load_network_config(Path(config.network_config)) if config.network_config else None
    result = fetch_selected(
        Path(config.workspace),
        load_config(config.source_config),
        config.source_ids,
        network=network,
    )
    if result["status"] != "SUCCEEDED":
        raise dg.Failure("Presidential source acquisition did not complete.", allow_retries=False)
    return {
        "fetch": result,
        "workspace": config.workspace,
        "review_id": config.review_id,
        "profile_id": config.profile_id,
    }


@dg.op
def presidential_live_publish(selection: dict):
    sources = selection["fetch"]["sources"]
    required = {"FF_MKT_001", "FEDFUNDS_001", "SHILLER_001"}
    if (
        len(sources) != len(required)
        or {item["source_id"] for item in sources} != required
        or any(item["input_kind"] != "CAPTURED_SOURCE" for item in sources)
    ):
        raise dg.Failure("Live publication requires all three real captures.", allow_retries=False)
    captures = {}
    for item in sources:
        captures[item["source_id"]] = item["capture_id"]
        outcome = ingest_capture(
            Path(selection["workspace"]), item["source_id"], item["capture_id"]
        )
        if outcome["status"] != "PARSED":
            raise dg.Failure(
                "Source normalization failed; publication was not advanced.", allow_retries=False
            )
    return publish_full_bundle(
        Path(selection["workspace"]),
        captures,
        selection["review_id"],
        profile_id=selection["profile_id"],
        created_at=datetime.now(UTC),
    )


@dg.job
def presidential_rebuild_job():
    presidential_offline_rebuild()


@dg.job
def presidential_live_fetch_job():
    presidential_live_publish(presidential_live_fetch())


@dg.schedule(
    name="presidential_research_refresh",
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
    review_id = os.environ.get("PRES_REVIEW_ID")
    profile_id = os.environ.get("PRES_PROFILE_ID")
    if not all((workspace, config, sources, review_id, profile_id)):
        return dg.SkipReason(
            "Workspace, source config, source IDs, review ID and profile are required."
        )
    return dg.RunRequest(
        run_key=f"presidential-live-{context.scheduled_execution_time.isoformat()}",
        run_config={
            "ops": {
                "presidential_live_fetch": {
                    "config": {
                        "workspace": workspace,
                        "source_config": config,
                        "review_id": review_id,
                        "profile_id": profile_id,
                        "network_config": os.environ.get("PRES_NETWORK_CONFIG"),
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
