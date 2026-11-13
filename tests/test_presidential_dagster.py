"""PR016-R12/R13: stopped jobs keep offline and live operations separate."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

dg = pytest.importorskip("dagster")
from market_data_pipeline.pipelines.presidential_research.dagster_defs import (  # noqa: E402
    defs,
    live_schedule,
    presidential_rebuild_job,
)


def test_definitions_and_schedule_are_stopped(monkeypatch):
    dg.Definitions.validate_loadable(defs)
    assert defs.schedules[0].default_status == dg.DefaultScheduleStatus.STOPPED
    monkeypatch.delenv("PRES_SCHEDULED_LIVE_ACCESS", raising=False)
    context = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 11, 13, tzinfo=UTC))
    assert isinstance(live_schedule(context), dg.SkipReason)


def test_offline_job_publishes_only_explicit_selected_inputs(tmp_path):
    selections = {
        "workspace": str(tmp_path),
        "ff_capture_id": "ff",
        "shiller_capture_id": "sh",
        "fedfunds_capture_id": "fed",
        "review_id": "review",
        "profile_id": "profile",
    }
    with (
        patch(
            "market_data_pipeline.pipelines.presidential_research.dagster_defs.fetch_selected",
            side_effect=AssertionError("network forbidden"),
        ),
        patch(
            "market_data_pipeline.pipelines.presidential_research.dagster_defs.publish_full_bundle",
            return_value={"status": "PUBLISHED"},
        ) as publish,
    ):
        result = presidential_rebuild_job.execute_in_process(
            run_config={"ops": {"presidential_offline_rebuild": {"config": selections}}}
        )
    assert result.success
    assert publish.call_args.args[1] == {
        "FF_MKT_001": "ff",
        "SHILLER_001": "sh",
        "FEDFUNDS_001": "fed",
    }


def test_live_job_checks_review_and_rejects_fixture_before_publication(tmp_path):
    from market_data_pipeline.pipelines.presidential_research.dagster_defs import (
        presidential_live_fetch_job,
    )

    config = {
        "workspace": str(tmp_path),
        "source_config": "fixture.toml",
        "source_ids": ["FF_MKT_001", "FEDFUNDS_001", "SHILLER_001"],
        "review_id": "review",
        "profile_id": "local-review",
    }
    returned = {
        "status": "SUCCEEDED",
        "sources": [
            {"source_id": name, "capture_id": name, "input_kind": "SYNTHETIC_FIXTURE"}
            for name in config["source_ids"]
        ],
    }
    prefix = "market_data_pipeline.pipelines.presidential_research.dagster_defs."
    with (
        patch(
            prefix + "load_review_version",
            return_value=({"evidence_kind": "REVIEWED_HISTORY"}, b"", {}),
        ),
        patch(prefix + "load_config", return_value=object()),
        patch(prefix + "fetch_selected", return_value=returned),
        patch(prefix + "ingest_capture") as ingest,
        patch(prefix + "publish_full_bundle") as publish,
    ):
        result = presidential_live_fetch_job.execute_in_process(
            run_config={"ops": {"presidential_live_fetch": {"config": config}}},
            raise_on_error=False,
        )
    assert not result.success
    ingest.assert_not_called()
    publish.assert_not_called()
