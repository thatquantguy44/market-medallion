from datetime import UTC, datetime

import pytest

dg = pytest.importorskip("dagster")
from market_data_pipeline.pipelines.sec_fails_to_deliver.dagster_defs import (  # noqa: E402
    defs,
    schedule,
)


def test_sec_ftd_definitions_load_and_schedule_is_stopped(monkeypatch):
    dg.Definitions.validate_loadable(defs)
    assert defs.schedules[0].default_status == dg.DefaultScheduleStatus.STOPPED
    monkeypatch.delenv("SEC_FTD_SCHEDULED_LIVE_ACCESS", raising=False)
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 4, 8, 30, tzinfo=UTC)
    )
    assert isinstance(schedule(context), dg.SkipReason)


def test_sec_ftd_schedule_emits_only_explicit_config_and_partition(monkeypatch):
    monkeypatch.setenv("SEC_FTD_SCHEDULED_LIVE_ACCESS", "true")
    monkeypatch.setenv("SEC_FTD_CONFIG", "/durable/sec-ftd.yml")
    monkeypatch.setenv("SEC_FTD_PARTITION", "october_2026_a")
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 4, 8, 30, tzinfo=UTC)
    )
    request = schedule(context)
    assert request.run_key.startswith("sec-ftd-october_2026_a-")
    assert request.run_config["ops"]["sec_fails_to_deliver"]["config"] == {
        "config_path": "/durable/sec-ftd.yml",
        "partition": "october_2026_a",
    }
