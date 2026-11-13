"""Job-level success, configuration and failure contracts without external services."""

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("dagster")
from market_data_pipeline.pipelines.fomc_meeting_minutes import dagster_defs as module
from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import OperationSettings

ASSET = "fomc_meeting_minutes_refresh"


@pytest.fixture
def lifecycle(monkeypatch):
    calls = []
    result = {
        "status": "completed",
        "error_code": None,
        "run_id": "00000000-0000-4000-8000-000000000001",
        "requests": 7,
        "probe_requests": 1,
        "received_bytes": 1234,
        "selected_documents": 2,
        "document_outcomes": {"acquired": 3},
        "source_outcomes": {"not_listed": 1},
        "pointers_before": {"inventory": None, "text": None, "chunk": None},
        "pointers_after": {"inventory": "inventory-id", "text": "text-id", "chunk": "chunk-id"},
    }
    report = {
        "status": "healthy",
        "issues": [],
        "pointers": deepcopy(result["pointers_after"]),
        "chunk_coverage": {"chunks": 17, "eligible_chunks": 13, "review_chunks": 4},
        "source_gap_outcomes": {"unacquired": 400, "review_required": 5},
    }
    cfg = load_config(Path("config/fomc_meeting_minutes/pipeline.example.yml"))
    monkeypatch.setattr(module, "load_config", lambda p: cfg)
    monkeypatch.setattr(module, "load_operations", lambda p: OperationSettings())

    @contextmanager
    def store(config):
        calls.append("store")
        yield object()

    def run(*args, **kwargs):
        calls.append(kwargs)
        return result

    def health(*args, **kwargs):
        calls.append("health")
        return report

    monkeypatch.setattr(module, "PostgresInventoryStore", store)
    monkeypatch.setattr(module, "run_operation", run)
    monkeypatch.setattr(module, "health", health, raising=False)
    return calls, result, report


def execute(**config):
    return module.defs.resolve_job_def("fomc_meeting_minutes_job").execute_in_process(
        run_config={
            "ops": {
                ASSET: {
                    "config": {
                        "config_path": "invented-config.yml",
                        "operations_path": "invented-operations.yml",
                        "live": True,
                        **config,
                    }
                }
            }
        },
        raise_on_error=False,
    )


def metadata(result):
    return {
        k: v.value
        for k, v in result.failure_data_for_node(ASSET).user_failure_data.metadata.items()
    }


@pytest.mark.parametrize(
    "bounds,expected", [({}, (1990, 1990)), ({"start_year": 1995, "end_year": 1996}, (1995, 1996))]
)
def test_legacy_default_and_explicit_bounds_keep_working(lifecycle, bounds, expected):
    calls, operation, report = lifecycle
    result = execute(**bounds)
    assert result.success
    assert (calls[1]["start_year"], calls[1]["end_year"]) == expected
    assert calls[-1] == "health"
    event = result.asset_materializations_for_node(ASSET)[0]
    values = {k: v.value for k, v in event.metadata.items()}
    assert values["operation_run_id"] == operation["run_id"]
    assert values["catalog_pointers_before"] == operation["pointers_before"]
    assert values["archive_health"] == "healthy"
    assert values["chunk_catalog_id"] == "chunk-id"
    assert values["chunk_coverage"] == report["chunk_coverage"]
    assert values["source_gap_outcomes"]["review_required"] == 5
    assert values["full_history_complete"] is False
    assert values["text_or_ocr_accuracy_certified"] is False


def test_recent_refresh_uses_new_york_year_at_execution(lifecycle, monkeypatch):
    calls, _, _ = lifecycle
    monkeypatch.setattr(
        module, "_now_utc", lambda: datetime(2027, 1, 1, 0, 30, tzinfo=UTC), raising=False
    )
    result = execute(recent_years=True, mode="refresh")
    assert result.success
    assert (calls[1]["start_year"], calls[1]["end_year"], calls[1]["mode"]) == (
        2025,
        2026,
        "refresh",
    )


@pytest.mark.parametrize(
    "config",
    [
        {"recent_years": True},
        {"recent_years": True, "mode": "refresh", "start_year": 2025, "end_year": 2026},
        {"start_year": 1995},
        {"end_year": 1996},
        {"start_year": 1996, "end_year": 1995},
        {"start_year": 1990, "end_year": 1993},
    ],
)
def test_invalid_window_fails_before_database_access(lifecycle, config):
    calls, _, _ = lifecycle
    result = execute(**config)
    assert not result.success and calls == []
    assert metadata(result)["error_code"] == "preflight_failed"
    assert not result.asset_materializations_for_node(ASSET)


@pytest.mark.parametrize("status", ["partial", "failed"])
def test_incomplete_operation_fails_without_materialization_or_health(lifecycle, status):
    calls, operation, _ = lifecycle
    operation.update(status=status, error_code="request_budget")
    result = execute()
    assert not result.success and "health" not in calls
    assert not result.asset_materializations_for_node(ASSET)
    values = metadata(result)
    assert values["operation_run_id"] == operation["run_id"]
    assert values["error_code"] == "request_budget" and values["operation_committed"] is False
    assert values["catalog_pointers_before"] == operation["pointers_before"]
    assert not any(e.is_step_up_for_retry for e in result.all_events)


@pytest.mark.parametrize("outcome", ["degraded", "catalog_changed"])
def test_post_commit_health_refuses_false_success_and_keeps_operation_id(lifecycle, outcome):
    _, operation, report = lifecycle
    if outcome == "degraded":
        report.update(status="degraded", issues=["operation_stale"])
    else:
        report["pointers"]["chunk"] = "newer-catalog"
    result = execute()
    assert not result.success and not result.asset_materializations_for_node(ASSET)
    values = metadata(result)
    assert values["phase"] == "health" and values["operation_committed"] is True
    assert values["operation_run_id"] == operation["run_id"]
    assert values["chunk_catalog_id"] == "chunk-id"


@pytest.mark.parametrize(
    "stage,code",
    [
        ("load_config", "preflight_failed"),
        ("PostgresInventoryStore", "operation_failed"),
        ("run_operation", "operation_failed"),
        ("health", "post_run_health_failed"),
    ],
)
def test_unexpected_errors_do_not_leak_exception_text_or_context(
    lifecycle, monkeypatch, capsys, stage, code
):
    sentinel = "sensitive://user:DO_NOT_LOG_PASSWORD@private"

    def broken(*args, **kwargs):
        raise RuntimeError(sentinel)

    monkeypatch.setattr(module, stage, broken)
    result = execute()
    assert not result.success
    assert metadata(result)["error_code"] == code
    assert sentinel not in repr(result.failure_data_for_node(ASSET))
    captured = capsys.readouterr()
    assert sentinel not in captured.out + captured.err


def test_source_owned_retries_are_not_multiplied_by_dagster():
    job = module.defs.resolve_job_def("fomc_meeting_minutes_job")
    assert job.tags["dagster/max_retries"] == "0"
    assert job.op_retry_policy.max_retries == 0
