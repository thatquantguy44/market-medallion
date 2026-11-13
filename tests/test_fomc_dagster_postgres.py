"""Real Dagster → owned PostgreSQL → offline provider → verified catalogs."""

from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from test_fomc_acquisition import Response
from test_fomc_operation_postgres import BASE
from test_fomc_operation_postgres import operation_archive as operation_archive
from test_fomc_postgres import store as store

pytest.importorskip("dagster")
from market_data_pipeline.pipelines.fomc_meeting_minutes import dagster_defs as module
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore

__all__ = ["operation_archive", "store"]
pytestmark = pytest.mark.postgres
ASSET = "fomc_meeting_minutes_refresh"


@pytest.fixture
def dagster_archive(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    value = yaml.safe_load(Path("config/fomc_meeting_minutes/pipeline.example.yml").read_text())
    value.update(root=str(cfg.root), manifest=str(cfg.manifest), postgres=asdict(store.config))
    config_path = cfg.root / "pipeline.yml"
    operations_path = cfg.root / "operations.yml"
    config_path.write_text(yaml.safe_dump(value))
    operations_path.write_text(yaml.safe_dump(settings.to_dict()))

    def execute(**options):
        return module.defs.resolve_job_def("fomc_meeting_minutes_job").execute_in_process(
            run_config={
                "ops": {
                    ASSET: {
                        "config": {
                            "config_path": str(config_path),
                            "operations_path": str(operations_path),
                            "start_year": 1990,
                            "end_year": 1990,
                            "live": True,
                            **options,
                        }
                    }
                }
            },
            raise_on_error=False,
        )

    return execute, operation_archive, operations_path


def failure_values(result):
    return {
        k: v.value
        for k, v in result.failure_data_for_node(ASSET).user_failure_data.metadata.items()
    }


def success_values(result):
    assert result.success
    return {
        k: v.value for k, v in result.asset_materializations_for_node(ASSET)[0].metadata.items()
    }


def test_job_full_flow_repeat_refresh_and_source_retry_use_one_route_per_run(dagster_archive):
    execute, archive, _ = dagster_archive
    store, cfg, _, _, calls, probes, responses = archive
    audit = OperationStore(store)
    first = success_values(execute())
    assert first["data_requests"] == 7 and first["archive_health"] == "healthy"
    assert first["chunk_coverage"]["complete_selected_unit_coverage"]
    assert first["chunk_coverage"]["review_chunks"] > 0
    assert first["full_history_complete"] is first["text_or_ocr_accuracy_certified"] is False
    assert first["chunk_catalog_id"] == audit.pointers()["chunk"]
    original = {
        str(i): PostgresBronzeStore(store).read_capture(str(i))
        for (i,) in store.connection.execute(
            store._query("SELECT capture_id FROM {schema}.bronze_captures")
        )
    }
    versions = store.connection.execute(
        store._query("SELECT count(*) FROM {schema}.text_extractions")
    ).fetchone()[0]
    repeat = success_values(execute())
    assert repeat["data_requests"] == 3 and repeat["document_outcomes"] == {"reused": 3}
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.text_extractions")
        ).fetchone()[0]
        == versions
    )
    assert success_values(execute(mode="refresh"))["data_requests"] == 7
    responses[BASE + "fomc_historical_year.htm"] = [Response(b"wait", 429, {"Retry-After": "0"})]
    retry = success_values(execute(mode="refresh"))
    assert retry["data_requests"] == 8
    assert len(probes) == 4 and len(calls) == 25
    rejected = store.connection.execute(
        store._query(
            "SELECT capture_id FROM {schema}.bronze_captures WHERE receipt->>'http_status'='429'"
        )
    ).fetchall()
    assert (
        len(rejected) == 1
        and PostgresBronzeStore(store).read_capture(str(rejected[0][0]))[1] == b"wait"
    )
    assert all(PostgresBronzeStore(store).read_capture(i) == r for i, r in original.items())
    assert audit.latest(root=cfg.root)["result"]["run_id"] == retry["operation_run_id"]


def test_rejected_source_fails_job_and_preserves_committed_pointers(dagster_archive):
    execute, archive, _ = dagster_archive
    store, cfg, _, _, _, _, responses = archive
    success_values(execute())
    before = OperationStore(store).pointers()
    responses[BASE + "files/invented19900103.pdf"] = [Response(b"missing", 404)]
    result = execute(mode="refresh")
    assert not result.success and not result.asset_materializations_for_node(ASSET)
    values = failure_values(result)
    assert values["error_code"] == "source_rejected" and values["operation_committed"] is False
    assert OperationStore(store).pointers() == before
    assert OperationStore(store).latest(root=cfg.root)["result"]["status"] == "partial"


@pytest.mark.parametrize(
    "setting,value,code",
    [("max_requests", 3, "request_budget"), ("max_total_bytes", 20, "byte_budget")],
)
def test_budget_failure_is_observable_and_cannot_materialize(dagster_archive, setting, value, code):
    execute, archive, path = dagster_archive
    store, cfg, _, _, _, _, _ = archive
    settings = yaml.safe_load(path.read_text())
    settings[setting] = value
    path.write_text(yaml.safe_dump(settings))
    result = execute()
    assert not result.success and not result.asset_materializations_for_node(ASSET)
    assert failure_values(result)["error_code"] == code
    assert OperationStore(store).pointers() == {"inventory": None, "text": None, "chunk": None}
    assert OperationStore(store).latest(root=cfg.root)["result"]["status"] == "partial"


def test_sql_publication_failure_cannot_advance_any_pointer(dagster_archive):
    execute, archive, _ = dagster_archive
    store, cfg, _, _, _, _, _ = archive
    success_values(execute())
    before = OperationStore(store).pointers()
    store.connection.execute(
        store._query(
            "ALTER TABLE {schema}.chunk_catalogs ADD CONSTRAINT dagster_reject "
            "CHECK(false) NOT VALID"
        )
    )
    result = execute(mode="refresh")
    assert not result.success and not result.asset_materializations_for_node(ASSET)
    assert failure_values(result)["error_code"] == "publication_failed"
    assert OperationStore(store).pointers() == before
    assert OperationStore(store).latest(root=cfg.root)["result"]["status"] == "partial"


@pytest.mark.parametrize("raises", [False, True])
def test_post_commit_health_failure_preserves_committed_evidence(
    dagster_archive, monkeypatch, raises
):
    execute, archive, _ = dagster_archive
    store, cfg, _, _, calls, probes, _ = archive
    real_health = module.health

    def unhealthy(*args, **kwargs):
        report = real_health(*args, **kwargs)
        if raises:
            raise RuntimeError("connection://DO_NOT_LOG_SECRET")
        return {**report, "status": "degraded", "issues": ["injected_health_failure"]}

    monkeypatch.setattr(module, "health", unhealthy)
    result = execute()
    assert not result.success and not result.asset_materializations_for_node(ASSET)
    values = failure_values(result)
    assert values["operation_committed"] is True and values["phase"] == "health"
    audit = OperationStore(store)
    latest = audit.latest(root=cfg.root)
    assert latest["result"]["status"] == "completed"
    assert values["operation_run_id"] == latest["intent"]["run_id"]
    assert values["chunk_catalog_id"] == audit.pointers()["chunk"]
    assert "DO_NOT_LOG_SECRET" not in repr(result.failure_data_for_node(ASSET))
    assert len(calls) == 7 and len(probes) == 1


def test_concurrent_job_is_refused_before_intent_or_probe(dagster_archive):
    execute, archive, _ = dagster_archive
    store, _, _, _, calls, probes, _ = archive
    lock = "fomc-operation:" + store.config.schema
    store.connection.execute("SELECT pg_advisory_lock(hashtextextended(%s,0))", (lock,))
    try:
        result = execute()
    finally:
        store.connection.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (lock,))
    assert not result.success and failure_values(result)["error_code"] == "operation_failed"
    assert calls == probes == []
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.operation_runs")
        ).fetchone()[0]
        == 0
    )


def test_recent_window_reaches_the_real_runner_and_audit(dagster_archive, monkeypatch):
    execute, archive, _ = dagster_archive
    store, cfg, _, _, _, _, _ = archive
    monkeypatch.setattr(module, "_now_utc", lambda: datetime(1991, 7, 1, tzinfo=UTC))
    values = success_values(
        execute(recent_years=True, mode="refresh", start_year=None, end_year=None)
    )
    assert (values["start_year"], values["end_year"], values["mode"]) == (1990, 1991, "refresh")
    intent = OperationStore(store).latest(root=cfg.root)["intent"]
    assert intent["years"] == [1990, 1991] and intent["mode"] == "refresh"
