"""FOMC-AC06: bounded fake-source runs and atomic catalog publication in owned schemas."""

from dataclasses import replace
from pathlib import Path

import pytest
from test_fomc_acquisition import Response
from test_fomc_postgres import store as store
from test_fomc_text import pdf_bytes

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import OperationSettings
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.operations import health, run_operation

__all__ = ["store"]
pytestmark = pytest.mark.postgres
BASE = "https://www.federalreserve.gov/monetarypolicy/"


@pytest.fixture
def operation_archive(store, tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
        PostgresDependencyStore,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
        PostgresObjectStore,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore

    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    PostgresObjectStore(bronze).migrate()
    PostgresDependencyStore(bronze).migrate()
    text = PostgresTextStore(bronze)
    text.migrate()
    PostgresChunkStore(text).migrate()
    operations = OperationStore(store)
    operations.migrate()
    cfg = replace(
        load_config(Path("config/fomc_meeting_minutes/pipeline.example.yml")),
        root=tmp_path,
        postgres=store.config,
    )
    settings = replace(OperationSettings(), request_interval_seconds=0)
    payloads = {
        BASE + "fomc_historical_year.htm": (
            b'<div id="article"><h3>Historical Materials by Year</h3><div class'
            b'="panel"><h5 class="panel-heading">1990 - 1999</h5><a href="fomch'
            b'istorical1990.htm">1990</a></div></div>'
        ),
        BASE + "fomccalendars.htm": (
            b'<div class="panel"><h4 class="panel-heading">1991 FOMC Meetings</'
            b'h4><div class="fomc-meeting"><div class="fomc-meeting__month">Mar'
            b'ch</div><div class="fomc-meeting__date">15</div></div></div>'
        ),
        BASE + "fomchistorical1990.htm": (
            b'<div class="panel"><h5 class="panel-heading">January 3 Meeting - '
            b'1990</h5><p>Minutes: <a href="invented19900103.htm">HTML</a> | <a'
            b' href="files/invented19900103.pdf">PDF</a></p></div>'
        ),
        BASE + "invented19900103.htm": (
            b'<div id="article"><h2>Opening</h2><p>All original minutes</p><a h'
            b'ref="invented-addendum.htm">Addendum to these minutes</a><img src'
            b'="files/invented.png" alt="Chart"></div>'
        ),
        BASE + "files/invented19900103.pdf": pdf_bytes(["First page", "Appendix footnote"]),
        BASE + "invented-addendum.htm": b'<div id="article"><p>Complete addendum</p></div>',
        BASE + "files/invented.png": b"\x89PNG\r\n\x1a\ninvented",
    }
    calls = []
    probes = []
    responses = {}

    class FakeHttp:
        route = "direct"
        fallback_used = False

        def __call__(self, request):
            url = request.full_url
            calls.append(url)
            if url in responses and responses[url]:
                return responses[url].pop(0)
            return Response(payloads[url])

    def prepare(*args, **kwargs):
        probes.append(True)
        return FakeHttp()

    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition.prepare_fomc_http",
        prepare,
    )
    return store, cfg, settings, payloads, calls, probes, responses


def test_bounded_run_publishes_source_text_chunks_and_reuses_only_backfill_originals(
    operation_archive,
):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    result = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    assert result["status"] == "completed"
    assert result["requests"] == 7 and len(probes) == 1
    assert result["selected_documents"] == 2 and result["document_outcomes"]["acquired"] == 3
    assert result["pointers_after"]["chunk"] and result["pointers_after"]["text"]
    assert health(cfg, settings, inventory_store=store)["status"] == "healthy"
    before = len(calls)
    repeat = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    assert repeat["status"] == "completed" and len(calls) - before == 3
    assert repeat["document_outcomes"]["reused"] == 3
    refreshed = run_operation(
        cfg,
        settings,
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        mode="refresh",
        live=True,
    )
    assert refreshed["status"] == "completed" and refreshed["requests"] == 7
    assert len(probes) == 3


def test_missing_source_or_budget_failure_keeps_all_catalog_pointers(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    first = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    responses[BASE + "files/invented19900103.pdf"] = [Response(b"missing", 404)]
    result = run_operation(
        cfg,
        settings,
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        mode="refresh",
        live=True,
    )
    assert result["status"] == "partial" and result["error_code"] == "source_rejected"
    assert result["pointers_after"] == first["pointers_after"]
    assert health(cfg, settings, inventory_store=store)["last_operation_status"] == "partial"
    limited = replace(settings, max_total_bytes=20)
    result = run_operation(
        cfg, limited, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    assert result["status"] == "partial" and result["received_bytes"] <= 20
    assert result["pointers_after"] == first["pointers_after"]


def test_gates_and_local_health_make_no_probe(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    for kwargs in [
        {"live": False},
        {"live": True, "start_year": 1989},
        {"live": True, "scheduled": True},
    ]:
        args = {"start_year": 1990, "end_year": 1990, **kwargs}
        with pytest.raises(ValueError):
            run_operation(cfg, settings, inventory_store=store, **args)
    assert health(cfg, settings, inventory_store=store)["status"] == "empty"
    assert calls == probes == []


def test_retry_keeps_rejected_bytes_and_one_route(operation_archive):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore

    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    responses[BASE + "files/invented19900103.pdf"] = [
        Response(b"throttled", 429, {"Retry-After": "1"})
    ]
    waits = []
    result = run_operation(
        cfg,
        settings,
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        live=True,
        sleep=waits.append,
    )
    assert result["status"] == "completed" and result["requests"] == 8 and waits == [1]
    assert len(probes) == 1
    ids = [
        str(r[0])
        for r in store.connection.execute(
            store._query(
                "SELECT capture_id FROM {schema}.bronze_captures WHERE receipt->>'"
                "http_status'='429'"
            )
        )
    ]
    receipt, body = PostgresBronzeStore(store).read_capture(ids[0])
    assert body == b"throttled" and receipt["headers"]["retry-after"] == "1"


def test_publication_failure_rolls_back_inventory_text_and_chunk_pointers(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    first = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    store.connection.execute(
        store._query(
            "ALTER TABLE {schema}.chunk_catalogs ADD CONSTRAINT fail_test CHECK(false) NOT VALID"
        )
    )
    failed = run_operation(
        cfg,
        settings,
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        mode="refresh",
        live=True,
    )
    assert failed["status"] == "partial" and failed["error_code"] == "publication_failed"
    assert failed["pointers_after"] == first["pointers_after"]
    store.connection.execute(
        store._query("ALTER TABLE {schema}.chunk_catalogs DROP CONSTRAINT fail_test")
    )
    recovered = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    assert recovered["status"] == "completed" and recovered["document_outcomes"]["reused"] == 3


def test_retry_after_above_budget_and_request_exhaustion_stop_without_promotion(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    responses[BASE + "files/invented19900103.pdf"] = [
        Response(b"wait", 503, {"Retry-After": "120"})
    ]
    waits = []
    failed = run_operation(
        cfg,
        settings,
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        live=True,
        sleep=waits.append,
    )
    assert failed["status"] == "partial" and waits == []
    assert all(v is None for v in failed["pointers_after"].values())
    limited = run_operation(
        cfg,
        replace(settings, max_requests=5),
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        live=True,
    )
    assert limited["error_code"] == "request_budget" and limited["requests"] == 3


def test_corrupt_operation_mirror_and_local_recovery_do_not_probe(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    result = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    operations = OperationStore(store)
    before = len(calls)
    assert operations.recover(result["run_id"], root=cfg.root)["result"] == result
    assert len(calls) == before
    path = cfg.root / "operations" / result["run_id"] / "intent.json"
    body = path.read_bytes()
    path.write_bytes(b"{}")
    with pytest.raises(ValueError):
        operations.read_run(result["run_id"], root=cfg.root)
    path.write_bytes(body)
    assert operations.read_run(result["run_id"], root=cfg.root)["result"] == result


def test_capture_database_failure_retains_exact_bytes_for_offline_recovery(operation_archive):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        read_capture,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.recovery import recover_attempt

    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    store.connection.execute(
        store._query(
            "ALTER TABLE {schema}.bronze_captures ADD CONSTRAINT fail_test CHECK(false) NOT VALID"
        )
    )
    failed = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    assert failed["error_code"] == "capture_write_failed" and failed["received_bytes_complete"]
    path = next((cfg.root / "captures").glob("*/receipt.json"))
    receipt, body = read_capture(cfg.root, path.parent.name)
    assert body == payloads[BASE + "fomc_historical_year.htm"]
    assert failed["received_bytes"] == len(body) and failed["requests"] == 1
    store.connection.execute(
        store._query("ALTER TABLE {schema}.bronze_captures DROP CONSTRAINT fail_test")
    )
    before = len(calls)
    recover_attempt(cfg.root, receipt["capture_id"], PostgresBronzeStore(store))
    assert len(calls) == before
    assert PostgresBronzeStore(store).read_capture(receipt["capture_id"]) == (receipt, body)


def test_recovery_cannot_promote_a_local_completion_for_rolled_back_catalogs(operation_archive):
    from datetime import UTC, datetime

    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    operations = OperationStore(store)
    identity = operations.begin({"started_at": datetime.now(UTC).isoformat()}, root=cfg.root)
    result = {
        "status": "completed",
        "pointers_after": {
            "inventory": "00000000-0000-0000-0000-000000000001",
            "text": "f" * 64,
            "chunk": "e" * 64,
        },
    }
    with pytest.raises(ValueError, match="uncommitted"):
        operations.append(identity, result, root=cfg.root, kind="result")
    with pytest.raises(ValueError, match="uncommitted"):
        operations.recover(identity, root=cfg.root)
    assert operations.read_run(identity, root=cfg.root)["result"] is None
    assert calls == probes == []


def test_nonselected_inventory_pages_and_prior_catalog_history_are_retained(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    first = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    before = len(calls)
    next_year = run_operation(
        cfg, settings, inventory_store=store, start_year=1991, end_year=1991, live=True
    )
    assert next_year["status"] == "completed" and len(calls) - before == 2
    assert next_year["selected_documents"] == 0 and next_year["source_outcomes"] == {
        "not_listed": 1
    }
    assert next_year["pointers_after"]["inventory"] != first["pointers_after"]["inventory"]
    assert health(cfg, settings, inventory_store=store)["inventory_document_references"] == 2


def test_elapsed_budget_keeps_received_bytes_but_stops_before_promotion(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    elapsed = {"seconds": 0}

    class Slow(Response):
        def read(self, *args):
            elapsed["seconds"] = 5
            return super().read(*args)

    responses[BASE + "fomc_historical_year.htm"] = [
        Slow(payloads[BASE + "fomc_historical_year.htm"])
    ]
    result = run_operation(
        cfg,
        replace(settings, max_elapsed_seconds=2),
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        live=True,
        monotonic=lambda: elapsed["seconds"],
    )
    assert result["error_code"] == "elapsed_budget" and result["requests"] == 1
    assert result["received_bytes"] > 0 and all(
        v is None for v in result["pointers_after"].values()
    )


def test_dependency_cycles_and_invalid_assets_are_bounded_failures(operation_archive):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    original = payloads[BASE + "invented-addendum.htm"]
    payloads[BASE + "invented-addendum.htm"] = (
        b'<div id="article"><a href="invented19900103.htm">Addendum to these minutes</a></div>'
    )
    result = run_operation(
        cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
    )
    assert result["status"] == "partial" and result["error_code"] == "cyclic_dependency"
    assert all(v is None for v in result["pointers_after"].values())
    payloads[BASE + "invented-addendum.htm"] = original
    payloads[BASE + "files/invented.png"] = b"not PNG"
    result = run_operation(
        cfg,
        settings,
        inventory_store=store,
        start_year=1990,
        end_year=1990,
        mode="refresh",
        live=True,
    )
    assert result["error_code"] == "invalid_asset"
    assert all(v is None for v in result["pointers_after"].values())


def test_concurrent_operation_is_refused_before_intent_or_probe(operation_archive):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import (
        PostgresInventoryStore,
    )

    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    with PostgresInventoryStore(store.config) as other:
        key = "fomc-operation:" + store.config.schema
        other.connection.execute("SELECT pg_advisory_lock(hashtextextended(%s,0))", (key,))
        try:
            with pytest.raises(ValueError, match="already running"):
                run_operation(
                    cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True
                )
        finally:
            other.connection.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (key,))
    assert calls == probes == []
