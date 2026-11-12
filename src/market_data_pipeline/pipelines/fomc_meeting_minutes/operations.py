"""FOMC-AC06: explicit bounded runs, coherent promotion and no-HTTP local inspection."""

from collections import Counter
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import (
    select_documents,
    selected_years,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import PDF_VERSION, digest
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.batch import Batch, RunAbort
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.catalog import PostgresTextCatalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunk_catalog import (
    PostgresChunkCatalog,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text_replay import _inventory
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest
from market_data_pipeline.shared.files import utc_timestamp


def validate_run(
    cfg, settings, *, start_year, end_year, mode="backfill", live=False, scheduled=False
):
    if live is not True or (scheduled and not settings.scheduled_live_access):
        raise ValueError("FOMC live/scheduled access gate is closed")
    if type(scheduled) is not bool or mode not in {"backfill", "refresh"}:
        raise ValueError("FOMC operation mode/access setting is invalid")
    years = selected_years(start_year, end_year, cfg.start_year, settings)
    try:
        installed = version("pypdf")
    except PackageNotFoundError:
        installed = None
    if installed != PDF_VERSION:
        raise ValueError("install the pinned FOMC text dependency before live acquisition")
    return years


def run_operation(
    cfg,
    settings,
    *,
    inventory_store,
    start_year,
    end_year,
    mode="backfill",
    live=False,
    scheduled=False,
    now=None,
    monotonic=None,
    sleep=None,
):
    years = validate_run(
        cfg,
        settings,
        start_year=start_year,
        end_year=end_year,
        mode=mode,
        live=live,
        scheduled=scheduled,
    )
    manifest = load_manifest(cfg.manifest)
    store = inventory_store
    audit = OperationStore(store)
    clock = now or (lambda: datetime.now(UTC))
    lock = "fomc-operation:" + store.config.schema
    if not store.connection.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (lock,)
    ).fetchone()[0]:
        raise ValueError("another FOMC operation is already running")
    try:
        before = audit.pointers()
        run_id = audit.begin(
            {
                "started_at": utc_timestamp(clock()),
                "manifest_sha256": digest(manifest.to_dict()),
                "years": list(years),
                "mode": mode,
                "scheduled": scheduled,
                "settings": settings.to_dict(),
                "pointers_before": before,
            },
            root=cfg.root,
        )
        batch = Batch(
            cfg,
            settings,
            store,
            manifest,
            years,
            mode,
            run_id,
            now=clock,
            monotonic=monotonic,
            sleep=sleep,
        )

        def result(status, error_code, after):
            return {
                "schema_id": "fomc-operation-result/1",
                "run_id": run_id,
                "status": status,
                "error_code": error_code,
                "finished_at": utc_timestamp(clock()),
                "years": list(years),
                "mode": mode,
                "requests": batch.run.requests if batch.run else 0,
                "reserved_probe_requests": 2,
                "probe_requests": 1 + int(batch.run.client.fallback_used) if batch.run else None,
                "received_bytes": batch.received,
                "received_bytes_complete": not batch.unknown_capture_bytes,
                "startup_attempt_id": batch.startup_attempt_id,
                "selected_documents": batch.selected_documents,
                "document_outcomes": dict(batch.outcomes),
                "source_outcomes": batch.source_outcomes,
                "excluded_prior_captures": batch.excluded,
                "pointers_before": before,
                "pointers_after": after,
                "full_history_complete": False,
                "scheduled": scheduled,
            }

        completed = None
        phase = "preflight"
        try:
            batch.load_cache()
            if before["text"]:
                batch.text_catalog.read(manifest, root=cfg.root)
            phase = "startup"
            batch.start()
            phase = "acquisition"
            documents = batch.inventories()
            for document in documents:
                batch.visit(document.source_url, "document")
            phase = "publication"

            def finalize(after):
                nonlocal completed
                completed = result("completed", None, after)
                audit.append(run_id, completed, root=cfg.root, kind="result")

            batch.publish(before, finalize)
            return completed
        except Exception as exc:
            code = (
                str(exc)
                if isinstance(exc, RunAbort)
                else {
                    "preflight": "local_preflight_failed",
                    "startup": "startup_failed",
                    "acquisition": "acquisition_failed",
                    "publication": "publication_failed",
                }[phase]
            )
            failed = result("partial" if phase != "preflight" else "failed", code, audit.pointers())
            try:
                audit.append(run_id, failed, root=cfg.root, kind="result")
            except Exception:
                failed["audit_recovery_required"] = True
            return failed
    finally:
        try:
            store.connection.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (lock,))
        except Exception:
            pass


def plan(cfg, settings, *, inventory_store, start_year, end_year, mode="backfill"):
    years = selected_years(start_year, end_year, cfg.start_year, settings)
    if mode not in {"backfill", "refresh"}:
        raise ValueError("invalid FOMC operation mode")
    inventory = inventory_store.read_inventory()
    documents = select_documents(_inventory(inventory), years, settings) if inventory else ()
    return {
        "schema_id": "fomc-operation-plan/1",
        "years": list(years),
        "mode": mode,
        "known_documents": len(documents),
        "document_urls": [d.source_url for d in documents],
        "inventory_bootstrap_required": inventory is None,
        "settings": settings.to_dict(),
        "request_performed": False,
        "source_completeness_certified": False,
    }


def health(cfg, settings, *, inventory_store, now=None):
    manifest = load_manifest(cfg.manifest)
    store = inventory_store
    audit = OperationStore(store)
    pointers = audit.pointers()
    latest = audit.latest(root=cfg.root)
    text = PostgresTextStore(PostgresBronzeStore(store))
    tc = PostgresTextCatalog(text)
    cc = PostgresChunkCatalog(PostgresChunkStore(text))
    inventory = store.read_inventory()
    text_value = tc.read(manifest, root=cfg.root)
    chunks = cc.read(manifest, root=cfg.root)
    issues = []
    if any(pointers.values()) and not all(pointers.values()):
        issues.append("incomplete_catalog_chain")
    if text_value and text_value["inventory_run_id"] != pointers["inventory"]:
        issues.append("inventory_text_mismatch")
    if chunks and chunks["text_catalog_id"] != pointers["text"]:
        issues.append("text_chunk_mismatch")
    final = latest["result"] if latest else None
    last_status = final["status"] if final else "interrupted" if latest else "not_run"
    if latest and last_status != "completed":
        issues.append("latest_operation_incomplete")
    age = None
    if final:
        finished = datetime.fromisoformat(final["finished_at"].replace("Z", "+00:00"))
        age = max(0, ((now or datetime.now(UTC)) - finished).total_seconds() / 3600)
        if age > settings.max_run_age_hours:
            issues.append("operation_stale")
    status = (
        "degraded"
        if issues
        else "empty"
        if not inventory
        else "healthy"
        if final
        else "ready_not_run"
    )
    return {
        "schema_id": "fomc-health/1",
        "status": status,
        "issues": issues,
        "pointers": pointers,
        "last_operation_status": last_status,
        "last_operation_id": latest["intent"]["run_id"] if latest else None,
        "last_operation_age_hours": age,
        "inventory_meetings": len(inventory["meetings"]) if inventory else 0,
        "inventory_document_references": len(inventory["documents"]) if inventory else 0,
        "source_gap_outcomes": dict(Counter(g["outcome"] for g in text_value["gaps"]))
        if text_value
        else {},
        "chunk_coverage": chunks["coverage"] if chunks else None,
        "request_performed": False,
        "full_history_complete": False,
        "text_or_ocr_accuracy_certified": False,
    }
