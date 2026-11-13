"""FOMC-QAC01/04/12: verified offline reports in isolated backend schemas."""

import pytest
from test_fomc_operation_postgres import operation_archive as operation_archive
from test_fomc_postgres import store as store

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality import (
    quality_report,
    write_report,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.operations import run_operation

__all__ = ["store", "operation_archive"]
pytestmark = pytest.mark.postgres


def test_report_reads_verified_generations_without_network_or_source_writes(
    operation_archive, monkeypatch, tmp_path
):
    store, cfg, settings, payloads, calls, probes, responses = operation_archive
    run_operation(cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True)
    before = OperationStore(store).pointers()
    n_calls = len(calls)
    n_probes = len(probes)

    def forbidden(*args, **kwargs):
        raise AssertionError("quality attempted a transform or network")

    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition.prepare_fomc_http",
        forbidden,
    )
    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.data.text.extract_text", forbidden
    )
    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks.chunk_text", forbidden
    )
    report = quality_report(cfg, inventory_store=store)
    assert report["totals"]["selected_versions"] == 3
    assert report["totals"]["review_versions"] == 1
    assert OperationStore(store).pointers() == before
    assert len(calls) == n_calls and len(probes) == n_probes
    path = tmp_path / "reports" / "quality.json"
    write_report(cfg, path, report)
    assert path.exists()
    assert write_report(cfg, path, report) == path
    changed = {**report, "report_id": "0" * 64}
    with pytest.raises(ValueError):
        write_report(cfg, path, changed)
    for target in [cfg.root / "text" / "report.json", cfg.root / "objects" / "x.json"]:
        with pytest.raises(ValueError):
            write_report(cfg, target, report)


def test_corrupt_original_or_mirror_refuses_quality_report(operation_archive):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import artifact_path

    store, cfg, settings, *_ = operation_archive
    run_operation(cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True)
    report = quality_report(cfg, inventory_store=store)
    identity = report["sources"][0]["extraction_id"]
    path = artifact_path(cfg.root, "extractions", identity)
    original = path.read_bytes()
    path.write_bytes(b"{}")
    try:
        with pytest.raises(ValueError):
            quality_report(cfg, inventory_store=store)
    finally:
        path.write_bytes(original)
    assert quality_report(cfg, inventory_store=store) == report


def test_busy_source_lock_and_incoherent_pointers_are_refused(operation_archive, monkeypatch):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import (
        PostgresInventoryStore,
    )

    store, cfg, settings, *_ = operation_archive
    run_operation(cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True)
    # An independent session holds the same run-scoped lock.
    with PostgresInventoryStore(store.config) as other:
        other.connection.execute(
            "SELECT pg_advisory_lock(hashtextextended(%s,0))",
            ("fomc-operation:" + store.config.schema,),
        )
        with pytest.raises(ValueError, match="running"):
            quality_report(cfg, inventory_store=store)
    saved = OperationStore.pointers

    def incoherent(self):
        return {**saved(self), "text": "0" * 64}

    monkeypatch.setattr(OperationStore, "pointers", incoherent)
    with pytest.raises(ValueError):
        quality_report(cfg, inventory_store=store)


def test_cli_reports_to_explicit_destination(operation_archive, monkeypatch, tmp_path, capsys):
    import json
    from contextlib import nullcontext

    from market_data_pipeline.pipelines.fomc_meeting_minutes import cli

    store, cfg, settings, *_ = operation_archive
    run_operation(cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True)
    monkeypatch.setattr(cli, "load_config", lambda path: cfg)
    monkeypatch.setattr(cli, "PostgresInventoryStore", lambda config: nullcontext(store))
    path = tmp_path / "reports" / "cli.json"
    assert (
        cli.main(
            [
                "quality-report",
                "--config",
                "unused.yml",
                "--operations",
                "config/fomc_meeting_minutes/operations.example.yml",
                "--output",
                str(path),
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "reported" and result["request_performed"] is False
    assert json.loads(path.read_text())["report_id"] == result["report_id"]


def test_snapshot_refuses_accidental_writes_and_releases_lock(operation_archive):
    import psycopg

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality import source_snapshot

    store, *_ = operation_archive
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        with source_snapshot(store):
            store.connection.execute(store._query("DELETE FROM {schema}.operation_records"))
    with source_snapshot(store):
        assert store.connection.execute("SHOW transaction_read_only").fetchone() == ("on",)
