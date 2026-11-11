"""FOMC-AC11: real storage in isolated FOMC schemas, never existing pipeline tables."""

import json
import os
import uuid
from copy import deepcopy
from dataclasses import replace
from datetime import UTC
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import inventory_from_dict
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

pytestmark = pytest.mark.postgres


@pytest.fixture
def store():
    path = os.environ.get("FOMC_TEST_POSTGRES_CONFIG")
    if not path:
        pytest.skip("set FOMC_TEST_POSTGRES_CONFIG for isolated-schema PostgreSQL tests")
    from psycopg import sql

    cfg = load_config(Path(path))
    settings = replace(cfg.postgres, schema="fomc_meeting_minutes_test_" + uuid.uuid4().hex[:12])
    with PostgresInventoryStore(settings) as catalog:
        catalog.initialize()
        try:
            yield catalog
        finally:
            catalog.connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(settings.schema))
            )


def test_fomc_ac11_real_postgres_inventory_snapshots_idempotence_and_rollback(store):
    import psycopg

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    value = json.loads(
        Path("tests/fixtures/fomc_meeting_minutes/invented-current.json").read_text()
    )
    first = inventory_from_dict(value, manifest)
    run_id = str(uuid.uuid4())
    saved = store.save_inventory(first, manifest, input_kind="invented_fixture", run_id=run_id)
    assert saved["inserted"] is True and saved["recorded_at"].utcoffset() is not None
    assert store.read_inventory() == first.to_dict()
    from psycopg import sql

    rows = store.connection.execute(
        sql.SQL(
            "SELECT meeting_id,committee_body,event_kind,meeting_dates,session_label_raw "
            "FROM {}.inventory_meetings WHERE run_id=%s ORDER BY meeting_id"
        ).format(sql.Identifier(store.config.schema)),
        (run_id,),
    ).fetchall()
    assert rows == [
        (m.meeting_id, m.committee_body, m.event_kind, list(m.meeting_dates), m.session_label_raw)
        for m in first.meetings
    ]
    rows = store.connection.execute(
        sql.SQL(
            "SELECT meeting_id,document_id,source_page_url,meeting_label_raw,minutes_status,"
            "document_label_raw,href_raw,provider_release_date "
            "FROM {}.inventory_entries WHERE run_id=%s ORDER BY ordinal"
        ).format(sql.Identifier(store.config.schema)),
        (run_id,),
    ).fetchall()
    assert rows == [
        (
            e.meeting_id,
            e.document_id,
            e.source_page_url,
            e.meeting_label_raw,
            e.minutes_status,
            e.document_label_raw,
            e.href_raw,
            e.provider_release_date,
        )
        for e in first.entries
    ]
    repeated = store.save_inventory(first, manifest, input_kind="invented_fixture", run_id=run_id)
    assert repeated["inserted"] is False and repeated["recorded_at"] == saved["recorded_at"]
    changed = deepcopy(value)
    changed["pages"][0]["meetings"][0]["documents"][0]["provider_release_date"] = "2026-04-09"
    revised = inventory_from_dict(changed, manifest)
    store.save_inventory(revised, manifest, input_kind="invented_fixture")
    assert (
        store.read_inventory() == revised.to_dict()
        and store.read_inventory(run_id) == first.to_dict()
    )
    store.save_inventory(first, manifest, input_kind="invented_fixture", run_id=run_id)
    assert store.read_inventory() == revised.to_dict(), (
        "retrying an old run must not roll back the pointer"
    )
    with pytest.raises(ValueError):
        store.save_inventory(revised, manifest, input_kind="invented_fixture", run_id=run_id)
    bad = replace(
        first,
        entries=(replace(first.entries[0], meeting_id="invented-nonexistent-meeting"),)
        + first.entries[1:],
    )
    with pytest.raises(psycopg.Error):
        store.save_inventory(bad, manifest, input_kind="invented_fixture")
    assert store.read_inventory() == revised.to_dict(), (
        "failed transaction advanced current inventory"
    )
    assert store.run_count() == 2


def test_fomc_ac11_real_postgres_reinitialization_keeps_current_state(store):
    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    value = json.loads(
        Path("tests/fixtures/fomc_meeting_minutes/invented-historical.json").read_text()
    )
    inventory = inventory_from_dict(value, manifest)
    store.save_inventory(inventory, manifest, input_kind="invented_fixture")
    store.initialize()
    assert store.read_inventory() == inventory.to_dict() and store.run_count() == 1


def test_fomc_ac11_real_postgres_detects_snapshot_corruption(store):
    from psycopg import sql

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    value = json.loads(
        Path("tests/fixtures/fomc_meeting_minutes/invented-current.json").read_text()
    )
    inventory = inventory_from_dict(value, manifest)
    store.save_inventory(inventory, manifest, input_kind="invented_fixture")
    with pytest.raises(RuntimeError, match="restore isolated test mutation"):
        with store.connection.transaction():
            store.connection.execute(
                sql.SQL("UPDATE {}.inventory_runs SET inventory_json='{}'::jsonb").format(
                    sql.Identifier(store.config.schema), sql.SQL("{}")
                )
            )
            with pytest.raises(ValueError, match="hash mismatch"):
                store.read_inventory()
            raise RuntimeError("restore isolated test mutation")
    assert store.read_inventory() == inventory.to_dict()


def test_fomc_ac11_real_postgres_empty_inventory_is_retained_without_document_claims(store):
    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    value = {
        "schema_id": manifest.schema_id,
        "pages": [
            {
                "source_page_url": "https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm",
                "meetings": [],
            }
        ],
    }
    inventory = inventory_from_dict(value, manifest)
    store.save_inventory(inventory, manifest, input_kind="invented_fixture")
    assert store.read_inventory() == inventory.to_dict() and store.run_count() == 1


def test_fomc_ac11_real_postgres_concurrent_identical_run_is_one_import(store):
    from concurrent.futures import ThreadPoolExecutor

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    value = json.loads(
        Path("tests/fixtures/fomc_meeting_minutes/invented-current.json").read_text()
    )
    inventory = inventory_from_dict(value, manifest)
    identity = str(uuid.uuid4())

    def save(_):
        with PostgresInventoryStore(store.config) as independent:
            return independent.save_inventory(
                inventory, manifest, input_kind="invented_fixture", run_id=identity
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(save, range(2)))
    assert sorted(row["inserted"] for row in results) == [False, True]
    assert results[0]["recorded_at"] == results[1]["recorded_at"]
    assert store.run_count() == 1 and store.read_inventory() == inventory.to_dict()


def test_fomc_bronze_real_postgres_migration_and_byte_exact_retries(store, tmp_path):
    from datetime import datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
        commit_capture,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    bronze.migrate()
    original = b"\x00\xffinvented-document\n"
    receipt = capture(
        tmp_path,
        original,
        manifest=manifest,
        store=bronze,
        source_url="https://www.federalreserve.gov/monetarypolicy/files/invented.pdf",
        kind="document",
        captured_at=datetime.now(UTC),
        status=200,
        headers={"Content-Type": "application/pdf"},
        max_bytes=100,
    )
    assert bronze.read_capture(receipt["capture_id"]) == (receipt, original)
    commit_capture(tmp_path, receipt["capture_id"], bronze)
    changed = dict(
        receipt, source_url="https://www.federalreserve.gov/monetarypolicy/files/other.pdf"
    )
    with pytest.raises(ValueError):
        bronze.save_capture(changed, original)
    assert store.read_inventory() is None
    assert bronze.read_capture(receipt["capture_id"]) == (receipt, original)


def test_fomc_bronze_links_captures_to_inventory_and_bundled_meetings(store, tmp_path):
    from datetime import datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    value = json.loads(
        Path("tests/fixtures/fomc_meeting_minutes/invented-historical.json").read_text()
    )
    inventory = inventory_from_dict(value, manifest)
    run = store.save_inventory(inventory, manifest, input_kind="invented_fixture")["run_id"]
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    page = inventory.source_pages[0]
    doc = next(d for d in inventory.documents if d.source_url.endswith("/invented-bundle.pdf"))
    capture_page = capture(
        tmp_path,
        b"inventory",
        manifest=manifest,
        store=bronze,
        source_url=page,
        kind="inventory",
        captured_at=datetime.now(UTC),
        status=200,
        headers={},
        max_bytes=100,
    )
    capture_doc = capture(
        tmp_path,
        b"bundled",
        manifest=manifest,
        store=bronze,
        source_url=doc.source_url,
        kind="document",
        captured_at=datetime.now(UTC),
        status=200,
        headers={},
        max_bytes=100,
    )
    bronze.link_inventory(capture_page["capture_id"], run)
    bronze.link_inventory(capture_page["capture_id"], run)
    bronze.link_document(capture_doc["capture_id"], run, doc.document_id)
    bronze.link_document(capture_doc["capture_id"], run, doc.document_id)
    linked = bronze.document_meetings(capture_doc["capture_id"])
    assert {row["meeting_id"] for row in linked} == {
        e.meeting_id for e in inventory.entries if e.document_id == doc.document_id
    }
    assert len(linked) == 2
    with pytest.raises(ValueError):
        bronze.link_document(capture_page["capture_id"], run, doc.document_id)
    with pytest.raises(ValueError):
        bronze.link_inventory(capture_doc["capture_id"], run)


def test_fomc_native_publication_refuses_issues_and_rolls_back_on_link_failure(
    store, tmp_path, monkeypatch
):
    from datetime import UTC, datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    page = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    template = b"""<div class="panel"><div class="panel-heading"><h4>2026 FOMC Meetings</h4></div>
    <div class="fomc-meeting"><div class="fomc-meeting__month">June</div>
    <div class="fomc-meeting__date">%s</div><div class="fomc-meeting__minutes"></div></div></div>"""

    def page_capture(day):
        return capture(
            tmp_path,
            template % day,
            manifest=manifest,
            store=bronze,
            source_url=page,
            kind="inventory",
            captured_at=datetime.now(UTC),
            status=200,
            headers={},
            max_bytes=1000,
        )["capture_id"]

    invalid = page_capture(b"16 (notation vote)")
    with pytest.raises(ValueError, match="issues"):
        bronze.publish_inventory_capture(invalid, manifest, root=tmp_path)
    assert store.run_count() == 0 and store.read_inventory() is None
    good = page_capture(b"16-17")
    link = bronze.link_inventory

    def fail_link(*args):
        raise RuntimeError("simulated link failure")

    monkeypatch.setattr(bronze, "link_inventory", fail_link)
    with pytest.raises(RuntimeError):
        bronze.publish_inventory_capture(good, manifest, root=tmp_path)
    assert store.run_count() == 0 and store.read_inventory() is None
    monkeypatch.setattr(bronze, "link_inventory", link)
    result = bronze.publish_inventory_capture(good, manifest, root=tmp_path)
    assert result["inserted"] and store.run_count() == 1
    assert store.read_inventory(result["run_id"]) == store.read_inventory()


def test_fomc_multipage_publication_rejects_partial_replacement(store, tmp_path):
    from datetime import UTC, datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    base = "https://www.federalreserve.gov/monetarypolicy/"
    samples = [
        (
            "fomccalendars.htm",
            b"""<div class="panel"><div class="panel-heading">
        <h4>2026 FOMC Meetings</h4></div><div class="fomc-meeting">
        <div class="fomc-meeting__month">June</div><div class="fomc-meeting__date">16-17</div>
        <div class="fomc-meeting__minutes"></div></div></div>""",
        ),
        (
            "fomchistorical1936.htm",
            b"""<div class="panel"><div class="panel-heading">
        <h5>May 25 Meeting - 1936</h5></div><div class="panel-body">
        <p><a href="/monetarypolicy/files/fake.pdf">Historical Minutes</a></p></div></div>""",
        ),
    ]
    ids = []
    for name, content in samples:
        item = capture(
            tmp_path,
            content,
            manifest=manifest,
            store=bronze,
            source_url=base + name,
            kind="inventory",
            captured_at=datetime.now(UTC),
            status=200,
            headers={},
            max_bytes=1000,
        )
        ids.append(item["capture_id"])
    saved = bronze.publish_inventory_captures(ids, manifest, root=tmp_path)
    inventory = store.read_inventory()
    assert len(inventory["source_pages"]) == 2 and len(inventory["meetings"]) == 2
    with pytest.raises(ValueError, match="drop"):
        bronze.publish_inventory_capture(ids[0], manifest, root=tmp_path)
    assert store.read_inventory() == inventory and store.run_count() == 1
    assert saved["inserted"]
