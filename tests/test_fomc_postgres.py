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

    invalid = page_capture(b"16 (unreviewed vote)")
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


@pytest.fixture
def discovered_captures(store, tmp_path):
    from datetime import datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    base = "https://www.federalreserve.gov/monetarypolicy/"
    samples = {
        "fomc_historical_year.htm": Path(
            "tests/fixtures/fomc_meeting_minutes/invented-year-index.html"
        ).read_bytes(),
        "fomccalendars.htm": b"""<div class="panel">
        <h4 class="panel-heading">1938 FOMC Meetings</h4>
        <div class="fomc-meeting"><div class="fomc-meeting__month">June</div>
        <div class="fomc-meeting__date">16-17</div></div></div>""",
    }
    for year in (1936, 1937):
        samples[f"fomchistorical{year}.htm"] = f"""<div class="panel">
        <h5 class="panel-heading">May 25 Meeting - {year}</h5><div class="panel-body">
        <p><a href="/monetarypolicy/files/fake{year}.pdf">Historical Minutes</a></p>
        </div></div>""".encode()
    captures = {}
    for name, content in samples.items():
        captures[name] = capture(
            tmp_path,
            content,
            manifest=manifest,
            store=bronze,
            source_url=base + name,
            kind="inventory",
            captured_at=datetime.now(UTC),
            status=200,
            headers={},
            max_bytes=2000,
        )["capture_id"]
    return bronze, manifest, captures


def test_fomc_discovered_publication_requires_every_expected_page(
    store, tmp_path, discovered_captures
):
    bronze, manifest, captures = discovered_captures
    ids = list(captures.values())
    with pytest.raises(ValueError, match="expected pages"):
        bronze.publish_inventory_captures(
            ids[:-1], manifest, root=tmp_path, historical_years=(1936, 1938)
        )
    assert store.run_count() == 0 and store.read_inventory() is None
    result = bronze.publish_inventory_captures(
        ids, manifest, root=tmp_path, historical_years=(1936, 1938)
    )
    evidence = bronze.read_inventory_discovery(result["run_id"])
    assert evidence["discovery"]["historical_years"] == [1936, 1937]
    assert evidence["discovery"]["calendar_years"] == [1938]
    assert len(evidence["discovery"]["expected_pages"]) == 4
    assert set(evidence["capture_ids"].values()) == set(ids)
    assert evidence["coverage_kind"] == "selected_inventory_pages"
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_inventory_links")
        ).fetchone()[0]
        == 4
    )
    assert len(store.read_inventory()["meetings"]) == 3


def test_fomc_discovery_publication_rollback_keeps_evidence_and_pointer_atomic(
    store, tmp_path, discovered_captures, monkeypatch
):
    bronze, manifest, captures = discovered_captures

    def fail_link(*args):
        raise RuntimeError("simulated discovery link failure")

    monkeypatch.setattr(bronze, "link_inventory", fail_link)
    with pytest.raises(RuntimeError, match="discovery link failure"):
        bronze.publish_inventory_captures(
            list(captures.values()), manifest, root=tmp_path, historical_years=(1936, 1938)
        )
    assert store.run_count() == 0 and store.read_inventory() is None
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_inventory_discovery")
        ).fetchone()[0]
        == 0
    )


def test_fomc_discovery_requires_both_roots_and_explicit_scope(
    store, tmp_path, discovered_captures
):
    bronze, manifest, captures = discovered_captures
    with pytest.raises(ValueError, match="explicit historical"):
        bronze.publish_inventory_captures(list(captures.values()), manifest, root=tmp_path)
    for omitted in ("fomc_historical_year.htm", "fomccalendars.htm"):
        ids = [identity for name, identity in captures.items() if name != omitted]
        with pytest.raises(ValueError, match="both source inventories"):
            bronze.publish_inventory_captures(
                ids, manifest, root=tmp_path, historical_years=(1936, 1938)
            )
    assert store.run_count() == 0


def test_fomc_discovery_migration_upgrades_owned_state_and_refuses_damage(store, tmp_path):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        DISCOVERY_MIGRATION,
        PostgresBronzeStore,
    )

    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    bronze.migrate()
    store.connection.execute(store._query("DROP TABLE {schema}.bronze_inventory_discovery"))
    with pytest.raises(ValueError, match="incomplete.*discovery"):
        bronze.migrate()
    store.connection.execute(
        store._query("DELETE FROM {schema}.bronze_schema_migrations WHERE migration_id=%s"),
        (DISCOVERY_MIGRATION,),
    )
    bronze.migrate()
    assert store.run_count() == 0
    store.connection.execute(
        store._query("DELETE FROM {schema}.bronze_schema_migrations WHERE migration_id=%s"),
        (DISCOVERY_MIGRATION,),
    )
    with pytest.raises(ValueError, match="unowned.*discovery"):
        bronze.migrate()
    assert bronze.read_inventory_discovery(str(uuid.uuid4())) is None


def test_fomc_discovery_evidence_corruption_is_detected(store, tmp_path, discovered_captures):
    bronze, manifest, captures = discovered_captures
    result = bronze.publish_inventory_captures(
        list(captures.values()), manifest, root=tmp_path, historical_years=(1936, 1938)
    )
    evidence = bronze.read_inventory_discovery(result["run_id"])
    assert evidence["discovery"]["issues"] == []
    with pytest.raises(RuntimeError, match="restore isolated mutation"):
        with store.connection.transaction():
            store.connection.execute(
                store._query(
                    "UPDATE {schema}.bronze_inventory_discovery SET evidence='{{}}'::jsonb"
                )
            )
            with pytest.raises(ValueError, match="discovery evidence hash"):
                bronze.read_inventory_discovery(result["run_id"])
            raise RuntimeError("restore isolated mutation")
    assert bronze.read_inventory_discovery(result["run_id"]) == evidence


def test_fomc_discovery_evidence_write_failure_rolls_back_entire_publication(
    store, tmp_path, discovered_captures
):
    from psycopg.errors import CheckViolation

    bronze, manifest, captures = discovered_captures
    store.connection.execute(
        store._query(
            "ALTER TABLE {schema}.bronze_inventory_discovery "
            "ADD CONSTRAINT simulated_rejection CHECK(false) NOT VALID"
        )
    )
    with pytest.raises(CheckViolation):
        bronze.publish_inventory_captures(
            list(captures.values()), manifest, root=tmp_path, historical_years=(1936, 1938)
        )
    assert store.run_count() == 0 and store.read_inventory() is None
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_inventory_links")
        ).fetchone()[0]
        == 0
    )
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_inventory_discovery")
        ).fetchone()[0]
        == 0
    )
    store.connection.execute(
        store._query(
            "ALTER TABLE {schema}.bronze_inventory_discovery DROP CONSTRAINT simulated_rejection"
        )
    )
    saved = bronze.publish_inventory_captures(
        list(captures.values()), manifest, root=tmp_path, historical_years=(1936, 1938)
    )
    assert bronze.read_inventory_discovery(saved["run_id"])


@pytest.fixture
def dependency_archive(store, tmp_path):
    from datetime import datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
        PostgresDependencyStore,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    dependencies = PostgresDependencyStore(bronze)
    dependencies.migrate()
    dependencies.migrate()
    base = "https://www.federalreserve.gov/monetarypolicy/"
    samples = {
        "parent": (
            base + "parent.htm",
            "document",
            b"""<div id="article">
        <a href="addendum.htm">Addendum to these minutes</a>
        <img src="files/figure.png" alt="Invented chart"></div>""",
        ),
        "addendum": (
            base + "addendum.htm",
            "document",
            b'<div id="article">Invented addendum</div>',
        ),
        "figure": (base + "files/figure.png", "asset", b"\x89PNG\r\n\x1a\ninvented"),
    }
    ids = {}
    for name, (url, kind, payload) in samples.items():
        ids[name] = capture(
            tmp_path,
            payload,
            manifest=manifest,
            store=bronze,
            source_url=url,
            kind=kind,
            captured_at=datetime.now(UTC),
            status=200,
            headers={},
            max_bytes=2000,
        )["capture_id"]
    return bronze, dependencies, manifest, ids


def test_fomc_dependency_links_require_exact_archives_and_retain_parentage(
    store, tmp_path, dependency_archive
):
    bronze, dependencies, manifest, ids = dependency_archive
    with pytest.raises(ValueError, match="expected dependencies"):
        dependencies.commit(ids["parent"], [ids["addendum"]], manifest, root=tmp_path)
    check = dependencies.commit(
        ids["parent"], [ids["addendum"], ids["figure"]], manifest, root=tmp_path
    )
    assert (
        dependencies.commit(
            ids["parent"], [ids["figure"], ids["addendum"]], manifest, root=tmp_path
        )
        == check
    )
    evidence = dependencies.read_check(check)
    assert evidence["parent_capture_id"] == ids["parent"]
    assert {d["relation"] for d in evidence["dependencies"]} == {
        "minutes_addendum",
        "content_asset",
    }
    assert len(evidence["dependencies"]) == 2
    assert store.run_count() == 0 and store.read_inventory() is None
    # A completed direct dependency check is not a recursive whole-archive claim.
    leaf = dependencies.commit(ids["addendum"], [], manifest, root=tmp_path)
    assert dependencies.read_check(leaf)["dependencies"] == []


def test_fomc_dependency_link_failure_is_atomic(store, tmp_path, dependency_archive):
    from psycopg.errors import CheckViolation

    bronze, dependencies, manifest, ids = dependency_archive
    store.connection.execute(
        store._query(
            "ALTER TABLE {schema}.bronze_dependencies "
            "ADD CONSTRAINT simulated_failure CHECK(false) NOT VALID"
        )
    )
    with pytest.raises(CheckViolation):
        dependencies.commit(
            ids["parent"], [ids["addendum"], ids["figure"]], manifest, root=tmp_path
        )
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_dependency_checks")
        ).fetchone()[0]
        == 0
    )
    assert bronze.read_capture(ids["parent"])


def test_fomc_dependency_tampering_blocks_link_and_check_read(store, tmp_path, dependency_archive):
    bronze, dependencies, manifest, ids = dependency_archive
    check = dependencies.commit(
        ids["parent"], [ids["addendum"], ids["figure"]], manifest, root=tmp_path
    )
    store.connection.execute(
        store._query("UPDATE {schema}.bronze_dependency_checks SET evidence='{{}}'::jsonb")
    )
    with pytest.raises(ValueError, match="hash"):
        dependencies.read_check(check)
    payload_path = tmp_path / "captures" / ids["figure"] / "payload.bin"
    payload_path.write_bytes(b"corruption")
    with pytest.raises(ValueError):
        dependencies.commit(
            ids["parent"], [ids["addendum"], ids["figure"]], manifest, root=tmp_path
        )


def test_fomc_dependency_assets_inherit_all_bundled_meeting_associations(
    store, tmp_path, dependency_archive
):
    bronze, dependencies, manifest, ids = dependency_archive
    dependencies.commit(ids["parent"], [ids["addendum"], ids["figure"]], manifest, root=tmp_path)
    page = {
        "source_page_url": "https://www.federalreserve.gov/monetarypolicy/fomchistorical1936.htm",
        "meetings": [],
    }
    for day in ("1936-06-01", "1936-07-01"):
        page["meetings"].append(
            {
                "committee_body": "fomc",
                "event_kind": "meeting",
                "meeting_dates": [day],
                "meeting_label_raw": day,
                "session_label_raw": None,
                "minutes_status": "listed",
                "documents": [
                    {
                        "document_kind": "minutes",
                        "document_label_raw": "Invented minutes",
                        "href": "/monetarypolicy/parent.htm",
                        "representation": "html",
                        "provider_release_date": None,
                    }
                ],
            }
        )
    inventory = inventory_from_dict({"schema_id": manifest.schema_id, "pages": [page]}, manifest)
    run = store.save_inventory(inventory, manifest, input_kind="invented_fixture")
    bronze.link_document(ids["parent"], run["run_id"], inventory.documents[0].document_id)
    inherited = dependencies.meeting_associations(ids["figure"])
    assert {r["meeting_id"] for r in inherited} == {m.meeting_id for m in inventory.meetings}
    assert len(inherited) == 2


def test_fomc_dependency_migration_refuses_partial_schema(store, dependency_archive):
    bronze, dependencies, manifest, ids = dependency_archive
    store.connection.execute(store._query("DROP TABLE {schema}.bronze_dependencies"))
    with pytest.raises(ValueError, match="incomplete"):
        dependencies.migrate()


def test_fomc_dependency_rejects_html_login_instead_of_declared_pdf(
    store, tmp_path, dependency_archive
):
    from datetime import datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import capture

    bronze, dependencies, manifest, ids = dependency_archive
    base = "https://www.federalreserve.gov/monetarypolicy/"
    parent = capture(
        tmp_path,
        b'<div id="article"><a href="addendum.pdf">Addendum to these minutes</a></div>',
        manifest=manifest,
        store=bronze,
        source_url=base + "pdf-parent.htm",
        kind="document",
        captured_at=datetime.now(UTC),
        status=200,
        headers={},
        max_bytes=1000,
    )
    wrong = capture(
        tmp_path,
        b"<html><form>Log in</form></html>",
        manifest=manifest,
        store=bronze,
        source_url=base + "addendum.pdf",
        kind="document",
        captured_at=datetime.now(UTC),
        status=200,
        headers={},
        max_bytes=1000,
    )
    with pytest.raises(ValueError, match="PDF container"):
        dependencies.commit(parent["capture_id"], [wrong["capture_id"]], manifest, root=tmp_path)
    assert bronze.read_capture(wrong["capture_id"])[1] == b"<html><form>Log in</form></html>"
