"""FOMC-AC05/12: immutable full-text mirrors and atomic publication/replay."""

import pytest
from test_fomc_postgres import dependency_archive as dependency_archive
from test_fomc_postgres import store as store

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore

__all__ = ["dependency_archive", "store"]
pytestmark = pytest.mark.postgres


@pytest.fixture
def text_archive(store, tmp_path, dependency_archive):
    bronze, deps, manifest, ids = dependency_archive
    deps.commit(ids["parent"], [ids["addendum"], ids["figure"]], manifest, root=tmp_path)
    text = PostgresTextStore(bronze)
    text.migrate()
    text.migrate()
    return text, manifest, ids


def test_text_mirror_readback_idempotence_and_corruption_refusal(text_archive, tmp_path):
    text, manifest, ids = text_archive
    result = text.extract_capture(ids["parent"], manifest, root=tmp_path)
    assert text.read_extraction(result["extraction_id"], root=tmp_path) == result
    assert text.extract_capture(ids["parent"], manifest, root=tmp_path) == result
    path = tmp_path / "text" / "extractions" / (result["extraction_id"] + ".json")
    data = path.read_bytes()
    path.write_bytes(data + b"corruption")
    with pytest.raises(ValueError, match="mirror"):
        text.read_extraction(result["extraction_id"], root=tmp_path)
    path.write_bytes(data)
    conn, store = text.store.connection, text.store
    conn.execute(
        store._query("UPDATE {schema}.text_units SET unit_json=%s WHERE extraction_id=%s"),
        (store._Jsonb({}), result["extraction_id"]),
    )
    with pytest.raises(ValueError, match="unit"):
        text.read_extraction(result["extraction_id"], root=tmp_path)


def test_failed_database_save_leaves_recoverable_local_derivative(text_archive, tmp_path):
    from psycopg import errors

    text, manifest, ids = text_archive
    conn, store = text.store.connection, text.store
    conn.execute(
        store._query(
            "ALTER TABLE {schema}.text_capture_links ADD CONSTRAINT fail_test CHECK(false)"
        )
    )
    with pytest.raises(errors.CheckViolation):
        text.extract_capture(ids["addendum"], manifest, root=tmp_path)
    assert list((tmp_path / "text" / "extractions").glob("*.json"))
    assert (
        conn.execute(store._query("SELECT count(*) FROM {schema}.text_extractions")).fetchone()[0]
        == 0
    )
    conn.execute(store._query("ALTER TABLE {schema}.text_capture_links DROP CONSTRAINT fail_test"))
    result = text.extract_capture(ids["addendum"], manifest, root=tmp_path)
    assert text.read_extraction(result["extraction_id"], root=tmp_path) == result


@pytest.fixture
def published_text(text_archive, tmp_path):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import (
        inventory_from_dict,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.catalog import PostgresTextCatalog

    text, manifest, ids = text_archive
    page = {
        "source_page_url": "https://www.federalreserve.gov/monetarypolicy/fomchistorical2020.htm",
        "meetings": [],
    }
    for day in ["2020-03-02", "2020-03-15"]:
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
                        "provider_release_date": "2020-04-08",
                    }
                ],
            }
        )
    inventory = inventory_from_dict({"schema_id": manifest.schema_id, "pages": [page]}, manifest)
    run = text.store.save_inventory(inventory, manifest, input_kind="invented_fixture")
    text.bronze.link_document(ids["parent"], run["run_id"], inventory.documents[0].document_id)
    bindings = []
    for key in ["parent", "addendum"]:
        result = text.extract_capture(ids[key], manifest, root=tmp_path)
        bindings.append({"capture_id": ids[key], "extraction_id": result["extraction_id"]})
    catalog = PostgresTextCatalog(text)
    value = catalog.publish(bindings, manifest, root=tmp_path, inventory_run_id=run["run_id"])
    return text, catalog, value, bindings, manifest, ids


def test_catalog_preserves_bundled_meetings_and_inherited_addenda(published_text, tmp_path):
    text, catalog, value, bindings, manifest, ids = published_text
    assert catalog.read(manifest, root=tmp_path) == value
    assert len(value["representations"]) == 2
    assert all(len(r["meeting_ids"]) == 2 for r in value["representations"])
    assert value["representations"][0]["versions"][0]["first_seen_at"].startswith("2026-")
    assert value["inventory"]["entries"][0]["provider_release_date"] == "2020-04-08"
    assert (
        catalog.publish(
            bindings, manifest, root=tmp_path, inventory_run_id=value["inventory_run_id"]
        )
        == value
    )


def test_failed_catalog_write_leaves_old_pointer_and_retry_ignores_orphan(published_text, tmp_path):
    from psycopg.errors import CheckViolation

    text, catalog, value, bindings, manifest, ids = published_text
    store, conn = text.store, text.store.connection
    conn.execute(
        store._query(
            "ALTER TABLE {schema}.text_catalogs ADD CONSTRAINT fail_test CHECK(false) NOT VALID"
        )
    )
    with pytest.raises(CheckViolation):
        catalog.publish(
            bindings[:1], manifest, root=tmp_path, inventory_run_id=value["inventory_run_id"]
        )
    assert catalog.read(manifest, root=tmp_path) == value
    assert len(list((tmp_path / "text" / "catalogs").glob("*.json"))) == 2
    conn.execute(store._query("ALTER TABLE {schema}.text_catalogs DROP CONSTRAINT fail_test"))
    newer = catalog.publish(
        bindings[:1], manifest, root=tmp_path, inventory_run_id=value["inventory_run_id"]
    )
    catalog.publish(bindings, manifest, root=tmp_path, inventory_run_id=value["inventory_run_id"])
    assert catalog.read(manifest, root=tmp_path) == newer, (
        "retrying an older catalog moved the pointer"
    )


def test_failed_extraction_and_missing_original_cannot_advance_catalog(published_text, tmp_path):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import extract_text

    text, catalog, value, bindings, manifest, ids = published_text
    receipt, body = text.bronze.read_capture(ids["addendum"])
    failed = extract_text(body, receipt["source_url"], manifest, max_bytes=1)
    text.save_result(ids["addendum"], failed, manifest, root=tmp_path)
    with pytest.raises(ValueError, match="failed extraction"):
        catalog.publish(
            [{"capture_id": ids["addendum"], "extraction_id": failed["extraction_id"]}],
            manifest,
            root=tmp_path,
            inventory_run_id=value["inventory_run_id"],
        )
    assert catalog.read(manifest, root=tmp_path) == value
    (tmp_path / "captures" / ids["figure"] / "payload.bin").unlink()
    with pytest.raises(ValueError):
        text.extract_capture(ids["parent"], manifest, root=tmp_path)


def test_empty_target_replay_matches_full_catalog_without_extraction_or_network(
    published_text, tmp_path, monkeypatch
):
    import uuid
    from dataclasses import replace

    from psycopg import sql

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.catalog import PostgresTextCatalog
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
        PostgresDependencyStore,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import (
        PostgresInventoryStore,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text_replay import (
        replay_text_catalog,
    )

    text, catalog, value, bindings, manifest, ids = published_text
    cfg = replace(text.store.config, schema="fomc_meeting_minutes_test_" + uuid.uuid4().hex[:12])

    def forbidden(*args, **kwargs):
        raise AssertionError("replay attempted extraction or HTTP")

    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.io.text.extract_text", forbidden
    )
    monkeypatch.setattr("market_data_pipeline.shared.http.prepare_http", forbidden)
    with PostgresInventoryStore(cfg) as target:
        target.initialize()
        try:
            bronze = PostgresBronzeStore(target)
            bronze.migrate()
            PostgresDependencyStore(bronze).migrate()
            target_text = PostgresTextStore(bronze)
            target_text.migrate()
            target_catalog = PostgresTextCatalog(target_text)
            root = tmp_path.parent / (tmp_path.name + "-replayed")
            result = replay_text_catalog(
                catalog, target_catalog, manifest, source_root=tmp_path, target_root=root
            )
            assert result == value
            assert target_catalog.read(manifest, root=root) == value
            for capture_id in ids.values():
                assert (root / "captures" / capture_id / "receipt.json").read_bytes() == (
                    tmp_path / "captures" / capture_id / "receipt.json"
                ).read_bytes()
                assert bronze.read_capture(capture_id) == text.bronze.read_capture(capture_id)
            with pytest.raises(ValueError, match="empty"):
                replay_text_catalog(
                    catalog, target_catalog, manifest, source_root=tmp_path, target_root=root
                )
        finally:
            target.connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cfg.schema))
            )


def test_text_migration_refuses_partial_or_unowned_tables(text_archive):
    text, manifest, ids = text_archive
    text.store.connection.execute(
        text.store._query("UPDATE {schema}.text_state SET contract_id=%s"), ("unowned",)
    )
    with pytest.raises(ValueError, match="identity"):
        text.migrate()
    text.store.connection.execute(
        text.store._query("UPDATE {schema}.text_state SET contract_id=%s"),
        ("fomc-text-postgres/1",),
    )
    text.store.connection.execute(text.store._query("DROP TABLE {schema}.text_units"))
    with pytest.raises(ValueError, match="incomplete"):
        text.migrate()


def test_long_unicode_text_round_trips_without_truncation(text_archive, tmp_path):
    from datetime import UTC, datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import capture

    text, manifest, ids = text_archive
    expected = "Full Unicode Ω—中 café ¹ " * 3000
    receipt = capture(
        tmp_path,
        ('<div id="article"><p>' + expected + "</p></div>").encode(),
        manifest=manifest,
        store=text.bronze,
        source_url="https://www.federalreserve.gov/monetarypolicy/invented-unicode.htm",
        kind="document",
        captured_at=datetime.now(UTC),
        status=200,
        headers={},
        max_bytes=300000,
    )
    result = text.extract_capture(receipt["capture_id"], manifest, root=tmp_path)
    assert result["full_text"] == expected.strip()
    assert text.read_extraction(result["extraction_id"], root=tmp_path) == result
