"""FOMC-AC07/12: exact chunk mirrors, atomic generations and offline replay."""

import pytest
from test_fomc_postgres import dependency_archive as dependency_archive
from test_fomc_postgres import store as store
from test_fomc_text_postgres import published_text as published_text
from test_fomc_text_postgres import text_archive as text_archive

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunk_catalog import (
    PostgresChunkCatalog,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore

__all__ = ["dependency_archive", "store", "published_text", "text_archive"]
pytestmark = pytest.mark.postgres


@pytest.fixture
def chunk_archive(published_text, tmp_path):
    text, text_catalog, base, bindings, manifest, ids = published_text
    chunks = PostgresChunkStore(text)
    chunks.migrate()
    chunks.migrate()
    catalog = PostgresChunkCatalog(chunks)
    return chunks, catalog, base, manifest, ids


def test_chunk_mirrors_generations_and_review_filters(chunk_archive, tmp_path):
    chunks, catalog, base, manifest, ids = chunk_archive
    value = catalog.publish(manifest, root=tmp_path)
    assert value["text_catalog_id"] == base["catalog_id"]
    assert catalog.read(manifest, root=tmp_path) == value
    assert catalog.publish(manifest, root=tmp_path) == value
    all_rows = catalog.read_chunks(manifest, root=tmp_path, eligible_only=False)
    eligible = catalog.read_chunks(manifest, root=tmp_path)
    assert len(all_rows) == 2 and len(eligible) == 1
    assert eligible[0]["eligible"] and "addendum" in eligible[0]["source_url"]
    assert all(len(r["meeting_ids"]) == 2 for r in all_rows)
    assert value["coverage"]["review_chunks"] == 1
    assert value["upstream_gaps"] == base["gaps"]
    for selection in value["selections"]:
        result = chunks.read_set(selection["set_id"], root=tmp_path)
        assert result["document_version_id"] == selection["document_version_id"]
        assert result["extraction_id"] == selection["extraction_id"]
    assert chunks.store.read_inventory() == base["inventory"]
    assert catalog.text_catalog.read(manifest, root=tmp_path) == base


def test_chunk_row_corruption_and_missing_local_artifact_refuse_reads(chunk_archive, tmp_path):
    chunks, catalog, base, manifest, ids = chunk_archive
    value = catalog.publish(manifest, root=tmp_path)
    set_id = value["selections"][0]["set_id"]
    path = tmp_path / "text" / "chunks" / (set_id + ".json")
    body = path.read_bytes()
    path.unlink()
    with pytest.raises(ValueError, match="mirror"):
        catalog.read(manifest, root=tmp_path)
    path.write_bytes(body)
    chunks.store.connection.execute(
        chunks.store._query("UPDATE {schema}.chunks SET chunk_text=%s WHERE set_id=%s"),
        ("corrupt", set_id),
    )
    with pytest.raises(ValueError, match="row"):
        catalog.read(manifest, root=tmp_path)


def test_failed_chunk_save_recovers_from_local_artifact(chunk_archive, tmp_path):
    from psycopg.errors import CheckViolation

    chunks, catalog, base, manifest, ids = chunk_archive
    conn, store = chunks.store.connection, chunks.store
    conn.execute(store._query("ALTER TABLE {schema}.chunks ADD CONSTRAINT fail_test CHECK(false)"))
    with pytest.raises(CheckViolation):
        catalog.publish(manifest, root=tmp_path)
    assert list((tmp_path / "text" / "chunks").glob("*.json"))
    assert conn.execute(store._query("SELECT count(*) FROM {schema}.chunk_sets")).fetchone()[0] == 0
    assert catalog.read(manifest, root=tmp_path) is None
    conn.execute(store._query("ALTER TABLE {schema}.chunks DROP CONSTRAINT fail_test"))
    assert catalog.publish(manifest, root=tmp_path)["coverage"]["chunks"] == 2


def test_failed_generation_preserves_pointer_and_old_retry_cannot_rollback(chunk_archive, tmp_path):
    from psycopg.errors import CheckViolation

    chunks, catalog, base, manifest, ids = chunk_archive
    first = catalog.publish(manifest, root=tmp_path)
    conn, store = chunks.store.connection, chunks.store
    conn.execute(
        store._query(
            "ALTER TABLE {schema}.chunk_catalogs ADD CONSTRAINT fail_test CHECK(false) NOT VALID"
        )
    )
    with pytest.raises(CheckViolation):
        catalog.publish(manifest, root=tmp_path, max_tokens=25, overlap_tokens=3)
    assert catalog.read(manifest, root=tmp_path) == first
    assert len(list((tmp_path / "text" / "chunk_catalogs").glob("*.json"))) == 2
    conn.execute(store._query("ALTER TABLE {schema}.chunk_catalogs DROP CONSTRAINT fail_test"))
    newer = catalog.publish(manifest, root=tmp_path, max_tokens=25, overlap_tokens=3)
    catalog.publish(manifest, root=tmp_path)
    assert catalog.read(manifest, root=tmp_path) == newer


def test_current_version_filter_preserves_repeat_correction_and_reversion(chunk_archive, tmp_path):
    from datetime import UTC, datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import capture

    chunks, catalog, base, manifest, ids = chunk_archive
    text = chunks.text
    original, body = text.bronze.read_capture(ids["parent"])
    bindings = list(base["bindings"])
    doc = next(
        d for d in base["inventory"]["documents"] if d["source_url"] == original["source_url"]
    )
    for payload in [body, body.replace(b"</div>", b"<p>Correction</p></div>"), body]:
        receipt = capture(
            tmp_path,
            payload,
            manifest=manifest,
            store=text.bronze,
            source_url=original["source_url"],
            kind="document",
            captured_at=datetime.now(UTC),
            status=200,
            headers={},
            max_bytes=2000,
        )
        identity = receipt["capture_id"]
        text.bronze.link_document(identity, base["inventory_run_id"], doc["document_id"])
        text.dependencies.commit(
            identity, [ids["addendum"], ids["figure"]], manifest, root=tmp_path
        )
        result = text.extract_capture(identity, manifest, root=tmp_path)
        bindings.append({"capture_id": identity, "extraction_id": result["extraction_id"]})
    newer = catalog.text_catalog.publish(
        bindings, manifest, root=tmp_path, inventory_run_id=base["inventory_run_id"]
    )
    value = catalog.publish(manifest, root=tmp_path)
    assert value["coverage"]["sets"] == 4
    all_rows = catalog.read_chunks(manifest, root=tmp_path, eligible_only=False, current_only=False)
    current = catalog.read_chunks(manifest, root=tmp_path, eligible_only=False)
    assert len(all_rows) == 4 and len(current) == 2
    selected = [s for s in value["selections"] if s["source_url"] == original["source_url"]]
    assert len(selected) == 3 and sum(s["current"] for s in selected) == 1
    rows = [chunks.read_set(s["set_id"], root=tmp_path) for s in selected]
    assert len({r["set_id"] for r in rows}) == 3
    assert value["text_catalog_id"] == newer["catalog_id"]


def test_multiple_extractors_require_explicit_selection(chunk_archive, tmp_path):
    from copy import deepcopy

    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest

    chunks, catalog, base, manifest, ids = chunk_archive
    first = catalog.publish(manifest, root=tmp_path)
    binding = next(b for b in base["bindings"] if b["capture_id"] == ids["parent"])
    result = deepcopy(chunks.text.read_extraction(binding["extraction_id"], root=tmp_path))
    result["profile"]["algorithm"] = "invented-next-native/2"
    result["profile_sha256"] = digest(result["profile"])
    del result["extraction_id"]
    result["extraction_id"] = digest(result)
    chunks.text.save_result(ids["parent"], result, manifest, root=tmp_path)
    newer = catalog.text_catalog.publish(
        base["bindings"]
        + [{"capture_id": ids["parent"], "extraction_id": result["extraction_id"]}],
        manifest,
        root=tmp_path,
        inventory_run_id=base["inventory_run_id"],
    )
    with pytest.raises(ValueError, match="explicit selection"):
        catalog.publish(manifest, root=tmp_path)
    assert catalog.read(manifest, root=tmp_path) == first
    choices = {
        v["version_id"]: result["extraction_id"]
        if r["source_url"] == result["source_url"]
        else v["extraction_ids"][0]
        for r in newer["representations"]
        for v in r["versions"]
    }
    value = catalog.publish(manifest, root=tmp_path, selections=choices)
    assert any(s["extraction_id"] == result["extraction_id"] for s in value["selections"])


def test_missing_original_blocks_generation_without_moving_pointer(chunk_archive, tmp_path):
    chunks, catalog, base, manifest, ids = chunk_archive
    first = catalog.publish(manifest, root=tmp_path)
    path = tmp_path / "captures" / ids["figure"] / "payload.bin"
    body = path.read_bytes()
    path.unlink()
    try:
        with pytest.raises(ValueError):
            catalog.publish(manifest, root=tmp_path, max_tokens=25, overlap_tokens=3)
        pointer = chunks.store.connection.execute(
            chunks.store._query("SELECT current_catalog_id FROM {schema}.chunk_state")
        ).fetchone()[0]
        assert pointer == first["catalog_id"]
    finally:
        path.write_bytes(body)
    assert catalog.read(manifest, root=tmp_path) == first


def test_chunk_migration_refuses_partial_or_foreign_schema(chunk_archive):
    chunks, catalog, base, manifest, ids = chunk_archive
    store = chunks.store
    store.connection.execute(
        store._query("UPDATE {schema}.chunk_state SET contract_id=%s"), ("foreign",)
    )
    with pytest.raises(ValueError, match="identity"):
        chunks.migrate()
    store.connection.execute(
        store._query("UPDATE {schema}.chunk_state SET contract_id=%s"), ("fomc-chunks-postgres/1",)
    )
    store.connection.execute(store._query("DROP TABLE {schema}.chunks"))
    with pytest.raises(ValueError, match="incomplete"):
        chunks.migrate()


def test_chunk_empty_target_replay_preserves_ids_and_eligibility_without_tokenization(
    chunk_archive, tmp_path, monkeypatch
):
    import uuid
    from dataclasses import replace

    from psycopg import sql

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunk_replay import (
        replay_chunk_catalog,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
        PostgresDependencyStore,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import (
        PostgresInventoryStore,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore

    chunks, catalog, base, manifest, ids = chunk_archive
    value = catalog.publish(manifest, root=tmp_path)
    cfg = replace(chunks.store.config, schema="fomc_meeting_minutes_test_" + uuid.uuid4().hex[:12])

    def forbidden(*args, **kwargs):
        raise AssertionError("replay called a transform or network")

    for path in [
        "market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks.chunk_text",
        "market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunks._tokens",
        "market_data_pipeline.pipelines.fomc_meeting_minutes.io.text.extract_text",
        "market_data_pipeline.shared.http.prepare_http",
    ]:
        monkeypatch.setattr(path, forbidden)
    with PostgresInventoryStore(cfg) as target:
        target.initialize()
        try:
            bronze = PostgresBronzeStore(target)
            bronze.migrate()
            PostgresDependencyStore(bronze).migrate()
            text = PostgresTextStore(bronze)
            text.migrate()
            target_chunks = PostgresChunkStore(text)
            target_chunks.migrate()
            target_catalog = PostgresChunkCatalog(target_chunks)
            root = tmp_path.parent / (tmp_path.name + "-chunk-replay")
            replayed = replay_chunk_catalog(
                catalog, target_catalog, manifest, source_root=tmp_path, target_root=root
            )
            assert replayed == value
            assert target_catalog.read_chunks(manifest, root=root) == catalog.read_chunks(
                manifest, root=tmp_path
            )
            for capture_id in ids.values():
                assert (root / "captures" / capture_id / "receipt.json").read_bytes() == (
                    tmp_path / "captures" / capture_id / "receipt.json"
                ).read_bytes()
            with pytest.raises(ValueError, match="empty"):
                replay_chunk_catalog(
                    catalog, target_catalog, manifest, source_root=tmp_path, target_root=root
                )
        finally:
            target.connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cfg.schema))
            )


def test_default_publication_refuses_a_text_pointer_change_mid_build(
    chunk_archive, tmp_path, monkeypatch
):
    chunks, catalog, base, manifest, ids = chunk_archive
    first = catalog.publish(manifest, root=tmp_path)
    original = chunks.make_set
    changed = False

    def move_text_pointer(*args, **kwargs):
        nonlocal changed
        value = original(*args, **kwargs)
        if not changed:
            changed = True
            binding = next(b for b in base["bindings"] if b["capture_id"] == ids["parent"])
            catalog.text_catalog.publish(
                [binding], manifest, root=tmp_path, inventory_run_id=base["inventory_run_id"]
            )
        return value

    monkeypatch.setattr(chunks, "make_set", move_text_pointer)
    with pytest.raises(ValueError, match="text catalog changed"):
        catalog.publish(manifest, root=tmp_path, max_tokens=25, overlap_tokens=3)
    assert catalog.read(manifest, root=tmp_path) == first
