"""T03 content-object migration and durable recovery in isolated FOMC schemas."""

from datetime import UTC
from pathlib import Path

import pytest
from test_fomc_postgres import dependency_archive as dependency_archive
from test_fomc_postgres import store as store

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

__all__ = ["dependency_archive", "store"]  # Register the shared pytest fixtures.

pytestmark = pytest.mark.postgres


def test_fomc_object_migration_deduplicates_originals_and_keeps_capture_history(store, tmp_path):
    from datetime import datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
        commit_capture,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
        PostgresObjectStore,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    rows = []
    for name in ("one.pdf", "two.pdf"):
        rows.append(
            capture(
                tmp_path,
                b"same original",
                manifest=manifest,
                store=bronze,
                source_url="https://www.federalreserve.gov/monetarypolicy/" + name,
                kind="document",
                captured_at=datetime.now(UTC),
                status=200,
                headers={},
                max_bytes=100,
            )
        )
    objects = PostgresObjectStore(bronze)
    objects.migrate()
    objects.migrate()
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_objects")
        ).fetchone()[0]
        == 1
    )
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_capture_objects")
        ).fetchone()[0]
        == 2
    )
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_captures WHERE payload IS NOT NULL")
        ).fetchone()[0]
        == 0
    )
    for r in rows:
        assert bronze.read_capture(r["capture_id"]) == (r, b"same original")
        commit_capture(tmp_path, r["capture_id"], bronze)
    new = capture(
        tmp_path,
        b"same original",
        manifest=manifest,
        store=bronze,
        source_url="https://www.federalreserve.gov/monetarypolicy/one.pdf",
        kind="document",
        captured_at=datetime.now(UTC),
        status=200,
        headers={},
        max_bytes=100,
    )
    assert new["capture_id"] not in {r["capture_id"] for r in rows}
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_objects")
        ).fetchone()[0]
        == 1
    )
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_capture_objects")
        ).fetchone()[0]
        == 3
    )
    assert store.run_count() == 0 and store.read_inventory() is None


def test_fomc_object_migration_corrupt_legacy_payload_rolls_back(
    store, tmp_path, dependency_archive
):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
        PostgresObjectStore,
    )

    bronze, dependencies, manifest, ids = dependency_archive
    store.connection.execute(
        store._query("UPDATE {schema}.bronze_captures SET payload=%s WHERE capture_id=%s"),
        (b"corrupt", ids["figure"]),
    )
    with pytest.raises(ValueError, match="legacy.*hash"):
        PostgresObjectStore(bronze).migrate()
    tables = {
        r[0]
        for r in store.connection.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname=%s", (store.config.schema,)
        )
    }
    assert "bronze_objects" not in tables and "bronze_object_state" not in tables
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_captures WHERE payload IS NOT NULL")
        ).fetchone()[0]
        == 3
    )


def test_fomc_object_migration_keeps_dependency_checks_readable(
    store, tmp_path, dependency_archive
):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
        PostgresObjectStore,
    )

    bronze, dependencies, manifest, ids = dependency_archive
    check = dependencies.commit(
        ids["parent"], [ids["addendum"], ids["figure"]], manifest, root=tmp_path
    )
    before = dependencies.read_check(check)
    PostgresObjectStore(bronze).migrate()
    assert dependencies.read_check(check) == before
    assert (
        dependencies.commit(
            ids["parent"], [ids["figure"], ids["addendum"]], manifest, root=tmp_path
        )
        == check
    )
    store.connection.execute(
        store._query("UPDATE {schema}.bronze_objects SET payload=%s,size_bytes=%s WHERE sha256=%s"),
        (b"corrupt", 7, bronze.read_capture(ids["figure"])[0]["sha256"]),
    )
    with pytest.raises(ValueError, match="hash"):
        bronze.read_capture(ids["figure"])


def test_fomc_object_migration_serializes_with_capture(store, tmp_path, dependency_archive):
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
        PostgresBronzeStore,
        capture,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
        PostgresObjectStore,
    )

    bronze, dependencies, manifest, ids = dependency_archive

    def migrate():
        with PostgresInventoryStore(store.config) as other:
            PostgresObjectStore(PostgresBronzeStore(other)).migrate()

    def write():
        with PostgresInventoryStore(store.config) as other:
            return capture(
                tmp_path,
                b"concurrent original",
                manifest=manifest,
                store=PostgresBronzeStore(other),
                source_url="https://www.federalreserve.gov/monetarypolicy/concurrent.pdf",
                kind="document",
                captured_at=datetime.now(UTC),
                status=200,
                headers={},
                max_bytes=100,
            )["capture_id"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        migration = pool.submit(migrate)
        writer = pool.submit(write)
        migration.result()
        identity = writer.result()
    assert bronze.read_capture(identity)[1] == b"concurrent original"
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_capture_objects")
        ).fetchone()[0]
        == 4
    )
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_captures WHERE payload IS NOT NULL")
        ).fetchone()[0]
        == 0
    )


@pytest.mark.parametrize("boundary", ["database", "marker"])
def test_fomc_real_backend_interrupted_capture_recovers_same_identity(
    store, tmp_path, monkeypatch, boundary
):
    from datetime import datetime

    from psycopg.errors import CheckViolation

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io import bronze as module
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
        PostgresObjectStore,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.recovery import recover_attempt

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = module.PostgresBronzeStore(store)
    bronze.migrate()
    PostgresObjectStore(bronze).migrate()
    original = module._write
    if boundary == "database":
        store.connection.execute(
            store._query(
                "ALTER TABLE {schema}.bronze_captures "
                "ADD CONSTRAINT simulated_failure CHECK(false) NOT VALID"
            )
        )
    else:

        def fail(path, data):
            if path.name == "committed.json":
                raise OSError("marker interrupted")
            original(path, data)

        monkeypatch.setattr(module, "_write", fail)
    with pytest.raises(CheckViolation if boundary == "database" else OSError):
        module.capture(
            tmp_path,
            b"recovery original",
            manifest=manifest,
            store=bronze,
            source_url="https://www.federalreserve.gov/monetarypolicy/recovery.pdf",
            kind="document",
            captured_at=datetime.now(UTC),
            status=200,
            headers={},
            max_bytes=100,
        )
    identity = next((tmp_path / "attempts").glob("*.json")).stem
    if boundary == "database":
        assert (
            store.connection.execute(
                store._query("SELECT count(*) FROM {schema}.bronze_objects")
            ).fetchone()[0]
            == 0
        )
        store.connection.execute(
            store._query("ALTER TABLE {schema}.bronze_captures DROP CONSTRAINT simulated_failure")
        )
    else:
        monkeypatch.setattr(module, "_write", original)
    assert recover_attempt(tmp_path, identity, bronze)["status"] == "committed"
    assert recover_attempt(tmp_path, identity, bronze)["status"] == "committed"
    assert bronze.read_capture(identity) == module.read_capture(tmp_path, identity)
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_captures")
        ).fetchone()[0]
        == 1
    )
    assert (
        store.connection.execute(
            store._query("SELECT count(*) FROM {schema}.bronze_objects")
        ).fetchone()[0]
        == 1
    )
    assert store.run_count() == 0 and store.read_inventory() is None
