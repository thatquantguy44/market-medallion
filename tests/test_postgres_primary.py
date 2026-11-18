"""PG-AC04 explicit primary identity; no secrets, relocation or silent fallback."""

from dataclasses import asdict

import pytest

from market_data_pipeline.shared.postgres import Settings, bind, binding


def settings(**changes):
    values = dict(
        host="127.0.0.1",
        port=55438,
        database="test_database",
        user="test_login",
        role="test_writer",
        schema="test_pipeline",
        password_env="TEST_PG_PASSWORD",
        sslmode="prefer",
        connect_timeout=5,
    )
    return Settings(**(values | changes))


def test_binding_is_explicit_idempotent_and_contains_only_environment_reference(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_PG_PASSWORD", "invented-secret-never-save")
    root = tmp_path / "root"
    root.mkdir()
    assert binding(root, "test_pipeline") is None
    bind(root, "test_pipeline", settings(), "store-001")
    marker = root / ".postgres-primary.json"
    before = (marker.read_bytes(), marker.stat().st_mtime_ns)
    assert b"invented-secret" not in before[0]
    value = binding(root, "test_pipeline")
    assert value["store_id"] == "store-001"
    assert value["postgres"] == asdict(settings())
    bind(root, "test_pipeline", settings(), "store-001")
    assert (marker.read_bytes(), marker.stat().st_mtime_ns) == before
    with pytest.raises(ValueError):
        bind(root, "test_pipeline", settings(schema="different"), "store-001")


def test_binding_cannot_be_relabelled_or_relocated(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    bind(root, "test_pipeline", settings(), "store-001")
    with pytest.raises(ValueError):
        binding(root, "other_pipeline")
    root.rename(tmp_path / "moved")
    with pytest.raises(ValueError):
        binding(tmp_path / "moved", "test_pipeline")


def test_unsafe_or_corrupt_binding_fails_instead_of_returning_sqlite_selection(tmp_path):
    target = tmp_path / "outside"
    target.write_text("{}")
    marker = tmp_path / ".postgres-primary.json"
    marker.symlink_to(target)
    with pytest.raises(ValueError):
        binding(tmp_path, "test_pipeline")
    marker.unlink()
    marker.write_text("{broken")
    with pytest.raises(ValueError):
        binding(tmp_path, "test_pipeline")


@pytest.mark.parametrize(
    "change",
    [
        dict(port=True),
        dict(port=0),
        dict(role="bad;role"),
        dict(schema="public"),
        dict(password_env="PASSWORD=bad"),
        dict(sslmode="invalid"),
        dict(connect_timeout=0),
    ],
)
def test_postgres_settings_refuse_invalid_targets(change):
    with pytest.raises(ValueError):
        settings(**change)


def test_missing_password_is_sanitized_and_does_not_fall_back(monkeypatch):
    monkeypatch.delenv("TEST_PG_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="TEST_PG_PASSWORD"):
        settings().conninfo()


@pytest.fixture
def native_settings(monkeypatch):
    import os
    import uuid
    from pathlib import Path

    import yaml

    path = os.environ.get("MARKET_MEDALLION_TEST_POSTGRES_CONFIG")
    if not path:
        pytest.skip("set MARKET_MEDALLION_TEST_POSTGRES_CONFIG to a disposable database config")
    import psycopg
    from psycopg import sql

    values = yaml.safe_load(Path(path).read_text())["postgres"]
    original = Settings(**values)
    schema = "primary_test_" + uuid.uuid4().hex[:12]
    with psycopg.connect(original.conninfo(), autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    selected = Settings(**(values | {"schema": schema, "role": None}))
    try:
        yield selected
    finally:
        with psycopg.connect(original.conninfo(), autocommit=True) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_native_postgres_transactions_exact_rows_and_read_only_refusal(native_settings):
    import psycopg

    from market_data_pipeline.shared.postgres import connect

    with connect(native_settings) as db:
        db.execute("CREATE TABLE observations (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO observations VALUES (?,?)", ("rolled-back", "7"))
        db.rollback()
        assert db.execute("SELECT count(*) FROM observations").fetchone()[0] == 0
        with db:
            db.execute("INSERT INTO observations VALUES (?,?)", ("0007", "12345678901234567890"))
        row = db.execute("SELECT id,body FROM observations").fetchone()
        assert dict(row) == {"id": "0007", "body": "12345678901234567890"}
        assert tuple(row) == ("0007", "12345678901234567890")
    with connect(native_settings, read_only=True) as db:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            db.execute("INSERT INTO observations VALUES ('denied','0')")
        assert db.execute("SELECT count(*) FROM observations").fetchone()[0] == 1


def test_native_postgres_reader_keeps_one_coherent_snapshot(native_settings):
    from market_data_pipeline.shared.postgres import connect

    with connect(native_settings) as db:
        db.execute("CREATE TABLE observations (id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO observations VALUES ('first')")
    with connect(native_settings, read_only=True) as reader:
        reader.execute("BEGIN")
        assert reader.execute("SELECT count(*) FROM observations").fetchone()[0] == 1
        with connect(native_settings) as writer:
            writer.execute("INSERT INTO observations VALUES ('second')")
        assert reader.execute("SELECT count(*) FROM observations").fetchone()[0] == 1
        reader.commit()
        assert reader.execute("SELECT count(*) FROM observations").fetchone()[0] == 2
