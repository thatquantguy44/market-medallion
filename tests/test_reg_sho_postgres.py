"""S11-T20 actual PostgreSQL parity against a dedicated disposable database."""

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import gold, run
from market_data_pipeline.pipelines.reg_sho_thresholds.replay import replay
from market_data_pipeline.pipelines.reg_sho_thresholds.research_export import (
    export_research_artifact,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import PostgresWarehouse

pytestmark = pytest.mark.postgres
FIXTURE = Path("tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt").read_bytes()
CLOCK = datetime(2026, 11, 4, 12, tzinfo=UTC)


def _database():
    dsn = os.environ.get("MARKET_DATA_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set MARKET_DATA_TEST_POSTGRES_DSN to a dedicated disposable database")
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    values = conninfo_to_dict(dsn)
    required = {"host", "port", "dbname", "user", "password"}
    if not required <= values.keys():
        pytest.skip("test DSN must contain host, port, dbname, user and password")
    return psycopg, dsn, values


@pytest.fixture
def postgres_schema():
    psycopg, dsn, _values = _database()
    schema = f"reg_sho_test_{uuid.uuid4().hex[:12]}"
    from psycopg import sql

    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield schema
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def _config(tmp_path, monkeypatch, schema, source):
    _psycopg, _dsn, values = _database()
    monkeypatch.setenv("REG_SHO_TEST_PASSWORD", values["password"])
    value = yaml.safe_load(Path("config/reg_sho_thresholds/pipeline.example.yml").read_text())
    value.update(
        warehouse="postgres",
        root=str(tmp_path / "postgres-root"),
        postgres={
            "host": values["host"],
            "port": int(values["port"]),
            "database": values["dbname"],
            "user": values["user"],
            "role": None,
            "schema": schema,
            "password_env": "REG_SHO_TEST_PASSWORD",
            "sslmode": values.get("sslmode", "disable"),
            "connect_timeout": 15,
        },
        partitions={
            "fixture": {
                "provider": "nasdaq",
                "market": "NASDAQ",
                "list_date": "2026-11-03",
                "manifest": str(Path("manifests/reg-sho-nasdaq.yml").resolve()),
                "input": str(source),
                "source_url": (
                    "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth20261103.txt"
                ),
            }
        },
    )
    path = tmp_path / "postgres.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_postgres_revisions_withdrawals_cutoffs_and_native_types(
    tmp_path, monkeypatch, postgres_schema
):
    source = tmp_path / "nasdaq.txt"
    source.write_bytes(FIXTURE)
    path = _config(tmp_path, monkeypatch, postgres_schema, source)
    first = run(path, "fixture", clock=lambda: CLOCK)
    source.write_bytes(FIXTURE.replace(b"BETA|BETA INVENTED FUND", b"GAMM|GAMMA INVENTED FUND"))
    second = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))

    assert first["inserted_versions"] == 2
    assert second["inserted_versions"] == 2
    assert [row["raw_symbol"] for row in gold(path)["rows"]] == ["ALFA", "GAMM"]
    assert [row["raw_symbol"] for row in gold(path, as_of=CLOCK)["rows"]] == [
        "ALFA",
        "BETA",
    ]

    config = load_config(path)
    with PostgresWarehouse(config.root, config.postgres) as warehouse:
        assert len(warehouse.select_all("membership")) == 4
        columns = {
            row["column_name"]: row["data_type"]
            for row in warehouse.connection.execute(
                "SELECT column_name,data_type FROM information_schema.columns "
                "WHERE table_schema=? AND table_name='reg_sho_membership'",
                (postgres_schema,),
            )
        }
        assert columns["list_date"] == "date"
        assert columns["first_seen_at"] == "timestamp with time zone"
        assert columns["capture_sequence"] == "bigint"
        before = warehouse.connection.execute(
            "SELECT value FROM reg_sho_sequence WHERE id=1"
        ).fetchone()["value"]
        with pytest.raises(RuntimeError), warehouse.transaction():
            warehouse.connection.execute("UPDATE reg_sho_sequence SET value=99 WHERE id=1")
            raise RuntimeError("force rollback")
        after = warehouse.connection.execute(
            "SELECT value FROM reg_sho_sequence WHERE id=1"
        ).fetchone()["value"]
        assert after == before

    wrong_root = tmp_path / "wrong-root"
    with pytest.raises(ValueError, match="binding"):
        with PostgresWarehouse(wrong_root, config.postgres):
            pass
    assert not (wrong_root / ".warehouse.json").exists()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                run,
                path,
                "fixture",
                clock=lambda: CLOCK + timedelta(seconds=2),
            )
            for _ in range(2)
        ]
        concurrent = [future.result() for future in futures]
    assert len({item["capture_sequence"] for item in concurrent}) == 2
    assert all(item["inserted_versions"] == 0 for item in concurrent)

    source.write_bytes(
        Path("tests/fixtures/reg_sho_thresholds/nasdaq-empty.txt")
        .read_bytes()
        .replace(b"20261104", b"20261103")
    )
    empty = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=3))
    assert empty["coverage_state"] == "COMPLETE_EMPTY"
    assert empty["inserted_versions"] == 2
    assert gold(path)["rows"] == []

    source.write_bytes(b"not a threshold list\n")
    rejected = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=4))
    assert rejected["status"] == "REJECTED"
    assert rejected["coverage_state"] == "FAILED"
    assert gold(path)["rows"] == []
    exported = export_research_artifact(path, tmp_path / "research")
    assert exported["membership_count"] == 0
    assert exported["coverage_count"] == 1


def test_sqlite_history_replays_exactly_into_postgres(tmp_path, monkeypatch, postgres_schema):
    source = tmp_path / "nasdaq.txt"
    source.write_bytes(FIXTURE)
    value = yaml.safe_load(Path("config/reg_sho_thresholds/pipeline.example.yml").read_text())
    value.update(
        root=str(tmp_path / "sqlite-root"),
        partitions={
            "fixture": {
                "provider": "nasdaq",
                "market": "NASDAQ",
                "list_date": "2026-11-03",
                "manifest": str(Path("manifests/reg-sho-nasdaq.yml").resolve()),
                "input": str(source),
                "source_url": (
                    "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth20261103.txt"
                ),
            }
        },
    )
    sqlite_config = tmp_path / "sqlite.yml"
    sqlite_config.write_text(yaml.safe_dump(value))
    run(sqlite_config, "fixture", clock=lambda: CLOCK)
    source.write_bytes(FIXTURE.replace(b"BETA|BETA INVENTED FUND", b"GAMM|GAMMA INVENTED FUND"))
    run(sqlite_config, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))

    postgres_config = _config(tmp_path, monkeypatch, postgres_schema, source)
    result = replay(tmp_path / "sqlite-root", postgres_config)
    assert result["capture_count"] == 2
    assert gold(postgres_config)["rows"] == gold(sqlite_config)["rows"]
    assert gold(postgres_config, as_of=CLOCK)["rows"] == gold(sqlite_config, as_of=CLOCK)["rows"]
