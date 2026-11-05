"""S11-T19 actual PostgreSQL parity against an explicitly dedicated database."""

import hashlib
import io
import json
import os
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import gold, run
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import current_snapshot
from market_data_pipeline.pipelines.sec_fails_to_deliver.replay import replay
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import PostgresWarehouse

pytestmark = pytest.mark.postgres
FIXTURE = Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes()
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
        pytest.skip("test DSN must explicitly contain host, port, dbname, user and password")
    return psycopg, dsn, values


def _config(tmp_path, monkeypatch, schema, root):
    _psycopg, _dsn, values = _database()
    monkeypatch.setenv("SEC_FTD_TEST_PASSWORD", values["password"])
    source = tmp_path / "fixture.zip"
    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    value.update(
        warehouse="postgres",
        root=str(root),
        postgres={
            "host": values["host"],
            "port": int(values["port"]),
            "database": values["dbname"],
            "user": values["user"],
            "role": None,
            "schema": schema,
            "password_env": "SEC_FTD_TEST_PASSWORD",
            "sslmode": values.get("sslmode", "disable"),
            "connect_timeout": 15,
        },
        partitions={
            "fixture": {
                "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
                "input": str(source),
            }
        },
    )
    path = tmp_path / f"{schema}.yml"
    path.write_text(yaml.safe_dump(value))
    return path, source


def _bundle(path, payload):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    path.write_bytes(stream.getvalue())


def _baseline_three():
    rows = FIXTURE.splitlines(keepends=True)
    return (
        b"".join(rows[:3]) + b"20261001|000000003|FIX.C|5|INVENTED C|1.25\nTrailer record count 3\n"
        b"Trailer total quantity of shares 1180591620717411303554\n"
    )


def _without_first():
    rows = _baseline_three().splitlines(keepends=True)
    return (
        rows[0]
        + rows[2]
        + rows[3]
        + b"Trailer record count 2\nTrailer total quantity of shares 1180591620717411303429\n"
    )


@pytest.fixture
def postgres_schema():
    psycopg, dsn, _ = _database()
    schema = f"sec_ftd_test_{uuid.uuid4().hex[:12]}"
    from psycopg import sql

    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield schema
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_postgres_corrections_reversions_cutoffs_exact_types_and_publication(
    tmp_path, monkeypatch, postgres_schema
):
    path, source = _config(tmp_path, monkeypatch, postgres_schema, tmp_path / "postgres-root")
    results = []
    for offset, payload in enumerate(
        (FIXTURE, FIXTURE.replace(b"12.3400", b"12.3500"), FIXTURE, FIXTURE)
    ):
        _bundle(source, payload)
        results.append(
            run(path, "fixture", clock=lambda offset=offset: CLOCK + timedelta(seconds=offset))
        )
    assert [item["inserted_versions"] for item in results] == [2, 1, 1, 0]
    assert gold(path, limit=20, as_of=CLOCK, as_of_sequence=1)["rows"][0][
        "prior_close_price"
    ] == Decimal("12.3400")
    assert gold(path, limit=20, as_of=CLOCK + timedelta(seconds=1))["rows"][0][
        "prior_close_price"
    ] == Decimal("12.3500")
    assert gold(path, limit=20)["rows"][0]["quantity_fails"] == Decimal(125)
    config = load_config(path)
    assert current_snapshot(config.root).name == results[-1]["snapshot_id"]
    with PostgresWarehouse(config.root, config.storage, config.postgres) as warehouse:
        assert len(warehouse.select_all("facts")) == 4
        assert len(warehouse.select_runs()) == 4
        assert warehouse.select_publications()[-1]["status"] == "PUBLISHED"
        before = warehouse.connection.execute(
            "SELECT value FROM sec_ftd_sequence WHERE id=1"
        ).fetchone()["value"]
        with pytest.raises(RuntimeError), warehouse.transaction():
            warehouse.connection.execute("UPDATE sec_ftd_sequence SET value=99 WHERE id=1")
            raise RuntimeError("force rollback")
        after = warehouse.connection.execute(
            "SELECT value FROM sec_ftd_sequence WHERE id=1"
        ).fetchone()["value"]
        assert after == before
        columns = {
            row["column_name"]: row["data_type"]
            for row in warehouse.connection.execute(
                "SELECT column_name,data_type FROM information_schema.columns "
                "WHERE table_schema=? AND table_name='sec_ftd_facts'",
                (postgres_schema,),
            )
        }
        assert columns["settlement_date"] == "date"
        assert columns["first_seen_at"] == "timestamp with time zone"
        assert columns["quantity_fails"] == "numeric"

        warehouse.connection.execute(
            "CREATE FUNCTION fail_sec_ftd_fact() RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN IF NEW.cusip='000000001' THEN RAISE EXCEPTION 'injected'; END IF; "
            "RETURN NEW; END $$"
        )
        warehouse.connection.execute(
            "CREATE TRIGGER fail_sec_ftd_fact BEFORE INSERT ON sec_ftd_facts "
            "FOR EACH ROW EXECUTE FUNCTION fail_sec_ftd_fact()"
        )

    _bundle(source, FIXTURE.replace(b"12.3400", b"12.3600").replace(b"FIX-B", b"FIX-C"))
    failed = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=4))
    assert failed["status"] == "FAILED" and failed["error_code"] == "WAREHOUSE_WRITE_FAILED"
    with PostgresWarehouse(config.root, config.storage, config.postgres) as warehouse:
        assert len(warehouse.select_all("facts")) == 4
        assert warehouse.select_runs()[-1]["status"] == "FAILED"
        warehouse.connection.execute("DROP TRIGGER fail_sec_ftd_fact ON sec_ftd_facts")
        warehouse.connection.execute("DROP FUNCTION fail_sec_ftd_fact()")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(run, path, "fixture", clock=lambda: CLOCK + timedelta(seconds=5))
            for _ in range(2)
        ]
        retried = [future.result() for future in futures]
    assert sorted(item["inserted_versions"] for item in retried) == [0, 2]

    other_root = tmp_path / "wrong-root"
    with pytest.raises(ValueError, match="binding"):
        with PostgresWarehouse(other_root, config.storage, config.postgres):
            pass
    assert not (other_root / ".warehouse.json").exists()


def test_sqlite_history_replays_exactly_into_postgres_and_publishes(
    tmp_path, monkeypatch, postgres_schema
):
    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    source_zip = tmp_path / "source.zip"
    value.update(
        warehouse="sqlite",
        root=str(tmp_path / "sqlite-source"),
        partitions={
            "fixture": {
                "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
                "input": str(source_zip),
            }
        },
    )
    source_config = tmp_path / "sqlite.yml"
    source_config.write_text(yaml.safe_dump(value))
    _bundle(source_zip, _baseline_three())
    run(source_config, "fixture", clock=lambda: CLOCK)
    _bundle(source_zip, _without_first())
    proof = tmp_path / "completeness.json"
    proof.write_text(
        json.dumps(
            {
                "schema_version": "sec-ftd-completeness/1",
                "source": "sec",
                "dataset": "sec-fails-to-deliver",
                "reporting_regime": "semi-monthly-2009+",
                "period_start": "2026-10-01",
                "period_end": "2026-10-15",
                "member_name": "cnsfails202610a.txt",
                "raw_sha256": hashlib.sha256(source_zip.read_bytes()).hexdigest(),
                "expected_settlement_dates": ["2026-10-01", "2026-10-15"],
                "source_reference": "invented-source-fixture/1",
                "calendar_reference": "invented-calendar/1",
                "review_reference": "invented-review-only/1",
            }
        )
    )
    source_value = yaml.safe_load(source_config.read_text())
    source_value["partitions"]["fixture"]["coverage_evidence"] = str(proof)
    source_config.write_text(yaml.safe_dump(source_value))
    run(source_config, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))
    target_config, _ = _config(tmp_path, monkeypatch, postgres_schema, tmp_path / "postgres-replay")
    result = replay(load_config(source_config).root, target_config)
    assert result["status"] == "SUCCEEDED" and result["fact_version_count"] == 4
    assert current_snapshot(load_config(target_config).root).name == result["snapshot_id"]
    assert gold(target_config, limit=20)["rows"] == gold(source_config, limit=20)["rows"]
    withdrawn = gold(target_config, limit=20, include_withdrawn=True)["rows"]
    assert len(withdrawn) == 3
    assert next(row for row in withdrawn if row["cusip"] == "000000001")["row_state"] == "WITHDRAWN"


def test_failed_postgres_replay_removes_only_its_new_identity_bound_store(
    tmp_path, monkeypatch, postgres_schema
):
    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    source_zip = tmp_path / "source.zip"
    value.update(
        warehouse="sqlite",
        root=str(tmp_path / "sqlite-source"),
        partitions={
            "fixture": {
                "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
                "input": str(source_zip),
            }
        },
    )
    source_config = tmp_path / "sqlite.yml"
    source_config.write_text(yaml.safe_dump(value))
    _bundle(source_zip, FIXTURE)
    run(source_config, "fixture", clock=lambda: CLOCK)
    target_config, _ = _config(tmp_path, monkeypatch, postgres_schema, tmp_path / "postgres-replay")
    import market_data_pipeline.pipelines.sec_fails_to_deliver.replay as replay_module

    original = replay_module._rebuild

    def fail(*_args):
        raise RuntimeError("injected replay failure")

    monkeypatch.setattr(replay_module, "_rebuild", fail)
    with pytest.raises(RuntimeError, match="injected"):
        replay(load_config(source_config).root, target_config)
    assert not load_config(target_config).root.exists()
    psycopg, dsn, _ = _database()
    with psycopg.connect(dsn) as connection:
        tables = connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s",
            (postgres_schema,),
        ).fetchall()
    assert tables == []
    monkeypatch.setattr(replay_module, "_rebuild", original)
    assert replay(load_config(source_config).root, target_config)["status"] == "SUCCEEDED"
