"""PG-AC03/04/05 FINRA native warehouse/catalog primary parity and recovery."""

from datetime import timedelta

import pytest
from test_postgres_primary import native_settings as native_settings
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_gold_query import load as load
from test_ssv_parser import manifest as manifest
from test_ssv_pipeline import audits, gold, ingest
from test_ssv_publication import publish, read, recover

from market_data_pipeline.shared.postgres import binding

OPTIONS = dict(approved_use=fixture_grant(), operation="fixture")
__all__ = ["native_settings", "load", "manifest"]


def migrate(root, settings, manifest, **kw):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.postgres import migrate

    return migrate(root, settings, manifest, **OPTIONS, **kw)


def test_native_primary_preserves_exact_source_and_publication_then_ignores_sqlite(
    tmp_path, native_settings, manifest, load
):
    load(values=[("SYN", "12345678901234567890", "0", "12345678901234567891")])
    publish(tmp_path, manifest)
    original, frozen = gold(tmp_path, manifest), read(tmp_path, manifest)
    files = {
        str(p.relative_to(tmp_path)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    assert migrate(tmp_path, native_settings, manifest)["status"] == "MIGRATED"
    assert (
        binding(tmp_path, "finra_short_sale_volume")["postgres"]["schema"] == native_settings.schema
    )
    assert gold(tmp_path, manifest) == original and read(tmp_path, manifest) == frozen
    assert all(
        (tmp_path / name).read_bytes() == body and (tmp_path / name).stat().st_mtime_ns == stamp
        for name, (body, stamp) in files.items()
    )
    (tmp_path / "warehouse/ssv.sqlite").rename(tmp_path / "warehouse/ssv.recovery")
    (tmp_path / "parquet/catalog.sqlite").rename(tmp_path / "parquet/catalog.recovery")
    assert gold(tmp_path, manifest) == original and read(tmp_path, manifest) == frozen
    assert migrate(tmp_path, native_settings, manifest)["status"] == "ALREADY_PRIMARY"
    load(values=[("SYN", "50", "5", "100")])
    assert gold(tmp_path, manifest)["rows"][0]["short_volume"] == "50"
    assert gold(tmp_path, manifest, as_of=NOW)["rows"] == original["rows"]
    publish(tmp_path, manifest)
    assert read(tmp_path, manifest)["tables"]["gold"][0]["short_volume"] == "50"
    assert not (tmp_path / "warehouse/ssv.sqlite").exists()
    assert not (tmp_path / "parquet/catalog.sqlite").exists()


@pytest.mark.parametrize(
    "phase",
    ["staged", "before_commit", "committed", "finalized", "pointer", "before_mark", "published"],
)
def test_native_catalog_preserves_failure_recovery_and_frozen_source(
    tmp_path, native_settings, manifest, load, phase
):
    load()
    publish(tmp_path, manifest)
    migrate(tmp_path, native_settings, manifest)
    load(values=[("SYN", "50", "0", "100")])

    def fail(point):
        if point == phase:
            raise OSError("invented failure")

    with pytest.raises(OSError):
        publish(tmp_path, manifest, _fault=fail)
    visible = read(tmp_path, manifest)
    assert visible["tables"]["gold"][0]["short_volume"] == (
        "50" if phase in {"pointer", "before_mark", "published"} else "40"
    )
    load(values=[("SYN", "70", "0", "100")])
    recover(tmp_path, manifest)
    assert read(tmp_path, manifest)["tables"]["gold"][0]["short_volume"] == (
        "40" if phase in {"staged", "before_commit"} else "50"
    )
    assert read(tmp_path, manifest)["recovery_required"] is False


@pytest.mark.parametrize(
    "stage", ["after_start", "after_versions", "after_membership", "before_commit"]
)
def test_native_warehouse_rolls_back_failed_replacement(tmp_path, native_settings, manifest, stage):
    ingest(tmp_path, manifest)
    migrate(tmp_path, native_settings, manifest)
    before = gold(tmp_path, manifest)

    def fail(point):
        if point == stage:
            raise OSError("invented failure")

    with pytest.raises(RuntimeError):
        ingest(tmp_path, manifest, clock=NOW + timedelta(seconds=1), fault=fail)
    assert gold(tmp_path, manifest) == before
    assert audits(tmp_path, manifest)[0]["status"] == "FAILED"


@pytest.mark.parametrize("phase", ["before_commit", "before_bind"])
def test_native_migration_failure_never_selects_partial_target_and_can_retry(
    tmp_path, native_settings, manifest, load, phase
):
    load()
    publish(tmp_path, manifest)
    old = gold(tmp_path, manifest)

    def fail(point):
        if point == phase:
            raise OSError("invented migration failure")

    with pytest.raises(OSError):
        migrate(tmp_path, native_settings, manifest, fault=fail)
    assert binding(tmp_path, "finra_short_sale_volume") is None
    assert gold(tmp_path, manifest) == old
    migrate(tmp_path, native_settings, manifest)
    assert gold(tmp_path, manifest) == old


def test_native_first_publication_after_primary_selection_never_creates_sqlite_catalog(
    tmp_path, native_settings, manifest, load
):
    load()
    migrate(tmp_path, native_settings, manifest)
    publish(tmp_path, manifest)
    assert read(tmp_path, manifest)["tables"]["gold"][0]["short_volume"] == "40"
    assert not (tmp_path / "parquet/catalog.sqlite").exists()


def test_native_unavailable_primary_never_reads_retained_sqlite(
    tmp_path, native_settings, manifest, load, monkeypatch
):
    load()
    migrate(tmp_path, native_settings, manifest)
    with monkeypatch.context() as context:
        context.delenv(native_settings.password_env)
        with pytest.raises(ValueError):
            gold(tmp_path, manifest)
    assert gold(tmp_path, manifest)["rows"][0]["short_volume"] == "40"


def test_native_resealed_metadata_missing_fields_is_a_contract_error(
    tmp_path, native_settings, manifest, load
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.postgres import CONTRACT, DDL_SHA
    from market_data_pipeline.shared.postgres import connect

    load()
    migrate(tmp_path, native_settings, manifest)
    damaged = {"format": CONTRACT, "ddl_sha256": DDL_SHA}
    with connect(native_settings) as db:
        db.execute(
            "UPDATE primary_identity SET body=?,body_sha256=?",
            (encoded(damaged).decode(), digest(damaged)),
        )
    with pytest.raises(ValueError):
        gold(tmp_path, manifest)


def test_native_changed_schema_refuses_without_using_recovery_sqlite(
    tmp_path, native_settings, manifest, load
):
    from market_data_pipeline.shared.postgres import connect

    load()
    migrate(tmp_path, native_settings, manifest)
    with connect(native_settings) as db:
        db.execute("ALTER TABLE versions ADD COLUMN unexpected TEXT")
    with pytest.raises(ValueError):
        gold(tmp_path, manifest)


def test_native_migration_preserves_equal_capture_attempt_insertion_order(
    tmp_path, native_settings, manifest
):
    from uuid import uuid4

    from market_data_pipeline.pipelines.finra_short_sale_volume.pipeline import ingest_capture

    ingest(tmp_path, manifest)
    capture_id = str(uuid4())

    def fail(point):
        if point == "after_start":
            raise OSError("invented interruption")

    with pytest.raises(RuntimeError):
        ingest(
            tmp_path, manifest, capture_id=capture_id, clock=NOW + timedelta(seconds=1), fault=fail
        )
    ingest_capture(
        tmp_path, capture_id, manifest, **OPTIONS, clock=lambda: NOW + timedelta(seconds=2)
    )
    before = audits(tmp_path, manifest)
    assert len(before) == 3
    migrate(tmp_path, native_settings, manifest)
    assert audits(tmp_path, manifest) == before


def test_native_cli_migrates_primary_and_retries_original_capture(tmp_path, native_settings):
    import json
    from dataclasses import asdict

    import yaml
    from test_ssv_cli import config_file, invoke
    from test_ssv_parser import FIXTURES

    config = config_file(tmp_path)
    loaded = invoke(
        config,
        "run",
        "--input",
        str(FIXTURES / "invented-valid.json"),
        "--receipt",
        str(FIXTURES / "invented-valid.receipt.json"),
    )
    assert loaded.returncode == 0, loaded.stderr
    original = json.loads(loaded.stdout)
    postgres = tmp_path / "postgres.yml"
    postgres.write_text(yaml.safe_dump({"postgres": asdict(native_settings)}))
    selected = invoke(config, "migrate-postgres", "--postgres-config", str(postgres))
    assert selected.returncode == 0, selected.stderr
    assert json.loads(selected.stdout)["status"] == "MIGRATED"
    again = invoke(config, "ingest-capture", "--capture-id", original["capture_id"])
    assert again.returncode == 0 and json.loads(again.stdout) == original
    assert (
        invoke(config, "publish", "--trade-date", "2026-11-09", "--scope", "nyse_trf").returncode
        == 0
    )
    assert not (tmp_path / "local-store/parquet/catalog.sqlite").exists()


def test_native_health_and_retention_are_read_only_and_keep_primary_binding(
    tmp_path, native_settings, manifest
):
    from test_ssv_operations import health

    from market_data_pipeline.pipelines.finra_short_sale_volume import operations

    root = tmp_path / "source"
    ingest(root, manifest)
    prior = health(root, manifest)
    migrate(root, native_settings, manifest)
    before = {
        str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    }
    current = health(root, manifest)
    retained = operations.retention_dry_run(root, manifest, **OPTIONS)
    assert current["partitions"] == prior["partitions"]
    assert retained["version_count"] == 2
    marker = next(row for row in retained["files"] if row["path"] == ".postgres-primary.json")
    assert marker["action"] == "KEEP" and "STORE_IDENTITY" in marker["reasons"]
    assert {
        str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    } == before


def test_native_primary_replays_new_pg_only_history_into_explicit_recovery_target(
    tmp_path, native_settings, manifest
):
    from test_ssv_parser import FIXTURES

    from market_data_pipeline.pipelines.finra_short_sale_volume import replay

    root = tmp_path / "source"
    ingest(root, manifest)
    migrate(root, native_settings, manifest)
    import json

    values = json.loads((FIXTURES / "invented-valid.json").read_bytes())
    values[0]["shortParQuantity"] = 41
    changed = json.dumps(values).encode()
    ingest(root, manifest, payload=changed, clock=NOW + timedelta(seconds=1))
    assert gold(root, manifest)["rows"][0]["short_volume"] == "41"
    result = replay.replay(root, tmp_path / "recovery", manifest, **OPTIONS)
    assert result["status"] == "SUCCEEDED"
    assert gold(tmp_path / "recovery", manifest)["rows"] == gold(root, manifest)["rows"]


def test_native_cli_connection_failure_is_sanitized_and_does_not_bind(tmp_path, native_settings):
    from dataclasses import asdict

    import yaml
    from test_ssv_cli import config_file, invoke
    from test_ssv_parser import FIXTURES

    config = config_file(tmp_path)
    assert (
        invoke(
            config,
            "run",
            "--input",
            str(FIXTURES / "invented-valid.json"),
            "--receipt",
            str(FIXTURES / "invented-valid.receipt.json"),
        ).returncode
        == 0
    )
    postgres = tmp_path / "unavailable.yml"
    postgres.write_text(yaml.safe_dump({"postgres": asdict(native_settings) | {"port": 1}}))
    result = invoke(config, "migrate-postgres", "--postgres-config", str(postgres))
    assert result.returncode == 2 and "Traceback" not in result.stderr
    assert str(tmp_path) not in result.stderr
    assert binding(tmp_path / "local-store", "finra_short_sale_volume") is None


def test_native_symbol_order_matches_original_binary_order(
    tmp_path, native_settings, manifest, load
):
    symbols = ["A", "A.A", "AA", "A-A", "A/A", "a", "Z"]
    load(values=[(name, "10", "0", "20") for name in symbols])
    original = gold(tmp_path, manifest)
    migrate(tmp_path, native_settings, manifest)
    assert gold(tmp_path, manifest)["rows"] == original["rows"]
