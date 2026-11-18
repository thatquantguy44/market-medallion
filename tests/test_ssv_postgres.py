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
