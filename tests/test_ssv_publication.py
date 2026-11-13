"""SSV-AC10/C07: frozen files, pointer and catalog survive publication failures."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_gold_query import load as load
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume import publication
from market_data_pipeline.pipelines.finra_short_sale_volume.io import catalog

__all__ = ["load", "manifest"]
OPTIONS = dict(approved_use=fixture_grant(), operation="fixture")


def publish(root, manifest, **kwargs):
    return publication.publish(
        root, manifest, trade_date="2026-11-09", scopes=["nyse_trf"], **OPTIONS, **kwargs
    )


def read(root, manifest, **kwargs):
    return publication.read(root, manifest, **OPTIONS, **kwargs)


def recover(root, manifest, **kwargs):
    return publication.recover(root, manifest, **OPTIONS, **kwargs)


def test_ssv_ac10_publish_is_typed_coherent_and_preserves_operational_bytes(
    tmp_path, manifest, load
):
    load("2026-11-06", values=[("SYN", "20", "0", "100")])
    load(values=[("SYN", "50", "5", "100")])
    original = {
        str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
    }
    result = publish(tmp_path, manifest)
    assert result["state"] == "PUBLISHED" and result["recovery_required"] is False
    assert result["row_counts"]["silver"] == 2
    selected = read(tmp_path, manifest)
    assert selected["tables"]["research"][0]["short_share_change"] == "0.3"
    assert selected["tables"]["gold"][0]["short_volume"] == "50"
    assert selected["generation_id"] == result["generation_id"]
    assert all((tmp_path / name).read_bytes() == body for name, body in original.items())
    assert publish(tmp_path, manifest)["generation_id"] == result["generation_id"]


@pytest.mark.parametrize(
    "phase",
    ["staged", "before_commit", "committed", "finalized", "pointer", "before_mark", "published"],
)
def test_ssv_ac10_failure_recovers_prior_or_exact_new_generation(tmp_path, manifest, load, phase):
    load()
    prior = publish(tmp_path, manifest)
    load(values=[("SYN", "50", "0", "100")])

    def fail(at):
        if at == phase:
            raise OSError("private invented fault")

    with pytest.raises(OSError):
        publish(tmp_path, manifest, _fault=fail)
    visible = read(tmp_path, manifest)
    advanced = phase in {"pointer", "before_mark", "published"}
    assert visible["tables"]["gold"][0]["short_volume"] == ("50" if advanced else "40")
    assert visible["recovery_required"] == (
        phase in {"committed", "finalized", "pointer", "before_mark"}
    )
    # Recovery must finish frozen 50 rather than recollecting newer operational 70.
    load(values=[("SYN", "70", "0", "100")])
    restored = recover(tmp_path, manifest)
    selected = read(tmp_path, manifest)
    committed = phase not in {"staged", "before_commit"}
    assert selected["tables"]["gold"][0]["short_volume"] == ("50" if committed else "40")
    assert selected["state"] == restored["state"] == "PUBLISHED"
    assert selected["recovery_required"] is False
    assert (selected["generation_id"] == prior["generation_id"]) != committed
    with catalog.connect(tmp_path, manifest, **OPTIONS) as db:
        assert not catalog.pending(db)
        assert catalog.rows(db, selected["generation_id"]) == selected["tables"]
    assert not list((tmp_path / "parquet/.staging").iterdir())


@pytest.mark.parametrize(
    "damage", ["parquet", "manifest", "catalog", "pointer", "missing", "symlink"]
)
def test_ssv_ac10_published_corruption_refuses_without_repair(tmp_path, manifest, load, damage):
    load()
    result = publish(tmp_path, manifest)
    final = tmp_path / "parquet/snapshots" / result["generation_id"]
    if damage == "parquet":
        (final / "gold.parquet").write_bytes(b"bad")
    elif damage == "manifest":
        (final / "manifest.json").write_text("{}")
    elif damage == "catalog":
        import sqlite3

        with sqlite3.connect(tmp_path / "parquet/catalog.sqlite") as db:
            db.execute("DELETE FROM snapshot_rows WHERE table_name='gold'")
    elif damage == "pointer":
        (tmp_path / "parquet/current.json").write_text('{"generation_id":"../../escape"}')
    elif damage == "missing":
        (tmp_path / "parquet/current.json").unlink()
    else:
        path = final / "gold.parquet"
        path.rename(final / "saved")
        path.symlink_to(final / "saved")
    with pytest.raises(ValueError):
        read(tmp_path, manifest)
    with pytest.raises(ValueError):
        publish(tmp_path, manifest)


def test_ssv_ac10_corrupt_pending_keeps_previous_reader_but_blocks_recovery(
    tmp_path, manifest, load
):
    load()
    prior = publish(tmp_path, manifest)
    load(values=[("SYN", "50", "0", "100")])

    def fail(at):
        if at == "committed":
            raise OSError

    with pytest.raises(OSError):
        publish(tmp_path, manifest, _fault=fail)
    stage = next((tmp_path / "parquet/.staging").iterdir())
    (stage / "gold.parquet").write_bytes(b"corrupt")
    assert read(tmp_path, manifest)["generation_id"] == prior["generation_id"]
    for action in (recover, publish):
        with pytest.raises(ValueError):
            action(tmp_path, manifest)
    assert stage.exists()


def test_ssv_ac10_generation_id_conflict_and_return_to_older_projection(tmp_path, manifest, load):
    load()
    first = publish(tmp_path, manifest)
    with pytest.raises(ValueError):
        publish(tmp_path, manifest, generation_id="../bad")
    second = publish(tmp_path, manifest, as_of=NOW + timedelta(seconds=10))
    third = publish(tmp_path, manifest)
    assert len({r["generation_id"] for r in (first, second, third)}) == 3
    assert read(tmp_path, manifest)["generation_id"] == third["generation_id"]
    with pytest.raises(ValueError):
        publish(tmp_path, manifest, generation_id=first["generation_id"])


def test_ssv_ac10_concurrent_unchanged_publications_serialize(tmp_path, manifest, load):
    load()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: publish(tmp_path, manifest), range(2)))
    assert results[0]["generation_id"] == results[1]["generation_id"]


def test_ssv_ac10_empty_tables_and_bounds(tmp_path, manifest, load):
    load(values=[])
    result = publish(tmp_path, manifest)
    selected = read(tmp_path, manifest)
    assert result["row_counts"]["gold"] == result["row_counts"]["research"] == 0
    assert selected["tables"]["coverage"][0]["latest_attempt_status"] == "VALID_EMPTY"
    for options in ({"max_rows": 1}, {"max_bytes": 10}):
        with pytest.raises(ValueError, match="budget"):
            read(tmp_path, manifest, **options)


def test_ssv_ac10_admission_and_missing_read_never_create_root(tmp_path, manifest):
    absent = tmp_path / "absent"
    for action in (publication.publish, publication.read, publication.recover):
        with pytest.raises(PermissionError):
            action(absent, manifest, approved_use=fixture_grant(), operation="transform")
    with pytest.raises(ValueError):
        read(absent, manifest)
    assert not absent.exists()


@pytest.mark.parametrize("phase", ["staged", "committed", "pointer"])
def test_ssv_ac10_first_publication_failure_has_no_false_success(tmp_path, manifest, load, phase):
    load()

    def fail(at):
        if at == phase:
            raise OSError

    with pytest.raises(OSError):
        publish(tmp_path, manifest, _fault=fail)
    with catalog.connect(tmp_path, manifest, **OPTIONS) as db:
        assert catalog.latest(db) is None
    if phase == "pointer":
        selected = read(tmp_path, manifest)
        assert selected["state"] == "COMMITTED" and selected["published_at"] is None
    else:
        with pytest.raises(ValueError, match="no published"):
            read(tmp_path, manifest)
    result = recover(tmp_path, manifest)
    assert result["state"] == ("NO_PUBLICATION" if phase == "staged" else "PUBLISHED")


def test_ssv_ac10_interrupted_file_write_cleans_only_uncommitted_stage(
    tmp_path, manifest, load, monkeypatch
):
    load()
    prior = publish(tmp_path, manifest)
    load(values=[("SYN", "50", "0", "100")])
    original = publication.parquet.write_table

    def interrupted(path, name, rows, **options):
        if name == "gold":
            raise OSError("private interrupted write")
        return original(path, name, rows, **options)

    monkeypatch.setattr(publication.parquet, "write_table", interrupted)
    with pytest.raises(OSError):
        publish(tmp_path, manifest)
    assert not list((tmp_path / "parquet/.staging").iterdir())
    assert read(tmp_path, manifest)["generation_id"] == prior["generation_id"]


@pytest.mark.parametrize("owned", [True, False])
def test_ssv_ac10_orphan_cleanup_requires_exact_owner_and_safe_contents(
    tmp_path, manifest, load, owned
):
    from uuid import uuid4

    from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import encoded

    load()
    publish(tmp_path, manifest)
    generation_id = str(uuid4())
    stage = tmp_path / "parquet/.staging" / generation_id
    stage.mkdir()
    marker = publication._owner(tmp_path, generation_id) if owned else {}
    (stage / "owner.json").write_bytes(encoded(marker))
    (stage / "silver.parquet").write_bytes(b"invented interrupted file")
    if owned:
        assert recover(tmp_path, manifest)["state"] == "PUBLISHED"
        assert not stage.exists()
    else:
        with pytest.raises(ValueError):
            recover(tmp_path, manifest)
        assert stage.exists()


def test_ssv_ac10_pointer_rollback_refuses_even_an_intact_older_generation(
    tmp_path, manifest, load
):
    load()
    publish(tmp_path, manifest)
    path = tmp_path / "parquet/current.json"
    prior = path.read_bytes()
    load(values=[("SYN", "50", "0", "100")])
    publish(tmp_path, manifest)
    path.write_bytes(prior)
    for action in (read, recover, publish):
        with pytest.raises(ValueError, match="regressed"):
            action(tmp_path, manifest)
    assert path.read_bytes() == prior


@pytest.mark.parametrize(
    "values",
    [
        [("SYN", "9" * 4096, "-0.00", "9" * 4096)],
        [("SYN", "40.125000", "0.000125", "100.500000")],
        [("SYN", "1E-4096", "0", "9" * 4096)],
    ],
)
def test_ssv_ac10_pipeline_to_parquet_preserves_extreme_decimal_text(
    tmp_path, manifest, load, values
):
    import pyarrow.parquet as pq

    from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import decode_decimal

    load(values=values)
    result = publish(tmp_path, manifest)
    selected = read(tmp_path, manifest)
    fact = selected["tables"]["gold"][0]
    assert fact["short_volume"] == values[0][1]
    assert fact["short_exempt_volume"] == values[0][2]
    final = tmp_path / "parquet/snapshots" / result["generation_id"]
    typed = pq.read_table(final / "gold.parquet").to_pylist()[0]
    for field in ("short_volume", "short_exempt_volume", "total_volume", "short_share"):
        assert decode_decimal(typed[field]) == fact[field]


def test_ssv_ac10_publication_preserves_removal_nulls_and_partial_audit(tmp_path, manifest, load):
    load()
    load(values=[])
    load(values=[("SYN", "70", "0", "100")], total=2)
    publish(tmp_path, manifest)
    tables = read(tmp_path, manifest)["tables"]
    assert tables["gold"] == [] and tables["research"] == []
    assert tables["silver"][-1]["removed"] is True
    assert tables["silver"][-1]["short_volume"] is None
    assert tables["audit"][-1]["status"] == "PARTIAL"
    assert tables["coverage"][0]["latest_attempt_status"] == "PARTIAL"
    assert tables["coverage"][0]["committed_rows"] == 0
