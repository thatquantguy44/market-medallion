"""SSV-AC10: atomic immutable publication facts and a recoverable journal."""

import pytest
from test_ssv_bronze import fixture_grant
from test_ssv_gold_query import load as load
from test_ssv_parser import manifest as manifest
from test_ssv_snapshot import snapshot

from market_data_pipeline.pipelines.finra_short_sale_volume.io import catalog

__all__ = ["manifest", "load"]
ID = "00000000-0000-0000-0000-000000000010"


def metadata(projection):
    return dict(
        generation_id=ID,
        ordinal=1,
        previous_id=None,
        generated_at="2026-11-13T16:00:00.000000Z",
        request=projection["request"],
        research=projection["research"],
        source_warehouse_id=projection["source_warehouse_id"],
        source_state_sha256=projection["state_sha256"],
    )


def open_catalog(root, manifest, create=False):
    return catalog.connect(
        root, manifest, approved_use=fixture_grant(), operation="fixture", create=create
    )


def test_ssv_ac10_catalog_facts_and_pending_record_commit_atomically(tmp_path, manifest, load):
    load()
    projection = snapshot(tmp_path, manifest)
    record = metadata(projection)
    with open_catalog(tmp_path, manifest, True) as db:
        catalog.commit(db, record, projection["tables"])
        assert catalog.get(db, ID)["state"] == "COMMITTED"
        assert catalog.rows(db, ID) == projection["tables"]
        assert catalog.pending(db)[0]["generation_id"] == ID
        catalog.mark(db, ID, "2026-11-13T16:01:00.000000Z")
        assert catalog.get(db, ID)["state"] == "PUBLISHED" and catalog.pending(db) == []
    with open_catalog(tmp_path, manifest) as db:
        assert catalog.rows(db, ID) == projection["tables"]


def test_ssv_ac10_catalog_rollback_keeps_no_partial_generation(tmp_path, manifest, load):
    load()
    projection = snapshot(tmp_path, manifest)

    def fail():
        raise OSError("invented-private-diagnostic")

    with open_catalog(tmp_path, manifest, True) as db:
        with pytest.raises(OSError):
            catalog.commit(db, metadata(projection), projection["tables"], before_commit=fail)
        assert catalog.get(db, ID) is None
        assert db.execute("SELECT COUNT(*) FROM snapshot_rows").fetchone()[0] == 0


@pytest.mark.parametrize("damage", ["missing_db", "binding", "schema"])
def test_ssv_ac10_bound_catalog_damage_never_reinitializes(tmp_path, manifest, load, damage):
    load()
    with open_catalog(tmp_path, manifest, True):
        pass
    if damage == "missing_db":
        (tmp_path / "parquet/catalog.sqlite").unlink()
    elif damage == "binding":
        (tmp_path / "parquet.json").write_text("{}")
    else:
        import sqlite3

        with sqlite3.connect(tmp_path / "parquet/catalog.sqlite") as db:
            db.execute("CREATE TABLE foreign_table(value TEXT)")
    with pytest.raises(ValueError):
        with open_catalog(tmp_path, manifest, True):
            pass


def test_ssv_ac10_catalog_read_missing_root_and_admission_have_no_side_effects(tmp_path, manifest):
    root = tmp_path / "absent"
    with pytest.raises(ValueError):
        with open_catalog(root, manifest):
            pass
    with pytest.raises(PermissionError):
        with catalog.connect(
            root, manifest, approved_use=fixture_grant(), operation="transform", create=True
        ):
            pass
    assert not root.exists()
