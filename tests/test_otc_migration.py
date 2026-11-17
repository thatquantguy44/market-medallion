"""OTC-AC07 explicit fixture-only v1->v2 migration; preserve old evidence."""

import base64
import json
import sqlite3
from pathlib import Path

import pytest
from test_otc_bronze import USE

from market_data_pipeline.pipelines.finra_otc_transparency import pipeline
from market_data_pipeline.pipelines.finra_otc_transparency.catalog import select_partitions

FIXTURE = Path(__file__).parent / "fixtures/finra_otc_transparency/warehouse_v1.json"


def restore_legacy(root):
    fixture = json.loads(FIXTURE.read_text())
    root.mkdir()
    for name, encoded in fixture["files_base64"].items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(encoded))
    with sqlite3.connect(root / "otc.sqlite3") as db:
        db.executescript(fixture["sql_dump"])
        db.execute("PRAGMA application_id=" + str(fixture["application_id"]))
        db.execute("PRAGMA user_version=" + str(fixture["user_version"]))
    return fixture


def selection():
    return select_partitions(
        [{"reporting_week_start": "2026-10-19", "product": "ATS_WEEKLY_DETAIL", "tier": "T1"}]
    )


def state(root):
    return {
        str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    }


def legacy_tables(root):
    with sqlite3.connect(root / "otc.sqlite3") as db:
        return {
            table: db.execute("SELECT * FROM " + table + " ORDER BY 1,2").fetchall()
            for table in ("otc_attempts", "otc_publications", "otc_rows")
        }


def test_frozen_v1_is_readable_without_automatic_migration_or_mutation(tmp_path):
    root = tmp_path / "old"
    restore_legacy(root)
    before = state(root)
    panel = pipeline.inspect_panel(root, partitions=selection(), approved_use=USE)
    assert len(panel["rows"]) == 3 and panel["status"] == "DEGRADED_FIXTURE"
    assert state(root) == before


def test_explicit_migration_preserves_legacy_tables_and_raw_evidence(tmp_path):
    root = tmp_path / "old"
    fixture = restore_legacy(root)
    before = state(root)
    tables = legacy_tables(root)
    result = pipeline.migrate_warehouse(root, approved_use=USE)
    assert result["status"] == "MIGRATED" and result["seeded_snapshots"] == 1
    assert legacy_tables(root) == tables
    assert {p: v for p, v in state(root).items() if p != "otc.sqlite3"} == {
        p: v for p, v in before.items() if p != "otc.sqlite3"
    }
    panel = pipeline.inspect_panel(root, partitions=selection(), approved_use=USE)
    assert len(panel["rows"]) == 3
    first = fixture["attempts"][0]
    assert all(r["first_seen_at"] == first["captured_at"] for r in panel["rows"])
    assert panel["partitions"][0]["latest_attempt"]["status"] == "REJECTED"
    saved = state(root)
    assert pipeline.migrate_warehouse(root, approved_use=USE)["status"] == "ALREADY_CURRENT"
    assert state(root) == saved
    # Explicit v2 processing must retain the original deferred v1 terminal attempt.
    deferred = fixture["attempts"][1]["capture_id"]
    assert pipeline.publish_capture(root, deferred, approved_use=USE)["status"] == "PUBLISHED"
    assert legacy_tables(root) == tables
    assert any(
        r["weekly_shares"] == "9007199254740994"
        for r in pipeline.inspect_panel(root, partitions=selection(), approved_use=USE)["rows"]
    )


def test_old_warehouse_requires_explicit_migration_before_processing(tmp_path):
    root = tmp_path / "old"
    fixture = restore_legacy(root)
    before = state(root)
    with pytest.raises(ValueError, match="migrat"):
        pipeline.publish_capture(root, fixture["attempts"][1]["capture_id"], approved_use=USE)
    assert state(root) == before


@pytest.mark.parametrize("boundary", ["seed", "metadata", "commit"])
def test_migration_failure_is_atomic_and_leaves_raw_and_legacy_contract_intact(
    tmp_path, monkeypatch, boundary
):
    from market_data_pipeline.pipelines.finra_otc_transparency.io import history, warehouse

    root = tmp_path / "old"
    restore_legacy(root)
    before = state(root)
    tables = legacy_tables(root)
    if boundary == "seed":
        original = history.append_snapshot

        def fail(*a, **kw):
            original(*a, **kw)
            raise sqlite3.OperationalError("injected after seeding")

        monkeypatch.setattr(history, "append_snapshot", fail)
    elif boundary == "commit":

        def fail(db):
            raise sqlite3.OperationalError("injected commit failure")

        monkeypatch.setattr(warehouse, "_commit", fail)
    else:
        with sqlite3.connect(root / "otc.sqlite3") as db:
            db.execute(
                "CREATE TRIGGER fail BEFORE UPDATE ON otc_metadata "
                "BEGIN SELECT RAISE(ABORT,'metadata'); END"
            )
    with pytest.raises(sqlite3.Error):
        pipeline.migrate_warehouse(root, approved_use=USE)
    assert legacy_tables(root) == tables
    with sqlite3.connect(root / "otc.sqlite3") as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert (
            db.execute("SELECT count(*) FROM sqlite_master WHERE name LIKE 'otc2_%'").fetchone()[0]
            == 0
        )
    assert {p: v for p, v in state(root).items() if p != "otc.sqlite3"} == {
        p: v for p, v in before.items() if p != "otc.sqlite3"
    }


def test_migration_refuses_corrupt_source_and_exhausted_budget_without_upgrading(tmp_path):
    root = tmp_path / "old"
    fixture = restore_legacy(root)
    before = state(root)
    with pytest.raises(ValueError):
        pipeline.migrate_warehouse(root, approved_use=USE, max_captures=2)
    assert state(root) == before
    raw = root / "bronze/captures" / fixture["attempts"][0]["capture_id"] / "parts/file.bin"
    raw.write_bytes(raw.read_bytes() + b"corrupt")
    with pytest.raises(ValueError):
        pipeline.migrate_warehouse(root, approved_use=USE)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1


def test_migration_permission_gate_runs_before_any_filesystem_access(tmp_path):
    root = tmp_path / "denied"
    with pytest.raises(PermissionError):
        pipeline.migrate_warehouse(root, approved_use=None)
    assert not root.exists()
