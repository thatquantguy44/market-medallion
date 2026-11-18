"""PG-AC03/04/05 native presidential facts, sealed publication and recovery."""

import hashlib
from datetime import timedelta

import pytest
from test_postgres_primary import native_settings as native_settings
from test_presidential_bundle_assembly import NOW as PUB_NOW
from test_presidential_bundle_assembly import fixture as bundle_fixture
from test_presidential_warehouse import NOW, capture

from market_data_pipeline.pipelines.presidential_research import postgres, warehouse
from market_data_pipeline.pipelines.presidential_research.backup import (
    backup_selected_snapshot,
    restore_backup,
)
from market_data_pipeline.pipelines.presidential_research.publication import (
    publish_full_bundle,
    read_current,
    recover_publications,
)
from market_data_pipeline.shared.postgres import binding, connect

__all__ = ["native_settings"]


def files(root):
    return {
        str(p.relative_to(root)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file() and p.name != ".writer.lock"
    }


def test_native_presidential_revision_cutoff_missingness_and_recovery_absent(
    tmp_path, native_settings
):
    root = tmp_path / "store"
    a = capture(root, "8.980")
    warehouse.ingest_capture(root, "FEDFUNDS_001", a["capture_id"])
    before = warehouse.observed_as_of(root, NOW)
    original = files(root)
    assert postgres.migrate(root, native_settings)["status"] == "MIGRATED"
    assert all(files(root)[k] == v for k, v in original.items())
    assert warehouse.observed_as_of(root, NOW) == before
    (root / "silver/presidential.sqlite3").rename(root / "silver/recovery.sqlite3")
    assert postgres.migrate(root, native_settings)["status"] == "ALREADY_PRIMARY"
    for value in ("8.980", "9.010", "8.980"):
        receipt = capture(root, value)
        result = warehouse.ingest_capture(root, "FEDFUNDS_001", receipt["capture_id"])
        assert warehouse.ingest_capture(root, "FEDFUNDS_001", receipt["capture_id"])["reused"]
    assert warehouse.observed_as_of(root, NOW, capture_sequence=a["capture_sequence"]) == before
    from test_presidential_warehouse import CONFIG

    from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture

    receipt, _ = import_local_capture(
        root,
        CONFIG,
        "FEDFUNDS_001",
        b"observation_date,FEDFUNDS\n1970-01-01,.\n1970-02-01,3.50\n",
        input_kind="SYNTHETIC_FIXTURE",
        admitted_at=NOW,
    )
    result = warehouse.ingest_capture(root, "FEDFUNDS_001", receipt["capture_id"])
    latest = warehouse.observed_as_of(root, NOW)
    assert latest[0]["values"]["policy_rate_percent"] is None
    assert latest[0]["values"]["source_token"] == "." and result["missing_count"] == 1
    assert warehouse.observed_as_of(root, NOW - timedelta(microseconds=1)) == []
    warehouse.export_parquet(root, tmp_path / "facts.parquet", cutoff=NOW)
    assert not (root / "silver/presidential.sqlite3").exists()


@pytest.mark.parametrize("point", ["before_commit", "before_bind"])
def test_native_presidential_migration_retry(tmp_path, native_settings, point):
    receipt = capture(tmp_path, "1.000")
    warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])

    def fail(phase):
        if phase == point:
            raise RuntimeError("injected migration boundary")

    with pytest.raises(RuntimeError):
        postgres.migrate(tmp_path, native_settings, fault=fail)
    assert binding(tmp_path, postgres.PIPELINE) is None
    assert postgres.migrate(tmp_path, native_settings)["status"] == "MIGRATED"
    assert warehouse.observed_as_of(tmp_path, NOW)[0]["values"]["source_token"] == "1.000"


@pytest.mark.parametrize("point", ["captures", "facts", "capture_facts", "commit"])
def test_native_presidential_failed_write_rolls_back_all_membership(
    tmp_path, native_settings, monkeypatch, point
):
    a = capture(tmp_path, "1")
    warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", a["capture_id"])
    postgres.migrate(tmp_path, native_settings)
    before = warehouse.observed_as_of(tmp_path, NOW)
    b = capture(tmp_path, "2")
    original = postgres.NativeConnection.execute

    def execute(self, statement, parameters=()):
        if statement.startswith("INSERT INTO " + point + " "):
            raise ValueError("injected SQL boundary")
        return original(self, statement, parameters)

    with monkeypatch.context() as m:
        if point == "commit":

            def fail(self):
                self.rollback()
                raise ValueError("injected commit boundary")

            m.setattr(postgres.NativeConnection, "commit", fail)
        else:
            m.setattr(postgres.NativeConnection, "execute", execute)
        with pytest.raises(ValueError, match="injected"):
            warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", b["capture_id"])
    assert warehouse.observed_as_of(tmp_path, NOW) == before
    with connect(native_settings, read_only=True) as db:
        assert db.execute("SELECT count(*) FROM captures").fetchone()[0] == 1
    assert warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", b["capture_id"])["new_versions"] == 1


@pytest.mark.parametrize("point", ["after_stage", "after_rename", "after_journal", "after_pointer"])
def test_native_presidential_publication_backup_restore_without_sqlite(
    tmp_path, native_settings, monkeypatch, point
):
    root, ids, review = bundle_fixture(tmp_path, monkeypatch)
    first = publish_full_bundle(
        root, ids, review["review_id"], profile_id="fixture-complete", created_at=PUB_NOW
    )
    before = read_current(root)
    postgres.migrate(root, native_settings)
    (root / "silver/presidential.sqlite3").unlink()
    assert read_current(root) == before

    def fail(stage):
        if stage == point:
            raise RuntimeError("injected publication")

    with pytest.raises(RuntimeError, match="injected"):
        publish_full_bundle(
            root,
            ids,
            review["review_id"],
            profile_id="fixture-complete",
            created_at=PUB_NOW + timedelta(hours=1),
            fault=fail,
        )
    current = read_current(root)
    if point != "after_pointer":
        assert current["catalog"]["snapshotId"] == first["snapshotId"]
    recover_publications(root)
    assert read_current(root) == current
    with connect(native_settings, read_only=True) as db:
        assert (
            db.execute(
                "SELECT status FROM publication_journal WHERE snapshot_id=?",
                (current["catalog"]["snapshotId"],),
            ).fetchone()[0]
            == "PUBLISHED"
        )
    target = tmp_path / "backup.zip"
    backup_selected_snapshot(root, target, backed_up_at=PUB_NOW + timedelta(days=1))
    restored = tmp_path / "restored"
    restore_backup(target, restored, restored_at=PUB_NOW + timedelta(days=2))
    assert read_current(restored)["catalog"]["snapshotId"] == current["catalog"]["snapshotId"]
    assert (restored / "silver/presidential.sqlite3").exists()
    assert binding(restored, postgres.PIPELINE) is None


def test_native_presidential_schema_corruption_and_outage_refuse(
    tmp_path, native_settings, monkeypatch
):
    a = capture(tmp_path, "1")
    warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", a["capture_id"])
    postgres.migrate(tmp_path, native_settings)
    with monkeypatch.context() as m:
        m.delenv(native_settings.password_env)
        with pytest.raises(ValueError, match="password"):
            warehouse.observed_as_of(tmp_path, NOW)
    with connect(native_settings) as db:
        db.execute("ALTER TABLE facts ADD COLUMN corrupted TEXT")
    with pytest.raises(ValueError, match="identity/schema"):
        warehouse.observed_as_of(tmp_path, NOW)


@pytest.mark.parametrize("table", ["captures", "facts", "capture_facts"])
def test_database_rejection_rolls_back_presidential_transaction(tmp_path, native_settings, table):
    a = capture(tmp_path, "1")
    warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", a["capture_id"])
    postgres.migrate(tmp_path, native_settings)
    b = capture(tmp_path, "2")
    original = postgres.NativeConnection.execute
    from unittest.mock import patch

    def execute(self, statement, parameters=()):
        if statement.startswith("INSERT INTO " + table + " "):
            self.raw.execute(
                "CREATE FUNCTION reject_write() RETURNS trigger LANGUAGE plpgsql AS "
                "$$BEGIN RAISE EXCEPTION 'injected'; END$$"
            )
            self.raw.execute(
                "CREATE TRIGGER reject_write BEFORE INSERT ON "
                + table
                + " FOR EACH ROW EXECUTE FUNCTION reject_write()"
            )
        return original(self, statement, parameters)

    with patch.object(postgres.NativeConnection, "execute", execute):
        with pytest.raises(ValueError, match="statement failed"):
            warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", b["capture_id"])
    with connect(native_settings, read_only=True) as db:
        assert db.execute("SELECT count(*) FROM captures").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM facts").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM capture_facts").fetchone()[0] == 1


def test_native_presidential_cli_explicit_migration(tmp_path, native_settings, capsys):
    import json
    from dataclasses import asdict

    import yaml

    from market_data_pipeline.pipelines.presidential_research.cli import main

    a = capture(tmp_path, "1")
    warehouse.ingest_capture(tmp_path, "FEDFUNDS_001", a["capture_id"])
    config = tmp_path / "pg.yml"
    config.write_text(yaml.safe_dump({"postgres": asdict(native_settings)}))
    args = ["migrate-postgres", "--workspace", str(tmp_path), "--postgres-config", str(config)]
    assert main(args) == 0 and json.loads(capsys.readouterr().out)["status"] == "MIGRATED"
    assert main(args) == 0 and json.loads(capsys.readouterr().out)["status"] == "ALREADY_PRIMARY"
