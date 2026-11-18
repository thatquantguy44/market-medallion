"""PG-AC03/04/05 NAAIM retained delayed primary, without provider access."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_postgres_primary import native_settings as native_settings

from market_data_pipeline.pipelines.naaim_exposure_public_delayed import (
    operations,
    pipeline,
    postgres,
)
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.research_export import (
    export_research_artifact,
)
from market_data_pipeline.shared.postgres import binding, connect

__all__ = ["native_settings"]
NOW = datetime(2026, 11, 17, 12, tzinfo=UTC)
PAYLOAD = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()


def ingest(root, payload=PAYLOAD, when=NOW):
    return pipeline.ingest(
        payload,
        root,
        permission_reference="invented fixture permission",
        captured_at=when.isoformat(),
    )


def files(root):
    return {
        str(p.relative_to(root)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file() and p.name != ".writer.lock"
    }


def test_native_naaim_exact_migration_and_new_revision_reversion_without_json(
    tmp_path, native_settings
):
    ingest(tmp_path)
    before = pipeline._versions(tmp_path / "silver/versions.json")
    latest = pipeline.read_latest(tmp_path)
    original = files(tmp_path)
    assert postgres.migrate(tmp_path, native_settings)["versions"] == 2
    assert all(files(tmp_path)[k] == v for k, v in original.items())
    (tmp_path / "silver/versions.json").rename(tmp_path / "silver/recovery.json")
    (tmp_path / "gold/latest.json").unlink()
    assert pipeline._versions(tmp_path / "silver/versions.json") == before
    assert pipeline.read_latest(tmp_path) == latest
    assert postgres.migrate(tmp_path, native_settings)["status"] == "ALREADY_PRIMARY"
    assert ingest(tmp_path)["inserted_versions"] == 0
    assert (
        ingest(tmp_path, PAYLOAD.replace(b"71.25", b"71.250000000000000000000000000000000000001"))[
            "inserted_versions"
        ]
        == 1
    )
    assert pipeline.read_latest(tmp_path)["value"] == "71.250000000000000000000000000000000000001"
    assert ingest(tmp_path)["inserted_versions"] == 1
    versions = pipeline._versions(tmp_path / "silver/versions.json")
    assert len(versions) == 4 and versions[:2] == before
    assert versions[-1]["first_seen_at"] == before[0]["first_seen_at"]
    assert pipeline.read_latest(tmp_path)["provider_delay_months"] == 3
    assert not (tmp_path / "silver/versions.json").exists()
    export_research_artifact(
        tmp_path, tmp_path / "export.json", research_use_reference="invented fixture permission"
    )
    assert operations.health_report(tmp_path, now=NOW)["healthy"]


@pytest.mark.parametrize("point", ["before_commit", "before_bind"])
def test_naaim_native_migration_failure_retry(tmp_path, native_settings, point):
    ingest(tmp_path)

    def fail(phase):
        if phase == point:
            raise RuntimeError("injected migration")

    with pytest.raises(RuntimeError):
        postgres.migrate(tmp_path, native_settings, fault=fail)
    assert binding(tmp_path, postgres.PIPELINE) is None
    assert postgres.migrate(tmp_path, native_settings)["status"] == "MIGRATED"


@pytest.mark.parametrize(
    "point", ["after_capture", "after_event", "after_versions", "before_commit"]
)
def test_native_naaim_failed_ingestion_cannot_publish_partial_success(
    tmp_path, native_settings, monkeypatch, point
):
    ingest(tmp_path)
    postgres.migrate(tmp_path, native_settings)
    before = postgres.versions(tmp_path)
    latest = postgres.latest(tmp_path)
    counts = None
    with connect(native_settings, read_only=True) as db:
        counts = {
            n: db.execute("SELECT count(*) FROM " + n).fetchone()[0]
            for n in ("retained_captures", "capture_events", "versions")
        }
    real = postgres.publish

    def publish(*args, **kwargs):
        def fail(phase):
            if phase == point:
                raise RuntimeError("injected write")

        return real(*args, **kwargs, fault=fail)

    with monkeypatch.context() as m:
        m.setattr(postgres, "publish", publish)
        with pytest.raises(RuntimeError):
            ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))
    assert postgres.versions(tmp_path) == before and postgres.latest(tmp_path) == latest
    with connect(native_settings, read_only=True) as db:
        assert {n: db.execute("SELECT count(*) FROM " + n).fetchone()[0] for n in counts} == counts
    assert ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))["inserted_versions"] == 1


@pytest.mark.parametrize("table", ["retained_captures", "capture_events", "versions", "latest"])
def test_native_naaim_database_failure_is_transactional(
    tmp_path, native_settings, monkeypatch, table
):
    ingest(tmp_path)
    postgres.migrate(tmp_path, native_settings)
    before = postgres.versions(tmp_path)
    from market_data_pipeline.shared.postgres import Connection

    execute = Connection.execute

    def rejecting(self, statement, parameters=()):
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
        return execute(self, statement, parameters)

    with monkeypatch.context() as m:
        m.setattr(Connection, "execute", rejecting)
        with pytest.raises(ValueError, match="unavailable or invalid"):
            ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))
    assert postgres.versions(tmp_path) == before


def test_native_naaim_derived_gold_failure_is_recoverable_primary_remains_authoritative(
    tmp_path, native_settings, monkeypatch
):
    ingest(tmp_path)
    postgres.migrate(tmp_path, native_settings)
    atomic = pipeline.atomic_json

    def fail(path, value):
        if path == tmp_path / "gold/latest.json":
            raise OSError("injected derived-file failure")
        return atomic(path, value)

    with monkeypatch.context() as m:
        m.setattr(pipeline, "atomic_json", fail)
        with pytest.raises(OSError):
            ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))
    assert pipeline.read_latest(tmp_path)["value"] == "72.25"
    assert json.loads((tmp_path / "gold/latest.json").read_text())["value"] == "71.25"
    export_research_artifact(
        tmp_path, tmp_path / "export.json", research_use_reference="invented fixture permission"
    )
    assert ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))["inserted_versions"] == 0
    assert json.loads((tmp_path / "gold/latest.json").read_text())["value"] == "72.25"


@pytest.mark.parametrize("point", ["after_clear", "before_commit"])
def test_native_naaim_retention_rollback_never_removes_source_files(
    tmp_path, native_settings, monkeypatch, point
):
    ingest(tmp_path, when=NOW - timedelta(days=400))
    ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))
    postgres.migrate(tmp_path, native_settings)
    before = files(tmp_path)
    versions = postgres.versions(tmp_path)
    replace = postgres.replace_retained

    def fail(*args, **kwargs):
        def fault(phase):
            if phase == point:
                raise RuntimeError("injected retention")

        return replace(*args, **kwargs, fault=fault)

    with monkeypatch.context() as m:
        m.setattr(postgres, "replace_retained", fail)
        with pytest.raises(RuntimeError):
            operations.retain_captures(tmp_path, now=NOW, apply=True)
    assert files(tmp_path) == before and postgres.versions(tmp_path) == versions


def test_native_naaim_retention_dry_run_apply_and_empty(tmp_path, native_settings):
    first = ingest(tmp_path, when=NOW - timedelta(days=400))
    ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))
    recovery = (tmp_path / "silver/versions.json").read_bytes()
    postgres.migrate(tmp_path, native_settings)
    before = files(tmp_path)
    plan = operations.retain_captures(tmp_path, now=NOW)
    assert plan["remove_capture_ids"] == [first["capture_id"]] and files(tmp_path) == before
    applied = operations.retain_captures(tmp_path, now=NOW, apply=True)
    assert applied["applied"] and len(postgres.versions(tmp_path)) == 2
    assert not (tmp_path / "bronze" / first["capture_id"]).exists()
    assert (tmp_path / "silver/versions.json").read_bytes() == recovery
    assert operations.health_report(tmp_path, now=NOW)["healthy"]
    export_research_artifact(
        tmp_path, tmp_path / "export.json", research_use_reference="invented fixture permission"
    )
    operations.retain_captures(tmp_path, now=NOW + timedelta(days=400), apply=True)
    assert postgres.versions(tmp_path) == []
    assert (
        "no_captures"
        in operations.health_report(tmp_path, now=NOW + timedelta(days=400))["reasons"]
    )


def test_naaim_migration_records_missing_raw_without_inventing_it(tmp_path, native_settings):
    first = ingest(tmp_path)
    (tmp_path / "bronze" / first["capture_id"] / "payload.html").unlink()
    out = postgres.migrate(tmp_path, native_settings)
    assert out["missing_raw_capture_ids"] == [first["capture_id"]]
    assert len(postgres.versions(tmp_path)) == 2


def test_naaim_outage_schema_and_typed_corruption_refuse_no_fallback(
    tmp_path, native_settings, monkeypatch
):
    ingest(tmp_path)
    postgres.migrate(tmp_path, native_settings)
    with monkeypatch.context() as m:
        m.delenv(native_settings.password_env)
        with pytest.raises(ValueError, match="password"):
            pipeline._versions(tmp_path / "silver/versions.json")
        with pytest.raises(ValueError, match="password"):
            pipeline.read_latest(tmp_path)
    with connect(native_settings) as db:
        db.execute("UPDATE versions SET naaim_number=0")
    with pytest.raises(ValueError, match="integrity"):
        postgres.versions(tmp_path)
    with connect(native_settings) as db:
        db.execute("ALTER TABLE versions ADD COLUMN corrupted TEXT")
    with pytest.raises(ValueError, match="identity/schema"):
        pipeline.read_latest(tmp_path)


def test_naaim_cli_migrates_without_provider_contact(
    tmp_path, native_settings, monkeypatch, capsys
):
    from dataclasses import asdict

    import yaml

    from market_data_pipeline.pipelines.naaim_exposure_public_delayed import cli

    ingest(tmp_path)
    config = tmp_path / "pg.yml"
    config.write_text(yaml.safe_dump({"postgres": asdict(native_settings)}))

    def forbidden(*args, **kwargs):
        raise AssertionError("provider contact forbidden")

    monkeypatch.setattr(cli, "run_live", forbidden)
    assert cli.main(["--root", str(tmp_path), "--migrate-postgres", str(config)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "MIGRATED"


@pytest.mark.parametrize("mode", ["ingest", "retention"])
def test_native_naaim_commit_rejection_leaves_primary_and_files_consistent(
    tmp_path, native_settings, monkeypatch, mode
):
    from market_data_pipeline.shared.postgres import Connection

    ingest(tmp_path, when=NOW - timedelta(days=400))
    postgres.migrate(tmp_path, native_settings)
    before = postgres.versions(tmp_path)
    raw = files(tmp_path)
    commit = Connection.commit

    def reject(self):
        self.rollback()
        import psycopg

        raise psycopg.OperationalError("injected commit rejection")

    # __exit__ uses this method after all SQL writes have succeeded.
    with monkeypatch.context() as m:
        m.setattr(Connection, "commit", reject)
        with pytest.raises(ValueError, match="unavailable or invalid"):
            if mode == "ingest":
                ingest(tmp_path, PAYLOAD.replace(b"71.25", b"72.25"))
            else:
                operations.retain_captures(tmp_path, now=NOW, apply=True)
    assert postgres.versions(tmp_path) == before
    assert all(files(tmp_path)[k] == v for k, v in raw.items())
    assert Connection.commit is commit


def test_native_naaim_read_retention_without_legacy_files(tmp_path, native_settings):
    result = ingest(tmp_path)
    postgres.migrate(tmp_path, native_settings)
    (tmp_path / "silver/versions.json").unlink()
    (tmp_path / "silver" / f"{result['capture_id']}.json").unlink()
    (tmp_path / "captures" / f"{result['run_id']}.json").unlink()
    (tmp_path / "gold/latest.json").unlink()
    assert operations.health_report(tmp_path, now=NOW)["healthy"]
    assert operations.retain_captures(tmp_path, now=NOW)["remove_capture_ids"] == []
    operations.retain_captures(tmp_path, now=NOW, apply=True)
    assert len(postgres.versions(tmp_path)) == 2
    export_research_artifact(
        tmp_path, tmp_path / "export.json", research_use_reference="invented fixture permission"
    )
    assert not (tmp_path / "silver/versions.json").exists()


def test_naaim_committed_unbound_copy_rejects_changed_source_receipt(tmp_path, native_settings):
    result = ingest(tmp_path)

    def fail(phase):
        if phase == "before_bind":
            raise RuntimeError("injected unbound copy")

    with pytest.raises(RuntimeError):
        postgres.migrate(tmp_path, native_settings, fault=fail)
    path = tmp_path / "captures" / f"{result['run_id']}.json"
    original = path.read_bytes()
    body = json.loads(original)
    body["permission_reference"] = "different invented reference"
    path.write_text(json.dumps(body))
    with pytest.raises(ValueError, match="differs from retained source"):
        postgres.migrate(tmp_path, native_settings)
    path.write_bytes(original)
    assert postgres.migrate(tmp_path, native_settings)["status"] == "MIGRATED"
