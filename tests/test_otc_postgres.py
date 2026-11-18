"""PG-AC03/04/05 OTC native primary preserves T07 fixture history and gates."""

from datetime import UTC, datetime

import pytest
from test_otc_bronze import USE
from test_otc_migration import restore_legacy, selection, state
from test_otc_pipeline import PROFILES, partition, payload, run
from test_otc_versions import archived
from test_postgres_primary import native_settings as native_settings

from market_data_pipeline.pipelines.finra_otc_transparency import pipeline
from market_data_pipeline.shared.postgres import binding

__all__ = ["native_settings"]


def migrate(root, settings, **kw):
    from market_data_pipeline.pipelines.finra_otc_transparency.io.postgres import migrate

    return migrate(root, settings, approved_use=USE, **kw)


def panel(root, profile, **kw):
    return pipeline.inspect_panel(root, partitions=[partition(profile)], approved_use=USE, **kw)


def history(root, profile, **kw):
    return pipeline.observation_history(
        root, partitions=[partition(profile)], approved_use=USE, **kw
    )


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_native_otc_preserves_history_and_runs_without_recovery_sqlite(
    tmp_path, native_settings, profile
):
    root = tmp_path / "source"
    first = run(root, profile)
    run(root, profile)
    before, versions = panel(root, profile), history(root, profile)
    first_panel = panel(
        root, profile, as_of=first["captured_at"], as_of_sequence=first["capture_sequence"]
    )
    original = state(root)
    assert migrate(root, native_settings)["status"] == "MIGRATED"
    assert panel(root, profile) == before and history(root, profile) == versions
    assert all(state(root)[k] == v for k, v in original.items())
    (root / "otc.sqlite3").rename(root / "otc.recovery")
    assert panel(root, profile) == before
    assert pipeline.migrate_warehouse(root, approved_use=USE)["status"] == "ALREADY_CURRENT"
    assert migrate(root, native_settings)["status"] == "ALREADY_PRIMARY"
    changed = payload(profile).replace(b"12345678901234567890", b"12345678901234567891")
    correction = run(root, profile, payload=changed)
    assert any(r["weekly_shares"] == "12345678901234567891" for r in panel(root, profile)["rows"])
    assert (
        panel(root, profile, as_of=first["captured_at"], as_of_sequence=first["capture_sequence"])[
            "rows"
        ]
        == first_panel["rows"]
    )
    run(root, profile)
    shortened = b"\n".join(payload(profile).splitlines()[:-1]) + b"\n"
    frozen = panel(root, profile)["rows"]
    run(root, profile, payload=shortened, expected_rows=2, body_complete=False)
    assert panel(root, profile)["rows"] == frozen
    run(root, profile, payload=shortened, expected_rows=2)
    assert len(panel(root, profile)["rows"]) == 2
    assert sum(v["state"] == "REMOVED" for v in history(root, profile)["versions"]) == 1
    assert (
        len(
            panel(
                root,
                profile,
                as_of=correction["captured_at"],
                as_of_sequence=correction["capture_sequence"],
            )["rows"]
        )
        == 3
    )
    assert not (root / "otc.sqlite3").exists()


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_native_otc_equal_clocks_keep_original_durable_sequence(tmp_path, native_settings, profile):
    root = tmp_path / "source"
    stamp = datetime(2026, 11, 16, 15, tzinfo=UTC)
    a = archived(root, profile, payload(profile), stamp)
    pipeline.publish_capture(root, a["capture_id"], approved_use=USE)
    migrate(root, native_settings)
    b = archived(root, profile, payload(profile).replace(b"12345678901234567890", b"7"), stamp)
    pipeline.publish_capture(root, b["capture_id"], approved_use=USE)
    assert any(
        r["weekly_shares"] == "12345678901234567890"
        for r in panel(root, profile, as_of=stamp, as_of_sequence=a["capture_sequence"])["rows"]
    )
    assert any(
        r["weekly_shares"] == "7"
        for r in panel(root, profile, as_of=stamp, as_of_sequence=b["capture_sequence"])["rows"]
    )


def test_native_otc_legacy_upgrade_stays_explicit_and_preserves_old_evidence(
    tmp_path, native_settings
):
    root = tmp_path / "source"
    fixture = restore_legacy(root)
    before = state(root)
    with pytest.raises(ValueError, match="migrat"):
        migrate(root, native_settings)
    assert state(root) == before
    pipeline.migrate_warehouse(root, approved_use=USE)
    expected = pipeline.inspect_panel(root, partitions=selection(), approved_use=USE)
    migrate(root, native_settings)
    assert pipeline.inspect_panel(root, partitions=selection(), approved_use=USE) == expected
    pipeline.publish_capture(root, fixture["attempts"][1]["capture_id"], approved_use=USE)
    assert any(
        r["weekly_shares"] == "9007199254740994"
        for r in pipeline.inspect_panel(root, partitions=selection(), approved_use=USE)["rows"]
    )


@pytest.mark.parametrize("phase", ["before_commit", "before_bind"])
def test_native_otc_migration_failure_does_not_select_incomplete_primary(
    tmp_path, native_settings, phase
):
    root = tmp_path / "source"
    run(root)
    before = panel(root, PROFILES[0])

    def fail(point):
        if point == phase:
            raise OSError("invented migration failure")

    with pytest.raises(OSError):
        migrate(root, native_settings, fault=fail)
    assert binding(root, "finra_otc_transparency") is None
    assert panel(root, PROFILES[0]) == before
    migrate(root, native_settings)
    assert panel(root, PROFILES[0]) == before


def test_native_otc_empty_initialization_invents_no_source_observations(tmp_path, native_settings):
    from market_data_pipeline.pipelines.finra_otc_transparency.io.postgres import initialize_empty

    root = tmp_path / "empty"
    assert initialize_empty(root, native_settings)["status"] == "INITIALIZED"
    assert panel(root, PROFILES[0])["rows"] == []
    assert history(root, PROFILES[0])["versions"] == []
    assert not list((root / "bronze/captures").iterdir())
    assert not (root / "otc.sqlite3").exists()


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
@pytest.mark.parametrize("boundary", ["versions", "members", "snapshot", "status", "commit"])
def test_native_otc_replacement_fault_rolls_back_every_history_component(
    tmp_path, native_settings, profile, boundary, monkeypatch
):
    from market_data_pipeline.pipelines.finra_otc_transparency.io import history as engine
    from market_data_pipeline.pipelines.finra_otc_transparency.io import warehouse

    root = tmp_path / "source"
    run(root, profile)
    migrate(root, native_settings)
    original_panel, original_history = panel(root, profile), history(root, profile)
    if boundary == "commit":
        commit = warehouse._commit
        calls = 0

        def fail(db):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("invented commit failure")
            return commit(db)

        monkeypatch.setattr(warehouse, "_commit", fail)
    else:
        append = engine.append_snapshot

        def fail(db, *args):
            db.execute(
                "CREATE FUNCTION injected_failure() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'invented transaction failure'; END $$"
            )
            if boundary == "status":
                db.execute(
                    "CREATE TRIGGER injected BEFORE UPDATE OF status ON otc2_attempts "
                    "FOR EACH ROW WHEN (NEW.status='PUBLISHED') EXECUTE FUNCTION injected_failure()"
                )
            else:
                table = {
                    "versions": "otc2_versions",
                    "members": "otc2_members",
                    "snapshot": "otc2_snapshots",
                }[boundary]
                db.execute(
                    "CREATE TRIGGER injected BEFORE INSERT ON "
                    + table
                    + " FOR EACH ROW EXECUTE FUNCTION injected_failure()"
                )
            return append(db, *args)

        monkeypatch.setattr(engine, "append_snapshot", fail)
    raw = payload(profile).replace(b"12345678901234567890", b"7")
    raw = b"\n".join(raw.splitlines()[:-1]) + b"\n"
    with pytest.raises((OSError, ValueError)):
        run(root, profile, payload=raw, expected_rows=2)
    current = panel(root, profile)
    assert current["rows"] == original_panel["rows"]
    assert current["partitions"][0]["latest_attempt"]["status"] == "FAILED"
    assert history(root, profile) == original_history


def test_native_otc_missing_logical_identity_is_a_contract_error(tmp_path, native_settings):
    from market_data_pipeline.shared.postgres import connect

    root = tmp_path / "source"
    run(root)
    migrate(root, native_settings)
    with connect(native_settings) as db:
        db.execute("DELETE FROM otc_metadata")
    with pytest.raises(ValueError):
        panel(root, PROFILES[0])


def test_native_otc_unavailable_primary_and_storage_ceiling_refuse_fallback(
    tmp_path, native_settings, monkeypatch
):
    from market_data_pipeline.pipelines.finra_otc_transparency.io import warehouse

    root = tmp_path / "source"
    run(root)
    migrate(root, native_settings)
    with monkeypatch.context() as context:
        context.delenv(native_settings.password_env)
        with pytest.raises(ValueError):
            panel(root, PROFILES[0])
    with monkeypatch.context() as context:
        context.setattr(warehouse, "MAX_DB_BYTES", 1)
        with pytest.raises(ValueError):
            panel(root, PROFILES[0])
    assert len(panel(root, PROFILES[0])["rows"]) == 3


def test_native_otc_cli_empty_init_has_no_invented_ack_or_observations(tmp_path, native_settings):
    import json
    from dataclasses import asdict

    import yaml
    from test_otc_cli import invoke

    pg = tmp_path / "postgres.yml"
    pg.write_text(yaml.safe_dump({"postgres": asdict(native_settings)}))
    root = tmp_path / "empty"
    result = invoke("init-postgres", "--root", root, "--postgres-config", pg)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["versions"] == 0
    assert not list((root / "bronze/captures").iterdir())
    again = invoke("init-postgres", "--root", root, "--postgres-config", pg)
    assert again.returncode == 0 and json.loads(again.stdout)["status"] == "ALREADY_PRIMARY"
    assert not (root / "otc.sqlite3").exists()


def test_native_otc_cli_fixture_migration_preserves_readback_without_sqlite(
    tmp_path, native_settings
):
    from dataclasses import asdict

    import yaml
    from test_otc_cli import invoke

    root = tmp_path / "source"
    run(root)
    expected = panel(root, PROFILES[0])
    pg = tmp_path / "postgres.yml"
    pg.write_text(yaml.safe_dump({"postgres": asdict(native_settings)}))
    result = invoke(
        "migrate-postgres", "--invented-fixture", "--root", root, "--postgres-config", pg
    )
    assert result.returncode == 0, result.stderr
    (root / "otc.sqlite3").rename(root / "otc.recovery")
    assert panel(root, PROFILES[0]) == expected
    result = invoke("migrate-warehouse", "--invented-fixture", "--root", root)
    assert result.returncode == 0, result.stderr
