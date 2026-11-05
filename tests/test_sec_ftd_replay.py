"""S11-A09: offline replay preserves original admission, ordering and knowledge."""

import hashlib
import json
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from test_sec_ftd_revisions import (
    CLOCK,
    FIXTURE,
    baseline_three,
    bundle,
    capture,
    evidence,
    setup,
    without_first,
)

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import gold, run
from market_data_pipeline.pipelines.sec_fails_to_deliver.replay import replay
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import SQLiteWarehouse


def destination(path, root=None):
    value = yaml.safe_load(path.read_text())
    value["root"] = str(root or path.parent / "replayed")
    target = path.parent / "destination.yml"
    target.write_text(yaml.safe_dump(value))
    return target


def state(root, settings):
    with SQLiteWarehouse(root, settings) as warehouse:
        return {
            name: [
                dict(row)
                for row in warehouse.connection.execute(
                    f"SELECT * FROM sec_ftd_{name} ORDER BY {key}"
                )
            ]
            for name, key in (
                ("facts", "version_id"),
                ("coverage", "version_id"),
                ("audit", "run_id"),
            )
        }


def source_bytes(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_equal_clock_corrections_reversions_unchanged_and_cutoffs_replay_exactly(
    tmp_path, monkeypatch
):
    from market_data_pipeline.shared import http

    path, source = setup(tmp_path)
    for payload in (FIXTURE, FIXTURE.replace(b"12.3400", b"12.3500"), FIXTURE, FIXTURE):
        capture(path, source, payload, 0)
    config = load_config(path)
    before = source_bytes(config.root)

    def forbidden(*args, **kwargs):
        pytest.fail("replay must never initialize HTTP routing")

    monkeypatch.setattr(http, "prepare_http", forbidden)
    target = destination(path)
    result = replay(config.root, target)
    assert result["replayed"] == 4 and result["succeeded"] == 4
    rebuilt = load_config(target)
    assert state(config.root, config.storage) == state(rebuilt.root, rebuilt.storage)
    for sequence in range(5):
        assert gold(path, limit=20, as_of=CLOCK, as_of_sequence=sequence) == gold(
            target, limit=20, as_of=CLOCK, as_of_sequence=sequence
        )
    assert gold(path, limit=20, as_of=CLOCK - timedelta(microseconds=1)) == gold(
        target, limit=20, as_of=CLOCK - timedelta(microseconds=1)
    )
    assert source_bytes(config.root) == before
    assert (
        json.loads((config.root / ".warehouse.json").read_text())["warehouse_id"]
        != json.loads((rebuilt.root / ".warehouse.json").read_text())["warehouse_id"]
    )
    following = capture(target, source, FIXTURE.replace(b"12.3400", b"12.3600"), 1)
    assert following["capture_sequence"] == 5


def test_withdrawal_reappearance_and_proof_replay_without_live_input_paths(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, baseline_three(), 0)
    for payload, second in ((without_first(), 1), (baseline_three(), 2)):
        raw = bundle(payload)
        source.write_bytes(raw)
        evidence(path, raw)
        run(path, "fixture", clock=lambda second=second: CLOCK + timedelta(seconds=second))
    config = load_config(path)
    target = destination(path)
    source.unlink()
    (tmp_path / "completeness.json").unlink()
    value = yaml.safe_load(target.read_text())
    value["network"] = {"mode": "proxy", "proxy_url_env": "UNSET_PROXY", "ca_bundle": "missing.pem"}
    value["partitions"]["fixture"]["manifest"] = "missing-manifest.yml"
    target.write_text(yaml.safe_dump(value))
    assert replay(config.root, target)["replayed"] == 3
    rebuilt = load_config(target)
    assert state(config.root, config.storage) == state(rebuilt.root, rebuilt.storage)
    for second in (0, 1, 2):
        assert gold(
            path, limit=20, as_of=CLOCK + timedelta(seconds=second), include_withdrawn=True
        ) == gold(target, limit=20, as_of=CLOCK + timedelta(seconds=second), include_withdrawn=True)


def test_legacy_receipt_and_historical_t04_rejection_are_not_readmitted(tmp_path):
    path, source = setup(tmp_path)
    first = capture(path, source, FIXTURE, 0)
    changed = capture(path, source, FIXTURE.replace(b"12.3400", b"12.3500"), 1)
    config = load_config(path)
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        warehouse.connection.execute(
            "DELETE FROM sec_ftd_facts WHERE run_id=?", (changed["run_id"],)
        )
        warehouse.connection.execute(
            "UPDATE sec_ftd_audit SET status='REJECTED',row_count=0,"
            "error_code='REVISION_REQUIRES_T05' WHERE run_id=?",
            (changed["run_id"],),
        )
        warehouse.connection.execute(
            "UPDATE sec_ftd_coverage SET coverage_state='FAILED',"
            "validation_state='REJECTED' WHERE run_id=?",
            (changed["run_id"],),
        )
    for run_id in (first["run_id"], changed["run_id"]):
        directory = config.root / "bronze" / run_id
        (directory / "outcome.json").unlink()
    receipt_path = config.root / "bronze" / first["run_id"] / "receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["receipt_version"] = "sec-ftd-capture/1"
    del receipt["coverage_evidence_sha256"]
    receipt_path.write_text(json.dumps(receipt))
    target = destination(path)
    result = replay(config.root, target)
    assert result["rejected"] == 1 and result["legacy_outcomes_reconciled"] == 2
    rebuilt = load_config(target)
    assert state(config.root, config.storage) == state(rebuilt.root, rebuilt.storage)
    assert gold(path, limit=20) == gold(target, limit=20)
    assert all((p / "outcome.json").is_file() for p in (rebuilt.root / "bronze").iterdir())


def test_sealed_archives_replay_without_source_database(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    config = load_config(path)
    expected = gold(path, limit=20)
    sealed_config = destination(path, tmp_path / "sealed")
    replay(config.root, sealed_config)
    sealed = load_config(sealed_config)
    (sealed.root / "warehouse.sqlite3").unlink()
    target = destination(path)
    assert replay(sealed.root, target)["replayed"] == 1
    assert gold(target, limit=20) == expected


@pytest.mark.parametrize("kind", ["rejected", "missing", "oversized"])
def test_original_failed_and_rejected_outcomes_survive_replay(tmp_path, kind):
    path, source = setup(tmp_path)
    source.write_bytes(b"not a ZIP" if kind == "rejected" else b"x" * 101)
    if kind == "missing":
        source.unlink()
    if kind == "oversized":
        manifest = yaml.safe_load(Path("manifests/sec-fails-to-deliver.yml").read_text())
        manifest["limits"]["archive_bytes"] = 100
        manifest_path = tmp_path / "bounded.yml"
        manifest_path.write_text(yaml.safe_dump(manifest))
        settings = yaml.safe_load(path.read_text())
        settings["partitions"]["fixture"]["manifest"] = str(manifest_path)
        path.write_text(yaml.safe_dump(settings))
    original = run(path, "fixture", clock=lambda: CLOCK)
    config = load_config(path)
    target = destination(path)
    result = replay(config.root, target)
    assert result["replayed"] == 1 and result["succeeded"] == 0
    rebuilt = load_config(target)
    assert state(config.root, config.storage) == state(rebuilt.root, rebuilt.storage)
    assert result["failed" if original["status"] == "FAILED" else "rejected"] == 1


@pytest.mark.parametrize("damage", ["payload", "outcome", "missing_receipt", "parser", "sequence"])
def test_invalid_source_fails_before_destination_creation(tmp_path, damage):
    path, source = setup(tmp_path)
    first = capture(path, source, FIXTURE, 0)
    config = load_config(path)
    directory = config.root / "bronze" / first["run_id"]
    if damage == "payload":
        (directory / "payload.bin").write_bytes(b"corrupt")
    elif damage == "outcome":
        value = json.loads((directory / "outcome.json").read_text())
        value["audit"]["status"] = "REJECTED"
        (directory / "outcome.json").write_text(json.dumps(value))
    elif damage == "missing_receipt":
        (directory / "receipt.json").unlink()
    else:
        value = json.loads((directory / "receipt.json").read_text())
        value["parser_version" if damage == "parser" else "capture_sequence"] = (
            "unsupported" if damage == "parser" else 2
        )
        (directory / "receipt.json").write_text(json.dumps(value))
    target = destination(path)
    with pytest.raises((ValueError, OSError)):
        replay(config.root, target)
    assert not load_config(target).root.exists()


def test_unresolved_started_audit_and_missing_outcome_cannot_be_guessed(tmp_path):
    path, source = setup(tmp_path)
    result = capture(path, source, FIXTURE, 0)
    config = load_config(path)
    (config.root / "bronze" / result["run_id"] / "outcome.json").unlink()
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        warehouse.connection.execute("UPDATE sec_ftd_audit SET status='STARTED',completed_at=NULL")
    target = destination(path)
    with pytest.raises(ValueError, match="terminal"):
        replay(config.root, target)
    assert not load_config(target).root.exists()


def test_target_must_be_empty_nonoverlapping_and_explicit_supported_backend(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    config = load_config(path)
    for root in (config.root, config.root / "inside", config.root.parent):
        with pytest.raises(ValueError):
            replay(config.root, destination(path, root))
    target = destination(path)
    load_config(target).root.mkdir()
    sentinel = load_config(target).root / "keep.txt"
    sentinel.write_text("preserve me")
    with pytest.raises(ValueError, match="empty"):
        replay(config.root, target)
    assert sentinel.read_text() == "preserve me"
    value = yaml.safe_load(target.read_text())
    value["root"], value["warehouse"] = str(tmp_path / "postgres"), "postgres"
    target.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError, match="set password environment variable"):
        replay(config.root, target)
    assert not (tmp_path / "postgres").exists()


def test_mid_replay_failure_never_publishes_partial_destination(tmp_path, monkeypatch):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    config = load_config(path)
    original = SQLiteWarehouse.promote_partition

    def fail(self, *args, **kwargs):
        if self.root != config.root:
            raise ValueError("injected replay failure")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(SQLiteWarehouse, "promote_partition", fail)
    target = destination(path)
    with pytest.raises(ValueError, match="injected"):
        replay(config.root, target)
    assert not load_config(target).root.exists()
    assert not list(tmp_path.glob(".replayed.replay-*"))


def test_replay_cli_accepts_an_empty_target_and_reports_original_outcomes(tmp_path, capsys):
    from market_data_pipeline.pipelines.sec_fails_to_deliver.cli import main

    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    config = load_config(path)
    target = destination(path)
    load_config(target).root.mkdir()
    assert (
        main(
            [
                "replay",
                "--source-root",
                str(config.root),
                "--config",
                str(target),
                "--max-captures",
                "5",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "SUCCEEDED" and result["replayed"] == 1
    assert (load_config(target).root / "replay.json").exists()


@pytest.mark.parametrize("maximum", [0, True, 10001])
def test_invalid_replay_budget_writes_nothing(tmp_path, maximum):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    target = destination(path)
    with pytest.raises(ValueError, match="max-captures"):
        replay(load_config(path).root, target, max_captures=maximum)
    assert not load_config(target).root.exists()


def test_original_readonly_warehouse_cannot_write(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    config = load_config(path)
    before = source_bytes(config.root)
    with SQLiteWarehouse(config.root, config.storage, read_only=True) as warehouse:
        with pytest.raises(ValueError, match="read-only"):
            with warehouse.transaction():
                pass
        with pytest.raises(sqlite3.OperationalError):
            warehouse.connection.execute("DELETE FROM sec_ftd_facts")
    assert source_bytes(config.root) == before


def test_fact_fingerprint_mismatch_never_publishes_a_rebuild(tmp_path):
    path, source = setup(tmp_path)
    original = capture(path, source, FIXTURE, 0)
    config = load_config(path)
    sealed_config = destination(path, tmp_path / "sealed")
    replay(config.root, sealed_config)
    sealed = load_config(sealed_config)
    outcome_path = sealed.root / "bronze" / original["run_id"] / "outcome.json"
    value = json.loads(outcome_path.read_text())
    value["facts"]["sha256"] = "b" * 64
    value["sha256"] = hashlib.sha256(
        json.dumps(
            {k: v for k, v in value.items() if k != "sha256"}, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    outcome_path.write_text(json.dumps(value))
    inventory_path = sealed.root / "replay.json"
    inventory = json.loads(inventory_path.read_text())
    inventory["capture_inventory"][0]["outcome_sha256"] = value["sha256"]
    inventory["capture_inventory_sha256"] = hashlib.sha256(
        json.dumps(inventory["capture_inventory"], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    inventory_path.write_text(json.dumps(inventory))
    (sealed.root / "warehouse.sqlite3").unlink()
    target = destination(path)
    with pytest.raises(ValueError, match="fact versions differ"):
        replay(sealed.root, target)
    assert not load_config(target).root.exists()
    assert not list(tmp_path.glob(".replayed.replay-*"))


def test_archived_proof_tampering_blocks_replay_without_reading_live_proof(tmp_path):
    path, source = setup(tmp_path)
    raw = bundle(baseline_three())
    source.write_bytes(raw)
    proof = evidence(path, raw)
    original = run(path, "fixture", clock=lambda: CLOCK)
    config = load_config(path)
    archived = config.root / "bronze" / original["run_id"] / "coverage-evidence.json"
    archived.write_bytes(proof.read_bytes() + b" ")
    target = destination(path)
    with pytest.raises(ValueError, match="evidence digest"):
        replay(config.root, target)
    assert not load_config(target).root.exists()


def test_final_verification_detects_later_damage_to_an_earlier_run(tmp_path, monkeypatch):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    capture(path, source, FIXTURE.replace(b"12.3400", b"12.3500"), 1)
    config = load_config(path)
    promote = SQLiteWarehouse.promote_partition

    def damage_previous(self, *args, **kwargs):
        result = promote(self, *args, **kwargs)
        if self.root != config.root and args[2]["capture_sequence"] == 2:
            self.connection.execute(
                "UPDATE sec_ftd_facts SET description_raw='damaged fixture' "
                "WHERE capture_sequence=1"
            )
        return result

    monkeypatch.setattr(SQLiteWarehouse, "promote_partition", damage_previous)
    target = destination(path)
    with pytest.raises(ValueError, match="final replay state"):
        replay(config.root, target)
    assert not load_config(target).root.exists()


def test_database_free_replay_cannot_guess_a_missing_root_inventory(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    config = load_config(path)
    (config.root / "warehouse.sqlite3").unlink()
    target = destination(path)
    with pytest.raises(ValueError, match="inventory"):
        replay(config.root, target)
    assert not load_config(target).root.exists()


def test_database_free_inventory_detects_a_missing_final_capture(tmp_path):
    import shutil

    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    last = capture(path, source, FIXTURE.replace(b"12.3400", b"12.3500"), 1)
    config = load_config(path)
    sealed_config = destination(path)
    replay(config.root, sealed_config)
    sealed = load_config(sealed_config)
    (sealed.root / "warehouse.sqlite3").unlink()
    shutil.rmtree(sealed.root / "bronze" / last["run_id"])
    target = destination(path, tmp_path / "final")
    with pytest.raises(ValueError, match="inventory"):
        replay(sealed.root, target)
    assert not load_config(target).root.exists()
