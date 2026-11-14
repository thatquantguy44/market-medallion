"""SSV-AC13: offline release health and conservative reference-safe retention."""

import json
import socket
from datetime import timedelta

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_parser import manifest as manifest
from test_ssv_pipeline import ingest
from test_ssv_source import Script, config, data_reply
from test_ssv_source import run as acquire

from market_data_pipeline.pipelines.finra_short_sale_volume import operations

__all__ = ["manifest"]
OPTIONS = dict(approved_use=fixture_grant(), operation="fixture")


def health(root, manifest, **changes):
    options = dict(start="2026-11-09", end="2026-11-09", scopes=["nyse_trf"], clock=lambda: NOW)
    options.update(changes)
    return operations.health(root, manifest, **OPTIONS, **options)


def inventory(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_ssv_ac13_healthy_local_capture_is_not_network_success(tmp_path, manifest, monkeypatch):
    ingest(tmp_path, manifest)
    before = inventory(tmp_path)
    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: pytest.fail("network"))
    report = health(tmp_path, manifest)
    row = report["partitions"][0]
    assert row["coverage"] == "COMPLETE" and row["refresh_status"] == "LOCAL_ONLY"
    assert row["release_status"] == "UNKNOWN_API_TIMING"
    assert row["last_acquisition_success_at"] is None
    assert row["last_local_success_at"] == "2026-11-13T15:00:00.000000Z"
    assert report["status"] == "UNKNOWN" and inventory(tmp_path) == before


def test_ssv_ac13_no_response_refresh_is_visible_above_old_gold(tmp_path, manifest):
    cfg = config(tmp_path, manifest)
    acquire(cfg, Script(manifest, [data_reply()]))
    from urllib.error import URLError

    acquire(
        cfg, Script(manifest, [URLError("offline")] * 3), clock=lambda: NOW + timedelta(hours=1)
    )
    before = inventory(tmp_path)
    report = health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))
    row = report["partitions"][0]
    assert row["coverage"] == "COMPLETE" and row["refresh_status"] == "FAILED"
    assert row["last_acquisition_success_at"] == "2026-11-13T15:00:00.000000Z"
    assert row["trailing_failure"]["status"] == "TRANSPORT_FAILED"
    assert row["committed_capture_id"] is not None and report["status"] == "DEGRADED"
    assert inventory(tmp_path) == before


@pytest.mark.parametrize(
    "day,release",
    [
        ("2026-11-26", "CLOSED"),
        ("2026-11-27", "UNKNOWN_API_TIMING"),
        ("2027-01-04", "UNKNOWN_CALENDAR"),
    ],
)
def test_ssv_ac13_holiday_early_close_and_unknown_year(tmp_path, manifest, day, release):
    # No response is used as calendar evidence; missing roots are never initialized.
    report = health(tmp_path / "missing", manifest, start=day, end=day)
    assert report["partitions"][0]["release_status"] == release
    assert not (tmp_path / "missing").exists()


def test_ssv_ac13_deadline_requires_explicit_api_evidence_and_handles_late_release(
    tmp_path, manifest
):
    policy = operations.default_policy()
    policy["api_release_time"] = "18:00"
    with pytest.raises(ValueError):
        health(tmp_path, manifest, policy=policy)
    policy["api_timing_reference"] = "invented operator API timing evidence"
    at = NOW.replace(day=9, hour=22, minute=59)
    assert (
        health(tmp_path, manifest, policy=policy, clock=lambda: at)["partitions"][0][
            "release_status"
        ]
        == "PENDING"
    )
    assert (
        health(tmp_path, manifest, policy=policy, clock=lambda: at + timedelta(minutes=1))[
            "partitions"
        ][0]["release_status"]
        == "OVERDUE"
    )


def test_ssv_ac13_valid_empty_does_not_admit_adf_or_impute_missing(tmp_path, manifest):
    ingest(tmp_path, manifest, payload=b"[]", total=0)
    row = health(tmp_path, manifest)["partitions"][0]
    assert row["coverage"] == "VALID_EMPTY"
    with pytest.raises(ValueError):
        health(tmp_path, manifest, scopes=["adf"])
    assert (
        health(tmp_path, manifest, scopes=["nasdaq_chicago_trf"])["partitions"][0]["coverage"]
        == "MISSING"
    )


def test_ssv_ac13_stale_capture_and_unprocessed_archive_are_not_healthy(tmp_path, manifest):
    ingest(tmp_path, manifest)
    assert health(tmp_path, manifest, clock=lambda: NOW + timedelta(days=2))["partitions"][0][
        "stale_capture"
    ]
    from uuid import uuid4

    from test_ssv_parser import receipt

    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    bronze.archive_input(
        tmp_path,
        b"[]",
        manifest,
        receipt(b"[]", total=0),
        **OPTIONS,
        run_id=str(uuid4()),
        archived_at=NOW + timedelta(hours=1),
    )
    report = health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))
    assert len(report["unresolved_capture_ids"]) == 1 and report["status"] == "DEGRADED"


def test_ssv_ac13_retention_explains_versions_membership_and_all_bytes(tmp_path, manifest):
    first = ingest(tmp_path, manifest)
    second = ingest(tmp_path, manifest, payload=b"[]", total=0, clock=NOW + timedelta(hours=1))
    before = inventory(tmp_path)
    report = operations.retention_dry_run(tmp_path, manifest, **OPTIONS)
    captures = {r["capture_id"]: r for r in report["captures"]}
    assert "VERSION_ORIGIN" in captures[first["capture_id"]]["reasons"]
    assert "CURRENT_PARTITION" in captures[second["capture_id"]]["reasons"]
    assert report["version_count"] == 4 and report["deletable_bytes"] == 0
    assert report["policy"] == "PRESERVE_ALL" and report["dry_run"] is True
    assert report["total_bytes"] == sum(len(b) for b in before.values())
    assert len(report["files"]) == len(before) and inventory(tmp_path) == before


@pytest.mark.parametrize("damage", ["binding", "receipt", "symlink", "budget"])
def test_ssv_ac13_corrupt_evidence_refuses_without_repair(tmp_path, manifest, damage):
    ingest(tmp_path, manifest)
    options = {}
    if damage == "binding":
        (tmp_path / "warehouse.json").unlink()
    elif damage == "receipt":
        next((tmp_path / "bronze/captures").glob("*/receipt.json")).write_text("{}")
    elif damage == "symlink":
        (tmp_path / "unowned").symlink_to(tmp_path / "warehouse.json")
    else:
        options["max_total_bytes"] = 1
    before = inventory(tmp_path)
    with pytest.raises(ValueError):
        operations.retention_dry_run(tmp_path, manifest, **OPTIONS, **options)
    assert inventory(tmp_path) == before


def test_ssv_ac13_acquisition_recovers_failure_with_new_success(tmp_path, manifest):
    from urllib.error import URLError

    cfg = config(tmp_path, manifest)
    acquire(cfg, Script(manifest, [URLError("offline")] * 3))
    acquire(cfg, Script(manifest, [data_reply()]), clock=lambda: NOW + timedelta(hours=1))
    row = health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))["partitions"][0]
    assert row["refresh_status"] == "SUCCESS" and row["trailing_failure"] is None
    assert row["last_acquisition_success_at"] == "2026-11-13T16:00:00.000000Z"


def test_ssv_ac13_interrupted_run_has_no_fabricated_success(tmp_path, manifest):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.acquisition_journal import (
        open_run,
    )
    from market_data_pipeline.pipelines.finra_short_sale_volume.source import make_plan

    cfg = config(tmp_path, manifest)
    plan = make_plan(
        cfg,
        start="2026-11-09",
        end="2026-11-09",
        scopes=["nyse_trf"],
        overlap_days=0,
        clock=lambda: NOW,
    )
    with open_run(cfg, plan, clock=lambda: NOW):
        pass
    before = inventory(tmp_path)
    row = health(tmp_path, manifest)["partitions"][0]
    assert row["refresh_status"] == "RUNNING" and row["last_acquisition_success_at"] is None
    assert inventory(tmp_path) == before


@pytest.mark.parametrize("status", ["partial", "rejected"])
def test_ssv_ac13_failed_local_refresh_preserves_committed_coverage(tmp_path, manifest, status):
    first = ingest(tmp_path, manifest)
    ingest(
        tmp_path,
        manifest,
        payload=b"[{" if status == "rejected" else None,
        total=3,
        clock=NOW + timedelta(hours=1),
    )
    report = health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))
    row = report["partitions"][0]
    assert report["status"] == "DEGRADED" and row["committed_capture_id"] == first["capture_id"]
    assert row["latest_local_attempt_status"] == status.upper()


def test_ssv_ac13_publication_advance_recovery_and_retention_references(tmp_path, manifest):
    from test_ssv_publication import publish

    first = ingest(tmp_path, manifest)
    generation = publish(tmp_path, manifest)
    report = operations.retention_dry_run(tmp_path, manifest, **OPTIONS)
    assert report["publication"]["status"] == "PUBLISHED"
    assert generation["generation_id"] in report["captures"][0]["publication_generations"]
    assert all(
        generation["generation_id"] in v["publication_generations"] for v in report["versions"]
    )
    second = ingest(tmp_path, manifest, payload=b"[]", total=0, clock=NOW + timedelta(hours=1))
    assert (
        health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))["publication"]["status"]
        == "SOURCE_ADVANCED"
    )

    def fail(phase):
        if phase == "committed":
            raise OSError("invented interruption")

    with pytest.raises(OSError):
        publish(tmp_path, manifest, _fault=fail)
    before = inventory(tmp_path)
    assert (
        health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))["publication"]["status"]
        == "RECOVERY_REQUIRED"
    )
    retained = operations.retention_dry_run(tmp_path, manifest, **OPTIONS)
    assert retained["deletable_bytes"] == 0 and inventory(tmp_path) == before
    assert {c["capture_id"] for c in retained["captures"]} == {
        first["capture_id"],
        second["capture_id"],
    }


def test_ssv_ac13_collection_protects_both_passes_and_removal_history(tmp_path, manifest):
    from test_ssv_page_collection import page

    from market_data_pipeline.pipelines.finra_short_sale_volume import pipeline
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import page_collection

    passes = [[page(tmp_path, manifest, 0), page(tmp_path, manifest, 2)] for _ in range(2)]
    saved = page_collection.archive(
        tmp_path,
        manifest,
        passes,
        **OPTIONS,
        run_id="00000000-0000-0000-0000-000000000001",
        archived_at=NOW,
        max_pages=3,
        page_bytes=3000000,
        page_rows=5000,
    )
    pipeline.ingest_capture(
        tmp_path,
        saved["capture_id"],
        manifest,
        **OPTIONS,
        max_rows=15000,
        max_bytes=18000000,
        clock=lambda: NOW,
    )
    before = inventory(tmp_path)
    report = operations.retention_dry_run(tmp_path, manifest, **OPTIONS)
    pages = [r for r in report["captures"] if r["capture_id"] != saved["capture_id"]]
    assert len(pages) == 4 and report["version_count"] == 3
    assert all(
        "COLLECTION_INPUT" in r["reasons"] and r["collection_parents"] == [saved["capture_id"]]
        for r in pages
    )
    assert inventory(tmp_path) == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("retention", "DELETE_AFTER_365_DAYS"),
        ("max_capture_age_hours", True),
        ("api_release_time", "99:00"),
        ("unknown", 1),
        ("closed_dates", ["2027-01-01"]),
    ],
)
def test_ssv_ac13_policy_rejects_unreviewed_or_invalid_values(tmp_path, manifest, field, value):
    policy = operations.default_policy()
    policy[field] = value
    with pytest.raises(ValueError):
        health(tmp_path, manifest, policy=policy)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "damage", ["result_status", "event_sequence", "missing_raw", "plan", "binding", "missing_event"]
)
def test_ssv_ac13_corrupt_acquisition_journal_fails_closed(tmp_path, manifest, damage):
    from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import encoded

    acquire(config(tmp_path, manifest), Script(manifest, [data_reply()]))
    run_dir = next((tmp_path / "acquisition/runs").iterdir())
    if damage == "missing_raw":
        next((tmp_path / "bronze/captures").glob("*/payload.bin")).unlink()
    else:
        path = run_dir / "result.json"
        if damage in {"event_sequence", "missing_event"}:
            path = next((run_dir / "events").iterdir())
        elif damage == "plan":
            path = run_dir / "request.json"
        elif damage == "binding":
            path = tmp_path / "acquisition.json"
        value = json.loads(path.read_bytes())
        if damage == "result_status":
            value["complete_partitions"] = 0
        elif damage == "event_sequence":
            value["event_sequence"] = 2
        elif damage == "plan":
            value["plan"]["partitions"][0]["market_code"] = "D"
        elif damage == "binding":
            value["manifest_sha256"] = "0" * 64
        if damage == "missing_event":
            path.unlink()
        else:
            path.write_bytes(encoded(value))
    before = inventory(tmp_path)
    with pytest.raises(ValueError):
        health(tmp_path, manifest)
    assert inventory(tmp_path) == before


def test_ssv_ac13_busy_writer_refuses_without_creating_lockfiles(tmp_path, manifest):
    import fcntl

    ingest(tmp_path, manifest)
    before = inventory(tmp_path)
    with (tmp_path / ".writer.lock").open("rb") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="busy"):
            health(tmp_path, manifest)
    assert inventory(tmp_path) == before


def test_ssv_ac13_pre_release_empty_does_not_satisfy_release_deadline(tmp_path, manifest):
    policy = operations.default_policy()
    policy.update(api_release_time="18:00", api_timing_reference="invented API timing evidence")
    early = NOW.replace(day=9, hour=22)
    ingest(tmp_path, manifest, payload=b"[]", total=0, clock=early)
    report = health(tmp_path, manifest, policy=policy, clock=lambda: early + timedelta(hours=2))
    row = report["partitions"][0]
    assert row["coverage"] == "VALID_EMPTY" and row["pre_release_capture"] is True
    assert row["release_status"] == "OVERDUE" and report["status"] == "DEGRADED"


def test_ssv_ac13_post_release_empty_is_observed_without_zero_imputation(tmp_path, manifest):
    policy = operations.default_policy()
    policy.update(api_release_time="18:00", api_timing_reference="invented API timing evidence")
    after = NOW.replace(day=9, hour=23)
    ingest(tmp_path, manifest, payload=b"[]", total=0, clock=after)
    row = health(tmp_path, manifest, policy=policy, clock=lambda: after + timedelta(minutes=1))[
        "partitions"
    ][0]
    assert row["release_status"] == "OBSERVED" and row["pre_release_capture"] is False


def test_ssv_ac13_latest_publication_is_compared_independently_of_older_generations(
    tmp_path, manifest
):
    from test_ssv_publication import publish

    from market_data_pipeline.pipelines.finra_short_sale_volume import publication

    ingest(tmp_path, manifest)
    publish(tmp_path, manifest)
    ingest(tmp_path, manifest, payload=b"[]", total=0, clock=NOW + timedelta(hours=1))
    publish(tmp_path, manifest)
    latest = publication.publish(
        tmp_path, manifest, **OPTIONS, trade_date="2026-11-09", scopes=["nyse_trf"], as_of=NOW
    )
    row = health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))["publication"]
    assert row["current_generation_id"] == latest["generation_id"]
    assert row["status"] == "SOURCE_ADVANCED"


def test_ssv_ac13_actual_cli_health_and_retention_are_read_only(tmp_path):
    from test_ssv_cli import config_file, invoke

    path = config_file(tmp_path)
    from test_ssv_parser import FIXTURES

    assert (
        invoke(
            path,
            "run",
            "--input",
            str(FIXTURES / "invented-valid.json"),
            "--receipt",
            str(FIXTURES / "invented-valid.receipt.json"),
        ).returncode
        == 0
    )
    root = tmp_path / "local-store"
    before = inventory(root)
    result = invoke(
        path,
        "health",
        "--start-date",
        "2026-11-09",
        "--end-date",
        "2026-11-09",
        "--scope",
        "nyse_trf",
    )
    assert result.returncode == 2, result.stderr
    assert json.loads(result.stdout)["observed_partitions"] == 1
    retained = invoke(path, "retention-dry-run")
    assert retained.returncode == 0, retained.stderr
    assert json.loads(retained.stdout)["deletion_enabled"] is False
    assert inventory(root) == before


def test_ssv_ac13_missing_terminal_parse_evidence_is_not_healthy(tmp_path, manifest):
    ingest(tmp_path, manifest)
    next((tmp_path / "bronze/outcomes").glob("*/*.json")).unlink()
    before = inventory(tmp_path)
    with pytest.raises(ValueError):
        health(tmp_path, manifest)
    assert inventory(tmp_path) == before


def test_ssv_ac13_current_publication_is_fresh_before_and_after_identical_refresh(
    tmp_path, manifest
):
    from test_ssv_publication import publish

    ingest(tmp_path, manifest)
    publish(tmp_path, manifest)
    assert health(tmp_path, manifest)["publication"]["status"] == "PUBLISHED"
    ingest(tmp_path, manifest, clock=NOW + timedelta(hours=1))
    assert (
        health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))["publication"]["status"]
        == "SOURCE_ADVANCED"
    )
    publish(tmp_path, manifest)
    assert (
        health(tmp_path, manifest, clock=lambda: NOW + timedelta(hours=2))["publication"]["status"]
        == "PUBLISHED"
    )


def test_ssv_ac13_unknown_files_are_held_with_manual_review_reason(tmp_path, manifest):
    ingest(tmp_path, manifest)
    (tmp_path / "operator-note.txt").write_text("invented operational note")
    report = operations.retention_dry_run(tmp_path, manifest, **OPTIONS)
    note = next(r for r in report["files"] if r["path"] == "operator-note.txt")
    assert note["action"] == "KEEP" and "UNCLASSIFIED_HOLD" in note["reasons"]


def test_ssv_ac13_retention_versions_explain_membership_references(tmp_path, manifest):
    first = ingest(tmp_path, manifest)
    second = ingest(tmp_path, manifest, clock=NOW + timedelta(hours=1))
    report = operations.retention_dry_run(tmp_path, manifest, **OPTIONS)
    assert len(report["versions"]) == 2
    assert all(
        v["membership_capture_ids"] == sorted([first["capture_id"], second["capture_id"]])
        for v in report["versions"]
    )


@pytest.mark.parametrize(
    "budget", ["max_captures", "max_files", "max_rows", "max_runs", "max_events"]
)
def test_ssv_ac13_invalid_budgets_refuse_before_read(tmp_path, manifest, budget):
    with pytest.raises(ValueError):
        health(tmp_path / "absent", manifest, **{budget: True})
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("damage", ["missing", "fifo"])
def test_ssv_ac13_lock_files_must_be_existing_regular_files(tmp_path, manifest, damage):
    import os

    ingest(tmp_path, manifest)
    path = tmp_path / ".writer.lock"
    path.unlink()
    if damage == "fifo":
        os.mkfifo(path)
    with pytest.raises(ValueError):
        health(tmp_path, manifest)


def test_ssv_ac13_policy_file_roundtrip_and_duplicate_rejection(tmp_path):
    from pathlib import Path

    import yaml

    example = (
        Path(__file__).resolve().parents[1]
        / "config/finra_short_sale_volume/operations.example.yml"
    )
    assert operations.load_policy(str(example)) == operations.default_policy()
    path = tmp_path / "policy.yml"
    path.write_text(yaml.safe_dump(operations.default_policy()) + "retention: PRESERVE_ALL\n")
    with pytest.raises(ValueError):
        operations.load_policy(path)
