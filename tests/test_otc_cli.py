"""OTC-AC06 real module CLI, explicit invented-data acknowledgement and readback."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

MODULE = "market_data_pipeline.pipelines.finra_otc_transparency"
ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/finra_otc_transparency/ats_t1_weekly.txt"
MANIFEST = ROOT / "manifests/finra-otc-transparency.yml"


def invoke(*args):
    return subprocess.run(
        [sys.executable, "-m", MODULE, *map(str, args)], capture_output=True, text=True, check=False
    )


def test_actual_module_run_validate_and_inspect_roundtrip(tmp_path):
    validated = invoke("validate", "--manifest", MANIFEST)
    assert validated.returncode == 0, validated.stderr
    assert len(json.loads(validated.stdout)["profiles"]) == 6
    root = tmp_path / "panel"
    run = invoke(
        "run",
        "--invented-fixture",
        "--manifest",
        MANIFEST,
        "--root",
        root,
        "--input",
        FIXTURE,
        "--week",
        "2026-10-19",
        "--product",
        "ATS_WEEKLY_DETAIL",
        "--tier",
        "T1",
        "--expected-rows",
        "3",
    )
    assert run.returncode == 0, run.stderr
    capture = json.loads(run.stdout)["capture_id"]
    inspected = invoke(
        "inspect",
        "--invented-fixture",
        "--root",
        root,
        "--partition",
        "2026-10-19:ATS_WEEKLY_DETAIL:T1",
    )
    assert inspected.returncode == 0, inspected.stderr
    panel = json.loads(inspected.stdout)
    assert len(panel["rows"]) == 3
    assert panel["rows"][0]["capture_id"] == capture
    assert "12345678901234567890" in [r["weekly_shares"] for r in panel["rows"]]
    retry = invoke("ingest-capture", "--invented-fixture", "--root", root, "--capture-id", capture)
    assert retry.returncode == 0, retry.stderr
    assert json.loads(retry.stdout) == json.loads(run.stdout)


def test_cli_requires_invented_acknowledgement_before_input_or_root_access(tmp_path):
    result = invoke(
        "run",
        "--manifest",
        MANIFEST,
        "--root",
        tmp_path / "denied",
        "--input",
        tmp_path / "SECRET-missing",
        "--week",
        "2026-10-19",
        "--product",
        "ATS_WEEKLY_DETAIL",
        "--tier",
        "T1",
    )
    assert result.returncode == 2
    assert not (tmp_path / "denied").exists()


@pytest.mark.parametrize("expected", [None, 4])
def test_cli_nonpublishable_fixture_has_nonzero_status_and_no_false_zero(tmp_path, expected):
    args = [
        "run",
        "--invented-fixture",
        "--manifest",
        MANIFEST,
        "--root",
        tmp_path / "panel",
        "--input",
        FIXTURE,
        "--week",
        "2026-10-19",
        "--product",
        "ATS_WEEKLY_DETAIL",
        "--tier",
        "T1",
    ]
    if expected is not None:
        args += ["--expected-rows", str(expected)]
    result = invoke(*args)
    assert result.returncode == 2, result.stderr
    assert json.loads(result.stdout)["status"] in ("UNVERIFIED", "REJECTED")


def test_read_error_is_sanitized_and_never_creates_a_store(tmp_path):
    result = invoke(
        "inspect",
        "--invented-fixture",
        "--root",
        tmp_path / "SECRET",
        "--partition",
        "2026-10-19:ATS_WEEKLY_DETAIL:T1",
    )
    assert result.returncode == 1
    assert json.loads(result.stderr)["error"] == "OTC_LOCAL_OPERATION_FAILED"
    assert "SECRET" not in result.stderr and not (tmp_path / "SECRET").exists()


def test_cli_history_and_equal_clock_cutoff_are_explicit_and_bounded(tmp_path):
    root = tmp_path / "panel"
    args = [
        "run",
        "--invented-fixture",
        "--manifest",
        MANIFEST,
        "--root",
        root,
        "--input",
        FIXTURE,
        "--week",
        "2026-10-19",
        "--product",
        "ATS_WEEKLY_DETAIL",
        "--tier",
        "T1",
        "--expected-rows",
        "3",
    ]
    first = invoke(*args)
    assert first.returncode == 0, first.stderr
    first_value = json.loads(first.stdout)
    second = invoke(*args)
    assert second.returncode == 0, second.stderr
    common = [
        "--invented-fixture",
        "--root",
        root,
        "--partition",
        "2026-10-19:ATS_WEEKLY_DETAIL:T1",
    ]
    history = invoke("history", *common)
    assert history.returncode == 0, history.stderr
    assert len(json.loads(history.stdout)["versions"]) == 3
    past = invoke(
        "inspect",
        *common,
        "--as-of",
        first_value["captured_at"],
        "--as-of-sequence",
        first_value["capture_sequence"],
    )
    assert past.returncode == 0, past.stderr
    assert (
        json.loads(past.stdout)["partitions"][0]["publication"]["capture_id"]
        == first_value["capture_id"]
    )
    assert invoke("history", *common, "--max-versions", "2").returncode == 1


def test_cli_migration_keeps_real_fixture_legacy_evidence_and_is_idempotent(tmp_path):
    from test_otc_migration import legacy_tables, restore_legacy

    root = tmp_path / "old"
    restore_legacy(root)
    before = legacy_tables(root)
    result = invoke("migrate-warehouse", "--invented-fixture", "--root", root)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["seeded_snapshots"] == 1
    assert legacy_tables(root) == before
    repeat = invoke("migrate-warehouse", "--invented-fixture", "--root", root)
    assert repeat.returncode == 0, repeat.stderr
    assert json.loads(repeat.stdout)["status"] == "ALREADY_CURRENT"
