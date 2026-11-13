"""SSV-AC06: real local CLI processes, strict config and portable script."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from test_ssv_bronze import fixture_grant
from test_ssv_parser import FIXTURES, receipt

from market_data_pipeline.pipelines.finra_short_sale_volume.config import load_config

REPO = Path(__file__).resolve().parents[1]


def config_file(tmp_path, **changes):
    value = {
        "schema_id": "finra-ssv-local-config/1",
        "root": "local-store",
        "manifest": str(REPO / "manifests/finra-short-sale-volume.yml"),
        "operation": "fixture",
        "approved_use": fixture_grant(),
        "limits": {"max_bytes": 20971520, "max_rows": 5000},
    }
    value.update(changes)
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def invoke(config, *args):
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "market_data_pipeline.pipelines.finra_short_sale_volume",
            *args,
            "--config",
            str(config),
        ],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )


def test_ssv_ac06_cli_and_script_complete_local_loop(tmp_path):
    config = config_file(tmp_path)
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    wire = tmp_path / "receipt.json"
    wire.write_bytes((FIXTURES / "invented-valid.receipt.json").read_bytes())
    assert json.loads(wire.read_bytes()) == receipt(payload)
    result = invoke(
        config,
        "run",
        "--input",
        str((FIXTURES / "invented-valid.json").resolve()),
        "--receipt",
        str(wire),
    )
    assert result.returncode == 0, result.stderr
    saved = json.loads(result.stdout)
    assert saved["status"] == "APPLIED"
    snapshot = invoke(config, "gold", "--trade-date", "2026-11-09", "--facility", "nyse_trf")
    assert snapshot.returncode == 0 and len(json.loads(snapshot.stdout)["rows"]) == 2
    again = invoke(config, "ingest-capture", "--capture-id", saved["capture_id"])
    assert again.returncode == 0 and json.loads(again.stdout) == saved
    for command in ["coverage", "audit", "validate-config"]:
        args = (
            ["--trade-date", "2026-11-09", "--facility", "nyse_trf"]
            if command == "coverage"
            else []
        )
        assert invoke(config, command, *args).returncode == 0
    revisions = invoke(
        config,
        "history",
        "--trade-date",
        "2026-11-09",
        "--facility",
        "nyse_trf",
        "--symbol",
        "000SYN.A",
    )
    assert revisions.returncode == 0 and len(json.loads(revisions.stdout)) == 1
    assert json.loads(revisions.stdout)[0]["short_volume"] == "40"
    cutoff = invoke(
        config,
        "gold",
        "--trade-date",
        "2026-11-09",
        "--facility",
        "nyse_trf",
        "--as-of",
        "1900-01-01T00:00:00+00:00",
    )
    assert cutoff.returncode == 0 and json.loads(cutoff.stdout)["rows"] == []
    env = dict(os.environ, FINRA_SSV_PYTHON=sys.executable)
    result = subprocess.run(
        [
            str(REPO / "scripts/run_finra_short_sale_volume.sh"),
            "gold",
            "--config",
            str(config),
            "--trade-date",
            "2026-11-09",
            "--facility",
            "nyse_trf",
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0 and json.loads(result.stdout) == json.loads(snapshot.stdout)


@pytest.mark.parametrize(
    "change",
    [
        {"surprise": "invented-secret"},
        {"operation": "acquire"},
        {"limits": {"max_bytes": True, "max_rows": 5000}},
        {"approved_use": None},
    ],
)
def test_ssv_ac06_config_rejects_invalid_before_store_creation(tmp_path, change):
    path = config_file(tmp_path, **change)
    with pytest.raises((ValueError, PermissionError)):
        load_config(path)
    assert not (tmp_path / "local-store").exists()
    result = invoke(path, "validate-config")
    assert result.returncode == 2 and "invented-secret" not in result.stderr


def test_ssv_ac06_config_duplicate_keys_are_not_silently_overridden(tmp_path):
    path = config_file(tmp_path)
    path.write_text(path.read_text() + "operation: fixture\n")
    with pytest.raises(ValueError):
        load_config(path)


def test_ssv_ac06_cli_rejected_input_returns_nonzero_without_diagnostics(tmp_path):
    path = config_file(tmp_path)
    payload = b"[{"
    wire = tmp_path / "receipt.json"
    wire.write_text(json.dumps(dict(receipt(payload), diagnostic="invented-private-diagnostic")))
    data = tmp_path / "input.json"
    data.write_bytes(payload)
    result = invoke(path, "run", "--input", str(data), "--receipt", str(wire))
    assert result.returncode == 2 and json.loads(result.stdout)["status"] == "REJECTED"
    assert "private" not in result.stdout + result.stderr


def test_ssv_ac06_config_example_is_fixture_only_and_has_no_io():
    config = load_config(REPO / "config/finra_short_sale_volume/pipeline.example.yml")
    assert config.operation == "fixture" and config.approved_use["route"] == "invented_fixture"
