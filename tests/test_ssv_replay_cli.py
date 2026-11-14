"""SSV-AC11: actual CLI/script replay uses the destination config and original source."""

import json
import os
import subprocess
import sys

from test_ssv_cli import REPO, config_file, invoke
from test_ssv_parser import FIXTURES


def test_ssv_ac11_cli_replay_reopens_target_and_refuses_populated_retry(tmp_path):
    source_config_dir = tmp_path / "source-config"
    target_config_dir = tmp_path / "target-config"
    source_config_dir.mkdir()
    target_config_dir.mkdir()
    source_config = config_file(source_config_dir)
    target_config = config_file(target_config_dir)
    initial = invoke(
        source_config,
        "run",
        "--input",
        str(FIXTURES / "invented-valid.json"),
        "--receipt",
        str(FIXTURES / "invented-valid.receipt.json"),
    )
    assert initial.returncode == 0, initial.stderr
    source = source_config_dir / "local-store"
    command = [
        str(REPO / "scripts/run_finra_short_sale_volume.sh"),
        "replay",
        "--config",
        str(target_config),
        "--source-root",
        str(source),
    ]
    result = subprocess.run(
        command,
        cwd=tmp_path,
        env=dict(os.environ, FINRA_SSV_PYTHON=sys.executable),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    saved = json.loads(result.stdout)
    assert saved["status"] == "SUCCEEDED" and saved["replayed"] == 1
    options = ["gold", "--trade-date", "2026-11-09", "--facility", "nyse_trf"]
    assert json.loads(invoke(target_config, *options).stdout) == json.loads(
        invoke(source_config, *options).stdout
    )
    assert invoke(target_config, "replay", "--source-root", str(source)).returncode == 2
    assert str(tmp_path) not in invoke(target_config, "replay", "--source-root", str(source)).stderr
