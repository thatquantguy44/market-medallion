"""SSV-AC10: actual CLI/script publication, inspection, recovery and sanitized failures."""

import json
import os
import subprocess
import sys

from test_ssv_cli import REPO, config_file, invoke
from test_ssv_parser import FIXTURES


def test_ssv_ac10_cli_and_portable_script_publish_inspect_recover(tmp_path):
    config = config_file(tmp_path)
    loaded = invoke(
        config,
        "run",
        "--input",
        str(FIXTURES / "invented-valid.json"),
        "--receipt",
        str(FIXTURES / "invented-valid.receipt.json"),
    )
    assert loaded.returncode == 0, loaded.stderr
    result = invoke(config, "publish", "--trade-date", "2026-11-09", "--scope", "nyse_trf")
    assert result.returncode == 0, result.stderr
    published = json.loads(result.stdout)
    assert published["state"] == "PUBLISHED"
    selected = invoke(config, "published", "--table", "gold")
    assert selected.returncode == 0, selected.stderr
    assert len(json.loads(selected.stdout)["tables"]["gold"]) == 2
    result = subprocess.run(
        [
            str(REPO / "scripts/run_finra_short_sale_volume.sh"),
            "recover-publication",
            "--config",
            str(config),
        ],
        cwd=tmp_path,
        env=dict(os.environ, FINRA_SSV_PYTHON=sys.executable),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == published
    failed = invoke(config, "published", "--max-rows", "1")
    assert failed.returncode == 2 and str(tmp_path) not in failed.stderr
    failed = invoke(
        config,
        "publish",
        "--trade-date",
        "2026-11-09",
        "--scope",
        "listed_trfs",
        "--scope",
        "nyse_trf",
    )
    assert failed.returncode == 2
