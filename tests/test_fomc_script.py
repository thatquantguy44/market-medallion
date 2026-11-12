"""The checkout script works across directories and preserves explicit access gates."""

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_fomc.sh"


def test_script_uses_checkout_python_and_preserves_caller_paths_and_closed_gate(tmp_path):
    checkout = tmp_path / "checkout with spaces"
    (checkout / "scripts").mkdir(parents=True)
    script = checkout / "scripts" / SCRIPT.name
    shutil.copy2(SCRIPT, script)
    (checkout / ".venv" / "bin").mkdir(parents=True)
    python = checkout / ".venv" / "bin" / "python"
    # A relocated interpreter symlink loses its real venv; retain that environment.
    python.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + ' "$@"\n')
    python.chmod(0o755)
    caller = tmp_path / "caller with spaces"
    caller.mkdir()
    (caller / ".env").write_text("exit 99\n")
    cfg = yaml.safe_load(Path("config/fomc_meeting_minutes/pipeline.example.yml").read_text())
    cfg["manifest"] = str(Path("manifests/fomc-meeting-minutes.yml").resolve())
    cfg["postgres"]["password_env"] = "FOMC_SCRIPT_UNSET_PASSWORD"
    (caller / "my pipeline.yml").write_text(yaml.safe_dump(cfg))
    shutil.copy2("config/fomc_meeting_minutes/operations.example.yml", caller / "my operations.yml")
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in {"FOMC_PYTHON", "FOMC_SCRIPT_UNSET_PASSWORD"}
    }
    args = ["--config", "my pipeline.yml", "--operations", "my operations.yml"]
    result = subprocess.run(
        [str(script), "validate-config", *args], cwd=caller, env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["request_performed"] is False
    result = subprocess.run(
        [str(script), "run", *args, "--start-year", "1991", "--end-year", "1991"],
        cwd=caller,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert json.loads(result.stdout) == {
        "status": "failed",
        "error_code": "invalid_input_or_unavailable_state",
        "request_performed": False,
    }


def test_script_supports_explicit_python_and_actionable_missing_runtime_error(tmp_path):
    env = {**os.environ, "FOMC_PYTHON": sys.executable}
    result = subprocess.run(
        [str(SCRIPT), "--help"], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "recover-run" in result.stdout and "validate-config" in result.stdout
    env["FOMC_PYTHON"] = str(tmp_path / "missing interpreter")
    result = subprocess.run(
        [str(SCRIPT), "--help"], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert result.returncode == 2
    assert "FOMC_PYTHON" in result.stderr and "setup" in result.stderr
    assert not result.stdout
