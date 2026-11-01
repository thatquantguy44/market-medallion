import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def cli(*args, env=None, module="market_data_pipeline"):
    return subprocess.run(
        [sys.executable, "-m", module, *map(str, args)],
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.mark.parametrize("module", ["market_data_pipeline", "market_data_pipeline.pipelines.aaii"])
def test_cli_run_asof_replay_and_invalid_input(tmp_path, module):
    def invoke(*args, **kwargs):
        return cli(*args, module=module, **kwargs)

    root = tmp_path / "output"
    result = invoke("run", "--input", "tests/fixtures/aaii/sentiment.csv", "--root", root)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["inserted_versions"] == 2
    result = invoke("gold", "--root", root, "--as-of", "2000-01-01T00:00:00Z")
    assert result.returncode == 0
    assert json.loads(result.stdout) == []
    assert invoke("gold", "--root", root, "--as-of", "2026-10-26").returncode != 0
    assert invoke("replay", "--source-root", root, "--root", tmp_path / "replay").returncode == 0
    environment = {k: v for k, v in os.environ.items() if k != "MARKET_DATA_POSTGRES_DSN"}
    result = invoke(
        "run",
        "--input",
        "tests/fixtures/aaii/sentiment.csv",
        "--root",
        tmp_path / "pg",
        "--warehouse",
        "postgres",
        env=environment,
    )
    assert result.returncode != 0
    assert "MARKET_DATA_POSTGRES_DSN" in result.stderr
    assert not (tmp_path / "pg").exists()


def test_dagster_job_materializes_fixture(tmp_path):
    dagster = pytest.importorskip("dagster")
    from market_data_pipeline.orchestration.dagster_defs import defs as repository_defs
    from market_data_pipeline.pipelines.aaii.dagster_defs import aaii_sentiment, defs

    assert repository_defs is not defs

    dagster.Definitions.validate_loadable(repository_defs)
    job = repository_defs.resolve_job_def("aaii_sentiment_job")
    result = job.execute_in_process(
        run_config={
            "ops": {
                "aaii_sentiment": {
                    "config": {
                        "input_path": str(Path("tests/fixtures/aaii/sentiment.csv").resolve()),
                        "root": str(tmp_path),
                    }
                }
            }
        }
    )
    assert result.success
    assert result.asset_materializations_for_node("aaii_sentiment")
    assert aaii_sentiment.key.path == ["aaii_sentiment"]
    assert defs.schedules[0].default_status == dagster.DefaultScheduleStatus.STOPPED
    assert {schedule.name for schedule in repository_defs.schedules} == {
        "aaii_thursday_local_import",
        "aaii_weekly_live",
        "naaim_delayed_daily_live",
    }
