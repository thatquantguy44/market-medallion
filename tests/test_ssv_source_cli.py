"""SSV-AC12: actual CLI planning and live gates make no provider request."""

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from test_ssv_acquisition_config import config_file
from test_ssv_cli import invoke


def args():
    day = (datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=3)).isoformat()
    return ["--start-date", day, "--end-date", day, "--scope", "nyse_trf", "--overlap-days", "0"]


def test_ssv_ac12_cli_plan_needs_no_credentials_or_root_creation(tmp_path):
    path = config_file(
        tmp_path,
        credentials=dict(
            client_id_env="UNSET_FINRA_TEST_ID", client_secret_env="UNSET_FINRA_TEST_SECRET"
        ),
    )
    result = invoke(path, "acquisition-plan", *args())
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert len(value["partitions"]) == 1 and value["page_limit"] == 5000
    assert not (tmp_path / "store").exists()


def test_ssv_ac12_cli_requires_both_live_flags_and_configured_credentials(tmp_path):
    for configured_live, flags in [(False, ["--live"]), (True, []), (True, ["--live"])]:
        path = config_file(
            tmp_path,
            live=configured_live,
            credentials=dict(
                client_id_env="UNSET_FINRA_TEST_ID", client_secret_env="UNSET_FINRA_TEST_SECRET"
            ),
        )
        result = invoke(path, "acquire", *args(), *flags)
        assert result.returncode == 2 and str(tmp_path) not in result.stderr
        assert not (tmp_path / "store").exists()
