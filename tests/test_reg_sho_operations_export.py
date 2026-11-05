import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from market_data_pipeline.pipelines.reg_sho_thresholds.acquisition import acquire_partition
from market_data_pipeline.pipelines.reg_sho_thresholds.episodes import derive_episodes
from market_data_pipeline.pipelines.reg_sho_thresholds.operations import health, plan
from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import acquire_and_run
from market_data_pipeline.pipelines.reg_sho_thresholds.research_export import (
    export_research_artifact,
)


class Response:
    status = 200
    code = 200

    def __init__(self, payload):
        self.payload = payload
        self.headers = {"Content-Type": "text/plain", "Content-Length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, maximum):
        return self.payload[:maximum]


def _config(tmp_path, *, enabled=True):
    value = yaml.safe_load(Path("config/reg_sho_thresholds/pipeline.example.yml").read_text())
    value["root"] = str(tmp_path / "root")
    value["acquisition"]["enabled"] = enabled
    value["partitions"] = {
        "fixture": {
            "provider": "nasdaq",
            "market": "NASDAQ",
            "list_date": "2026-11-03",
            "manifest": str(Path("manifests/reg-sho-nasdaq.yml").resolve()),
            "input": str(tmp_path / "not-used.txt"),
            "source_url": (
                "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth20261103.txt"
            ),
        }
    }
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_injected_acquisition_runs_once_and_research_export_is_atomic(tmp_path, monkeypatch):
    path = _config(tmp_path)
    monkeypatch.setenv("REG_SHO_USER_AGENT", "MarketMedallion operator@example.test")
    payload = Path("tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt").read_bytes()
    requests = []

    def transport(request):
        requests.append(request)
        return Response(payload)

    now = datetime(2026, 11, 4, 4, tzinfo=UTC)
    result = acquire_and_run(path, "fixture", transport=transport, clock=lambda: now)
    assert result["status"] == "SUCCEEDED" and result["transport"] == "https"
    assert len(requests) == 1
    receipt = json.loads(
        (tmp_path / "root" / "bronze" / result["run_id"] / "receipt.json").read_text()
    )
    assert receipt["acquisition"]["http_route"] == "injected"
    assert receipt["source_url"].endswith("nasdaqth20261103.txt")

    destination = tmp_path / "exchange"
    exported = export_research_artifact(path, destination, generated_at=now)
    pointer = json.loads((destination / "current.json").read_text())
    generation = destination / pointer["generation"]
    assert exported["membership_count"] == 2
    assert pq.read_table(generation / "membership.parquet").num_rows == 2
    manifest = json.loads((generation / "manifest.json").read_text())
    assert manifest["semantics"]["missing_list_behavior"] == "UNKNOWN_NOT_EMPTY"


def test_health_and_plan_never_request_and_disabled_acquisition_fails(tmp_path, monkeypatch):
    path = _config(tmp_path, enabled=False)
    monkeypatch.setenv("REG_SHO_USER_AGENT", "operator@example.test")
    assert health(path)["request_performed"] is False
    assert plan(path, provider="nasdaq")["partition_count"] == 1
    with pytest.raises(ValueError, match="disabled"):
        acquire_partition(path, "fixture", transport=lambda _request: Response(b"unused"))


def test_episodes_do_not_bridge_missing_coverage_and_require_complete_absence():
    dates = [date(2026, 11, day) for day in (2, 3, 4, 5, 6)]
    coverage = [
        {
            "provider": "nasdaq",
            "market": "NASDAQ",
            "list_date": current,
            "coverage_state": "UNAVAILABLE" if current.day == 4 else "COMPLETE",
        }
        for current in dates
    ]
    membership = [
        {
            "provider": "nasdaq",
            "market": "NASDAQ",
            "list_date": current,
            "raw_symbol": "ALFA",
            "rule_family": "REG_SHO_203",
            "row_state": "REPORTED",
        }
        for current in (dates[1], dates[3])
    ]
    episodes = derive_episodes(membership, coverage, dates)
    assert len(episodes) == 2
    assert episodes[0]["entry_confirmed"] is True
    assert episodes[0]["gap_after"] is True and episodes[0]["exit_confirmed"] is False
    assert episodes[1]["gap_before"] is True
    assert episodes[1]["exit_confirmed"] is True
