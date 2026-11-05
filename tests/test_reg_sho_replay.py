import hashlib
from datetime import UTC, datetime
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import gold, run
from market_data_pipeline.pipelines.reg_sho_thresholds.replay import replay


def _config(tmp_path, name, input_path):
    value = yaml.safe_load(Path("config/reg_sho_thresholds/pipeline.example.yml").read_text())
    value["root"] = str(tmp_path / name)
    value["partitions"] = {
        "fixture": {
            "provider": "nasdaq",
            "market": "NASDAQ",
            "list_date": "2026-11-03",
            "manifest": str(Path("manifests/reg-sho-nasdaq.yml").resolve()),
            "input": str(input_path),
            "source_url": (
                "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth20261103.txt"
            ),
        }
    }
    path = tmp_path / f"{name}.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_threshold_replay_preserves_versions_cutoffs_and_bronze(tmp_path):
    input_path = tmp_path / "input.txt"
    input_path.write_bytes(
        Path("tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt").read_bytes()
    )
    source_config = _config(tmp_path, "source", input_path)
    first = datetime(2026, 11, 4, 1, tzinfo=UTC)
    first_run = run(source_config, "fixture", clock=lambda: first)
    input_path.write_text(
        Path("tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt")
        .read_text()
        .replace("BETA|BETA INVENTED FUND|G|Y|Y|", "GAMM|GAMMA INVENTED FUND|S|Y|N|")
    )
    second = datetime(2026, 11, 4, 2, tzinfo=UTC)
    run(source_config, "fixture", clock=lambda: second)
    destination_config = _config(tmp_path, "destination", input_path)
    result = replay(tmp_path / "source", destination_config)
    assert result["capture_count"] == 2
    assert gold(destination_config)["rows"] == gold(source_config)["rows"]
    assert gold(destination_config, as_of=first)["rows"] == gold(source_config, as_of=first)["rows"]
    for run_id in (first_run["run_id"],):
        source_receipt = tmp_path / "source" / "bronze" / run_id / "receipt.json"
        target_receipt = tmp_path / "destination" / "bronze" / run_id / "receipt.json"
        assert _sha(source_receipt) == _sha(target_receipt)
    assert (tmp_path / "destination" / "replay.json").is_file()
