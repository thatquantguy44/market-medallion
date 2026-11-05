from datetime import UTC, datetime
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import gold, run


def _config(tmp_path, fixture="nasdaq-complete.txt"):
    input_path = tmp_path / "nasdaq.txt"
    input_path.write_bytes(Path(f"tests/fixtures/reg_sho_thresholds/{fixture}").read_bytes())
    value = yaml.safe_load(Path("config/reg_sho_thresholds/pipeline.example.yml").read_text())
    value["root"] = str(tmp_path / "root")
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
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path, input_path


def _clock(*values):
    iterator = iter(values)
    return lambda: next(iterator)


def test_complete_replacement_versions_corrections_and_withdrawals(tmp_path):
    path, input_path = _config(tmp_path)
    first = datetime(2026, 11, 4, 1, tzinfo=UTC)
    result = run(path, "fixture", clock=lambda: first)
    assert result["status"] == "SUCCEEDED" and result["inserted_versions"] == 2

    text = Path("tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt").read_text()
    text = text.replace("ALFA INVENTED CORPORATION", "ALFA INVENTED CORP REVISED")
    text = text.replace("BETA|BETA INVENTED FUND|G|Y|Y|", "GAMM|GAMMA INVENTED FUND|S|Y|N|")
    input_path.write_text(text)
    second = datetime(2026, 11, 4, 2, tzinfo=UTC)
    result = run(path, "fixture", clock=lambda: second)
    assert result["inserted_versions"] == 3
    current = gold(path)
    assert [row["raw_symbol"] for row in current["rows"]] == ["ALFA", "GAMM"]
    before = gold(path, as_of=first)
    assert [row["raw_symbol"] for row in before["rows"]] == ["ALFA", "BETA"]
    history = gold(path, include_withdrawn=True)
    beta = next(row for row in history["rows"] if row["raw_symbol"] == "BETA")
    assert beta["row_state"] == "WITHDRAWN"


def test_missing_list_is_unavailable_and_never_withdraws_membership(tmp_path):
    path, input_path = _config(tmp_path)
    run(path, "fixture", clock=lambda: datetime(2026, 11, 4, 1, tzinfo=UTC))
    input_path.unlink()
    result = run(path, "fixture", clock=lambda: datetime(2026, 11, 4, 2, tzinfo=UTC))
    assert result["status"] == "FAILED"
    assert result["coverage_state"] == "UNAVAILABLE"
    assert gold(path)["row_count"] == 2


def test_valid_empty_list_is_complete_empty_and_withdraws_prior_membership(tmp_path):
    path, input_path = _config(tmp_path)
    run(path, "fixture", clock=lambda: datetime(2026, 11, 4, 1, tzinfo=UTC))
    input_path.write_text(
        "Symbol|Security Name|Market Category|Reg SHO Threshold Flag|Rule 3210|Filler\n"
        "20261103230005\n"
    )
    result = run(path, "fixture", clock=lambda: datetime(2026, 11, 4, 2, tzinfo=UTC))
    assert result["coverage_state"] == "COMPLETE_EMPTY"
    assert result["inserted_versions"] == 2
    assert gold(path)["row_count"] == 0
