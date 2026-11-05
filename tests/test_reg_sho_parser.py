from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.reg_sho_thresholds.manifest import load_manifest
from market_data_pipeline.pipelines.reg_sho_thresholds.parser import parse_list


def _parse(provider, fixture, market, selected=date(2026, 11, 3)):
    manifest = load_manifest(Path(f"manifests/reg-sho-{provider}.yml"))
    return parse_list(Path(fixture).read_bytes(), manifest, list_date=selected, market=market)


def test_nasdaq_parser_preserves_flags_and_creation_envelope():
    parsed = _parse("nasdaq", "tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt", "NASDAQ")
    assert [row.raw_symbol for row in parsed.rows] == ["ALFA", "BETA"]
    assert parsed.rows[1].flags_raw_json == (
        '{"filler":"","market_category":"G","reg_sho_threshold_flag":"Y","rule_3210":"Y"}'
    )
    assert parsed.source_created_at_raw == "20261103230004"


def test_empty_nasdaq_and_nyse_lists_are_valid_provider_envelopes():
    nasdaq = _parse(
        "nasdaq",
        "tests/fixtures/reg_sho_thresholds/nasdaq-empty.txt",
        "NASDAQ",
        date(2026, 11, 4),
    )
    nyse = _parse("nyse", "tests/fixtures/reg_sho_thresholds/nyse-empty.txt", "NYSE")
    assert nasdaq.rows == () and nyse.rows == ()


def test_nyse_adapter_requires_selected_market_in_every_row():
    parsed = _parse(
        "nyse",
        "tests/fixtures/reg_sho_thresholds/nyse-american-complete.txt",
        "NYSE American",
    )
    assert len(parsed.rows) == 2
    assert all(row.market_category == "NYSE American" for row in parsed.rows)
    with pytest.raises(ValueError, match="market"):
        _parse("nyse", "tests/fixtures/reg_sho_thresholds/nyse-american-complete.txt", "NYSE Arca")


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda text: text.replace("20261103230004", "20261102230004"), "trailer"),
        (lambda text: text.replace("|Y|N|", "|N|N|"), "flags"),
        (lambda text: text.replace("ALFA|", "BETA|"), "duplicate"),
        (lambda text: text.replace("Rule 3210", "Rule 203"), "header"),
    ],
)
def test_nasdaq_schema_date_flags_and_duplicates_fail(mutator, message):
    manifest = load_manifest(Path("manifests/reg-sho-nasdaq.yml"))
    text = Path("tests/fixtures/reg_sho_thresholds/nasdaq-complete.txt").read_text()
    with pytest.raises(ValueError, match=message):
        parse_list(mutator(text).encode(), manifest, list_date=date(2026, 11, 3), market="NASDAQ")
