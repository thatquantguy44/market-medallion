"""SSV-AC07 / C06: exact semantic revisions and capture-time cutoff ordering."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_parser import FIXTURES, receipt
from test_ssv_parser import manifest as manifest
from test_ssv_pipeline import gold, ingest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import parse_page
from market_data_pipeline.pipelines.finra_short_sale_volume.data.versions import (
    content_hash,
    observation,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.pipeline import history, ingest_capture

__all__ = ["manifest"]


def trail(root, manifest, symbol="000SYN.A"):
    return history(
        root,
        manifest,
        approved_use=fixture_grant(),
        operation="fixture",
        trade_date="2026-11-09",
        facility_id="nyse_trf",
        symbol=symbol,
    )


def revised(value=50):
    return (
        (FIXTURES / "invented-valid.json")
        .read_bytes()
        .replace(b'"shortParQuantity": 40', f'"shortParQuantity": {value}'.encode())
    )


def test_ssv_ac07_repeat_correction_and_reversion_preserve_capture_versions(tmp_path, manifest):
    first = ingest(tmp_path, manifest)
    initial = gold(tmp_path, manifest)
    repeat = ingest(tmp_path, manifest, clock=NOW + timedelta(seconds=1))
    change = ingest(tmp_path, manifest, payload=revised(), clock=NOW + timedelta(seconds=2))
    revert = ingest(tmp_path, manifest, clock=NOW + timedelta(seconds=3))
    assert [r["new_versions"] for r in [first, repeat, change, revert]] == [2, 0, 1, 1]
    versions = trail(tmp_path, manifest)
    assert [v["short_volume"] for v in versions] == ["40", "50", "40"]
    assert len({v["version_id"] for v in versions}) == 3
    assert versions[0]["version_id"] == initial["rows"][0]["version_id"]
    assert versions[0]["first_seen_at"] == "2026-11-13T15:00:00.000000Z"
    assert [
        gold(tmp_path, manifest, as_of=NOW + timedelta(seconds=s))["rows"][0]["short_volume"]
        for s in [0, 1, 2, 3]
    ] == ["40", "40", "50", "40"]
    assert gold(tmp_path, manifest, as_of=NOW - timedelta(microseconds=1))["rows"] == []


def test_ssv_ac07_equal_time_cutoffs_follow_original_sequence(tmp_path, manifest):
    first = ingest(tmp_path, manifest)
    second = ingest(tmp_path, manifest, payload=revised())
    assert first["capture_sequence"] < second["capture_sequence"]
    assert gold(tmp_path, manifest, as_of=NOW)["rows"][0]["short_volume"] == "50"
    old = gold(tmp_path, manifest, as_of=NOW, as_of_sequence=first["capture_sequence"])
    assert old["rows"][0]["short_volume"] == "40" and old["capture_id"] == first["capture_id"]
    assert gold(tmp_path, manifest, as_of=NOW.astimezone(timezone(timedelta(hours=-5)))) == gold(
        tmp_path, manifest, as_of=NOW
    )


@pytest.mark.parametrize("quantity", ["40.0", "40.000000", "4.0e1", "4E+1"])
def test_ssv_ac07_scale_only_change_keeps_value_version_and_exact_original_text(
    tmp_path, manifest, quantity
):
    first = ingest(tmp_path, manifest)
    before = gold(tmp_path, manifest)["rows"][0]
    result = ingest(tmp_path, manifest, payload=revised(quantity))
    after = gold(tmp_path, manifest)["rows"][0]
    assert result["new_versions"] == 0 and len(trail(tmp_path, manifest)) == 1
    assert after["version_id"] == before["version_id"] and after["short_volume"] == "40"
    assert after["capture_id"] == first["capture_id"]
    assert gold(tmp_path, manifest)["capture_id"] == result["capture_id"]


@pytest.mark.parametrize(
    "values",
    [
        ("0", "-0.000"),
        ("12.345678", "12.345678000"),
        ("1000000000000000000000000000000000000", "1E+36"),
    ],
)
def test_ssv_ac07_content_hash_is_numeric_and_context_independent(manifest, values):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    row = parse_page(payload, manifest, receipt(payload)).rows[0]
    a = replace(
        row,
        short_volume=Decimal(values[0]),
        short_exempt_volume=Decimal("0"),
        total_volume=Decimal("1E+40"),
    )
    b = replace(a, short_volume=Decimal(values[1]), source_row=900)
    with localcontext() as context:
        context.prec = 2
        context.capitals = 0
        assert content_hash(observation(a)) == content_hash(observation(b))
    assert content_hash(observation(a)) != content_hash(observation(replace(a, symbol_raw="OTHER")))


def test_ssv_ac07_clock_regression_refuses_publication(tmp_path, manifest):
    ingest(tmp_path, manifest)
    before = gold(tmp_path, manifest)
    with pytest.raises(ValueError, match="order regressed"):
        ingest(tmp_path, manifest, payload=revised(), clock=NOW - timedelta(seconds=1))
    assert gold(tmp_path, manifest) == before and len(trail(tmp_path, manifest)) == 1


@pytest.mark.parametrize(
    "args",
    [
        {"as_of": datetime(2026, 11, 13)},
        {"as_of_sequence": 1},
        {"as_of": NOW, "as_of_sequence": True},
        {"as_of": NOW, "as_of_sequence": -1},
    ],
)
def test_ssv_ac07_invalid_cutoff_is_refused(tmp_path, manifest, args):
    ingest(tmp_path, manifest)
    with pytest.raises(ValueError):
        gold(tmp_path, manifest, **args)


def test_ssv_ac07_same_capture_concurrent_retries_are_idempotent(tmp_path, manifest):
    result = ingest(tmp_path, manifest)

    def retry(_):
        return ingest_capture(
            tmp_path,
            result["capture_id"],
            manifest,
            approved_use=fixture_grant(),
            operation="fixture",
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(retry, range(8))) == [result] * 8
    assert len(trail(tmp_path, manifest)) == 1


def test_ssv_ac07_terminal_parser_profile_cannot_be_changed(tmp_path, manifest):
    result = ingest(tmp_path, manifest)
    with pytest.raises(ValueError, match="profile conflict"):
        ingest_capture(
            tmp_path,
            result["capture_id"],
            manifest,
            approved_use=fixture_grant(),
            operation="fixture",
            max_rows=4999,
        )


def test_ssv_ac07_persisted_observation_is_independent_of_ambient_decimal_context(manifest):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    row = parse_page(payload, manifest, receipt(payload)).rows[0]
    row = replace(
        row,
        short_volume=Decimal("1E-2000"),
        short_exempt_volume=Decimal("0"),
        total_volume=Decimal("1"),
    )
    baseline = observation(row)
    with localcontext() as context:
        context.prec = 2
        context.capitals = 0
        assert observation(row) == baseline
