"""SSV-AC04: invented quantities, independent rational oracles and exact text round trips."""

import json
from dataclasses import FrozenInstanceError
from decimal import ROUND_UP, Decimal, Inexact, Rounded, localcontext
from fractions import Fraction

import pytest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import (
    RATIO_METHOD,
    RATIO_PRECISION,
    share_ratios,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import (
    quantity_from_text,
    quantity_to_text,
    validate_quantities,
)


@pytest.mark.parametrize(
    "short,exempt,total,expected",
    [
        ("40", "5", "100", ("0.40", "0.05")),
        ("12.345678", "0.000001", "24.691356", ("0.5", None)),
        ("0", "0", "100", ("0", "0")),
        ("100", "0", "100", ("1", "0")),
        ("100", "100", "100", ("1", "1")),
        ("0.000001", "0.000001", "0.000002", ("0.5", "0.5")),
        (str(2**80 + 1), "0", str(2**81 + 2), ("0.5", "0")),
        ("1E+4095", "0", "2E+4095", ("0.5", "0")),
        ("1E-4096", "0", "1E+4095", ("1E-8191", "0")),
    ],
)
def test_ssv_ac04_subset_ratios_have_matched_denominators_and_no_exempt_addition(
    short, exempt, total, expected
):
    values = tuple(Decimal(v) for v in (short, exempt, total))
    before = tuple(v.as_tuple() for v in values)
    result = share_ratios(*values)
    assert result.short_share == Decimal(expected[0])
    if expected[1] is not None:
        assert result.short_exempt_share == Decimal(expected[1])
    assert 0 <= result.short_exempt_share <= result.short_share <= 1
    assert tuple(v.as_tuple() for v in values) == before
    # Exact rational oracle bounds rounding error without repeating Decimal division.
    for numerator, ratio in zip(
        values[:2], (result.short_share, result.short_exempt_share), strict=True
    ):
        exact = Fraction(numerator) / Fraction(values[2])
        ulp = Fraction(Decimal((0, (1,), ratio.as_tuple().exponent)))
        assert abs(exact - Fraction(ratio)) <= ulp / 2


@pytest.mark.parametrize("zero", ["0", "0.000000", "-0.00", "0E-4096"])
def test_ssv_ac04_zero_denominator_is_null_only_for_valid_zero_quantities(zero):
    result = share_ratios(Decimal(zero), Decimal(zero), Decimal(zero))
    assert result.short_share is None and result.short_exempt_share is None
    with pytest.raises(FrozenInstanceError):
        result.short_share = Decimal(0)


@pytest.mark.parametrize(
    "values",
    [
        ("1", "0", "0"),
        ("0", "1", "0"),
        ("40", "41", "100"),
        ("101", "5", "100"),
        ("0.01", "0.010001", "1"),
        ("-1", "0", "100"),
        ("40", "-0.000001", "100"),
        ("0", "0", "-1"),
    ],
)
def test_ssv_ac04_invalid_quantity_relationships_fail_before_ratio_derivation(values):
    quantities = tuple(Decimal(v) for v in values)
    for operation in (validate_quantities, share_ratios):
        with pytest.raises(ValueError):
            operation(*quantities)


@pytest.mark.parametrize(
    "invalid",
    [
        0,
        0.1,
        True,
        None,
        "40",
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("1E+4096"),
        Decimal("1E-4097"),
        Decimal("9" * 4097),
    ],
)
def test_ssv_ac04_normalized_quantities_require_exact_finite_bounded_decimals(invalid):
    for index in range(3):
        values = [Decimal(0), Decimal(0), Decimal(100)]
        values[index] = invalid
        with pytest.raises(ValueError):
            share_ratios(*values)
    with pytest.raises(ValueError):
        quantity_to_text(invalid)


def test_ssv_ac04_recurring_ratios_are_fixed_and_caller_context_is_unchanged():
    with localcontext() as caller:
        caller.prec = 2
        caller.rounding = ROUND_UP
        caller.Emin, caller.Emax, caller.clamp, caller.capitals = -9, 9, 1, 0
        caller.traps[Inexact] = caller.traps[Rounded] = True
        caller.flags[Rounded] = True
        flags, traps = dict(caller.flags), dict(caller.traps)
        result = share_ratios(Decimal(2), Decimal(1), Decimal(3))
        assert result.short_share == Decimal("0." + "6" * 33 + "7")
        assert result.short_exempt_share == Decimal("0." + "3" * 34)
        assert (
            caller.prec,
            caller.rounding,
            caller.Emin,
            caller.Emax,
            caller.clamp,
            caller.capitals,
        ) == (2, ROUND_UP, -9, 9, 1, 0)
        assert dict(caller.flags) == flags and dict(caller.traps) == traps
    assert RATIO_PRECISION == 34 and RATIO_METHOD == "finra-short-sale-volume-ratios/1"


@pytest.mark.parametrize("last,rounded", [("2", "2"), ("3", "4")])
def test_ssv_ac04_half_even_ties_are_deterministic(last, rounded):
    value = Decimal("0." + "1" * 33 + last + "5")
    assert share_ratios(value, Decimal(0), Decimal(1)).short_share == Decimal(
        "0." + "1" * 33 + rounded
    )


@pytest.mark.parametrize(
    "text",
    [
        "0",
        "-0.000000",
        "40",
        "40.000000",
        "12.345678",
        "1E-4096",
        "1E+4095",
        "9" * 4096,
        str(2**80 + 1),
        "0.000000",
        "0E+4",
    ],
)
def test_ssv_ac04_quantity_json_text_round_trips_every_digit_and_scale(text):
    original = Decimal(text)
    with localcontext() as caller:
        caller.prec, caller.capitals = 2, 0
        caller.Emin, caller.Emax, caller.clamp = -9, 9, 1
        caller.traps[Inexact] = caller.traps[Rounded] = True
        caller.clear_flags()
        encoded = quantity_to_text(original)
        decoded = quantity_from_text(json.loads(json.dumps({"quantity": encoded}))["quantity"])
        assert not any(caller.flags.values())
    assert encoded == text and decoded.as_tuple() == original.as_tuple()


@pytest.mark.parametrize(
    "text",
    [
        None,
        40,
        0.1,
        True,
        "",
        " 40",
        "40 ",
        "+40",
        "040",
        "4_0",
        "NaN",
        "Infinity",
        "-1",
        "1e+2",
        "1E+99999999999999999999999",
        "9" * 4161,
    ],
)
def test_ssv_ac04_quantity_text_rejects_invalid_noncanonical_or_oversized_values(text):
    with localcontext() as caller:
        caller.clear_flags()
        with pytest.raises(ValueError):
            quantity_from_text(text)
        assert not any(caller.flags.values())


@pytest.mark.parametrize("shift", [-100, -6, 0, 6, 100])
def test_ssv_ac04_ratios_are_invariant_to_exact_common_quantity_scaling(shift):
    original = tuple(Decimal(v) for v in ("40.1250", "5.000001", "100.25"))
    scaled = tuple(
        Decimal((v.as_tuple().sign, v.as_tuple().digits, v.as_tuple().exponent + shift))
        for v in original
    )
    assert share_ratios(*scaled) == share_ratios(*original)


@pytest.mark.parametrize("fixture", ["invented-valid.json", "invented-fractional.json"])
def test_ssv_ac04_parsed_observations_round_trip_quantities_then_reproduce_ratios(fixture):
    import hashlib
    from pathlib import Path

    from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import parse_page
    from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import load_manifest

    payload = (Path("tests/fixtures/finra_short_sale_volume") / fixture).read_bytes()
    count = len(json.loads(payload))
    manifest = load_manifest(Path("manifests/finra-short-sale-volume.yml"))
    receipt = {
        "url": "https://api.finra.org/data/group/otcMarket/name/regShoDaily",
        "method": "POST",
        "request_data_version": 1,
        "request_payload": {
            "limit": 2,
            "offset": 0,
            "compareFilters": [
                {
                    "compareType": "EQUAL",
                    "fieldName": "tradeReportDate",
                    "fieldValue": "2026-11-09",
                },
                {"compareType": "EQUAL", "fieldName": "marketCode", "fieldValue": "N"},
            ],
        },
        "http_status": 200,
        "headers": {
            "Data-Version": "1",
            "Content-Type": "application/json",
            "Record-Total": str(count),
            "Record-Offset": "0",
            "Record-Limit": "2",
        },
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    for row in parse_page(payload, manifest, receipt).rows:
        original = (row.short_volume, row.short_exempt_volume, row.total_volume)
        encoded = json.dumps([quantity_to_text(value) for value in original])
        restored = tuple(quantity_from_text(value) for value in json.loads(encoded))
        assert [v.as_tuple() for v in original] == [v.as_tuple() for v in restored]
        assert share_ratios(*restored) == share_ratios(*original)
        assert not hasattr(row, "first_seen_at") and not hasattr(row, "provider_published_at")


def test_ssv_ac04_invalid_quantity_text_preserves_disabled_traps_and_flags():
    from decimal import InvalidOperation

    with localcontext() as caller:
        caller.traps[InvalidOperation] = False
        caller.clear_flags()
        for text in ("not-a-quantity", "1E+99999999999999999999999"):
            with pytest.raises(ValueError):
                quantity_from_text(text)
            assert not any(caller.flags.values())
        assert caller.traps[InvalidOperation] is False
