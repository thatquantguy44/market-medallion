"""SSV-AC09: exact matched-population metrics and local temporal research."""

from decimal import Decimal, Inexact, Rounded, localcontext
from fractions import Fraction

import pytest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import (
    decimal_text,
    matched_metrics,
    ratio_change,
)


def amounts(short, exempt, total):
    return tuple(Decimal(v) for v in (short, exempt, total))


def test_ssv_ac09_disjoint_metric_is_ratio_of_exact_sums_not_mean_of_ratios():
    result = matched_metrics([amounts("40", "5", "100"), amounts("9", "1", "10")])
    assert result["short_volume"] == "49" and result["total_volume"] == "110"
    assert result["short_share"] == "0.4454545454545454545454545454545455"
    assert result["short_exempt_share"] == "0.05454545454545454545454545454545455"
    assert Decimal(result["short_share"]) != (Decimal(".4") + Decimal(".9")) / 2
    error = abs(Fraction(Decimal(result["short_share"])) - Fraction(49, 110))
    assert error < Fraction(1, 10**33)


def test_ssv_ac09_exact_fractional_and_large_sums_ignore_ambient_context():
    triples = [
        amounts("12.345678", "0.000001", "24.691356"),
        amounts("9007199254740993", "0", "18014398509481986"),
    ]
    baseline = matched_metrics(triples)
    with localcontext() as context:
        context.prec = 2
        context.capitals = 0
        context.traps[Inexact] = context.traps[Rounded] = True
        before = context.copy()
        assert matched_metrics(triples) == baseline
        assert context.flags == before.flags
    assert baseline["short_volume"] == "9007199254741005.345678"
    assert baseline["short_exempt_volume"] == "0.000001"
    assert baseline["short_share"] == "0.5"


def test_ssv_ac09_sum_can_exceed_individual_source_precision_without_truncation():
    number = Decimal("9" * 4096)
    result = matched_metrics([(number, Decimal(0), number)] * 3)
    assert Fraction(Decimal(result["short_volume"])) == Fraction(number) * 3
    assert result["short_share"] == "1"


def test_ssv_ac09_extreme_exponent_alignment_remains_exact():
    result = matched_metrics(
        [amounts("1E+4095", "0", "1E+4095"), amounts("1E-4096", "0", "1E-4096")]
    )
    assert Fraction(Decimal(result["total_volume"])) == Fraction(10**4095) + Fraction(1, 10**4096)
    assert result["short_share"] == "1"


def test_ssv_ac09_zero_reported_denominator_keeps_zero_quantities_and_null_ratios():
    result = matched_metrics([amounts("0", "0", "0"), amounts("-0.00", "0.000", "0")])
    assert Decimal(result["total_volume"]) == 0
    assert result["short_share"] is result["short_exempt_share"] is None
    assert result["null_reason"] == "ZERO_TOTAL"


@pytest.mark.parametrize(
    "value", [[], [amounts("1", "0", "1")] * 4, [(1, 0, 1)], [(Decimal(2), Decimal(0), Decimal(1))]]
)
def test_ssv_ac09_metrics_refuse_invalid_or_unbounded_source_triples(value):
    with pytest.raises(ValueError):
        matched_metrics(value)


@pytest.mark.parametrize(
    "current,prior,expected",
    [
        ("0.5", "0.4", "0.1"),
        ("0.4", "0.5", "-0.1"),
        ("0", "0", "0"),
        (None, "0.5", None),
        ("0.5", None, None),
    ],
)
def test_ssv_ac09_ratio_change_is_defined_only_for_two_available_ratios(current, prior, expected):
    value = ratio_change(
        Decimal(current) if current is not None else None,
        Decimal(prior) if prior is not None else None,
    )
    assert decimal_text(value) == expected


@pytest.mark.parametrize("value", [1.0, Decimal("NaN"), Decimal("-1"), Decimal("2")])
def test_ssv_ac09_ratio_change_refuses_non_ratios(value):
    with pytest.raises(ValueError):
        ratio_change(value, Decimal("0.5"))
