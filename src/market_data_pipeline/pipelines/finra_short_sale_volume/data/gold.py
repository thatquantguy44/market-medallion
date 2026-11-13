"""SSV-AC04/09 pure exact matched quantities and fixed-method research ratios."""

from dataclasses import dataclass
from decimal import (
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DivisionByZero,
    Inexact,
    InvalidOperation,
    Overflow,
    Rounded,
    localcontext,
)

from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import (
    MAX_QUANTITY_DIGITS,
    validate_quantities,
)

RATIO_METHOD = "finra-short-sale-volume-ratios/1"
RATIO_PRECISION = 34


@dataclass(frozen=True)
class ShareRatios:
    """Unit-1 ratios under RATIO_METHOD; a valid zero denominator yields nulls."""

    short_share: Decimal | None
    short_exempt_share: Decimal | None


def share_ratios(
    short_volume: Decimal, short_exempt_volume: Decimal, total_volume: Decimal
) -> ShareRatios:
    """Derive two ratios from one matched date/facility/symbol observation.

    Only derived ratios round to 34 significant digits with half-even ties.
    Original quantities remain exact. This helper cannot establish scope or
    partition completeness; callers must supply quantities from the same row.
    """
    short, exempt, total = validate_quantities(short_volume, short_exempt_volume, total_volume)
    return _ratios(short, exempt, total)


def _ratios(short, exempt, total):
    if total.is_zero():
        return ShareRatios(None, None)
    # Explicit context operations avoid all ambient precision, traps and flags.
    # https://docs.python.org/3.12/library/decimal.html#decimal.Context
    context = Context(
        prec=RATIO_PRECISION,
        rounding=ROUND_HALF_EVEN,
        Emin=-999999,
        Emax=999999,
        capitals=1,
        clamp=0,
        flags=[],
        traps=[InvalidOperation, DivisionByZero, Overflow],
    )
    return ShareRatios(context.divide(short, total), context.divide(exempt, total))


SUM_DIGITS = 2 * MAX_QUANTITY_DIGITS + 8


def decimal_text(value: Decimal | None) -> str | None:
    """Bounded exact derived-number text, without source-row precision constraints."""
    if value is None:
        return None
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("FINRA derived values require finite Decimal numbers")
    if (
        len(value.as_tuple().digits) > SUM_DIGITS
        or abs(value.as_tuple().exponent) > SUM_DIGITS + RATIO_PRECISION
    ):
        raise ValueError("FINRA derived value exceeds its numeric budget")
    with localcontext(capitals=1):
        return str(value)


def matched_metrics(triples):
    """Sum one matched row per verified disjoint TRF; no missing-row imputation."""
    if not isinstance(triples, (list, tuple)) or not 1 <= len(triples) <= 3:
        raise ValueError("FINRA matched metrics require one to three source triples")
    context = Context(
        prec=SUM_DIGITS,
        rounding=ROUND_HALF_EVEN,
        Emin=-999999,
        Emax=999999,
        capitals=1,
        clamp=0,
        flags=[],
        traps=[Inexact, Rounded, InvalidOperation, Overflow],
    )
    totals = [Decimal(0), Decimal(0), Decimal(0)]
    for triple in triples:
        if not isinstance(triple, (list, tuple)) or len(triple) != 3:
            raise ValueError("FINRA matched metrics require quantity triples")
        values = validate_quantities(*triple)
        totals = [context.add(a, b) for a, b in zip(totals, values, strict=True)]
    ratios = _ratios(*totals)
    return dict(
        zip(
            ("short_volume", "short_exempt_volume", "total_volume"),
            (decimal_text(v) for v in totals),
            strict=True,
        ),
        short_share=decimal_text(ratios.short_share),
        short_exempt_share=decimal_text(ratios.short_exempt_share),
        null_reason="ZERO_TOTAL" if totals[2].is_zero() else None,
    )


def ratio_change(current: Decimal | None, prior: Decimal | None) -> Decimal | None:
    for value in (current, prior):
        if value is not None and (
            not isinstance(value, Decimal) or not value.is_finite() or not 0 <= value <= 1
        ):
            raise ValueError("FINRA ratio changes require finite unit-1 ratios")
    if current is None or prior is None:
        return None
    context = Context(
        prec=RATIO_PRECISION,
        rounding=ROUND_HALF_EVEN,
        Emin=-999999,
        Emax=999999,
        capitals=1,
        clamp=0,
        flags=[],
        traps=[InvalidOperation, Overflow],
    )
    return context.subtract(current, prior)
