"""SSV-AC04 pure per-observation ratios; aggregation/history/publication are later slices."""

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal, DivisionByZero, InvalidOperation, Overflow

from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import (
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
