"""SSV-AC04 normalized share quantities and lossless in-memory text conversion."""

from decimal import Decimal, InvalidOperation, localcontext

MAX_QUANTITY_DIGITS = 4096  # T03 operating budget, not a provider width/scale assertion.


def validate_quantity(value: object) -> Decimal:
    """Require an exact, finite, nonnegative Decimal within the T03 resource budget."""
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise ValueError("FINRA quantities must be finite nonnegative decimal numbers")
    parts = value.as_tuple()
    if (
        value.adjusted() >= MAX_QUANTITY_DIGITS
        or not -MAX_QUANTITY_DIGITS <= parts.exponent <= MAX_QUANTITY_DIGITS
        or len(parts.digits) > MAX_QUANTITY_DIGITS
    ):
        raise ValueError("FINRA quantity exceeds parser digit/exponent budget")
    return value


def validate_quantities(
    short_volume: Decimal, short_exempt_volume: Decimal, total_volume: Decimal
) -> tuple[Decimal, Decimal, Decimal]:
    """Validate one matched observation; exempt shares are already included in short."""
    values = tuple(validate_quantity(v) for v in (short_volume, short_exempt_volume, total_volume))
    short, exempt, total = values
    if not exempt <= short <= total:
        raise ValueError("FINRA quantities require exempt <= short <= total")
    return short, exempt, total


def quantity_to_text(value: Decimal) -> str:
    """Encode a quantity's full Decimal tuple, independent of caller capitalization.

    This is not a semantic-value hash: equal values with different scale retain
    different representations. It is not a provider JSON or ratio encoder.
    """
    validate_quantity(value)
    # Decimal string conversion is exact; capitals is its only context-dependent form.
    # https://docs.python.org/3.12/library/decimal.html#decimal.Context.capitals
    with localcontext(capitals=1):
        return str(value)


def quantity_from_text(text: str) -> Decimal:
    """Decode only the bounded representation emitted by quantity_to_text.

    Invalid input never changes caller flags, even when InvalidOperation traps
    are disabled. No numeric/float coercion or noncanonical aliases are accepted.
    """
    if not isinstance(text, str) or len(text) > MAX_QUANTITY_DIGITS + 64:
        raise ValueError("FINRA quantity text must be a bounded decimal string")
    try:
        with localcontext(capitals=1):
            value = validate_quantity(Decimal(text))
            if str(value) != text:
                raise ValueError("noncanonical FINRA quantity text")
            return value
    except InvalidOperation:
        raise ValueError("invalid FINRA quantity text") from None
