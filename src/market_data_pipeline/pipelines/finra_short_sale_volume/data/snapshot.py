"""SSV-AC10 exact decimal codec and fixed typed projection descriptions; no I/O."""

import re
from decimal import Decimal, InvalidOperation, localcontext

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest
from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import (
    RATIO_PRECISION,
    SUM_DIGITS,
)

CONTRACT = "finra-ssv-parquet/1"
DECIMAL_CODEC = "finra-exact-decimal-tuple/1"
MAX_RECORD_BYTES = 262144
DEFAULT_ROWS = 250000
DEFAULT_BYTES = 128 * 1024 * 1024


def limits(max_rows, max_bytes):
    if (
        type(max_rows) is not int
        or not 1 <= max_rows <= 1000000
        or type(max_bytes) is not int
        or not 1 <= max_bytes <= 1024**3
    ):
        raise ValueError("FINRA snapshot requires bounded positive row/byte limits")


def encode_decimal(text):
    if text is None:
        return None
    if not isinstance(text, str) or len(text) > SUM_DIGITS + 64:
        raise ValueError("invalid FINRA snapshot decimal text")
    try:
        with localcontext(capitals=1):
            value = Decimal(text)
            if not value.is_finite() or str(value) != text:
                raise ValueError
        parts = value.as_tuple()
        if len(parts.digits) > SUM_DIGITS or abs(parts.exponent) > SUM_DIGITS + RATIO_PRECISION:
            raise ValueError
        return dict(
            negative=bool(parts.sign),
            digits="".join(str(d) for d in parts.digits),
            exponent=parts.exponent,
        )
    except (ValueError, InvalidOperation):
        raise ValueError("invalid FINRA snapshot decimal text") from None


def decode_decimal(value):
    if value is None:
        return None
    if (
        not isinstance(value, dict)
        or set(value) != {"negative", "digits", "exponent"}
        or type(value["negative"]) is not bool
        or type(value["exponent"]) is not int
        or not isinstance(value["digits"], str)
        or not re.fullmatch("[0-9]{1," + str(SUM_DIGITS) + "}", value["digits"])
        or abs(value["exponent"]) > SUM_DIGITS + RATIO_PRECISION
    ):
        raise ValueError("invalid FINRA snapshot decimal struct")
    with localcontext(capitals=1):
        text = str(
            Decimal(
                (int(value["negative"]), tuple(int(d) for d in value["digits"]), value["exponent"])
            )
        )
    if encode_decimal(text) != value:
        raise ValueError("noncanonical FINRA snapshot decimal struct")
    return text


def field(name, kind, nullable=False):
    return (name, kind, nullable)


NUMBERS = [
    field(n, "decimal", True)
    for n in (
        "short_volume",
        "short_exempt_volume",
        "total_volume",
        "short_share",
        "short_exempt_share",
    )
]
FACT = [
    field("version_id", "s"),
    field("trade_date", "date"),
    field("facility_id", "s"),
    field("symbol_raw", "s"),
    field("removed", "b"),
    field("first_seen_at", "time"),
    field("capture_sequence", "i"),
    field("capture_id", "s"),
    field("source_row", "i", True),
    field("retrieved_at", "time", True),
    field("provider_published_at", "time", True),
    *NUMBERS,
]
COMPONENT = [
    field("facility_id", "s"),
    field("capture_id", "s", True),
    field("version_id", "s", True),
    field("source_capture_id", "s", True),
    field("source_row", "i", True),
    field("status", "s"),
    *NUMBERS[:3],
]
FIELDS = {
    "silver": FACT,
    "gold": [*FACT, field("observed_capture_id", "s"), field("observed_source_row", "i")],
    "research": [
        field("trade_date", "date"),
        field("facility_ids", "strings"),
        field("symbol_raw", "s"),
        field("status", "s"),
        *NUMBERS,
        field("prior_trade_date", "date", True),
        field("calendar_day_gap", "i", True),
        field("prior_status", "s"),
        *[
            field(n, "decimal", True)
            for n in (
                "prior_short_share",
                "prior_short_exempt_share",
                "short_share_change",
                "short_exempt_share_change",
            )
        ],
        field("evidence_known_at", "time", True),
        field("components", "components"),
        field("prior_components", "components"),
    ],
    "coverage": [
        field("trade_date", "date"),
        field("facility_id", "s"),
        field("capture_id", "s", True),
        field("committed_rows", "i", True),
        field("latest_attempt_status", "s"),
        field("latest_attempt_capture_id", "s", True),
    ],
    "captures": [
        field("capture_id", "s"),
        field("run_id", "s"),
        field("capture_sequence", "i"),
        field("archived_at", "time"),
        field("trade_date", "date"),
        field("facility_id", "s"),
        field("status", "s"),
        *[field(n, "i") for n in ("parsed_rows", "accepted_rows", "new_versions", "removed_rows")],
        field("completed_at", "time"),
        field("result_sha256", "s"),
    ],
    "audit": [
        field("attempt_id", "s"),
        field("capture_id", "s"),
        field("run_id", "s"),
        field("trade_date", "date"),
        field("facility_id", "s"),
        field("archived_at", "time"),
        field("capture_sequence", "i"),
        field("started_at", "time"),
        field("completed_at", "time", True),
        field("status", "s"),
        field("error_code", "s", True),
    ],
    "membership": [
        field("capture_id", "s"),
        field("symbol_raw", "s"),
        field("version_id", "s"),
        field("source_row", "i"),
    ],
}


def fingerprint(name):
    if name not in FIELDS:
        raise ValueError("unknown FINRA snapshot table")
    return digest(
        {
            "contract": CONTRACT,
            "table": name,
            "fields": FIELDS[name],
            "record_json": "canonical-utf8-json/1",
            "decimal_codec": DECIMAL_CODEC,
            "component_fields": COMPONENT,
        }
    )
