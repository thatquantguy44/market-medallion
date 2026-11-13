"""SSV-AC06/07: exact persisted observations and capture-bound version identities."""

from decimal import localcontext

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest
from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import (
    RATIO_METHOD,
    share_ratios,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import quantity_to_text

TRANSFORM = "finra-ssv-local-partition/1"


def observation(row):
    ratios = share_ratios(row.short_volume, row.short_exempt_volume, row.total_volume)
    body = {
        "trade_date": row.trade_date.isoformat(),
        "facility_id": row.facility_id,
        "symbol_raw": row.symbol_raw,
        "reporting_facility_code": row.reporting_facility_code,
        "market_raw": row.market_raw,
        "short_volume": quantity_to_text(row.short_volume),
        "short_exempt_volume": quantity_to_text(row.short_exempt_volume),
        "total_volume": quantity_to_text(row.total_volume),
        "short_share": _ratio_text(ratios.short_share) if ratios.short_share is not None else None,
        "short_exempt_share": _ratio_text(ratios.short_exempt_share)
        if ratios.short_exempt_share is not None
        else None,
        "ratio_method": RATIO_METHOD,
        "ratio_unit": "1",
        "removed": False,
        "source_row": row.source_row,
    }
    body["value_sha256"] = content_hash(body)
    return body


def version(body, saved):
    value = dict(body)
    value.update(
        capture_id=saved["capture_id"],
        run_id=saved["run_id"],
        capture_sequence=saved["capture_sequence"],
        first_seen_at=saved["archived_at"],
        retrieved_at=saved["retrieved_at"],
        provider_published_at=None,
        payload_sha256=saved["payload_sha256"],
        receipt_sha256=saved["receipt_sha256"],
        manifest_sha256=saved["manifest_sha256"],
        transform=TRANSFORM,
    )
    value["version_id"] = digest(value)
    return value


def content_hash(body):
    """Numeric equality ignores scale without Decimal.normalize/context rounding."""
    from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import (
        quantity_from_text,
    )

    values = {
        k: body[k]
        for k in (
            "trade_date",
            "facility_id",
            "symbol_raw",
            "reporting_facility_code",
            "market_raw",
            "removed",
        )
    }
    for key in ("short_volume", "short_exempt_volume", "total_volume"):
        if body["removed"]:
            values[key] = None
            continue
        number = quantity_from_text(body[key])
        parts = number.as_tuple()
        digits = "".join(str(d) for d in parts.digits)
        trimmed = digits.rstrip("0")
        values[key] = (
            [parts.sign, trimmed, parts.exponent + len(digits) - len(trimmed)]
            if trimmed
            else [0, "0", 0]
        )
    return digest(values)


def _ratio_text(value):
    with localcontext(capitals=1):
        return str(value)


def removal(previous):
    body = {
        k: previous[k]
        for k in (
            "trade_date",
            "facility_id",
            "symbol_raw",
            "reporting_facility_code",
            "market_raw",
            "ratio_method",
            "ratio_unit",
        )
    }
    body.update(
        short_volume=None,
        short_exempt_volume=None,
        total_volume=None,
        short_share=None,
        short_exempt_share=None,
        source_row=None,
        removed=True,
    )
    body["value_sha256"] = content_hash(body)
    return body
