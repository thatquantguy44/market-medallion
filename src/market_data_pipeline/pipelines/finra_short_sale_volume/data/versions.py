"""SSV-AC06/07: exact persisted observations and capture-bound version identities."""

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest
from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import (
    RATIO_METHOD,
    share_ratios,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import quantity_to_text

TRANSFORM = "finra-ssv-local-partition/1"


def observation(row):
    ratios = share_ratios(row.short_volume, row.short_exempt_volume, row.total_volume)
    return {
        "trade_date": row.trade_date.isoformat(),
        "facility_id": row.facility_id,
        "symbol_raw": row.symbol_raw,
        "reporting_facility_code": row.reporting_facility_code,
        "market_raw": row.market_raw,
        "short_volume": quantity_to_text(row.short_volume),
        "short_exempt_volume": quantity_to_text(row.short_exempt_volume),
        "total_volume": quantity_to_text(row.total_volume),
        "short_share": str(ratios.short_share) if ratios.short_share is not None else None,
        "short_exempt_share": str(ratios.short_exempt_share)
        if ratios.short_exempt_share is not None
        else None,
        "ratio_method": RATIO_METHOD,
        "ratio_unit": "1",
        "removed": False,
    }


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
