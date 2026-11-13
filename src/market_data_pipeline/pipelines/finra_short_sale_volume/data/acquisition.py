"""SSV-AC12: pure finite synchronous plans; no store, environment or HTTP access."""

from dataclasses import asdict, dataclass
from datetime import timedelta

from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import manifest_from_dict
from market_data_pipeline.pipelines.finra_short_sale_volume.registry import (
    select_scopes,
    validate_acquisition_window,
)


@dataclass(frozen=True)
class Limits:
    max_days: int = 7
    max_partitions: int = 21
    max_requests: int = 100
    max_response_bytes: int = 3000000
    max_total_bytes: int = 20 * 1024**2
    page_rows: int = 5000
    max_attempts: int = 3
    max_wait_seconds: int = 60
    max_duration_seconds: int = 300

    def __post_init__(self):
        bounds = dict(
            max_days=(1, 31),
            max_partitions=(1, 93),
            max_requests=(1, 1000),
            max_response_bytes=(1, 3000000),
            max_total_bytes=(1, 1024**3),
            page_rows=(1, 5000),
            max_attempts=(1, 5),
            max_wait_seconds=(0, 300),
            max_duration_seconds=(1, 3600),
        )
        for key, (low, high) in bounds.items():
            value = getattr(self, key)
            if type(value) is not int or not low <= value <= high:
                raise ValueError("FINRA acquisition limits require bounded integer values")


def plan(manifest, start, end, scopes, *, as_of, overlap_days=2, limits=None):
    limits = limits if limits is not None else Limits()
    if not isinstance(limits, Limits):
        raise ValueError("FINRA acquisition requires validated limits")
    if manifest_from_dict(manifest.to_dict()) != manifest:
        raise ValueError("FINRA acquisition requires a verified manifest")
    if type(overlap_days) is not int or not 0 <= overlap_days <= 30:
        raise ValueError("FINRA correction overlap must be in 0..30 days")
    first, last = validate_acquisition_window(start, end, as_of=as_of)
    effective = first - timedelta(days=overlap_days)
    validate_acquisition_window(effective.isoformat(), end, as_of=as_of)
    facilities = select_scopes(scopes)
    if not {f.id for f in facilities} <= {f.id for f in manifest.facilities}:
        raise ValueError("FINRA acquisition scope is outside the selected manifest")
    days = (last - effective).days + 1
    count = days * len(facilities)
    upper = 2 + (count + 2) * limits.max_attempts
    if days > limits.max_days or count > limits.max_partitions or upper > limits.max_requests:
        raise ValueError("FINRA acquisition plan exceeds day/partition/request budget")
    return dict(
        schema_id="finra-ssv-acquisition-plan/1",
        requested_start=start,
        requested_end=end,
        effective_start=effective.isoformat(),
        overlap_days=overlap_days,
        as_of=as_of.isoformat(),
        page_limit=limits.page_rows,
        offset=0,
        request_budget_upper_bound=upper,
        limits=asdict(limits),
        partitions=[
            dict(
                trade_date=(effective + timedelta(days=n)).isoformat(),
                facility_id=f.id,
                market_code=f.market_code,
            )
            for n in range(days)
            for f in facilities
        ],
    )
