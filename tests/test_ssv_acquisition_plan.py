"""SSV-AC12: pure bounded plans retain correction overlap and explicit scopes."""

from datetime import date

import pytest
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.acquisition import Limits, plan

__all__ = ["manifest"]


def test_ssv_ac12_plan_expands_correction_overlap_in_date_facility_order(manifest):
    result = plan(
        manifest, "2026-11-09", "2026-11-10", ["nyse_trf"], as_of=date(2026, 11, 13), overlap_days=2
    )
    assert [r["trade_date"] for r in result["partitions"]] == [
        "2026-11-07",
        "2026-11-08",
        "2026-11-09",
        "2026-11-10",
    ]
    assert all(r["facility_id"] == "nyse_trf" for r in result["partitions"])
    assert result["requested_start"] == "2026-11-09" and result["effective_start"] == "2026-11-07"
    assert result["page_limit"] == 5000 and result["offset"] == 0
    assert result["request_budget_upper_bound"] == 20


@pytest.mark.parametrize(
    "changes",
    [
        {"max_days": 0},
        {"max_requests": True},
        {"page_rows": 5001},
        {"max_response_bytes": 3000001},
        {"max_attempts": 6},
        {"max_wait_seconds": -1},
        {"max_duration_seconds": 0},
        {"max_total_bytes": 2**31},
        {"surprise": 2},
    ],
)
def test_ssv_ac12_invalid_limits_refuse(changes):
    with pytest.raises((ValueError, TypeError)):
        Limits(**changes)


@pytest.mark.parametrize(
    "options",
    [
        dict(overlap_days=40),
        dict(overlap_days=True),
        dict(limits=Limits(max_days=1)),
        dict(limits=Limits(max_partitions=1)),
        dict(limits=Limits(max_requests=1)),
    ],
)
def test_ssv_ac12_oversized_plan_refuses_before_io(manifest, options):
    with pytest.raises(ValueError):
        plan(
            manifest,
            "2026-11-09",
            "2026-11-10",
            ["listed_trfs"],
            as_of=date(2026, 11, 13),
            **options,
        )


def test_ssv_ac12_overlap_cannot_silently_cross_retention_or_scope(manifest):
    for start, end, scopes in [
        ("2025-11-13", "2025-11-13", ["nyse_trf"]),
        ("2026-11-13", "2026-11-14", ["nyse_trf"]),
        ("2026-11-09", "2026-11-09", ["listed_trfs", "nyse_trf"]),
    ]:
        with pytest.raises(ValueError):
            plan(manifest, start, end, scopes, as_of=date(2026, 11, 13))
