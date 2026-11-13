"""Recent refresh windows use explicit aware clocks and New York calendar years."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import (
    OperationSettings,
    recent_refresh_years,
)


@pytest.mark.parametrize(
    "at,floor,overlap,expected",
    [
        (datetime(2027, 1, 1, 0, 30, tzinfo=UTC), 1990, 1, (2025, 2026)),
        (datetime(2027, 1, 1, 5, tzinfo=UTC), 1990, 1, (2026, 2027)),
        (datetime(2026, 7, 1, tzinfo=UTC), 2026, 1, (2026,)),
        (datetime(2026, 7, 1, tzinfo=UTC), 1990, 0, (2026,)),
        (datetime(2026, 7, 1, tzinfo=UTC), 1990, 4, (2022, 2023, 2024, 2025, 2026)),
    ],
)
def test_recent_window_respects_new_york_year_floor_and_overlap(at, floor, overlap, expected):
    settings = replace(OperationSettings(), max_years=max(2, overlap + 1), overlap_years=overlap)
    assert recent_refresh_years(floor, settings, at) == expected


@pytest.mark.parametrize("at", [datetime(2026, 1, 1), "2026-01-01", None])
def test_recent_window_requires_an_explicit_timezone_aware_datetime(at):
    with pytest.raises(ValueError, match="aware"):
        recent_refresh_years(1990, OperationSettings(), at)


def test_recent_window_refuses_future_operating_floor():
    with pytest.raises(ValueError):
        recent_refresh_years(2028, OperationSettings(), datetime(2026, 7, 1, tzinfo=UTC))


@pytest.mark.parametrize("floor", [None, "1990", True])
def test_recent_window_rejects_invalid_floor(floor):
    with pytest.raises(ValueError):
        recent_refresh_years(floor, OperationSettings(), datetime(2026, 7, 1, tzinfo=UTC))
