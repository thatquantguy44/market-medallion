"""SSV-AC13: bounded calendar evidence; API deadlines require a separate reference."""

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import yaml

from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import _read
from market_data_pipeline.shared.yaml import UniqueLoader

CONTRACT = "finra-ssv-operations-policy/1"
CALENDAR = "https://www.finra.org/filing-reporting/market-transparency-reporting/holiday-calendar"


def default_policy():
    """2026 FINRA calendar reviewed 2026-11-14; no API release SLA is inferred."""
    return dict(
        schema_id=CONTRACT,
        calendar_start="2026-01-01",
        calendar_end="2026-12-31",
        calendar_reference=CALENDAR,
        closed_dates=[
            "2026-01-01",
            "2026-01-19",
            "2026-02-16",
            "2026-04-03",
            "2026-05-25",
            "2026-06-19",
            "2026-07-03",
            "2026-09-07",
            "2026-11-26",
            "2026-12-25",
        ],
        api_release_time=None,
        api_timing_reference=None,
        max_capture_age_hours=24,
        retention="PRESERVE_ALL",
    )


def day(value):
    if not isinstance(value, str):
        raise ValueError("FINRA health requires ISO dates")
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError("FINRA health requires canonical ISO dates")
    return result


def validate_policy(value):
    try:
        if not isinstance(value, dict) or set(value) != set(default_policy()):
            raise ValueError
        if value["schema_id"] != CONTRACT or value["retention"] != "PRESERVE_ALL":
            raise ValueError
        first, last = day(value["calendar_start"]), day(value["calendar_end"])
        if not 0 <= (last - first).days <= 3660:
            raise ValueError
        reference = value["calendar_reference"]
        if not isinstance(reference, str) or not reference.strip() or len(reference) > 1024:
            raise ValueError
        closed = value["closed_dates"]
        if not isinstance(closed, list) or len(closed) > 1000 or len(set(closed)) != len(closed):
            raise ValueError
        if any(not first <= day(d) <= last for d in closed):
            raise ValueError
        hours = value["max_capture_age_hours"]
        if type(hours) is not int or not 1 <= hours <= 87600:
            raise ValueError
        deadline, reference = value["api_release_time"], value["api_timing_reference"]
        if deadline is None:
            if reference is not None:
                raise ValueError
        elif (
            not isinstance(deadline, str)
            or time.fromisoformat(deadline).strftime("%H:%M") != deadline
            or not isinstance(reference, str)
            or not reference.strip()
            or len(reference) > 1024
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ValueError("invalid FINRA operations policy") from None
    return dict(value, closed_dates=list(closed))


def load_policy(path):
    try:
        return validate_policy(yaml.load(_read(path, 65536), Loader=UniqueLoader))
    except (yaml.YAMLError, RecursionError, UnicodeError):
        raise ValueError("invalid FINRA operations policy") from None


def release_status(trade_date, now, policy):
    """Calendar closure is independent of transport, emptiness and file-site timing."""
    selected = day(trade_date)
    if not day(policy["calendar_start"]) <= selected <= day(policy["calendar_end"]):
        return dict(status="UNKNOWN_CALENDAR", expected_at=None)
    if selected.weekday() >= 5 or trade_date in policy["closed_dates"]:
        return dict(status="CLOSED", expected_at=None)
    if policy["api_release_time"] is None:
        return dict(status="UNKNOWN_API_TIMING", expected_at=None)
    expected = datetime.combine(
        selected, time.fromisoformat(policy["api_release_time"]), ZoneInfo("America/New_York")
    )
    return dict(
        status="PENDING" if now < expected else "OVERDUE",
        expected_at=expected.isoformat(),
    )
