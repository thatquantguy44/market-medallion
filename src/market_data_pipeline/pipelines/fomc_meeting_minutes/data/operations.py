"""FOMC-AC06: pure operation settings, year selection and bounded retry policy."""

import math
from dataclasses import asdict, dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import validate_start_year
from market_data_pipeline.shared.yaml import UniqueLoader


@dataclass(frozen=True)
class OperationSettings:
    max_years: int = 2
    max_inventory_pages: int = 40
    max_documents: int = 40
    max_assets: int = 100
    max_requests: int = 100
    max_total_bytes: int = 100000000
    max_elapsed_seconds: int = 600
    max_attempts: int = 3
    request_interval_seconds: float = 1.0
    max_retry_delay_seconds: float = 30.0
    inventory_max_bytes: int = 2000000
    document_max_bytes: int = 10000000
    asset_max_bytes: int = 5000000
    max_capture_history: int = 5000
    overlap_years: int = 1
    max_run_age_hours: int = 168
    scheduled_live_access: bool = False

    def __post_init__(self):
        ranges = {
            "max_years": (1, 5),
            "max_inventory_pages": (3, 128),
            "max_documents": (1, 1000),
            "max_assets": (0, 1000),
            "max_requests": (3, 1000),
            "max_total_bytes": (2, 1000000000),
            "max_elapsed_seconds": (1, 3600),
            "max_attempts": (1, 5),
            "inventory_max_bytes": (1, 50000000),
            "document_max_bytes": (1, 50000000),
            "asset_max_bytes": (1, 50000000),
            "max_capture_history": (1, 100000),
            "overlap_years": (0, 4),
            "max_run_age_hours": (1, 8760),
        }
        for name, (low, high) in ranges.items():
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError("FOMC operation integer setting is outside its finite bounds")
        for name in ["request_interval_seconds", "max_retry_delay_seconds"]:
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 60:
                raise ValueError("FOMC operation delay must be finite in [0,60]")
        if type(self.scheduled_live_access) is not bool or self.overlap_years >= self.max_years:
            raise ValueError("FOMC scheduled access/overlap settings are invalid")

    def to_dict(self):
        return asdict(self)


def load_operations(path):
    try:
        value = yaml.load(Path(path).read_text(), Loader=UniqueLoader)
    except (yaml.YAMLError, UnicodeError):
        raise ValueError("invalid FOMC operation YAML") from None
    if not isinstance(value, dict) or set(value) != set(OperationSettings.__dataclass_fields__):
        raise ValueError("FOMC operation config requires exactly the documented fields")
    return OperationSettings(**value)


def selected_years(start, end, floor, settings):
    validate_start_year(floor)
    if (
        type(start) is not int
        or type(end) is not int
        or not floor <= start <= end <= 9999
        or end - start + 1 > settings.max_years
    ):
        raise ValueError("FOMC operation requires an explicit bounded year range in scope")
    return tuple(range(start, end + 1))


def recent_refresh_years(floor, settings, at):
    """Resolve a bounded overlap window from an explicit New York calendar clock."""
    if not isinstance(at, datetime) or at.utcoffset() is None:
        raise ValueError("FOMC recent refresh requires an aware datetime")
    validate_start_year(floor)
    year = at.astimezone(ZoneInfo("America/New_York")).year
    return selected_years(max(floor, year - settings.overlap_years), year, floor, settings)


def select_documents(inventory, years, settings):
    meetings = {
        m.meeting_id
        for m in inventory.meetings
        if m.committee_body == "fomc" and any(d.year in years for d in m.meeting_dates)
    }
    ids = {e.document_id for e in inventory.entries if e.meeting_id in meetings and e.document_id}
    documents = tuple(d for d in inventory.documents if d.document_id in ids)
    if len(documents) > settings.max_documents:
        raise ValueError("FOMC selected document limit exceeded")
    return documents


def retry_delay(receipt, attempt, now, settings):
    """RFC 9110 10.2.3: honor valid seconds/date or stop, never cap it down."""
    status = receipt.get("http_status")
    if attempt >= settings.max_attempts or status not in {None, 429, 500, 502, 503, 504}:
        return None
    value = receipt.get("headers", {}).get("retry-after", "")
    delay = None
    if value:
        try:
            if value.isdigit():
                delay = float(value)
            else:
                delay = max(0, (parsedate_to_datetime(value) - now).total_seconds())
        except (ValueError, TypeError, OverflowError):
            pass
    if delay is None:
        delay = float(2 ** (attempt - 1))
    if not math.isfinite(delay) or delay > settings.max_retry_delay_seconds:
        return None
    return max(delay, settings.request_interval_seconds)
