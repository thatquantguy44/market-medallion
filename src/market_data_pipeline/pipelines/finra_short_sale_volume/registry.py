"""SSV-AC02 verified API profile and disjoint reporting scopes; no provider I/O.

Evidence: specs/spec012/evidence/t02_source_contract.json. Source descriptions
are not aliases: API reporting-facility identifiers differ from market codes.
"""

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

API_PROFILE = MappingProxyType(
    {
        "source": "finra",
        "dataset": "finra-short-sale-volume",
        "route": "finra_query_api",
        "provider_group": "otcMarket",
        "provider_dataset": "regShoDaily",
        "schema_id": "finra-reg-sho-api/1",
        "data_version": 1,
        "format": "json",
        "partition_field": "tradeReportDate",
        "date_format": "%Y-%m-%d",
        "history_months": 12,
    }
)
API_FIELDS = (
    ("tradeReportDate", "Date"),
    ("securitiesInformationProcessorSymbolIdentifier", "String"),
    ("shortParQuantity", "Number"),
    ("shortExemptParQuantity", "Number"),
    ("totalParQuantity", "Number"),
    ("marketCode", "String"),
    ("reportingFacilityCode", "String"),
)


@dataclass(frozen=True)
class Facility:
    id: str
    reporting_facility_code: str
    market_code: str


FACILITIES = (
    Facility("nasdaq_carteret_trf", "NQTRF", "Q"),
    Facility("nasdaq_chicago_trf", "NCTRF", "B"),
    Facility("nyse_trf", "NYTRF", "N"),
)
_SCOPES = {facility.id: (facility.id,) for facility in FACILITIES}
_SCOPES["listed_trfs"] = tuple(facility.id for facility in FACILITIES)


def select_scopes(value: object) -> tuple[Facility, ...]:
    """Expand reviewed scope IDs and reject overlap, unknown aliases and exclusions."""
    if not isinstance(value, list) or not value:
        raise ValueError("FINRA scopes must be a nonempty list of reviewed scope IDs")
    selected = set()
    for scope in value:
        if not isinstance(scope, str) or scope not in _SCOPES:
            raise ValueError("unknown or excluded FINRA scope")
        members = _SCOPES[scope]
        if selected.intersection(members):
            raise ValueError("overlapping FINRA scopes would double count reporting facilities")
        selected.update(members)
    return tuple(facility for facility in FACILITIES if facility.id in selected)


def validate_acquisition_window(start: str, end: str, *, as_of: date) -> tuple[date, date]:
    """Guard new acquisition against a declared twelve-calendar-month cutoff.

    This does not certify availability or trading days. Do not apply a current
    cutoff to archived local input/replay: their original scope and clocks persist.
    """
    if type(as_of) is not date or as_of.year < 2:
        raise ValueError("FINRA acquisition cutoff must be an explicit date")
    try:
        if not isinstance(start, str) or not isinstance(end, str):
            raise ValueError
        first, last = date.fromisoformat(start), date.fromisoformat(end)
        if first.isoformat() != start or last.isoformat() != end:
            raise ValueError
    except ValueError as exc:
        raise ValueError("FINRA acquisition bounds require explicit ISO YYYY-MM-DD dates") from exc
    year = as_of.year - 1
    earliest = date(year, as_of.month, min(as_of.day, monthrange(year, as_of.month)[1]))
    if not earliest <= first <= last <= as_of:
        raise ValueError(
            "FINRA acquisition bounds must be ordered within the rolling twelve months"
        )
    return first, last
