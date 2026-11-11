"""SSV-T03: parse one receipt-bound production API page without I/O or clocks.

This validates an already captured page, not permission to acquire/persist it.
Operational callers must enforce the approved-use gate separately. Completeness
is local to this one date/facility response, never an assertion about other pages.
"""

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import (
    MAX_QUANTITY_DIGITS,
    validate_quantities,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import (
    Manifest,
    validate_data_version,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.registry import API_FIELDS, Facility

_ENDPOINT = "https://api.finra.org/data/group/otcMarket/name/regShoDaily"
_FIELD_NAMES = frozenset(name for name, _ in API_FIELDS)
_JSON_TYPE = re.compile(r'application/json(?:\s*;\s*charset=(?:utf-8|"utf-8"))?', re.I)


@dataclass(frozen=True)
class ShortVolumeRow:
    trade_date: date
    facility_id: str
    symbol_raw: str
    reporting_facility_code: str
    market_raw: str
    short_volume: Decimal
    short_exempt_volume: Decimal
    total_volume: Decimal
    source_row: int


@dataclass(frozen=True)
class ParsedPage:
    rows: tuple[ShortVolumeRow, ...]
    trade_date: date
    facility_id: str
    offset: int
    limit: int
    record_total: int
    payload_sha256: str
    schema_id: str
    data_version: int
    http_status: int

    @property
    def partition_complete(self) -> bool:
        return self.offset == 0 and len(self.rows) == self.record_total


def _day(value: object) -> date:
    if not isinstance(value, str):
        raise ValueError("FINRA trade date requires ISO YYYY-MM-DD")
    try:
        result = date.fromisoformat(value)
        if result.isoformat() != value:
            raise ValueError
    except ValueError:
        raise ValueError("FINRA trade date requires ISO YYYY-MM-DD") from None
    return result


def _number(token: str) -> Decimal:
    if len(token) > MAX_QUANTITY_DIGITS + 64:
        raise ValueError("FINRA numeric token exceeds parser budget")
    try:
        return Decimal(token)
    except InvalidOperation:
        raise ValueError("invalid FINRA numeric token") from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate FINRA JSON field")
        result[key] = value
    return result


def _reject_constant(_token: str) -> None:
    raise ValueError("nonfinite FINRA JSON number")


def _request(
    receipt: Mapping, manifest: Manifest, max_rows: int
) -> tuple[date, Facility, int, int]:
    if (
        receipt.get("url") != _ENDPOINT
        or receipt.get("method") != "POST"
        or type(receipt.get("request_data_version")) is not int
        or receipt["request_data_version"] != manifest.data_version
    ):
        raise ValueError("FINRA receipt requires the reviewed production POST and data version")
    query = receipt.get("request_payload")
    required = {"limit", "offset", "compareFilters"}
    if (
        not isinstance(query, Mapping)
        or not required <= query.keys()
        or query.keys() - required - {"fields", "async"}
    ):
        raise ValueError("unsupported FINRA query shape")
    limit, offset = query["limit"], query["offset"]
    if type(limit) is not int or not 0 < limit <= max_rows:
        raise ValueError("FINRA query limit must be positive and within parser budget")
    if type(offset) is not int or offset < 0:
        raise ValueError("FINRA query offset must be a nonnegative integer")
    if "async" in query and query["async"] is not False:
        raise ValueError("FINRA parser requires a synchronous query")
    if "fields" in query:
        fields = query["fields"]
        if (
            not isinstance(fields, list)
            or any(not isinstance(item, str) for item in fields)
            or len(fields) != len(_FIELD_NAMES)
            or set(fields) != _FIELD_NAMES
        ):
            raise ValueError("FINRA projection must contain every reviewed field once")
    filters = query["compareFilters"]
    if not isinstance(filters, list) or len(filters) != 2:
        raise ValueError("FINRA page requires one trade-date and one market equality filter")
    scope = {}
    for item in filters:
        if (
            not isinstance(item, Mapping)
            or set(item) != {"compareType", "fieldName", "fieldValue"}
            or not isinstance(item["compareType"], str)
            or item["compareType"].upper() != "EQUAL"
            or not isinstance(item["fieldName"], str)
            or item["fieldName"] not in {"tradeReportDate", "marketCode"}
            or item["fieldName"] in scope
        ):
            raise ValueError("invalid or duplicated FINRA page scope filter")
        scope[item["fieldName"]] = item["fieldValue"]
    day = _day(scope["tradeReportDate"])
    facilities = [f for f in manifest.facilities if f.market_code == scope["marketCode"]]
    if len(facilities) != 1:
        raise ValueError("FINRA query market must identify one selected reporting facility")
    return day, facilities[0], offset, limit


def _headers(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError("FINRA receipt headers must be a mapping")
    result = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str) or key.lower() in result:
            raise ValueError("invalid or duplicate FINRA response header")
        result[key.lower()] = item
    validate_data_version(result)
    return result


def _header_int(headers: dict[str, str], name: str) -> int:
    value = headers.get(name)
    if value is None or len(value) > MAX_QUANTITY_DIGITS or re.fullmatch(r"[0-9]+", value) is None:
        raise ValueError("missing or invalid FINRA integer response header")
    return int(Decimal(value))


def _rows(
    value: list, manifest: Manifest, day: date, facility: Facility
) -> tuple[ShortVolumeRow, ...]:
    result, seen = [], set()
    for source_row, item in enumerate(value, start=1):
        if not isinstance(item, dict) or set(item) != _FIELD_NAMES:
            raise ValueError("FINRA row fields differ from the reviewed schema")
        if _day(item["tradeReportDate"]) != day:
            raise ValueError("FINRA row trade date differs from the query")
        if manifest.resolve_facility(item["reportingFacilityCode"], item["marketCode"]) != facility:
            raise ValueError("FINRA row reporting facility differs from the query")
        symbol = item["securitiesInformationProcessorSymbolIdentifier"]
        if (
            not isinstance(symbol, str)
            or not symbol
            or not symbol.isprintable()
            or symbol.strip() != symbol
        ):
            raise ValueError("FINRA symbol must be nonempty printable text without padding")
        short, exempt, total = validate_quantities(
            item["shortParQuantity"], item["shortExemptParQuantity"], item["totalParQuantity"]
        )
        if symbol in seen:
            raise ValueError("duplicate FINRA observation within a date/facility page")
        seen.add(symbol)
        result.append(
            ShortVolumeRow(
                day,
                facility.id,
                symbol,
                facility.reporting_facility_code,
                facility.market_code,
                short,
                exempt,
                total,
                source_row,
            )
        )
    return tuple(result)


def parse_page(
    payload: bytes,
    manifest: Manifest,
    receipt: Mapping,
    *,
    max_bytes: int = 20 * 1024 * 1024,
    max_rows: int = 5000,
) -> ParsedPage:
    """Validate receipt-bound UTF-8 JSON, preserving exact decimal share quantities.

    Extra receipt diagnostics are ignored. In particular, no capture timestamp
    is assigned or reinterpreted, and archived trade dates have no current cutoff.
    """
    if type(max_bytes) is not int or max_bytes <= 0 or type(max_rows) is not int or max_rows <= 0:
        raise ValueError("FINRA parser budgets must be positive integers")
    if not isinstance(payload, bytes) or len(payload) > max_bytes:
        raise ValueError("FINRA payload must be bytes within the parser budget")
    if not isinstance(receipt, Mapping):
        raise ValueError("FINRA receipt must be a mapping")
    digest = hashlib.sha256(payload).hexdigest()
    if (
        type(receipt.get("bytes")) is not int
        or receipt["bytes"] != len(payload)
        or receipt.get("sha256") != digest
    ):
        raise ValueError("FINRA payload length or SHA256 differs from the receipt")
    day, facility, offset, requested_limit = _request(receipt, manifest, max_rows)
    status = receipt.get("http_status")
    if type(status) is not int or status not in (200, 204):
        raise ValueError("FINRA parser requires HTTP 200 or 204")
    headers = _headers(receipt.get("headers"))
    total = _header_int(headers, "record-total")
    response_offset = _header_int(headers, "record-offset")
    limit = _header_int(headers, "record-limit")
    effective_limit = requested_limit
    if "record-max-limit" in headers:
        maximum = _header_int(headers, "record-max-limit")
        if maximum == 0:
            raise ValueError("FINRA platform maximum limit must be positive")
        effective_limit = min(requested_limit, maximum)
    if response_offset != offset or limit != effective_limit or offset > total:
        raise ValueError("FINRA response pagination differs from the query or total")
    if headers.get("content-encoding", "identity").lower() != "identity":
        raise ValueError("FINRA parser requires unencoded payload bytes")
    if "content-length" in headers and _header_int(headers, "content-length") != len(payload):
        raise ValueError("FINRA content length differs from the payload")
    content_type = headers.get("content-type")
    if (status == 200 or content_type is not None) and (
        content_type is None or _JSON_TYPE.fullmatch(content_type) is None
    ):
        raise ValueError("FINRA parser requires UTF-8 application/json")
    if status == 204:
        if payload or total != 0 or offset != 0:
            raise ValueError("FINRA HTTP 204 requires an empty payload and zero total/offset")
        rows = ()
    else:
        try:
            decoded = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_int=_number,
                parse_float=_number,
                parse_constant=_reject_constant,
            )
        except (UnicodeError, json.JSONDecodeError, RecursionError):
            raise ValueError("invalid FINRA UTF-8 JSON payload") from None
        if not isinstance(decoded, list) or len(decoded) > max_rows:
            raise ValueError("FINRA JSON must be an array within the row budget")
        rows = _rows(decoded, manifest, day, facility)
    if len(rows) > limit or offset + len(rows) > total or (not rows and offset < total):
        raise ValueError("FINRA page row count is inconsistent with pagination evidence")
    if "total-records-on-page" in headers and (
        _header_int(headers, "total-records-on-page") != len(rows)
    ):
        raise ValueError("FINRA page row count differs from the response header")
    return ParsedPage(
        rows,
        day,
        facility.id,
        offset,
        limit,
        total,
        digest,
        manifest.schema_id,
        manifest.data_version,
        status,
    )
