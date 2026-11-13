"""Strict validation of pipeline-produced PRES ResearchDataset schemaVersion 1."""

import json
import math
import re
from datetime import date

MAX_DATASET_BYTES = 5 * 1024 * 1024
MAX_RECORDS = 100_000
PARTIES = {"D", "R", "OTHER", "TIED", "UNKNOWN"}


def _object(value, required, optional=()):
    if (
        not isinstance(value, dict)
        or not set(required) <= set(value)
        or set(value) - set(required) - set(optional)
    ):
        raise ValueError("Invalid dataset fields.")
    return value


def _text(value, limit=1000):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError("Invalid dataset text.")
    return value


def _date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Invalid dataset date.")
    try:
        if date.fromisoformat(value).isoformat() != value:
            raise ValueError("Invalid dataset date.")
    except ValueError as exc:
        raise ValueError("Invalid dataset date.") from exc
    return value


def _number(value, *, positive=False, nullable=False):
    if nullable and value is None:
        return None
    try:
        valid = (
            not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)
        )
    except OverflowError:
        valid = False
    if not valid or (positive and value <= 0):
        raise ValueError("Invalid dataset number.")
    return value


def _array(value):
    if not isinstance(value, list) or len(value) > MAX_RECORDS:
        raise ValueError("Invalid or oversized dataset array.")
    return value


def _unique(values):
    if len(values) != len(set(values)):
        raise ValueError("Duplicate dataset identity.")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key.")
        result[key] = value
    return result


def parse_dataset_bytes(payload: bytes) -> dict:
    if not isinstance(payload, bytes) or len(payload) > MAX_DATASET_BYTES:
        raise ValueError("Dataset exceeds size limit.")

    def reject_constant(value):
        raise ValueError(f"Invalid numeric constant: {value}")

    try:
        data = json.loads(payload, object_pairs_hook=_pairs, parse_constant=reject_constant)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid dataset JSON.") from exc
    _object(
        data,
        [
            "schemaVersion",
            "id",
            "title",
            "kind",
            "currency",
            "nominalReal",
            "returnBasis",
            "sources",
            "levels",
            "controls",
            "windows",
            "fundamentals",
        ],
    )
    if (
        type(data["schemaVersion"]) is not int
        or data["schemaVersion"] != 1
        or data["kind"] != "USER_SUPPLIED"
        or data["nominalReal"] != "NOMINAL"
        or data["returnBasis"] not in ("TOTAL_RETURN", "PRICE_RETURN")
    ):
        raise ValueError("Unsupported dataset version or provenance.")
    _text(data["id"], 200)
    _text(data["title"])
    if not isinstance(data["currency"], str) or not re.fullmatch(r"[A-Z]{3}", data["currency"]):
        raise ValueError("Invalid dataset currency.")
    sources = _array(data["sources"])
    if not sources:
        raise ValueError("Sources are required.")
    ids = []
    for source in sources:
        _object(source, ["id", "title", "locator"], ["note"])
        ids.append(_text(source["id"], 200))
        _text(source["title"])
        if source["locator"] is not None:
            _text(source["locator"], 4000)
        if "note" in source:
            _text(source["note"], 10_000)
    _unique(ids)
    source_ids = set(ids)

    def source_id(value):
        if _text(value, 200) not in source_ids:
            raise ValueError("Unknown source ID.")

    dates = []
    for row in _array(data["levels"]):
        _object(row, ["date", "value", "sourceId"])
        dates.append(_date(row["date"]))
        _number(row["value"], positive=True)
        source_id(row["sourceId"])
    _unique(dates)
    intervals, identifiers = [], []
    for row in _array(data["controls"]):
        _object(
            row,
            [
                "id",
                "startDate",
                "endDate",
                "presidentId",
                "presidentName",
                "presidentParty",
                "houseControl",
                "senateControl",
                "sourceId",
            ],
        )
        identifiers.append(_text(row["id"], 200))
        start, end = _date(row["startDate"]), _date(row["endDate"])
        if end <= start:
            raise ValueError("Invalid control interval.")
        intervals.append((start, end))
        _text(row["presidentId"], 200)
        _text(row["presidentName"])
        if any(
            not isinstance(row[key], str) or row[key] not in PARTIES
            for key in ("presidentParty", "houseControl", "senateControl")
        ):
            raise ValueError("Invalid political label.")
        source_id(row["sourceId"])
    _unique(identifiers)
    intervals.sort()
    if any(
        next_start < prior_end
        for (_, prior_end), (next_start, _) in zip(intervals, intervals[1:], strict=False)
    ):
        raise ValueError("Overlapping control intervals.")
    identifiers, windows = [], []
    for row in _array(data["windows"]):
        _object(row, ["id", "label", "kind", "startDate", "endDate", "decisionDate", "complete"])
        identifiers.append(_text(row["id"], 200))
        _text(row["label"])
        start, end, decision = (
            _date(row["startDate"]),
            _date(row["endDate"]),
            _date(row["decisionDate"]),
        )
        if (
            row["kind"] not in ("CALENDAR_YEAR", "PRESIDENTIAL_PERIOD")
            or type(row["complete"]) is not bool
            or end <= start
            or decision > start
        ):
            raise ValueError("Invalid window.")
        if row["kind"] == "CALENDAR_YEAR" and (
            not start.endswith("-12-31")
            or int(end[:4]) != int(start[:4]) + 1
            or (row["complete"] and not end.endswith("-12-31"))
        ):
            raise ValueError("Invalid calendar-year window.")
        windows.append((row["kind"], start, end))
    _unique(identifiers)
    _unique(windows)
    identities, definitions = [], set()
    for row in _array(data["fundamentals"]):
        _object(
            row,
            [
                "date",
                "availableDate",
                "valuation",
                "valuationDefinition",
                "policyRatePct",
                "sourceId",
            ],
        )
        observed = _date(row["date"])
        available = None if row["availableDate"] is None else _date(row["availableDate"])
        if available is not None and available < observed:
            raise ValueError("Fundamental availability precedes observation.")
        identities.append((observed, available))
        _number(row["valuation"], nullable=True)
        _number(row["policyRatePct"], nullable=True)
        definitions.add(_text(row["valuationDefinition"]))
        source_id(row["sourceId"])
    _unique(identities)
    if len(definitions) > 1:
        raise ValueError("Different valuation definitions cannot be pooled.")
    return data
