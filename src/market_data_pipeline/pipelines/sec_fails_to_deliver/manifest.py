"""Strict, offline-only contract for modern SEC semi-monthly FTD files."""

import calendar
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

from market_data_pipeline.shared.yaml import UniqueLoader

SOURCE_URL = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"
SCHEMA_VERSION = "sec-ftd-six-column/2"
HEADER = ("SETTLEMENT DATE", "CUSIP", "SYMBOL", "QUANTITY (FAILS)", "DESCRIPTION", "PRICE")
ACCESS = {
    "method": "local-file-only",
    "source_url": SOURCE_URL,
    "terms_url": "https://www.sec.gov/about/privacy-information",
    "automation": "disabled",
    "conformance": "local-sample-reconciled",
}


def _date(value: object) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("partition dates require YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("invalid partition date") from exc


def _fields(value: object, expected: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{label} fields must be exactly {sorted(expected)}")
    return value


@dataclass(frozen=True)
class Manifest:
    period_start: date
    period_end: date
    member_name: str
    max_archive_bytes: int = 8 * 1024 * 1024
    max_expanded_bytes: int = 64 * 1024 * 1024
    max_rows: int = 500_000
    max_record_bytes: int = 4096

    def __post_init__(self) -> None:
        start, end = self.period_start, self.period_end
        if type(start) is not date or type(end) is not date:
            raise ValueError("partition bounds require date labels")
        if start < date(2009, 7, 1) or start.day not in (1, 15, 16):
            raise ValueError("partition must use the semi-monthly regime beginning July 2009")
        last = calendar.monthrange(start.year, start.month)[1]
        allowed_ends = (14, 15) if start.day == 1 else (last,)
        if (end.year, end.month) != (start.year, start.month) or end.day not in allowed_ends:
            raise ValueError("partition requires explicit supported semi-monthly bounds")
        if (
            not isinstance(self.member_name, str)
            or len(self.member_name) > 128
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*\.txt", self.member_name)
            or ".." in self.member_name
        ):
            raise ValueError("member_name must be one explicit safe .txt basename")
        for value, maximum in (
            (self.max_archive_bytes, 16 * 1024 * 1024),
            (self.max_expanded_bytes, 128 * 1024 * 1024),
            (self.max_rows, 2_000_000),
            (self.max_record_bytes, 65536),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("resource limit must be a positive integer within the hard cap")


def manifest_from_dict(value: object) -> Manifest:
    value = _fields(
        value,
        {
            "source",
            "dataset",
            "schema_version",
            "reporting_regime",
            "format",
            "encoding",
            "header",
            "partition",
            "member_name",
            "limits",
            "access",
        },
        "manifest",
    )
    expected = {
        "source": "sec",
        "dataset": "sec-fails-to-deliver",
        "schema_version": SCHEMA_VERSION,
        "reporting_regime": "semi-monthly-2009+",
        "format": "zip-pipe",
        "encoding": "ascii",
        "header": list(HEADER),
        "access": ACCESS,
    }
    if any(value[key] != item for key, item in expected.items()):
        raise ValueError("only the offline SEC FTD documentary profile is supported")
    partition = _fields(value["partition"], {"start", "end"}, "partition")
    limits = _fields(
        value["limits"], {"archive_bytes", "expanded_bytes", "rows", "record_bytes"}, "limits"
    )
    return Manifest(
        period_start=_date(partition["start"]),
        period_end=_date(partition["end"]),
        member_name=value["member_name"],
        max_archive_bytes=limits["archive_bytes"],
        max_expanded_bytes=limits["expanded_bytes"],
        max_rows=limits["rows"],
        max_record_bytes=limits["record_bytes"],
    )


def parse_manifest(payload: bytes) -> Manifest:
    if not isinstance(payload, bytes) or len(payload) > 65536:
        raise ValueError("manifest must be UTF-8 YAML within 64 KiB")
    try:
        return manifest_from_dict(yaml.load(payload.decode("utf-8"), Loader=UniqueLoader))
    except (yaml.YAMLError, TypeError, UnicodeError) as exc:
        raise ValueError("invalid YAML manifest") from exc


def load_manifest(path: Path) -> Manifest:
    with Path(path).open("rb") as stream:
        return parse_manifest(stream.read(65537))
