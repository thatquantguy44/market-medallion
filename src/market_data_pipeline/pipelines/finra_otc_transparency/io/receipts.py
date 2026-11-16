"""OTC-AC05 canonical receipt construction; no filesystem or network access."""

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from urllib.parse import urlsplit
from uuid import UUID

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import (
    CATALOG_VERSION,
    Partition,
    iso_date,
    profile_for,
)
from market_data_pipeline.pipelines.finra_otc_transparency.data.identity import IDENTITY_VERSION
from market_data_pipeline.pipelines.finra_otc_transparency.data.weekly import (
    ATS_FIXTURE_SCHEMA,
    ATS_HEADER,
)
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import (
    Manifest,
    manifest_from_dict,
)
from market_data_pipeline.shared.files import utc_timestamp

CONTRACT = "finra-otc-bronze/1"
OUTCOME_VERSION = "finra-otc-parse-outcome/1"
MAX_BYTES = 64 * 1024 * 1024
DEFAULT_BYTES = 20 * 1024 * 1024
MAX_PARTS = 64
MAX_METADATA = 65536
MAX_SEQUENCES = 100000
_HEADERS = frozenset(
    {"content-type", "content-length", "last-modified", "etag", "date", "retry-after"}
)
_ROUTES = frozenset(
    {"invented_fixture", "approved_local_file", "finra_download_files", "finra_query_api"}
)


def encoded(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ValueError("invalid OTC archive JSON value") from None


def digest(value: object) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def identity(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("OTC lineage requires canonical UUID text")
    try:
        if str(UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise ValueError("OTC lineage requires canonical UUID text") from None
    return value


def clock(value: object) -> str:
    if not isinstance(value, datetime):
        raise ValueError("OTC capture clock requires an aware datetime")
    return utc_timestamp(value)


def clock_text(value: object) -> str:
    try:
        if (
            not isinstance(value, str)
            or clock(datetime.fromisoformat(value.replace("Z", "+00:00"))) != value
        ):
            raise ValueError
    except (ValueError, TypeError):
        raise ValueError("invalid canonical OTC capture clock") from None
    return value


def part_id(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
        raise ValueError("invalid OTC part identity")
    return value


def limits(max_bytes: object, max_rows: object) -> dict:
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_BYTES:
        raise ValueError("OTC archive byte limit is outside its finite local ceiling")
    if type(max_rows) is not int or not 1 <= max_rows <= 1000000:
        raise ValueError("OTC archive row limit is outside its finite local ceiling")
    return {"max_bytes": max_bytes, "max_rows": max_rows}


def _url(value):
    if value is None:
        return None
    try:
        if not isinstance(value, str) or len(value) > 2048 or any(ord(c) <= 32 for c in value):
            raise ValueError
        parts = urlsplit(value)
        if (
            parts.scheme != "https"
            or parts.hostname
            not in {
                "www.finra.org",
                "api.finra.org",
                "otctransparency.finra.org",
                "developer.finra.org",
            }
            or parts.username is not None
            or parts.password is not None
            or parts.port is not None
            or parts.query
            or parts.fragment
            or "?" in value
            or "#" in value
            or not parts.path.startswith("/")
        ):
            raise ValueError
    except ValueError:
        raise ValueError("OTC receipt requires a public credential-free FINRA HTTPS URL") from None
    return value


def _headers(value):
    if value is None:
        return {}
    if not isinstance(value, Mapping) or len(value) > 128:
        raise ValueError("invalid bounded OTC response headers")
    safe = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("invalid OTC header identity")
        key = key.lower()
        if key not in _HEADERS:
            continue
        if (
            key in safe
            or not isinstance(item, str)
            or len(item) > 1024
            or any(ord(c) < 32 or ord(c) == 127 for c in item)
        ):
            raise ValueError("invalid or duplicate public OTC response header")
        safe[key] = item
    return safe


@dataclass(frozen=True)
class Part:
    part_id: str
    payload: bytes
    source_url: str | None = None
    http_status: int | None = None
    retrieved_at: datetime | None = None
    response_headers: Mapping[str, str] | None = None
    body_complete: bool = True


def freeze_part(part: Part) -> Part:
    if not isinstance(part, Part):
        raise ValueError("OTC received parts require typed byte envelopes")
    return replace(part, response_headers=_headers(part.response_headers))


def part_record(part: Part, captured_at: str) -> dict:
    if not isinstance(part, Part) or not isinstance(part.payload, bytes):
        raise ValueError("OTC raw parts require exact bytes")
    if type(part.body_complete) is not bool:
        raise ValueError("OTC body completeness requires a boolean")
    if part.http_status is not None and (
        type(part.http_status) is not int or not 100 <= part.http_status <= 599
    ):
        raise ValueError("invalid OTC HTTP status")
    retrieved = clock(part.retrieved_at) if part.retrieved_at is not None else None
    if retrieved is not None and retrieved > captured_at:
        raise ValueError("OTC retrieval follows the capture clock")
    return {
        "part_id": part_id(part.part_id),
        "sha256": hashlib.sha256(part.payload).hexdigest(),
        "byte_count": len(part.payload),
        "source_url": _url(part.source_url),
        "http_status": part.http_status,
        "retrieved_at": retrieved,
        "headers": _headers(part.response_headers),
        "body_complete": part.body_complete,
    }


def manifest_snapshot(manifest: Manifest) -> dict:
    if not isinstance(manifest, Manifest):
        raise ValueError("OTC archive requires the reviewed manifest")
    value = {
        "source": "finra",
        "dataset": "finra-otc-transparency",
        "catalog_version": CATALOG_VERSION,
        "profiles": [p.to_dict() for p in manifest.profiles],
    }
    if manifest_from_dict(value) != manifest:
        raise ValueError("OTC archive manifest differs from the reviewed catalog")
    return value


def schema_snapshot(partition: Partition, manifest: dict) -> dict:
    """Verify stored descriptor types without admitting a historical parser."""
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"source", "dataset", "catalog_version", "profiles"}
        or manifest["source"] != "finra"
        or manifest["dataset"] != "finra-otc-transparency"
        or not isinstance(manifest["catalog_version"], str)
        or not re.fullmatch(r"finra-otc-weekly-catalog/[1-9][0-9]*", manifest["catalog_version"])
        or not isinstance(manifest["profiles"], list)
        or len(manifest["profiles"]) != 6
    ):
        raise ValueError("invalid OTC manifest snapshot")
    selected, seen = None, set()
    for row in manifest["profiles"]:
        if not isinstance(row, dict):
            raise ValueError("invalid OTC profile snapshot")
        expected = profile_for(row.get("product"), row.get("tier")).to_dict()
        if (
            set(row) != set(expected)
            or any(type(row[key]) is not type(expected[key]) for key in expected)
            or any(
                not value or len(value) > 1024 or any(ord(c) < 32 for c in value)
                for value in row.values()
                if isinstance(value, str)
            )
            or row["scope_id"] != expected["scope_id"]
            or not 1 <= row["expected_delay_weeks"] <= 52
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./:-]{0,255}", row["schema_profile"])
            or row["scope_id"] in seen
        ):
            raise ValueError("invalid typed OTC profile snapshot")
        iso_date(row["reviewed_on"], field="source_review_date")
        seen.add(row["scope_id"])
        if (row["product"], row["tier"]) == (partition.product, partition.tier):
            selected = row
    if selected is None:
        raise ValueError("OTC manifest snapshot does not identify the selected scope")
    known = selected["schema_profile"] == ATS_FIXTURE_SCHEMA
    return {
        "schema_profile": selected["schema_profile"],
        "profile": selected,
        "columns": list(ATS_HEADER) if known else None,
        "encoding": "utf-8" if known else None,
        "delimiter": "|" if known else None,
        "identity_version": IDENTITY_VERSION if known else None,
    }


def make_receipt(
    *,
    partition,
    manifest,
    parts,
    discovery,
    expected_parts,
    run_id,
    capture_id,
    sequence,
    captured_at,
    admission,
    capture_limits,
    schema=None,
):
    if not isinstance(partition, Partition):
        raise ValueError("OTC archive requires an explicit validated partition")
    capture_limits = limits(capture_limits["max_bytes"], capture_limits["max_rows"])
    if not isinstance(expected_parts, (list, tuple)) or not 1 <= len(expected_parts) <= MAX_PARTS:
        raise ValueError("OTC expected parts must be an explicit bounded nonempty ledger")
    expected = [part_id(p) for p in expected_parts]
    if len(set(expected)) != len(expected):
        raise ValueError("duplicate OTC expected part")
    if not isinstance(parts, Sequence) or isinstance(parts, (str, bytes)) or len(parts) > MAX_PARTS:
        raise ValueError("OTC received parts exceed their finite ledger")
    captured_at = clock_text(captured_at)
    records = sorted((part_record(p, captured_at) for p in parts), key=lambda p: p["part_id"])
    observed = [p["part_id"] for p in records]
    if len(set(observed)) != len(observed):
        raise ValueError("duplicate OTC received part")
    discovery_record = part_record(discovery, captured_at) if discovery is not None else None
    if (
        sum(p["byte_count"] for p in records)
        + (discovery_record["byte_count"] if discovery_record else 0)
        > capture_limits["max_bytes"]
    ):
        raise ValueError("OTC raw bytes exceed the capture budget")
    if type(sequence) is not int or not 1 <= sequence <= MAX_SEQUENCES:
        raise ValueError("invalid OTC capture sequence")
    if (
        not isinstance(admission, dict)
        or set(admission) != {"route", "operation", "profile_sha256"}
        or not isinstance(admission["route"], str)
        or admission["route"] not in _ROUTES
        or admission["operation"]
        != ("fixture" if admission["route"] == "invented_fixture" else "retain")
        or not isinstance(admission["profile_sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", admission["profile_sha256"])
    ):
        raise ValueError("invalid OTC admission receipt")
    declared_schema = schema_snapshot(partition, manifest)
    if schema is None:
        schema = declared_schema
    elif (
        not isinstance(schema, dict)
        or set(schema) != set(declared_schema)
        or schema.get("profile") != declared_schema["profile"]
        or schema.get("schema_profile") != declared_schema["schema_profile"]
    ):
        raise ValueError("OTC stored schema does not match the manifest scope")
    columns = schema["columns"]
    if (
        columns is not None
        and (
            not isinstance(columns, list)
            or not 1 <= len(columns) <= 64
            or any(not isinstance(c, str) or not c or len(c) > 256 for c in columns)
        )
    ) or any(
        schema[key] is not None
        and (not isinstance(schema[key], str) or not schema[key] or len(schema[key]) > 256)
        for key in ("encoding", "delimiter", "identity_version")
    ):
        raise ValueError("invalid typed OTC schema snapshot")
    missing = sorted(set(expected) - set(observed))
    unexpected = sorted(set(observed) - set(expected))
    truncated = [p["part_id"] for p in records if not p["body_complete"]]
    discovery_truncated = discovery_record is not None and not discovery_record["body_complete"]
    coverage = (
        "MISSING"
        if not records
        else "PARTIAL"
        if missing or unexpected or truncated or discovery_truncated
        else "RECEIVED_ALL"
    )
    value = {
        "format": CONTRACT,
        "source": "finra",
        "dataset": "finra-otc-transparency",
        "capture_id": identity(capture_id),
        "run_id": identity(run_id),
        "capture_sequence": sequence,
        "captured_at": captured_at,
        "provider_published_at": None,
        "partition": {
            "reporting_week_start": partition.reporting_week_start.isoformat(),
            "product": partition.product,
            "tier": partition.tier,
        },
        "manifest": manifest,
        "manifest_sha256": digest(manifest),
        "schema": schema,
        "schema_sha256": digest(schema),
        "admission": admission,
        "limits": capture_limits,
        "expected_parts": sorted(expected),
        "parts": records,
        "discovery": discovery_record,
        "missing_parts": missing,
        "unexpected_parts": unexpected,
        "truncated_parts": sorted(truncated),
        "coverage": coverage,
        "membership": "UNVERIFIED",
    }
    value["receipt_sha256"] = digest(value)
    if len(encoded(value)) > MAX_METADATA:
        raise ValueError("OTC receipt exceeds its metadata budget")
    return value
