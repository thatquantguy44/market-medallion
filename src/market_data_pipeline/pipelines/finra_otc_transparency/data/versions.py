"""OTC-AC07 semantic content, exact capture tokens and immutable occurrences."""

import re

from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest

VERSION = "finra-otc-observation/1"
TRANSFORM = "finra-otc-membership/2"
CONTENT_FIELDS = frozenset(
    {
        "reporting_week_start",
        "product",
        "tier",
        "scope_id",
        "schema_profile",
        "origin",
        "symbol_raw",
        "issue_name_raw",
        "tier_raw",
        "participant_key",
        "participant_name_raw",
        "mpid",
        "firm_crd",
        "identity_basis",
        "identifier_kind",
        "weekly_shares",
        "weekly_trades",
        "provider_update_date",
        "provider_initial_publication_date",
        "provider_published_at",
    }
)
TOKEN_FIELDS = ("weekly_shares_text", "weekly_trades_text")
LINEAGE_FIELDS = (
    "capture_id",
    "run_id",
    "capture_sequence",
    "captured_at",
    "receipt_sha256",
    "manifest_sha256",
    "schema_sha256",
)


def tokens(row: dict) -> dict:
    if not isinstance(row, dict) or not set(TOKEN_FIELDS).issubset(row):
        raise ValueError("OTC capture token fields are missing")
    value = {field: row[field] for field in TOKEN_FIELDS}
    for field in TOKEN_FIELDS:
        raw = value[field]
        if not isinstance(raw, str) or not re.fullmatch(r"[0-9]{1,38}", raw):
            raise ValueError("OTC observation has invalid exact integer token")
        if str(int(raw)) != row[field.removesuffix("_text")]:
            raise ValueError("OTC normalized quantity differs from its captured token")
    return value


def content(row: dict) -> dict:
    if set(row) != CONTENT_FIELDS | set(TOKEN_FIELDS) | set(LINEAGE_FIELDS) | {"first_seen_at"}:
        raise ValueError("OTC version input differs from the normalized row contract")
    tokens(row)
    return {field: row[field] for field in sorted(CONTENT_FIELDS)}


def make_version(
    receipt: dict, key: tuple[str, str], value: dict | None, previous: dict | None
) -> dict | None:
    if value is None and previous is None:
        raise ValueError("OTC cannot remove an observation with no prior evidence")
    state = "OBSERVED" if value is not None else "REMOVED"
    fingerprint = digest({"state": state, "content": value})
    if previous is not None and previous["content_sha256"] == fingerprint:
        return None
    descriptor = {
        "format": VERSION,
        "partition": receipt["partition"],
        "symbol_raw": key[0],
        "participant_key": key[1],
        "state": state,
        "content": value,
        "content_sha256": fingerprint,
        "previous_version_id": previous["version_id"] if previous else None,
        "first_seen_at": receipt["captured_at"],
        **{field: receipt[field] for field in LINEAGE_FIELDS},
    }
    descriptor["version_id"] = VERSION + ":" + digest(descriptor)
    descriptor["version_sha256"] = digest(descriptor)
    return descriptor


def verify_version(value: dict) -> dict:
    expected = {
        "format",
        "partition",
        "symbol_raw",
        "participant_key",
        "state",
        "content",
        "content_sha256",
        "previous_version_id",
        "first_seen_at",
        "version_id",
        "version_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected | set(LINEAGE_FIELDS):
        raise ValueError("OTC immutable version fields differ from the contract")
    descriptor = dict(value)
    checksum = descriptor.pop("version_sha256", None)
    if checksum != digest(descriptor):
        raise ValueError("OTC immutable version seal mismatch")
    version_id = descriptor.pop("version_id", None)
    if version_id != VERSION + ":" + digest(descriptor):
        raise ValueError("OTC immutable version identity mismatch")
    state = value.get("state")
    content_value = value.get("content")
    if (
        value.get("format") != VERSION
        or state not in ("OBSERVED", "REMOVED")
        or value.get("first_seen_at") != value.get("captured_at")
        or value.get("content_sha256") != digest({"state": state, "content": content_value})
        or (
            state == "REMOVED"
            and (content_value is not None or value.get("previous_version_id") is None)
        )
        or (
            state == "OBSERVED"
            and (not isinstance(content_value, dict) or set(content_value) != CONTENT_FIELDS)
        )
    ):
        raise ValueError("OTC version has inconsistent content/state/knowledge evidence")
    if state == "OBSERVED":
        if (
            any(content_value.get(k) != v for k, v in value["partition"].items())
            or content_value["symbol_raw"] != value["symbol_raw"]
            or content_value["participant_key"] != value["participant_key"]
        ):
            raise ValueError("OTC version content grain mismatch")
    return value
