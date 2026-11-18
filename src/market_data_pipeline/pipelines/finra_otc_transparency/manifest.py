"""OTC-T01 admission boundary, independent of transport, storage and credentials.

Route-specific review: specs/spec013/source_review.md (2026-11-15).
https://developer.finra.org/specific-terms-equity-data
https://developer.finra.org/terms-and-conditions
https://www.finra.org/terms-of-use
"""

from dataclasses import dataclass
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import (
    CATALOG_VERSION,
    PROFILES,
    ReportProfile,
    profile_for,
)
from market_data_pipeline.shared.yaml import UniqueLoader

_OPERATIONS = frozenset(
    {"fixture", "acquire", "local_import", "retain", "transform", "internal_export"}
)
_ROUTES = frozenset(
    {"invented_fixture", "approved_local_file", "finra_download_files", "finra_query_api"}
)
_FIELDS = frozenset(
    {
        "dataset",
        "purpose",
        "route",
        "authorization",
        "permission_reference",
        "credential_type",
        "provider_dataset",
        "attribution",
        "operations",
    }
)


def require_approved_use(value: object, *, operation: str) -> None:
    """Require explicit OTC use evidence before a future caller's I/O.

    An operator reference records admission; this does not authenticate credentials,
    accept agreements, independently verify a license, or establish conformance.
    No active provider profile is shipped. Public redistribution/model/commercial
    uses are outside this implementation even where terms offer conditional rights.
    """
    if not isinstance(operation, str) or operation not in _OPERATIONS:
        raise ValueError("unsupported OTC operation")
    if value is None:
        raise PermissionError("OTC approved-use profile required before provider I/O")
    if not isinstance(value, dict) or set(value) != _FIELDS:
        raise ValueError("OTC approved-use fields must match the reviewed contract")
    if value["dataset"] != "finra-otc-transparency":
        raise ValueError("OTC profile cannot admit another dataset")
    if value["purpose"] != "personal_noncommercial_internal_research":
        raise ValueError("OTC purpose must be personal noncommercial internal research")
    route = value["route"]
    if not isinstance(route, str) or route not in _ROUTES:
        raise ValueError("unsupported OTC route")
    authorization = value["authorization"]
    if not isinstance(authorization, str) or authorization not in (
        "pending",
        "provider_permission",
        "accepted_api_terms",
    ):
        raise ValueError("unsupported OTC authorization")
    reference = value["permission_reference"]
    if reference is not None and (
        not isinstance(reference, str)
        or not reference.strip()
        or len(reference) > 256
        or any(ord(c) < 32 or ord(c) == 127 for c in reference)
    ):
        raise ValueError("OTC permission_reference must be a nonempty nonsecret reference")
    operations = value["operations"]
    if (
        not isinstance(operations, list)
        or any(not isinstance(item, str) or item not in _OPERATIONS for item in operations)
        or len(operations) != len(set(operations))
    ):
        raise ValueError("OTC operations must be a unique list of supported operations")
    if operation not in operations:
        raise PermissionError("OTC operation is not explicitly admitted")
    if route == "invented_fixture":
        if (
            operations != ["fixture"]
            or authorization != "pending"
            or any(
                value[key] is not None
                for key in (
                    "permission_reference",
                    "credential_type",
                    "provider_dataset",
                    "attribution",
                )
            )
        ):
            raise PermissionError("invented OTC fixtures cannot authorize provider use")
        return
    # Real-data rights do not turn an actual response into invented fixture evidence.
    if "fixture" in operations:
        raise PermissionError("provider data cannot be classified as an invented OTC fixture")
    if value["attribution"] != "FINRA":
        raise PermissionError("real OTC operations require FINRA attribution")
    if route == "finra_query_api":
        if (
            authorization != "accepted_api_terms"
            or not reference
            or value["credential_type"] != "public"
            or value["provider_dataset"] != "weeklySummary"
        ):
            raise PermissionError(
                "OTC API use requires accepted terms, Public and weeklySummary scope"
            )
    elif (
        authorization != "provider_permission"
        or not reference
        or value["credential_type"] is not None
        or value["provider_dataset"] is not None
    ):
        raise PermissionError("OTC file use requires independent recorded provider permission")
    if route == "approved_local_file" and "acquire" in operations:
        raise PermissionError("OTC local-file permission cannot admit network acquisition")


# A source manifest describes available contracts; it never grants approved use.
@dataclass(frozen=True)
class Manifest:
    profiles: tuple[ReportProfile, ...]


def manifest_from_dict(value: object) -> Manifest:
    if not isinstance(value, dict) or set(value) != {
        "source",
        "dataset",
        "catalog_version",
        "profiles",
    }:
        raise ValueError("OTC manifest requires exactly the reviewed catalog fields")
    if (
        value["source"] != "finra"
        or value["dataset"] != "finra-otc-transparency"
        or value["catalog_version"] != CATALOG_VERSION
    ):
        raise ValueError("OTC manifest identity/version differs from the reviewed catalog")
    rows = value["profiles"]
    if not isinstance(rows, list) or len(rows) != len(PROFILES):
        raise ValueError("OTC manifest must declare all six weekly product/tier profiles")
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid OTC manifest profile")
        profile = profile_for(row.get("product"), row.get("tier"))
        expected = profile.to_dict()
        if (
            set(row) != set(expected)
            or any(type(row[key]) is not type(expected[key]) for key in expected)
            or row != expected
            or profile.scope_id in seen
        ):
            raise ValueError("OTC manifest profile differs from the reviewed catalog")
        seen.add(profile.scope_id)
    return Manifest(PROFILES)


def load_manifest(path: Path) -> Manifest:
    try:
        return manifest_from_dict(yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueLoader))
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ValueError("invalid OTC manifest YAML") from exc
