"""SSV-T01/T02 approved-use and source-contract validation, independent of provider I/O.

Evidence and current decisions: specs/spec012/source_review.md.
API access terms: https://developer.finra.org/specific-terms-equity-data
Website access terms: https://www.finra.org/terms-of-use
"""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.finra_short_sale_volume.registry import (
    API_FIELDS,
    API_PROFILE,
    Facility,
    select_scopes,
)
from market_data_pipeline.shared.yaml import UniqueLoader

_OPERATIONS = frozenset({"fixture", "local_import", "acquire", "transform", "internal_export"})
_ROUTES = frozenset(
    {"invented_fixture", "approved_local_file", "finra_daily_files", "finra_query_api"}
)
_FIELDS = frozenset(
    {"purpose", "route", "authorization", "permission_reference", "operations", "credential_type"}
)


def require_approved_use(value: object, *, operation: str) -> None:
    """Validate declared scope and require recorded admission before a caller's I/O.

    A permission reference is operator-supplied evidence, not independent license
    verification. An admitted API profile does not authenticate a credential or
    establish schema/partition conformance. Future adapters must do both.
    """
    if not isinstance(operation, str) or operation not in _OPERATIONS:
        raise ValueError("unsupported FINRA operation")
    if value is None:
        raise PermissionError("FINRA approved-use profile is required before provider I/O")
    if not isinstance(value, dict) or set(value) - _FIELDS:
        raise ValueError("unknown FINRA approved-use fields")
    if value.get("purpose") != "personal_noncommercial_internal_research":
        raise ValueError("FINRA purpose must be personal noncommercial internal research")
    route = value.get("route")
    if not isinstance(route, str) or route not in _ROUTES:
        raise ValueError("unsupported FINRA source route")
    authorization = value.get("authorization", "pending")
    if authorization not in ("pending", "provider_permission", "accepted_api_terms"):
        raise ValueError("unsupported FINRA authorization kind")
    reference = value.get("permission_reference")
    if reference is not None and (
        not isinstance(reference, str)
        or not reference.strip()
        or any(ord(char) < 32 for char in reference)
    ):
        raise ValueError("FINRA permission_reference must be a nonempty nonsecret reference")
    operations = value.get("operations", [])
    if (
        not isinstance(operations, list)
        or any(not isinstance(item, str) or item not in _OPERATIONS for item in operations)
        or len(operations) != len(set(operations))
    ):
        raise ValueError("FINRA operations must be a unique list of supported operations")
    credential_type = value.get("credential_type")
    if credential_type not in (None, "public"):
        raise ValueError("only FINRA Public credential metadata is in the selected scope")
    if operation not in operations:
        raise PermissionError("FINRA operation is not explicitly admitted")
    if route == "invented_fixture":
        if (
            operation != "fixture"
            or operations != ["fixture"]
            or authorization != "pending"
            or reference is not None
            or credential_type is not None
        ):
            raise PermissionError("invented FINRA fixtures cannot authorize provider use")
        return
    if operation == "fixture":
        raise PermissionError("provider data cannot be classified as an invented fixture")
    if route == "approved_local_file" and operation == "acquire":
        raise PermissionError("FINRA local-file permission does not admit network acquisition")
    if route == "finra_query_api":
        if authorization != "accepted_api_terms" or not reference or credential_type != "public":
            raise PermissionError("FINRA API use requires accepted terms and Public metadata")
    elif authorization != "provider_permission" or not reference or credential_type is not None:
        raise PermissionError("FINRA file use requires recorded provider permission")


@dataclass(frozen=True)
class Manifest:
    scopes: tuple[str, ...]
    facilities: tuple[Facility, ...]

    @property
    def data_version(self) -> int:
        return int(API_PROFILE["data_version"])

    @property
    def schema_id(self) -> str:
        return str(API_PROFILE["schema_id"])

    def to_dict(self) -> dict:
        return {**API_PROFILE, "fields": dict(API_FIELDS), "scopes": list(self.scopes)}

    def resolve_facility(self, reporting_facility_code: str, market_code: str) -> Facility:
        """Require an exact, selected provider identifier pair without aliases."""
        for facility in self.facilities:
            if (reporting_facility_code, market_code) == (
                facility.reporting_facility_code,
                facility.market_code,
            ):
                return facility
        raise ValueError("unknown, mismatched or unselected FINRA facility/market pair")


def manifest_from_dict(value: object) -> Manifest:
    """Load only the reviewed production API profile; this never grants use rights."""
    required = set(API_PROFILE) | {"fields", "scopes"}
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError(
            "FINRA manifest requires the reviewed profile fields; unknown fields rejected"
        )
    for key, expected in API_PROFILE.items():
        if type(value[key]) is not type(expected) or value[key] != expected:
            raise ValueError("FINRA manifest differs from the reviewed production API profile")
    if value["fields"] != dict(API_FIELDS):
        raise ValueError("FINRA manifest field names/types differ from the reviewed schema")
    facilities = select_scopes(value["scopes"])
    return Manifest(tuple(value["scopes"]), facilities)


def load_manifest(path: Path) -> Manifest:
    try:
        return manifest_from_dict(yaml.load(path.read_text(), Loader=UniqueLoader))
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ValueError("invalid FINRA manifest YAML") from exc


def validate_metadata(value: object) -> None:
    """Pin literal metadata names/types/partition; descriptions never define aliases."""
    if not isinstance(value, dict) or (
        value.get("datasetGroup") not in ("otcMarket", "OTCMARKET")
        or value.get("datasetName") not in ("regShoDaily", "REGSHODAILY")
        or value.get("partitionFields") != ["tradeReportDate"]
    ):
        raise ValueError("FINRA metadata has a different production identity or partition")
    fields = value.get("fields")
    if not isinstance(fields, list) or len(fields) != len(API_FIELDS):
        raise ValueError("FINRA metadata field set differs from the reviewed schema")
    expected, seen = dict(API_FIELDS), set()
    for field in fields:
        if not isinstance(field, dict):
            raise ValueError("invalid FINRA metadata field")
        name = field.get("name")
        if not isinstance(name, str) or name not in expected or name in seen:
            raise ValueError("unknown or duplicate FINRA metadata field")
        if field.get("type") != expected[name]:
            raise ValueError("FINRA metadata field type differs from the reviewed schema")
        if name == "tradeReportDate" and field.get("format") != "yyyy-MM-dd":
            raise ValueError("FINRA metadata date format differs from the reviewed schema")
        seen.add(name)


def validate_data_version(headers: Mapping[str, str]) -> None:
    """Require the dataset version; Data-API-Version is a separate transport header."""
    if not isinstance(headers, Mapping):
        raise ValueError("FINRA response headers must be a mapping")
    values = [v for k, v in headers.items() if isinstance(k, str) and k.lower() == "data-version"]
    if len(values) != 1 or not isinstance(values[0], str) or values[0].strip() != "1":
        raise ValueError("FINRA response must declare exactly one Data-Version: 1")
