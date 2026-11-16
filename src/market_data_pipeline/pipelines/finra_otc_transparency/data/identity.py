"""OTC-T04 evidence-scoped participant keys; no inferred legal-firm crosswalk.

Contract: specs/spec013/data_contract.md#otc-t04-identity-contract.
"""

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import dataclass

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import Partition, profile_for

IDENTITY_VERSION = "finra-otc-participant/1"
_ROUTES = frozenset(
    {"invented_fixture", "approved_local_file", "finra_download_files", "finra_query_api"}
)


def _text(value: object, field: str) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError(f"OTC identity {field} must be supplied text or null")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ValueError(f"OTC identity {field} is not valid UTF-8 text") from None
    return value


def _identifier(value: object, field: str, *, numeric: bool = False) -> str | None:
    token = _text(value, field)
    if token is not None and (
        any(c.isspace() for c in token) or (numeric and not re.fullmatch(r"[0-9]+", token))
    ):
        raise ValueError(f"OTC identity {field} has invalid identifier syntax")
    return token


@dataclass(frozen=True)
class ParticipantIdentity:
    partition: Partition
    schema_profile: str
    route: str
    participant_key: str | None
    identity_basis: str
    identifier_kind: str | None
    participant_name_raw: str | None
    mpid: str | None
    firm_crd: str | None

    def require_key(self) -> str:
        if self.participant_key is None:
            raise ValueError("unknown OTC participant identity cannot form an unambiguous row key")
        return self.participant_key


def participant_identity(
    *,
    partition: Partition,
    schema_profile: str,
    route: str,
    participant_name_raw: str | None,
    mpid: str | None = None,
    firm_crd: str | None = None,
    attribution: str = "REPORTED",
) -> ParticipantIdentity:
    """Preserve supplied evidence; name/group keys never establish firm continuity.

    MPID precedes CRD as the source-key basis; both supplied IDs remain retained.
    A group classification must come from reviewed source evidence, not its name.
    This pure transform grants no rights or route/schema conformance.
    """
    if not isinstance(partition, Partition):
        raise ValueError("OTC identity requires a validated population context")
    if schema_profile != profile_for(partition.product, partition.tier).schema_profile:
        raise ValueError("OTC identity schema differs from the selected profile")
    if not isinstance(route, str) or route not in _ROUTES:
        raise ValueError("OTC identity requires an explicit supported route context")
    if not isinstance(attribution, str) or attribution not in ("REPORTED", "AGGREGATED", "UNKNOWN"):
        raise ValueError("unsupported OTC source attribution evidence")
    name = _text(participant_name_raw, "participant name")
    mpid = _identifier(mpid, "MPID")
    firm_crd = _identifier(firm_crd, "CRD", numeric=True)
    kind, identifier, label, week = None, None, None, None
    if attribution != "REPORTED" and (mpid is not None or firm_crd is not None):
        raise ValueError("grouped/unknown OTC evidence cannot assert an individual firm identifier")
    if attribution == "AGGREGATED":
        if name is None:
            raise ValueError("OTC aggregate bucket needs an explicit source label")
        basis, label = "AGGREGATED", name
    elif attribution == "UNKNOWN":
        basis = "UNKNOWN"
    elif mpid is not None or firm_crd is not None:
        basis = "PROVIDER_ID"
        kind, identifier = ("MPID", mpid) if mpid is not None else ("CRD", firm_crd)
    elif name is not None:
        basis, label = "SOURCE_ROW_LABEL", name
    else:
        basis = "UNKNOWN"
    key = None
    if basis != "UNKNOWN":
        if basis != "PROVIDER_ID":
            week = partition.reporting_week_start.isoformat()
        descriptor = {
            "version": IDENTITY_VERSION,
            "route": route,
            "schema_profile": schema_profile,
            "scope_id": partition.scope_id,
            "basis": basis,
            "identifier_kind": kind,
            "identifier": identifier,
            "label": label,
            "reporting_week_start": week,
        }
        encoded = json.dumps(descriptor, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        key = IDENTITY_VERSION + ":" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return ParticipantIdentity(
        partition, schema_profile, route, key, basis, kind, name, mpid, firm_crd
    )


def require_unique_participants(rows: Iterable[tuple[str, ParticipantIdentity]]) -> None:
    """Refuse unknown, duplicate or mixed-context rows before population admission."""
    context, seen = None, set()
    for symbol, identity in rows:
        if not isinstance(identity, ParticipantIdentity) or _identifier(symbol, "symbol") is None:
            raise ValueError("invalid OTC row identity evidence")
        current = (identity.partition, identity.schema_profile, identity.route)
        if context is not None and current != context:
            raise ValueError("mixed OTC identity context cannot form one population")
        context = current
        key = (symbol, identity.require_key())
        if key in seen:
            raise ValueError("duplicate OTC security/participant key in selected partition")
        seen.add(key)
