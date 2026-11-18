"""ICI-T01 use admission, independent of transport, parsing and storage.

Source review: specs/spec020/source_review.md.
https://www.ici.org/permissions
https://www.ici.org/copyright-and-linking-policies
"""

import re

_DATASET = "ici-weekly-etf-fund-flows"
_SERIES = frozenset({"etf_net_issuance", "long_term_mutual_fund_flows"})
_OPERATIONS = frozenset(
    {"fixture", "local_import", "acquire", "retain", "transform", "internal_export"}
)
_ROUTES = frozenset({"invented_fixture", "approved_local_file", "approved_download"})
_FIELDS = frozenset(
    {
        "dataset",
        "series",
        "purpose",
        "route",
        "authorization",
        "permission_reference",
        "attribution",
        "operations",
    }
)
_REFERENCE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{7,79}\Z")


def require_approved_use(value: object, *, operation: str, series: str) -> None:
    """Require an explicit operation grant for one ICI series before future I/O.

    The caller supplies the approval reference. Validation does not verify an ICI
    agreement or grant rights itself. No active real-data profile ships with the
    package, and future adapters must call this before their operation.
    """
    if not isinstance(operation, str) or operation not in _OPERATIONS:
        raise ValueError("unsupported ICI operation")
    if not isinstance(series, str) or series not in _SERIES:
        raise ValueError("unsupported ICI series")
    if value is None:
        raise PermissionError("ICI approved-use profile is required")
    if not isinstance(value, dict) or set(value) != _FIELDS:
        raise ValueError("ICI approved-use fields must match the reviewed contract")
    if (
        value["dataset"] != _DATASET
        or not isinstance(value["series"], str)
        or value["series"] not in _SERIES
    ):
        raise ValueError("unsupported ICI dataset or series")
    if value["series"] != series:
        raise PermissionError("ICI approval does not cover the requested series")
    if value["purpose"] != "internal_research":
        raise ValueError("unsupported ICI purpose")
    route = value["route"]
    if not isinstance(route, str) or route not in _ROUTES:
        raise ValueError("unsupported ICI route")
    authorization = value["authorization"]
    if not isinstance(authorization, str) or authorization not in (
        "pending",
        "provider_permission",
        "subscriber_agreement",
    ):
        raise ValueError("unsupported ICI authorization")
    reference = value["permission_reference"]
    if reference is not None and (
        not isinstance(reference, str) or _REFERENCE.fullmatch(reference) is None
    ):
        raise ValueError("ICI permission_reference must be a nonsecret evidence ID")
    operations = value["operations"]
    if (
        not isinstance(operations, list)
        or any(not isinstance(item, str) or item not in _OPERATIONS for item in operations)
        or len(operations) != len(set(operations))
    ):
        raise ValueError("ICI operations must be a unique list of supported operations")
    if operation not in operations:
        raise PermissionError("ICI operation is not explicitly admitted")
    if route == "invented_fixture":
        if (
            operations != ["fixture"]
            or authorization != "pending"
            or reference is not None
            or value["attribution"] is not None
        ):
            raise PermissionError("invented ICI fixtures cannot authorize provider use")
        return
    if (
        "fixture" in operations
        or authorization == "pending"
        or reference is None
        or value["attribution"] != "Investment Company Institute"
        or not operations
    ):
        raise PermissionError("real ICI use requires recorded source-specific permission")
    if route == "approved_local_file" and "acquire" in operations:
        raise PermissionError("ICI local-file approval cannot admit network acquisition")
