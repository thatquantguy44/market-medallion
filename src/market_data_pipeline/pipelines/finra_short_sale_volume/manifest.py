"""SSV-T01 approved-use admission, independent of credentials and provider I/O.

Evidence and current decisions: specs/spec012/source_review.md.
API access terms: https://developer.finra.org/specific-terms-equity-data
Website access terms: https://www.finra.org/terms-of-use
"""

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
