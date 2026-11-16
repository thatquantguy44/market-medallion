"""OTC-AC01: independent route/use admission, without provider I/O."""

from copy import deepcopy

import pytest

from market_data_pipeline.pipelines.finra_otc_transparency.manifest import require_approved_use


def fixture_use():
    return {
        "dataset": "finra-otc-transparency",
        "purpose": "personal_noncommercial_internal_research",
        "route": "invented_fixture",
        "authorization": "pending",
        "permission_reference": None,
        "credential_type": None,
        "provider_dataset": None,
        "attribution": None,
        "operations": ["fixture"],
    }


def api_use():
    return {
        **fixture_use(),
        "route": "finra_query_api",
        "authorization": "accepted_api_terms",
        "permission_reference": "invented-test-acceptance-record",
        "credential_type": "public",
        "provider_dataset": "weeklySummary",
        "attribution": "FINRA",
        "operations": ["acquire", "retain", "transform", "internal_export"],
    }


def test_fixture_permission_does_not_grant_provider_operations():
    profile = fixture_use()
    require_approved_use(profile, operation="fixture")
    for operation in ("acquire", "local_import", "retain", "transform", "internal_export"):
        with pytest.raises(PermissionError):
            require_approved_use(profile, operation=operation)


@pytest.mark.parametrize("operation", ["acquire", "retain", "transform", "internal_export"])
def test_explicit_api_admission_is_operation_scoped_and_does_not_mutate_input(operation):
    profile = api_use()
    before = deepcopy(profile)
    require_approved_use(profile, operation=operation)
    assert profile == before
    profile["operations"].remove(operation)
    with pytest.raises(PermissionError):
        require_approved_use(profile, operation=operation)


@pytest.mark.parametrize("route", ["approved_local_file", "finra_download_files"])
def test_files_require_independent_provider_permission(route):
    profile = {**api_use(), "route": route}
    with pytest.raises(PermissionError):
        require_approved_use(profile, operation="retain")
    profile.update(
        authorization="provider_permission",
        credential_type=None,
        provider_dataset=None,
        operations=["local_import", "retain", "transform", "internal_export"],
    )
    require_approved_use(profile, operation="retain")
    profile["operations"].append("acquire")
    if route == "approved_local_file":
        with pytest.raises(PermissionError):
            require_approved_use(profile, operation="acquire")
    else:
        require_approved_use(profile, operation="acquire")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dataset", "finra-short-sale-volume"),
        ("purpose", "commercial_research"),
        ("route", "weeklySummaryMock"),
        ("route", []),
        ("operations", ["acquire", "acquire"]),
        ("operations", "acquire"),
        ("operations", [True]),
        ("authorization", "guessed_from_public_download"),
        ("unexpected", True),
    ],
)
def test_malformed_or_cross_source_profiles_fail_closed(field, value):
    with pytest.raises(ValueError):
        require_approved_use({**api_use(), field: value}, operation="acquire")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("authorization", "pending"),
        ("authorization", "provider_permission"),
        ("permission_reference", None),
        ("permission_reference", ""),
        ("permission_reference", "two\nlines"),
        ("credential_type", "firm"),
        ("credential_type", None),
        ("provider_dataset", "weeklySummaryMock"),
        ("provider_dataset", "weeklySummaryHistoric"),
        ("provider_dataset", "regShoDaily"),
        ("attribution", None),
    ],
)
def test_unadmitted_api_capabilities_are_denied(field, value):
    with pytest.raises((PermissionError, ValueError)):
        require_approved_use({**api_use(), field: value}, operation="acquire")


@pytest.mark.parametrize("operation", ["public_export", "commercial_export", "model", None])
def test_unsupported_use_is_not_silently_admitted(operation):
    with pytest.raises(ValueError):
        require_approved_use(api_use(), operation=operation)


def test_missing_profile_is_denied_and_provider_data_cannot_masquerade_as_fixture():
    with pytest.raises(PermissionError):
        require_approved_use(None, operation="acquire")
    profile = api_use()
    profile["operations"].append("fixture")
    with pytest.raises(PermissionError):
        require_approved_use(profile, operation="fixture")
    profile = fixture_use()
    profile["permission_reference"] = "an-actual-source"
    with pytest.raises(PermissionError):
        require_approved_use(profile, operation="fixture")
