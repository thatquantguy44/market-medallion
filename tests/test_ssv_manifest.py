"""SSV-AC01: approved-use admission is pure and precedes provider I/O."""

import socket
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import require_approved_use

PURPOSE = "personal_noncommercial_internal_research"


def profile(route="invented_fixture", **changes):
    result = {
        "purpose": PURPOSE,
        "route": route,
        "authorization": "pending",
        "permission_reference": None,
        "operations": ["fixture"],
        "credential_type": None,
    }
    result.update(changes)
    return result


def test_ssv_ac01_fixture_admission_and_denial_make_no_io(monkeypatch):
    def unexpected_io(*args, **kwargs):
        pytest.fail("approved-use admission attempted I/O")

    monkeypatch.setattr(socket, "create_connection", unexpected_io)
    monkeypatch.setattr(Path, "open", unexpected_io)
    require_approved_use(profile(), operation="fixture")
    for operation in ("local_import", "acquire", "transform", "internal_export"):
        with pytest.raises(PermissionError):
            require_approved_use(None, operation=operation)
        with pytest.raises(PermissionError):
            require_approved_use(profile(), operation=operation)


@pytest.mark.parametrize("route", ["approved_local_file", "finra_daily_files", "finra_query_api"])
def test_ssv_ac01_noncommercial_label_alone_is_not_provider_permission(route):
    with pytest.raises(PermissionError):
        require_approved_use(profile(route, operations=["transform"]), operation="transform")


def test_ssv_ac01_local_file_permission_never_grants_network_acquisition():
    grant = profile(
        "approved_local_file",
        authorization="provider_permission",
        permission_reference="invented-test-permission",
        operations=["local_import", "acquire"],
    )
    require_approved_use(grant, operation="local_import")
    with pytest.raises(PermissionError):
        require_approved_use(grant, operation="acquire")


def test_ssv_ac01_recorded_file_grants_are_operation_specific():
    grant = profile(
        "finra_daily_files",
        authorization="provider_permission",
        permission_reference="invented-test-permission",
        operations=["acquire"],
    )
    require_approved_use(grant, operation="acquire")
    with pytest.raises(PermissionError):
        require_approved_use(grant, operation="internal_export")


def test_ssv_ac01_api_requires_route_specific_terms_and_public_credential_metadata():
    grant = profile(
        "finra_query_api",
        authorization="accepted_api_terms",
        permission_reference="invented-test-acceptance",
        credential_type="public",
        operations=["acquire", "transform", "internal_export"],
    )
    for operation in grant["operations"]:
        require_approved_use(grant, operation=operation)
    for changes in (
        {"authorization": "provider_permission"},
        {"permission_reference": None},
        {"credential_type": None},
        {"authorization": "pending"},
    ):
        with pytest.raises(PermissionError):
            require_approved_use({**grant, **changes}, operation="acquire")


@pytest.mark.parametrize(
    "changes",
    [
        {"purpose": "commercial_internal_research"},
        {"purpose": "public_distribution"},
        {"route": "third_party_mirror"},
        {"authorization": True},
        {"permission_reference": " "},
        {"permission_reference": True},
        {"operations": "fixture"},
        {"operations": ["fixture", "fixture"]},
        {"operations": ["public_export"]},
        {"credential_type": "mock"},
        {"credential_type": "organization"},
        {"password": "invented-value-not-a-real-secret"},
    ],
)
def test_ssv_ac01_rejects_invalid_or_expanded_profiles(changes):
    with pytest.raises(ValueError):
        require_approved_use(profile(**changes), operation="fixture")


def test_ssv_ac01_invented_label_cannot_authorize_provider_operations():
    grant = profile(
        operations=["fixture", "acquire"],
        authorization="provider_permission",
        permission_reference="invented-test-permission",
    )
    with pytest.raises(PermissionError):
        require_approved_use(grant, operation="acquire")


def test_ssv_ac01_unknown_operation_is_rejected():
    with pytest.raises(ValueError):
        require_approved_use(profile(), operation="publish_publicly")
