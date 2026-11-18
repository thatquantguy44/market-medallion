"""ICI-T01: an approval profile is a pure, source-specific I/O gate."""

import socket
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.ici_weekly_flows.manifest import require_approved_use

ETF = "etf_net_issuance"
FUNDS = "long_term_mutual_fund_flows"


def profile(series=ETF, **changes):
    result = {
        "dataset": "ici-weekly-etf-fund-flows",
        "series": series,
        "purpose": "internal_research",
        "route": "invented_fixture",
        "authorization": "pending",
        "permission_reference": None,
        "attribution": None,
        "operations": ["fixture"],
    }
    result.update(changes)
    return result


def real_profile(series=ETF, **changes):
    result = profile(
        series,
        route="approved_local_file",
        authorization="provider_permission",
        permission_reference="SYNTHETIC_TEST_REF",
        attribution="Investment Company Institute",
        operations=["local_import", "retain", "transform"],
    )
    result.update(changes)
    return result


def test_fixture_only_and_missing_profile_make_no_io(monkeypatch):
    def unexpected_io(*args, **kwargs):
        pytest.fail("ICI admission attempted I/O")

    monkeypatch.setattr(socket, "create_connection", unexpected_io)
    monkeypatch.setattr(Path, "open", unexpected_io)
    for series in (ETF, FUNDS):
        require_approved_use(profile(series), operation="fixture", series=series)
        for operation in ("local_import", "acquire", "retain", "transform", "internal_export"):
            with pytest.raises(PermissionError):
                require_approved_use(None, operation=operation, series=series)
            with pytest.raises(PermissionError):
                require_approved_use(profile(series), operation=operation, series=series)


def test_approval_is_specific_to_series_and_operation():
    grant = real_profile()
    for operation in grant["operations"]:
        require_approved_use(grant, operation=operation, series=ETF)
    with pytest.raises(PermissionError, match="series"):
        require_approved_use(grant, operation="local_import", series=FUNDS)
    with pytest.raises(PermissionError, match="not explicitly admitted"):
        require_approved_use(grant, operation="internal_export", series=ETF)


def test_local_file_grant_cannot_be_extended_to_download():
    grant = real_profile(operations=["local_import", "acquire"])
    with pytest.raises(PermissionError, match="network acquisition"):
        require_approved_use(grant, operation="local_import", series=ETF)
    with pytest.raises(PermissionError, match="network acquisition"):
        require_approved_use(grant, operation="acquire", series=ETF)


def test_download_grant_requires_explicit_acquire_and_no_export():
    grant = real_profile(route="approved_download", operations=["acquire"])
    require_approved_use(grant, operation="acquire", series=ETF)
    with pytest.raises(PermissionError):
        require_approved_use(grant, operation="internal_export", series=ETF)


@pytest.mark.parametrize(
    "changes",
    [
        {"authorization": "subscriber_agreement"},
        {"permission_reference": "SYNTHETIC_TEST_REF"},
        {"operations": ["fixture", "acquire"]},
        {"attribution": "Investment Company Institute"},
    ],
)
def test_fixture_label_cannot_admit_real_use(changes):
    with pytest.raises(PermissionError):
        require_approved_use(profile(**changes), operation="fixture", series=ETF)


@pytest.mark.parametrize(
    "changes",
    [
        {"authorization": "pending"},
        {"permission_reference": None},
        {"attribution": None},
        {"purpose": "public_distribution"},
        {"operations": ["local_import", "local_import"]},
        {"operations": ["local_import", "public_export"]},
        {"permission_reference": "https://secret.example/token"},
        {"credential": "invented"},
        {"series": [ETF]},
    ],
)
def test_real_use_fails_closed_on_missing_or_expanded_claims(changes):
    with pytest.raises((ValueError, PermissionError)):
        require_approved_use({**real_profile(), **changes}, operation="local_import", series=ETF)


def test_unknown_operation_or_series_fails():
    with pytest.raises(ValueError):
        require_approved_use(profile(), operation="publish_publicly", series=ETF)
    with pytest.raises(ValueError):
        require_approved_use(profile(), operation="money_market", series="money_market")
