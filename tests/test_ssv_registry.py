"""SSV-AC02: explicit FINRA API schema, version and reporting-scope registry."""

from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import (
    load_manifest,
    manifest_from_dict,
    validate_data_version,
    validate_metadata,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.registry import (
    validate_acquisition_window,
)

MANIFEST = Path("manifests/finra-short-sale-volume.yml")


def metadata():
    # Invented metadata fixture; provider field spellings/types are the contract.
    return {
        "datasetGroup": "OTCMARKET",
        "datasetName": "REGSHODAILY",
        "partitionFields": ["tradeReportDate"],
        "fields": [
            {"name": "tradeReportDate", "type": "Date", "format": "yyyy-MM-dd"},
            {"name": "securitiesInformationProcessorSymbolIdentifier", "type": "String"},
            {"name": "shortParQuantity", "type": "Number"},
            {"name": "shortExemptParQuantity", "type": "Number"},
            {"name": "totalParQuantity", "type": "Number"},
            {"name": "marketCode", "type": "String"},
            {"name": "reportingFacilityCode", "type": "String"},
        ],
    }


def test_ssv_ac02_checked_manifest_matches_the_pinned_source_contract():
    raw = yaml.safe_load(MANIFEST.read_text())
    result = load_manifest(MANIFEST)
    assert result.data_version == 1
    assert result.schema_id == "finra-reg-sho-api/1"
    assert result.to_dict() == raw
    assert [(f.id, f.reporting_facility_code, f.market_code) for f in result.facilities] == [
        ("nasdaq_carteret_trf", "NQTRF", "Q"),
        ("nasdaq_chicago_trf", "NCTRF", "B"),
        ("nyse_trf", "NYTRF", "N"),
    ]
    result.to_dict()["fields"].clear()
    assert len(result.to_dict()["fields"]) == 7


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "mirror"),
        ("provider_dataset", "regShoDailyMock"),
        ("provider_group", "fixedIncomeMarket"),
        ("format", "txt"),
        ("data_version", 2),
        ("data_version", True),
        ("data_version", "1"),
        ("schema_id", "pre-2011"),
        ("history_months", 24),
        ("partition_field", "publicationDate"),
        ("date_format", "%m/%d/%Y"),
        ("credential_value", "invented-test-string"),
    ],
)
def test_ssv_ac02_unknown_or_expanded_contract_is_rejected(field, value):
    raw = yaml.safe_load(MANIFEST.read_text())
    raw[field] = value
    with pytest.raises(ValueError):
        manifest_from_dict(raw)


def test_ssv_ac02_manifest_rejects_missing_or_renamed_count_field():
    raw = yaml.safe_load(MANIFEST.read_text())
    raw["fields"]["shortParQuantity"] = "String"
    with pytest.raises(ValueError):
        manifest_from_dict(raw)
    del raw["fields"]["shortParQuantity"]
    with pytest.raises(ValueError):
        manifest_from_dict(raw)


def test_ssv_ac02_duplicate_yaml_contract_keys_are_rejected(tmp_path):
    path = tmp_path / "duplicate.yml"
    path.write_text(MANIFEST.read_text() + "data_version: 1\n")
    with pytest.raises(ValueError, match="duplicate"):
        load_manifest(path)


def test_ssv_ac02_metadata_field_order_and_unreliable_descriptions_do_not_change_schema():
    value = metadata()
    value["fields"].reverse()
    for field in value["fields"]:
        field["description"] = "Invented description; never treated as an identifier alias"
    validate_metadata(value)


@pytest.mark.parametrize(
    "mutation", ["mock", "partition", "missing", "extra", "duplicate", "type", "format"]
)
def test_ssv_ac02_metadata_drift_is_rejected(mutation):
    value = deepcopy(metadata())
    if mutation == "mock":
        value["datasetName"] += "MOCK"
    elif mutation == "partition":
        value["partitionFields"] += ["marketCode"]
    elif mutation == "missing":
        value["fields"].pop()
    elif mutation == "extra":
        value["fields"].append({"name": "unreviewedField", "type": "Number"})
    elif mutation == "duplicate":
        value["fields"][-1] = value["fields"][0]
    elif mutation == "type":
        value["fields"][2]["type"] = "String"
    elif mutation == "format":
        value["fields"][0]["format"] = "yyyyMMdd"
    with pytest.raises(ValueError):
        validate_metadata(value)


@pytest.mark.parametrize(
    "headers",
    [
        {"Data-Version": "1"},
        {"data-version": "1", "data-api-version": "1"},
    ],
)
def test_ssv_ac02_source_data_version_is_explicit(headers):
    validate_data_version(headers)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Data-API-Version": "1"},
        {"Data-Version": "2"},
        {"Data-Version": 1},
        {"data-version": "1", "Data-Version": "2"},
    ],
)
def test_ssv_ac02_missing_conflicting_or_transport_only_version_is_rejected(headers):
    with pytest.raises(ValueError):
        validate_data_version(headers)


@pytest.mark.parametrize(
    "scopes",
    [
        [],
        ["N"],
        ["NYTRF"],
        ["adf"],
        ["orf"],
        ["consolidated_trf_adf"],
        ["listed_trfs", "nyse_trf"],
        ["nyse_trf", "nyse_trf"],
        "listed_trfs",
        [True],
    ],
)
def test_ssv_ac02_unknown_excluded_or_overlapping_scopes_are_rejected(scopes):
    raw = yaml.safe_load(MANIFEST.read_text())
    raw["scopes"] = scopes
    with pytest.raises(ValueError):
        manifest_from_dict(raw)


def test_ssv_ac02_individual_disjoint_scopes_have_canonical_order():
    raw = yaml.safe_load(MANIFEST.read_text())
    raw["scopes"] = ["nyse_trf", "nasdaq_chicago_trf"]
    result = manifest_from_dict(raw)
    assert [(f.reporting_facility_code, f.market_code) for f in result.facilities] == [
        ("NCTRF", "B"),
        ("NYTRF", "N"),
    ]


@pytest.mark.parametrize("code,market", [("NQTRF", "Q"), ("NCTRF", "B"), ("NYTRF", "N")])
def test_ssv_ac02_paired_provider_identifiers_are_preserved(code, market):
    facility = load_manifest(MANIFEST).resolve_facility(code, market)
    assert (facility.reporting_facility_code, facility.market_code) == (code, market)


@pytest.mark.parametrize(
    "code,market",
    [
        ("NYTRF", "Q"),
        ("NQTRF", "N"),
        ("N", "N"),
        ("ADFN", "D"),
        ("ORF", "O"),
        ("NYTRF", "N,Q"),
        ("nytrf", "N"),
    ],
)
def test_ssv_ac02_unknown_mixed_or_mismatched_facility_pairs_are_rejected(code, market):
    with pytest.raises(ValueError):
        load_manifest(MANIFEST).resolve_facility(code, market)


def test_ssv_ac02_row_from_an_unselected_facility_is_rejected():
    raw = yaml.safe_load(MANIFEST.read_text())
    raw["scopes"] = ["nyse_trf"]
    with pytest.raises(ValueError):
        manifest_from_dict(raw).resolve_facility("NQTRF", "Q")


def test_ssv_ac02_acquisition_window_is_rolling_and_has_no_hidden_clock():
    assert validate_acquisition_window("2025-11-10", "2026-11-10", as_of=date(2026, 11, 10)) == (
        date(2025, 11, 10),
        date(2026, 11, 10),
    )
    with pytest.raises(ValueError):
        validate_acquisition_window("2025-11-10", "2026-11-10", as_of=date(2026, 11, 11))


def test_ssv_ac02_calendar_month_guard_clamps_february_without_guessing_a_trading_calendar():
    assert validate_acquisition_window("2023-02-28", "2024-02-29", as_of=date(2024, 2, 29)) == (
        date(2023, 2, 28),
        date(2024, 2, 29),
    )
    # A weekday/holiday calendar and actual partition availability are separate evidence.
    assert validate_acquisition_window("2026-11-07", "2026-11-08", as_of=date(2026, 11, 10))


@pytest.mark.parametrize(
    "start,end",
    [
        ("2025-11-09", "2026-11-10"),
        ("2026-11-10", "2026-11-11"),
        ("2026-11-10", "2026-11-09"),
        ("20261109", "2026-11-10"),
        ("11/09/2026", "2026-11-10"),
        ("2026-11-09T00:00:00Z", "2026-11-10"),
        (True, "2026-11-10"),
        ("2026-02-30", "2026-11-10"),
    ],
)
def test_ssv_ac02_outside_future_reversed_or_non_iso_windows_are_rejected(start, end):
    with pytest.raises(ValueError):
        validate_acquisition_window(start, end, as_of=date(2026, 11, 10))


def test_ssv_ac02_acquisition_cutoff_is_a_date_not_an_implicit_timestamp():
    with pytest.raises(ValueError):
        validate_acquisition_window("2026-11-09", "2026-11-10", as_of=datetime(2026, 11, 10))
