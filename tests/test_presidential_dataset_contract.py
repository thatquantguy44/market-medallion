"""PR016-R06/N02: strict unchanged research dataset v1."""

import copy
import json

import pytest

from market_data_pipeline.pipelines.presidential_research.dataset import parse_dataset_bytes


def fixture():
    return {
        "schemaVersion": 1,
        "id": "fixture",
        "title": "Fictional test dataset",
        "kind": "USER_SUPPLIED",
        "currency": "USD",
        "nominalReal": "NOMINAL",
        "returnBasis": "TOTAL_RETURN",
        "sources": [{"id": "f", "title": "Fixture", "locator": None}],
        "levels": [
            {"date": "1935-12-31", "value": 100.0, "sourceId": "f"},
            {"date": "1936-12-31", "value": 110.0, "sourceId": "f"},
        ],
        "controls": [
            {
                "id": "c",
                "startDate": "1935-01-03",
                "endDate": "1937-01-03",
                "presidentId": "p",
                "presidentName": "Fictional",
                "presidentParty": "D",
                "houseControl": "D",
                "senateControl": "D",
                "sourceId": "f",
            }
        ],
        "windows": [
            {
                "id": "y1936",
                "label": "1936",
                "kind": "CALENDAR_YEAR",
                "startDate": "1935-12-31",
                "endDate": "1936-12-31",
                "decisionDate": "1935-12-31",
                "complete": True,
            }
        ],
        "fundamentals": [],
    }


def encode(value):
    return json.dumps(value, allow_nan=False).encode()


def test_accepts_exact_v1_without_coercion():
    value = fixture()
    assert parse_dataset_bytes(encode(value)) == value


@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "string_number",
        "bool_number",
        "bad_date",
        "unknown_source",
        "duplicate",
        "overlap",
        "future_decision",
        "non_nominal",
        "demo",
        "invalid_party",
        "huge_number",
    ],
)
def test_rejects_invalid_or_unadmitted_dataset(mutation):
    value = copy.deepcopy(fixture())
    if mutation == "extra":
        value["unknown"] = 1
    if mutation == "string_number":
        value["levels"][0]["value"] = "100"
    if mutation == "bool_number":
        value["levels"][0]["value"] = True
    if mutation == "bad_date":
        value["levels"][0]["date"] = "1935-02-30"
    if mutation == "unknown_source":
        value["levels"][0]["sourceId"] = "missing"
    if mutation == "duplicate":
        value["levels"].append(copy.deepcopy(value["levels"][0]))
    if mutation == "overlap":
        value["controls"].append({**value["controls"][0], "id": "c2"})
    if mutation == "future_decision":
        value["windows"][0]["decisionDate"] = "1936-01-01"
    if mutation == "non_nominal":
        value["nominalReal"] = "REAL"
    if mutation == "demo":
        value["kind"] = "ILLUSTRATIVE"
    if mutation == "invalid_party":
        value["controls"][0]["presidentParty"] = []
    if mutation == "huge_number":
        value["levels"][0]["value"] = 10**400
    with pytest.raises(ValueError):
        parse_dataset_bytes(encode(value))


def test_rejects_duplicate_keys_and_nonfinite_json():
    with pytest.raises(ValueError):
        parse_dataset_bytes(b'{"schemaVersion":1,"schemaVersion":1}')
    with pytest.raises(ValueError):
        parse_dataset_bytes(b'{"value":NaN}')
