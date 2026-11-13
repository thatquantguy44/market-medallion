"""PR016-R06/N02: shared, byte-pinned cross-language boundary vectors."""

import hashlib
import json
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.presidential_research.canonical import canonical_json
from market_data_pipeline.pipelines.presidential_research.dataset import parse_dataset_bytes

VECTORS = Path("contracts/presidential_research/v1/boundary-vectors.json")
PIN = "7b8c29241d3f4a598e25254397c07f766efc1c3d5122575ee5f2fe4b131aa417"


def test_boundary_vectors_and_payload_digests():
    vector_bytes = VECTORS.read_bytes()
    assert hashlib.sha256(vector_bytes).hexdigest() == PIN
    vectors = json.loads(vector_bytes)
    assert vectors["schemaVersion"] == "presidential-boundary-vectors/1"
    for case in vectors["datasetCases"]:
        payload = case["payload"].encode()
        assert hashlib.sha256(payload).hexdigest() == case["sha256"], case["name"]
        if case["valid"]:
            parsed = parse_dataset_bytes(payload)
            canonical = canonical_json(parsed)
            assert canonical == case["canonical"], case["name"]
            assert hashlib.sha256(canonical.encode()).hexdigest() == case["canonicalSha256"]
        else:
            with pytest.raises(ValueError):
                parse_dataset_bytes(payload)
    for case in vectors["canonicalCases"]:
        assert canonical_json(case["value"]) == case["expected"], case["name"]


def test_pinned_structural_schema_is_self_describing():
    path = VECTORS.with_name("research-dataset.schema.json")
    payload = path.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == (
        "fca119b40d45307960417bb6e17fa1fce1bc1f65594e7fd0bd9fad762b5e75b3"
    )
    schema = json.loads(payload)
    assert schema["additionalProperties"] is False
    assert schema["properties"]["kind"] == {"const": "USER_SUPPLIED"}
