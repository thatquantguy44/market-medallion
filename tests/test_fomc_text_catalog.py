"""FOMC-AC04/05: observed revisions, capture cutoffs and extractor independence."""

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.catalog import build_catalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import inventory_from_dict
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest, extract_text
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest


@pytest.fixture
def catalog_input():
    import json

    m = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    inv = inventory_from_dict(
        json.loads(Path("tests/fixtures/fomc_meeting_minutes/invented-current.json").read_text()), m
    ).to_dict()
    doc = next(d for d in inv["documents"] if d["representation"] == "html")
    meetings = sorted(
        {e["meeting_id"] for e in inv["entries"] if e["document_id"] == doc["document_id"]}
    )
    records = []
    for i, text in enumerate(["original Ω", "original Ω", "corrected 中", "original Ω"], 1):
        body = ('<div id="article"><p>' + text + "</p></div>").encode()
        x = extract_text(body, doc["source_url"], m)
        records.append(
            {
                "receipt": {
                    "capture_id": f"00000000-0000-0000-0000-{i:012d}",
                    "captured_at": f"2026-11-11T12:00:0{i}+00:00",
                    "accepted": True,
                    "kind": "document",
                    "source_url": doc["source_url"],
                    "sha256": x["source_sha256"],
                },
                "extraction": x,
                "meeting_ids": meetings,
            }
        )
    return inv, records


def test_repeat_correction_reversion_and_actual_first_seen(catalog_input):
    inv, records = catalog_input
    value = build_catalog(inv, records, inventory_run_id="00000000-0000-0000-0000-000000000099")
    versions = value["representations"][0]["versions"]
    assert len(versions) == 3
    assert len(versions[0]["capture_ids"]) == 2
    assert versions[0]["source_sha256"] == versions[2]["source_sha256"]
    assert versions[0]["version_id"] != versions[2]["version_id"]
    assert versions[0]["first_seen_at"] == "2026-11-11T12:00:01.000000Z"
    assert value["gaps"], "unacquired declared documents cannot disappear"
    assert (
        build_catalog(inv, list(reversed(records)), inventory_run_id=value["inventory_run_id"])
        == value
    )


def test_capture_cutoff_excludes_later_corrections(catalog_input):
    inv, records = catalog_input
    value = build_catalog(
        inv,
        records,
        inventory_run_id="00000000-0000-0000-0000-000000000099",
        as_of=datetime(2026, 11, 11, 12, 0, 2, tzinfo=UTC),
    )
    assert len(value["representations"][0]["versions"]) == 1
    assert len(value["capture_ids"]) == 2


def test_new_extractor_is_a_derivative_not_a_source_revision(catalog_input):
    inv, records = catalog_input
    changed = deepcopy(records[0])
    x = changed["extraction"]
    x["profile"]["algorithm"] = "invented-next-extractor/2"
    x["profile_sha256"] = digest(x["profile"])
    del x["extraction_id"]
    x["extraction_id"] = digest(x)
    value = build_catalog(
        inv, [records[0], changed], inventory_run_id="00000000-0000-0000-0000-000000000099"
    )
    version = value["representations"][0]["versions"][0]
    assert len(value["representations"][0]["versions"]) == 1
    assert len(version["extraction_ids"]) == 2 and len(version["capture_ids"]) == 1


def test_pre_scope_or_forged_source_associations_are_refused(catalog_input):
    inv, records = catalog_input
    records[0]["meeting_ids"] = ["unknown-meeting"]
    with pytest.raises(ValueError, match="association"):
        build_catalog(inv, records[:1], inventory_run_id="00000000-0000-0000-0000-000000000099")
