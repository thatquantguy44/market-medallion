"""FOMC-QAC01–04/12: deterministic quality counts and exact source identities."""

from copy import deepcopy
from pathlib import Path

import pytest
from test_fomc_chunk_catalog import generation_input as generation_input
from test_fomc_text_catalog import catalog_input as catalog_input

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.catalog import build_catalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunk_catalog import build_generation
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunks import chunk_text
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality import (
    build_report,
    validate_report,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import extract_text
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

__all__ = ["catalog_input", "generation_input"]


def inputs(inventory, records):
    base = build_catalog(
        inventory, records, inventory_run_id="00000000-0000-0000-0000-000000000099"
    )
    extractions = {r["extraction"]["extraction_id"]: r["extraction"] for r in records}
    sets = [
        chunk_text(extractions[v["extraction_ids"][0]], document_version_id=v["version_id"])
        for rep in base["representations"]
        for v in rep["versions"]
    ]
    return inventory, base, build_generation(base, sets), extractions, sets


def test_unique_versions_and_reversions_are_counted_without_duplicate_issue_memberships(
    catalog_input,
):
    value = build_report(*inputs(*catalog_input))
    assert value["totals"]["selected_versions"] == 3
    assert value["totals"]["representations"] == 1
    assert value["totals"]["extractions"] == 2
    assert value["totals"]["review_versions"] == 0
    assert value["totals"]["chunks"] == value["totals"]["eligible_chunks"] == 3
    assert value["findings"] == [] and value["request_performed"] is False
    assert not value["text_or_ocr_accuracy_certified"]
    args = inputs(*catalog_input)
    assert build_report(*args[:-1], list(reversed(args[-1]))) == value
    assert validate_report(value) == value


def test_image_and_replacement_issues_overlap_without_inflating_union_totals(catalog_input):
    inventory, records = catalog_input
    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    records = deepcopy(records[:1])
    r = records[0]
    x = extract_text(
        (
            '<div id="article"><p>Value \ufffd with chart</p>'
            '<img src="files/invented.png" alt="Rates"></div>'
        ).encode(),
        r["receipt"]["source_url"],
        manifest,
    )
    r["extraction"] = x
    r["receipt"]["sha256"] = x["source_sha256"]
    value = build_report(*inputs(inventory, records))
    assert value["totals"]["review_versions"] == 1
    assert value["totals"]["review_chunks"] == 1
    assert len(value["findings"]) == 2
    assert {f["issue_code"] for f in value["findings"]} == {
        "image_content",
        "replacement_character",
    }
    assert len({cid for f in value["findings"] for cid in f["chunk_ids"]}) == 1
    assert value["totals"]["source_unit_characters"] == len(x["full_text"])
    for f in value["findings"]:
        assert f["source_sha256"] == r["receipt"]["sha256"]
        assert f["first_seen_at"].startswith("2026-11-11")
        assert f["meeting_dates"] and f["capture_ids"] == [r["receipt"]["capture_id"]]


def test_blank_units_are_visible_without_chunks(catalog_input):
    from test_fomc_text import pdf_bytes

    inventory, records = catalog_input
    doc = next(d for d in inventory["documents"] if d["representation"] == "pdf")
    mids = sorted(
        {e["meeting_id"] for e in inventory["entries"] if e["document_id"] == doc["document_id"]}
    )
    x = extract_text(
        pdf_bytes([""]),
        doc["source_url"],
        load_manifest(Path("manifests/fomc-meeting-minutes.yml")),
    )
    r = {
        "receipt": {
            **records[0]["receipt"],
            "source_url": doc["source_url"],
            "sha256": x["source_sha256"],
        },
        "extraction": x,
        "meeting_ids": mids,
    }
    value = build_report(*inputs(inventory, [r]))
    assert value["totals"]["empty_units"] == 1
    assert value["totals"]["chunks"] == 0
    assert value["findings"][0]["issue_code"] == "missing_native_text"
    assert value["findings"][0]["chunk_ids"] == []
    assert value["findings"][0]["page"] == 1


def test_corrupt_catalog_or_derivative_is_refused(catalog_input):
    args = list(inputs(*catalog_input))
    args[2] = deepcopy(args[2])
    args[2]["coverage"]["chunks"] += 1
    with pytest.raises(ValueError):
        build_report(*args)
    args = list(inputs(*catalog_input))
    args[3] = deepcopy(args[3])
    next(iter(args[3].values()))["full_text"] += "corrupt"
    with pytest.raises(ValueError):
        build_report(*args)


def test_unknown_codes_and_unassociated_meetings_fail_visibly(catalog_input):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import (
        _finish,
        _unit,
        profile,
    )

    inventory, records = catalog_input
    records = deepcopy(records[:1])
    r = records[0]
    x = _finish(
        b"invented",
        r["receipt"]["source_url"],
        profile(),
        [_unit("text", issues=["future_unknown"])],
    )
    # Simulate a future extractor that classifies a new nonfatal issue.
    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest

    x["status"] = "review_required"
    x.pop("extraction_id")
    x["extraction_id"] = digest(x)
    r["extraction"] = x
    r["receipt"]["sha256"] = x["source_sha256"]
    with pytest.raises(ValueError, match="issue"):
        build_report(*inputs(inventory, records))
    args = list(inputs(*catalog_input))
    args[0] = deepcopy(inventory)
    args[0]["meetings"] = []
    with pytest.raises(ValueError, match="meeting"):
        build_report(*args)


def test_report_identity_detects_tampering(catalog_input):
    value = build_report(*inputs(*catalog_input))
    value["totals"]["chunks"] += 1
    with pytest.raises(ValueError):
        validate_report(value)


def test_document_without_units_remains_a_visible_diagnostic(catalog_input):
    inventory, records = deepcopy(catalog_input)
    r = records[0]
    x = extract_text(
        b'<div id="article"></div>',
        r["receipt"]["source_url"],
        load_manifest(Path("manifests/fomc-meeting-minutes.yml")),
    )
    r["extraction"] = x
    r["receipt"]["sha256"] = x["source_sha256"]
    result = build_report(*inputs(inventory, [r]))
    assert result["totals"]["units"] == result["totals"]["chunks"] == 0
    assert result["totals"]["representations_without_units"] == 1
    assert result["findings"][0]["unit_ordinal"] is None
    assert result["findings"][0]["issue_code"] == "missing_native_text"


def test_output_refuses_conflicts_and_symlinked_canonical_paths(catalog_input, tmp_path):
    from types import SimpleNamespace

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality import write_report

    report = build_report(*inputs(*catalog_input))
    cfg = SimpleNamespace(root=tmp_path / "archive")
    cfg.root.mkdir()
    (cfg.root / "text").mkdir()
    (tmp_path / "alias").symlink_to(cfg.root / "text", target_is_directory=True)
    with pytest.raises(ValueError):
        write_report(cfg, tmp_path / "alias" / "report.json", report)
    output = tmp_path / "report.json"
    output.write_bytes(b"keep this")
    with pytest.raises(ValueError, match="conflict"):
        write_report(cfg, output, report)
    assert output.read_bytes() == b"keep this"


def test_cross_year_bundled_reference_has_two_memberships_and_one_unique_total(catalog_input):
    import json

    from test_fomc_text import pdf_bytes

    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import (
        inventory_from_dict,
    )

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    raw = json.loads(Path("tests/fixtures/fomc_meeting_minutes/invented-current.json").read_text())
    joint = raw["pages"][0]["meetings"][2]
    joint["meeting_dates"] = ["2020-12-31", "2021-01-02"]
    inv = inventory_from_dict(raw, manifest).to_dict()
    doc = next(d for d in inv["documents"] if d["source_url"].endswith("invented-joint.pdf"))
    mids = sorted(
        {e["meeting_id"] for e in inv["entries"] if e["document_id"] == doc["document_id"]}
    )
    x = extract_text(pdf_bytes(["Joint text"]), doc["source_url"], manifest)
    receipt = {
        **catalog_input[1][0]["receipt"],
        "source_url": doc["source_url"],
        "sha256": x["source_sha256"],
    }
    report = build_report(
        *inputs(inv, [{"receipt": receipt, "extraction": x, "meeting_ids": mids}])
    )
    assert report["totals"]["selected_versions"] == 1
    assert report["groups"]["year"]["2020"]["selected_versions"] == 1
    assert report["groups"]["year"]["2021"]["selected_versions"] == 1
    assert report["sources"][0]["release_claims"][0]["provider_release_date"] is None
