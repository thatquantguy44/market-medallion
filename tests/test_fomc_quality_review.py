"""FOMC-QAC05–07/12: bounded reproducible review and source-bound dispositions."""

from copy import deepcopy
from pathlib import Path

import pytest
from test_fomc_quality import catalog_input as catalog_input
from test_fomc_quality import inputs

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality import build_report
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_review import (
    make_review,
    select_cases,
    validate_plan,
    validate_review,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import extract_text
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

__all__ = ["catalog_input"]


@pytest.fixture
def review_report(catalog_input):
    inventory, records = catalog_input
    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    rows = []
    for i in range(1, 5):
        r = deepcopy(records[0])
        r["receipt"]["capture_id"] = f"00000000-0000-0000-0000-{i:012d}"
        r["receipt"]["captured_at"] = f"2026-11-11T12:00:0{i}+00:00"
        x = extract_text(
            (
                f'<div id="article"><p>Value {i} \ufffd</p>'
                '<img src="files/invented.png" alt="chart"></div>'
            ).encode(),
            r["receipt"]["source_url"],
            manifest,
        )
        r["receipt"]["sha256"] = x["source_sha256"]
        r["extraction"] = x
        rows.append(r)
    return build_report(*inputs(inventory, rows))


def test_selection_covers_issue_strata_and_early_late_cases(review_report):
    plan = select_cases(review_report)
    assert len(plan["cases"]) == 2
    assert len(plan["strata"]) == 2 and not plan["uncovered_strata"]
    assert not plan["deferred_boundaries"]
    assert validate_plan(plan, review_report) == plan
    assert plan["unselected_case_count"] == 2
    assert plan["whole_corpus_quality_certified"] is False
    assert {c["first_seen_at"] for c in plan["cases"]} == {
        "2026-11-11T12:00:01.000000Z",
        "2026-11-11T12:00:04.000000Z",
    }


def test_cap_discloses_deferred_boundaries_and_invalid_limits(review_report):
    plan = select_cases(review_report, max_cases=1)
    assert len(plan["cases"]) == 1 and plan["deferred_boundaries"]
    for limit in (0, 13, True, 1.5):
        with pytest.raises(ValueError):
            select_cases(review_report, max_cases=limit)
    plan["cases"][0]["source_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        validate_plan(plan, review_report)


def test_clean_report_has_no_invented_cases(catalog_input):
    report = build_report(*inputs(*catalog_input))
    assert select_cases(report)["cases"] == []


def review(plan, **changes):
    values = {
        "disposition": "unresolved",
        "reviewer": "fixture reviewer",
        "reviewed_at": "2026-11-13T15:00:00+00:00",
        "render": {
            "path": "renders/page.png",
            "sha256": "1" * 64,
            "input_sha256": plan["cases"][0]["source_sha256"],
            "tool": "fixture renderer 1",
        },
        "observations": ["Replacement characters are already present in source."],
    }
    values.update(changes)
    return make_review(plan, plan["cases"][0]["case_id"], **values)


def test_review_retains_exact_source_identity_and_leaves_eligibility_alone(review_report):
    plan = select_cases(review_report)
    record = review(plan)
    assert record["case"] == plan["cases"][0]
    assert record["eligibility_changed"] is False
    assert validate_review(record, plan) == record
    other = select_cases(review_report, max_cases=1)
    with pytest.raises(ValueError):
        validate_review(record, other)
    record["case"]["source_sha256"] = "2" * 64
    with pytest.raises(ValueError):
        validate_review(record, plan)


@pytest.mark.parametrize(
    "changes",
    [
        {"disposition": "approved_all_text"},
        {"reviewer": ""},
        {"reviewed_at": "2026-11-13"},
        {"observations": []},
        {
            "render": {
                "path": "../unsafe.png",
                "sha256": "1" * 64,
                "input_sha256": "2" * 64,
                "tool": "renderer",
            }
        },
        {
            "render": {
                "path": "renders/page.png",
                "sha256": "bad",
                "input_sha256": "2" * 64,
                "tool": "renderer",
            }
        },
    ],
)
def test_invalid_or_unbound_review_evidence_is_refused(review_report, changes):
    with pytest.raises(ValueError):
        review(select_cases(review_report), **changes)
