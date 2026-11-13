"""FOMC-QAC08–12: a no-fix outcome requires completed, bound source reviews."""

from collections import Counter
from copy import deepcopy

import pytest
from test_fomc_quality_review import catalog_input as catalog_input
from test_fomc_quality_review import review_report as review_report

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_remediation import (
    assess_remediation,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_review import (
    make_review,
    select_cases,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest

__all__ = ["catalog_input", "review_report"]


def reviewed(report, disposition="unresolved"):
    plan = select_cases(report)
    records = [
        make_review(
            plan,
            c["case_id"],
            disposition=disposition,
            reviewer="fixture",
            reviewed_at="2026-11-13T15:00:00Z",
            render={
                "path": "renders/" + c["case_id"] + ".png",
                "sha256": "1" * 64,
                "input_sha256": c["source_sha256"],
                "tool": "fixture 1",
            },
            observations=["Original source inspected."],
        )
        for c in plan["cases"]
    ]
    check = {
        "schema_id": "fomc-quality-review-check/1",
        "report_id": report["report_id"],
        "plan_id": plan["plan_id"],
        "pack_id": "2" * 64,
        "status": "reviewed",
        "reviewed_cases": len(records),
        "review_ids": sorted(r["review_id"] for r in records),
        "pending_case_ids": [],
        "request_performed": False,
        "eligibility_changed": False,
        "whole_corpus_quality_certified": False,
        "dispositions": dict(Counter(r["disposition"] for r in records)),
        "confirmed_native_defects": [
            r["case"]["case_id"] for r in records if r["disposition"] == "confirmed_native_defect"
        ],
        "unresolved_cases": [
            r["case"]["case_id"]
            for r in records
            if r["disposition"] in {"unresolved", "ocr_candidate"}
        ],
    }
    check["check_id"] = digest(check)
    return plan, records, check


def test_no_confirmed_native_defect_keeps_unresolved_work_and_skips_publication(review_report):
    plan, records, check = reviewed(review_report)
    value = assess_remediation(review_report, plan, records, check)
    assert value["status"] == "no_confirmed_native_defect"
    assert value["publication_required"] is False and value["eligibility_changed"] is False
    assert len(value["unresolved_case_ids"]) == 2
    assert value["operational_restore_replay"] == "not_required_no_derivative_publication"
    assert value["whole_corpus_quality_certified"] is False
    assert assess_remediation(review_report, plan, list(reversed(records)), check) == value


def test_confirmed_defect_requires_a_regression_and_new_derivatives(review_report):
    plan, records, check = reviewed(review_report, "confirmed_native_defect")
    value = assess_remediation(review_report, plan, records, check)
    assert value["status"] == "fix_required"
    assert value["publication_required"] is True
    assert len(value["confirmed_native_defect_case_ids"]) == 2
    assert value["operational_restore_replay"] == "required_before_publication_acceptance"
    assert value["derivative_publication_performed"] is False


@pytest.mark.parametrize(
    "change",
    [
        lambda p, r, c: c.update(status="pending_review"),
        lambda p, r, c: c.update(review_ids=[]),
        lambda p, r, c: c.update(pending_case_ids=["unknown"]),
        lambda p, r, c: c.update(report_id="0" * 64),
        lambda p, r, c: r.pop(),
        lambda p, r, c: r.append(deepcopy(r[0])),
    ],
)
def test_incomplete_duplicate_stale_or_tampered_evidence_cannot_close_remediation(
    review_report, change
):
    plan, records, check = reviewed(review_report)
    change(plan, records, check)
    check.pop("check_id")
    check["check_id"] = digest(check)
    with pytest.raises(ValueError):
        assess_remediation(review_report, plan, records, check)
