"""FOMC-QAC08–10: evidence-bound remediation scope, never speculative text repair."""

from collections import Counter

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_review import (
    validate_plan,
    validate_review,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest


def assess_remediation(report, plan, reviews, check):
    """Require completed render validation before selecting native fixes or no-fix.

    The CLI recomputes check_reviews from the actual pack/render bytes. This pure
    step binds that check to the exact review records, not merely their counts.
    It never changes extraction profiles, catalogs, source bytes or eligibility.
    """
    validate_plan(plan, report)
    try:
        body = dict(check)
        identity = body.pop("check_id")
        if (
            identity != digest(body)
            or check["schema_id"] != "fomc-quality-review-check/1"
            or check["report_id"] != report["report_id"]
            or check["plan_id"] != plan["plan_id"]
            or check["status"] != "reviewed"
            or check["pending_case_ids"]
            or check["request_performed"] is not False
            or check["eligibility_changed"] is not False
            or check["whole_corpus_quality_certified"] is not False
            or check["dispositions"] != dict(Counter(r["disposition"] for r in reviews))
            or sorted(check["confirmed_native_defects"])
            != sorted(
                r["case"]["case_id"]
                for r in reviews
                if r["disposition"] == "confirmed_native_defect"
            )
            or sorted(check["unresolved_cases"])
            != sorted(
                r["case"]["case_id"]
                for r in reviews
                if r["disposition"] in {"unresolved", "ocr_candidate"}
            )
        ):
            raise ValueError
        for record in reviews:
            validate_review(record, plan)
        ids = [r["case"]["case_id"] for r in reviews]
        if (
            len(ids) != len(set(ids))
            or set(ids) != {c["case_id"] for c in plan["cases"]}
            or check["reviewed_cases"] != len(reviews)
            or check["review_ids"] != sorted(r["review_id"] for r in reviews)
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError(
            "FOMC remediation requires complete bound source-review evidence"
        ) from None
    defects = sorted(
        r["case"]["case_id"] for r in reviews if r["disposition"] == "confirmed_native_defect"
    )
    unresolved = sorted(
        r["case"]["case_id"] for r in reviews if r["disposition"] in {"unresolved", "ocr_candidate"}
    )
    value = {
        "schema_id": "fomc-quality-remediation/1",
        "report_id": report["report_id"],
        "plan_id": plan["plan_id"],
        "review_check_id": check["check_id"],
        "review_ids": check["review_ids"],
        "status": "fix_required" if defects else "no_confirmed_native_defect",
        "confirmed_native_defect_case_ids": defects,
        "unresolved_case_ids": unresolved,
        "reviewed_cases": len(reviews),
        "uncovered_strata": plan["uncovered_strata"],
        "deferred_boundaries": plan["deferred_boundaries"],
        "unselected_flagged_unit_count": plan["unselected_case_count"],
        "publication_required": bool(defects),
        "derivative_publication_performed": False,
        "operational_restore_replay": "required_before_publication_acceptance"
        if defects
        else "not_required_no_derivative_publication",
        "eligibility_changed": False,
        "whole_corpus_quality_certified": False,
        "request_performed": False,
    }
    value["assessment_id"] = digest(value)
    return value
