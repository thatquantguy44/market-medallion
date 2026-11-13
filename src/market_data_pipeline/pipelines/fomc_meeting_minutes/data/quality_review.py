"""FOMC-QAC05/07: bounded deterministic cases and additive review evidence."""

import re
from copy import deepcopy
from pathlib import PurePosixPath

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.catalog import capture_time
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality import validate_report
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest

DISPOSITIONS = frozenset(
    {
        "sample_text_matches",
        "chart_or_image",
        "confirmed_native_defect",
        "ocr_candidate",
        "unresolved",
    }
)


def select_cases(report, *, max_cases=12):
    validate_report(report)
    if type(max_cases) is not int or not 1 <= max_cases <= 12:
        raise ValueError("FOMC quality review requires 1–12 cases")
    strata = {}
    flagged = [u for u in report["units"] if u["issues"]]
    if len({u["case_id"] for u in flagged}) != len(flagged):
        raise ValueError("FOMC quality duplicate review cases")

    def order(u):
        return (u["meeting_dates"], u["first_seen_at"], u["case_id"])

    for unit in flagged:
        for issue in unit["issues"]:
            key = "|".join((issue, unit["format"], unit["profile_sha256"]))
            strata.setdefault(key, []).append(unit)
    strata = {key: sorted(rows, key=order) for key, rows in sorted(strata.items())}
    candidates = []
    for boundary in (0, -1):
        for rows in strata.values():
            row = rows[boundary]
            if row["case_id"] not in {r["case_id"] for r in candidates}:
                candidates.append(row)
    cases = candidates[:max_cases]
    ids = {c["case_id"] for c in cases}
    descriptions = [
        {
            "stratum_id": key,
            "case_count": len(rows),
            "selected_case_ids": sorted(r["case_id"] for r in rows if r["case_id"] in ids),
            "earliest_case_id": rows[0]["case_id"],
            "latest_case_id": rows[-1]["case_id"],
        }
        for key, rows in strata.items()
    ]
    value = {
        "schema_id": "fomc-quality-review-plan/1",
        "report_id": report["report_id"],
        **{k: report[k] for k in ("inventory_run_id", "text_catalog_id", "chunk_catalog_id")},
        "profile": "stratified-earliest-latest/1",
        "max_cases": max_cases,
        "cases": deepcopy(cases),
        "strata": descriptions,
        "uncovered_strata": [s["stratum_id"] for s in descriptions if not s["selected_case_ids"]],
        "deferred_boundaries": [
            {"stratum_id": s["stratum_id"], "case_id": s[b]}
            for s in descriptions
            for b in ("earliest_case_id", "latest_case_id")
            if s[b] not in ids
        ],
        "unselected_case_count": len(flagged) - len(cases),
        "whole_corpus_quality_certified": False,
    }
    value["plan_id"] = digest(value)
    return value


def _plan_identity(plan):
    try:
        body = dict(plan)
        identity = body.pop("plan_id")
        if identity != digest(body) or plan["schema_id"] != "fomc-quality-review-plan/1":
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError("FOMC quality review plan integrity mismatch") from None


def validate_plan(plan, report):
    _plan_identity(plan)
    if select_cases(report, max_cases=plan["max_cases"]) != plan:
        raise ValueError("FOMC quality review plan differs from pinned report")
    return plan


def make_review(plan, case_id, *, disposition, reviewer, reviewed_at, render, observations):
    _plan_identity(plan)
    case = next((c for c in plan["cases"] if c["case_id"] == case_id), None)
    try:
        path = PurePosixPath(render["path"])
        if (
            case is None
            or disposition not in DISPOSITIONS
            or not isinstance(reviewer, str)
            or not reviewer.strip()
            or len(reviewer) > 200
            or not isinstance(render["tool"], str)
            or not render["tool"].strip()
            or len(render["tool"]) > 200
            or not path.parts
            or path.is_absolute()
            or ".." in path.parts
            or "\\" in render["path"]
            or ":" in render["path"]
            or not re.fullmatch("[0-9a-f]{64}", render["sha256"])
            or not re.fullmatch("[0-9a-f]{64}", render["input_sha256"])
            or not isinstance(observations, list)
            or not 1 <= len(observations) <= 20
            or any(not isinstance(n, str) or not n.strip() or len(n) > 4000 for n in observations)
        ):
            raise ValueError
        clock = capture_time(reviewed_at)
    except (KeyError, TypeError, ValueError):
        raise ValueError("FOMC quality review requires bound source/render observations") from None
    value = {
        "schema_id": "fomc-quality-review/1",
        "plan_id": plan["plan_id"],
        "report_id": plan["report_id"],
        "case": deepcopy(case),
        "disposition": disposition,
        "reviewer": reviewer,
        "reviewed_at": clock,
        "render": deepcopy(render),
        "observations": list(observations),
        "eligibility_changed": False,
        "whole_corpus_quality_certified": False,
    }
    value["review_id"] = digest(value)
    return value


def validate_review(value, plan):
    try:
        expected = make_review(
            plan,
            value["case"]["case_id"],
            **{
                k: value[k]
                for k in ("disposition", "reviewer", "reviewed_at", "render", "observations")
            },
        )
        if expected != value:
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError("FOMC quality review identity or source mismatch") from None
    return value
