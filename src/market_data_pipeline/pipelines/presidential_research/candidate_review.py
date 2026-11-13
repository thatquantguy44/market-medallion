"""Review-only control candidates, never an approval or promotion."""

import tempfile
from pathlib import Path

from .config import load_config
from .quality import check_controls, check_political_candidates, check_wikipedia_congress_crosscheck
from .storage import load_capture
from .warehouse import admitted_capture_facts


def validate_control_rows(config, controls):
    issues = check_controls(controls)
    errors = [issue.as_dict() for issue in issues if issue.severity == "ERROR"]
    for index, row in enumerate(controls, 2):
        if row.get("sourceId") not in config.manual_control_source_ids:
            errors.append(
                {
                    "check_id": "controls.source_role",
                    "severity": "ERROR",
                    "message": "Source is not registered as manual reviewed "
                    "political-control evidence.",
                    "location": f"row {index}",
                }
            )
    return {
        "schema_version": 1,
        "errors": len(errors),
        "issues": errors,
        "approval_status": "NOT_EVALUATED",
        "reason": "Control shape validation is not a review approval.",
    }


def prepare_candidates(root, house_capture_id, wikipedia_capture_id):
    """Join two explicitly admitted candidate sources while retaining review status."""
    house, house_facts = admitted_capture_facts(root, "HOUSE_GOV_001", house_capture_id)
    wiki, wiki_facts = admitted_capture_facts(root, "WIKI_CONGRESS_001", wikipedia_capture_id)
    if house["input_kind"] != wiki["input_kind"] or house["config_sha256"] != wiki["config_sha256"]:
        raise ValueError("Candidate source classifications or configurations differ.")
    _, _, config_bytes = load_capture(root, "HOUSE_GOV_001", house_capture_id)
    with tempfile.TemporaryDirectory(prefix="pres-candidate-") as tmp:
        path = Path(tmp) / "sources.toml"
        path.write_bytes(config_bytes)
        config = load_config(path)
    candidates = [item["values"] for item in house_facts]
    crosscheck = [item["values"] for item in wiki_facts]
    issues = check_political_candidates(candidates) + check_wikipedia_congress_crosscheck(
        crosscheck
    )
    if any(issue.severity == "ERROR" for issue in issues):
        raise ValueError("Candidate source quality checks failed.")
    by_congress = {row["congress"]: row for row in crosscheck}
    first = config.annual_start_year - 1
    selected = [
        row
        for row in candidates
        if int(row["start_year"]) <= config.annual_end_year and int(row["end_year"]) > first
    ]
    if not selected or any(row["congress"] not in by_congress for row in selected):
        raise ValueError("Candidate sample is empty or lacks a secondary cross-check.")
    result = []
    for candidate in selected:
        wiki_row = by_congress[candidate["congress"]]
        result.append(
            {
                "candidateId": f"house-congress-{candidate['congress']}",
                "congress": candidate["congress"],
                "startYear": candidate["start_year"],
                "endYear": candidate["end_year"],
                "presidentLabelCandidate": candidate["president_label"],
                "presidentPartyCandidate": candidate["president_party"],
                "houseControlCandidate": candidate["house_party"],
                "senateControlCandidate": candidate["senate_party"],
                "sourceId": candidate["source_id"],
                "sourceReviewStatus": candidate["review_status"],
                "wikipediaSenateCounts": (
                    f"D={wiki_row['senate_democrat_count']}; "
                    f"R={wiki_row['senate_republican_count']}; "
                    f"Other={wiki_row['senate_other_count']}; "
                    f"Vacancies={wiki_row['senate_vacancies']}"
                ),
                "wikipediaHouseCounts": (
                    f"D={wiki_row['house_democrat_count']}; "
                    f"R={wiki_row['house_republican_count']}; "
                    f"Other={wiki_row['house_other_count']}; "
                    f"Vacancies={wiki_row['house_vacancies']}"
                ),
                "wikipediaVariationHint": wiki_row["within_congress_variation_hint"],
                "wikipediaTrifectaLabel": wiki_row["trifecta_label"],
                "wikipediaSourceId": wiki_row["source_id"],
                "wikipediaReviewStatus": wiki_row["review_status"],
                "reviewRequired": "Verify dated president/House/Senate intervals, identity, ties, "
                "vacancies, and mid-Congress changes before creating controls.csv",
            }
        )
    return result, {
        "schema_version": 1,
        "artifact_kind": "POLITICAL_CONTROL_REVIEW_CANDIDATES",
        "controls_file_eligible": False,
        "input_kind": house["input_kind"],
        "candidate_count": len(result),
        "config_sha256": house["config_sha256"],
        "inputs": [
            {
                "source_id": source,
                "capture_id": receipt["capture_id"],
                "raw_sha256": receipt["sha256"],
            }
            for source, receipt in (("HOUSE_GOV_001", house), ("WIKI_CONGRESS_001", wiki))
        ],
        "decision_window": {
            "first_decision_date": f"{first}-12-31",
            "last_decision_date": f"{config.annual_end_year}-12-31",
        },
        "reason": "Congress-year candidates are not dated, reviewed political-control intervals.",
    }
