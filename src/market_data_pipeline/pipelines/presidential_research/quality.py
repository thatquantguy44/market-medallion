from __future__ import annotations

import calendar
import csv
import math
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PARTIES = {"D", "R", "OTHER", "TIED", "UNKNOWN"}


@dataclass(frozen=True)
class Issue:
    check_id: str
    severity: str
    message: str
    location: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "check_id": self.check_id,
            "severity": self.severity,
            "message": self.message,
            "location": self.location,
        }


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def check_monthly_returns(rows: list[dict[str, str]]) -> list[Issue]:
    issues: list[Issue] = []
    seen_months: set[str] = set()
    for row in rows:
        observed = row.get("date", "")
        try:
            parsed = date.fromisoformat(observed)
            value = float(row.get("market_return_decimal", ""))
        except ValueError:
            issues.append(
                Issue("market.valid", "ERROR", "Market row has an invalid date or return", observed)
            )
            continue
        month_key = parsed.strftime("%Y-%m")
        if parsed.day != calendar.monthrange(parsed.year, parsed.month)[1] or not math.isfinite(
            value
        ):
            issues.append(
                Issue(
                    "market.valid",
                    "ERROR",
                    "Market row must use a finite month-end return",
                    observed,
                )
            )
        if month_key in seen_months:
            issues.append(Issue("market.unique", "ERROR", "Duplicate market month", observed))
        seen_months.add(month_key)
    if not rows:
        issues.append(Issue("market.present", "ERROR", "No monthly market returns were parsed"))
    return issues


def check_policy_rates(rows: list[dict[str, str]]) -> list[Issue]:
    """Validate the normalized monthly FRED rate table without imputing gaps."""
    issues: list[Issue] = []
    seen: set[str] = set()
    for row in rows:
        observed = row.get("date", "")
        try:
            parsed = date.fromisoformat(observed)
            value = float(row.get("policy_rate_percent", ""))
        except ValueError:
            issues.append(
                Issue("policy.valid", "ERROR", "Policy row has an invalid date or rate", observed)
            )
            continue
        if parsed.day != 1 or not math.isfinite(value):
            issues.append(
                Issue(
                    "policy.valid",
                    "ERROR",
                    "Policy row must use a finite monthly rate dated on day one",
                    observed,
                )
            )
        if observed in seen:
            issues.append(Issue("policy.unique", "ERROR", "Duplicate policy-rate month", observed))
        seen.add(observed)
    if not rows:
        issues.append(Issue("policy.present", "ERROR", "No monthly policy rates were parsed"))
    return issues


def check_shiller_context(rows: list[dict[str, str]]) -> list[Issue]:
    """Check monthly price/dividend context while allowing source-marked missing CAPE."""
    issues: list[Issue] = []
    seen: set[str] = set()
    for row in rows:
        observed = row.get("date", "")
        try:
            parsed = date.fromisoformat(observed)
            price = float(row.get("price", ""))
            dividend_text = row.get("dividend", "").strip()
            dividend = None if not dividend_text else float(dividend_text)
            cape_text = row.get("cape", "").strip()
            cape = None if not cape_text else float(cape_text)
        except ValueError:
            issues.append(
                Issue(
                    "shiller.valid",
                    "ERROR",
                    "Shiller row has an invalid date, price, dividend, or CAPE",
                    observed,
                )
            )
            continue
        if (
            parsed.day < 28
            or not math.isfinite(price)
            or price <= 0
            or (dividend is not None and (not math.isfinite(dividend) or dividend < 0))
            or (cape is not None and not math.isfinite(cape))
        ):
            issues.append(
                Issue(
                    "shiller.valid",
                    "ERROR",
                    "Shiller row must use a positive month-end price, non-negative dividend, "
                    "and finite or missing CAPE",
                    observed,
                )
            )
        elif dividend is None:
            issues.append(
                Issue(
                    "shiller.dividend_missing",
                    "WARNING",
                    "Shiller trailing dividend is unavailable; this month cannot enter "
                    "the dividend-return reconstruction",
                    observed,
                )
            )
        if observed in seen:
            issues.append(Issue("shiller.unique", "ERROR", "Duplicate Shiller month", observed))
        seen.add(observed)
    if not rows:
        issues.append(Issue("shiller.present", "ERROR", "No Shiller context rows were parsed"))
    return issues


def check_political_candidates(rows: list[dict[str, str]]) -> list[Issue]:
    """Check candidate history but do not promote it to reviewed controls."""
    issues: list[Issue] = []
    seen: set[str] = set()
    for row in rows:
        congress = row.get("congress", "")
        try:
            start_year, end_year = int(row.get("start_year", "")), int(row.get("end_year", ""))
        except ValueError:
            issues.append(
                Issue(
                    "candidates.valid",
                    "ERROR",
                    "Candidate row has an invalid Congress year",
                    congress,
                )
            )
            continue
        if not congress or start_year >= end_year:
            issues.append(
                Issue(
                    "candidates.valid",
                    "ERROR",
                    "Candidate row has an invalid Congress interval",
                    congress,
                )
            )
        if congress in seen:
            issues.append(
                Issue("candidates.unique", "ERROR", "Duplicate Congress candidate", congress)
            )
        seen.add(congress)
        for field in ("house_party", "senate_party", "president_party"):
            if row.get(field) not in PARTIES:
                issues.append(
                    Issue(
                        "candidates.party",
                        "ERROR",
                        f"{field} must be one of {sorted(PARTIES)}",
                        congress,
                    )
                )
        if row.get("review_status") != "CANDIDATE_REQUIRES_DATED_REVIEW":
            issues.append(
                Issue(
                    "candidates.review",
                    "ERROR",
                    "Candidate history must retain its review status",
                    congress,
                )
            )
    if not rows:
        issues.append(
            Issue("candidates.present", "ERROR", "No political-control candidates were parsed")
        )
    return issues


_WIKI_COUNT = re.compile(r"^(?:\d+(?:[\/–-]\d+)*|—)$")


def check_wikipedia_congress_crosscheck(
    rows: list[dict[str, str]],
    first_decision_year: int | None = None,
    last_decision_year: int | None = None,
) -> list[Issue]:
    """Validate a secondary Congress-level count cross-check, not controls."""
    issues: list[Issue] = []
    seen: set[str] = set()
    intervals: list[tuple[int, int]] = []
    count_fields = (
        "senate_total",
        "senate_democrat_count",
        "senate_republican_count",
        "senate_other_count",
        "senate_vacancies",
        "house_total",
        "house_democrat_count",
        "house_republican_count",
        "house_other_count",
        "house_vacancies",
    )
    for row in rows:
        congress = row.get("congress", "")
        try:
            start_year, end_year = int(row.get("start_year", "")), int(row.get("end_year", ""))
        except ValueError:
            issues.append(
                Issue(
                    "wiki_crosscheck.valid",
                    "ERROR",
                    "Cross-check row has an invalid Congress year",
                    congress,
                )
            )
            continue
        if not congress or start_year >= end_year:
            issues.append(
                Issue(
                    "wiki_crosscheck.valid",
                    "ERROR",
                    "Cross-check row has an invalid Congress interval",
                    congress,
                )
            )
        if congress in seen:
            issues.append(
                Issue("wiki_crosscheck.unique", "ERROR", "Duplicate Congress cross-check", congress)
            )
        seen.add(congress)
        intervals.append((start_year, end_year))
        for field in count_fields:
            if not _WIKI_COUNT.fullmatch(row.get(field, "")):
                issues.append(
                    Issue(
                        "wiki_crosscheck.count",
                        "ERROR",
                        f"Invalid published count form in {field}",
                        congress,
                    )
                )
        if row.get("source_id") != "WIKI_CONGRESS_001":
            issues.append(
                Issue("wiki_crosscheck.source", "ERROR", "Unexpected source ID", congress)
            )
        if row.get("review_status") != "SECONDARY_CROSSCHECK_ONLY":
            issues.append(
                Issue(
                    "wiki_crosscheck.review",
                    "ERROR",
                    "Cross-check must retain secondary-only status",
                    congress,
                )
            )
        if row.get("within_congress_variation_hint") not in {"TRUE", "FALSE"}:
            issues.append(
                Issue(
                    "wiki_crosscheck.variation",
                    "ERROR",
                    "Variation hint must be TRUE or FALSE",
                    congress,
                )
            )
    if first_decision_year is not None and last_decision_year is not None:
        for year in range(first_decision_year, last_decision_year + 1):
            if not any(start <= year < end for start, end in intervals):
                issues.append(
                    Issue(
                        "wiki_crosscheck.coverage",
                        "ERROR",
                        "No Congress cross-check covers annual decision year",
                        str(year),
                    )
                )
    if not rows:
        issues.append(
            Issue(
                "wiki_crosscheck.present",
                "ERROR",
                "No Wikipedia Congress cross-check rows were parsed",
            )
        )
    return issues


def check_controls(rows: list[dict[str, str]]) -> list[Issue]:
    issues: list[Issue] = []
    seen: set[str] = set()
    ordered: list[tuple[date, date, str]] = []
    required = {
        "id",
        "startDate",
        "endDate",
        "presidentId",
        "presidentName",
        "presidentParty",
        "houseControl",
        "senateControl",
        "sourceId",
    }
    for index, row in enumerate(rows, 2):
        missing = sorted(field for field in required if not row.get(field, "").strip())
        if missing:
            issues.append(
                Issue(
                    "controls.required",
                    "ERROR",
                    f"Missing field(s): {', '.join(missing)}",
                    f"row {index}",
                )
            )
            continue
        identifier = row["id"]
        if identifier in seen:
            issues.append(Issue("controls.unique", "ERROR", "Duplicate interval ID", identifier))
        seen.add(identifier)
        try:
            start, end = date.fromisoformat(row["startDate"]), date.fromisoformat(row["endDate"])
        except ValueError:
            issues.append(Issue("controls.dates", "ERROR", "Invalid ISO date", identifier))
            continue
        if end <= start:
            issues.append(
                Issue("controls.dates", "ERROR", "endDate must follow startDate", identifier)
            )
        for field in ("presidentParty", "houseControl", "senateControl"):
            if row[field] not in PARTIES:
                issues.append(
                    Issue(
                        "controls.party",
                        "ERROR",
                        f"{field} must be one of {sorted(PARTIES)}",
                        identifier,
                    )
                )
        ordered.append((start, end, identifier))
    ordered.sort()
    for (_, previous_end, previous_id), (current_start, _, current_id) in zip(
        ordered, ordered[1:], strict=False
    ):
        if current_start < previous_end:
            issues.append(
                Issue(
                    "controls.overlap",
                    "ERROR",
                    f"Overlapping intervals {previous_id} and {current_id}",
                    current_id,
                )
            )
    if not rows:
        issues.append(Issue("controls.present", "ERROR", "No reviewed control intervals supplied"))
    return issues


def check_annual_coverage(
    rows: list[dict[str, str]], start_year: int, end_year: int
) -> list[Issue]:
    months = {row["date"][:7] for row in rows if row.get("date")}
    issues: list[Issue] = []
    for year in range(start_year, end_year + 1):
        missing = [
            f"{year}-{month:02d}" for month in range(1, 13) if f"{year}-{month:02d}" not in months
        ]
        if missing:
            issues.append(
                Issue(
                    "market.coverage",
                    "ERROR",
                    f"Missing monthly market observations: {', '.join(missing)}",
                    str(year),
                )
            )
    return issues
