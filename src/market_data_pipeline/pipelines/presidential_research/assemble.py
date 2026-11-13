# ruff: noqa: E501
from __future__ import annotations

import calendar
import csv
import math
from dataclasses import dataclass, replace
from datetime import date
from itertools import pairwise
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from typing import Literal

from .config import PipelineConfig
from .provenance import canonical_json, read_json, sha256_file, write_json
from .quality import (
    Issue,
    check_annual_coverage,
    check_controls,
    check_monthly_returns,
    read_csv,
)


class BuildError(ValueError):
    pass


@dataclass(frozen=True)
class ReconstructionDefinition:
    """A source-locked public reconstruction, rather than an index claim."""

    key: str
    expected_source_id: str
    dataset_id_prefix: str
    title_prefix: str
    return_basis: str
    dataset_kind: str
    construction: str


RECONSTRUCTIONS = {
    "crsp_market_proxy": ReconstructionDefinition(
        key="crsp_market_proxy",
        expected_source_id="FF_MKT_001",
        dataset_id_prefix="annual-crsp-market-proxy",
        title_prefix="Public CRSP-market-proxy annual reconstruction",
        return_basis="TOTAL_RETURN",
        dataset_kind="PUBLIC_CRSP_MARKET_PROXY_RECONSTRUCTION",
        construction="Compounded monthly Kenneth French Mkt-RF plus RF returns.",
    ),
    "shiller_sp_composite_approximation": ReconstructionDefinition(
        key="shiller_sp_composite_approximation",
        expected_source_id="SHILLER_001",
        dataset_id_prefix="annual-shiller-sp-composite-approximation",
        title_prefix="Shiller S&P Composite dividend-reinvestment-style annual reconstruction",
        return_basis="TOTAL_RETURN",
        dataset_kind="PUBLIC_SHILLER_SP_COMPOSITE_APPROXIMATION",
        construction=(
            "Compounded monthly nominal returns reconstructed as "
            "(P_t + D_t / 12) / P_(t-1) - 1 from the Shiller workbook."
        ),
    ),
}

INHERITED_FUNDAMENTALS_SOURCE_ID = "INHERITED_CAPE_FEDFUNDS_001"
INHERITED_VALUATION_DEFINITION = "Shiller workbook CAPE field"


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _monthly_levels(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    level = 100.0
    output: list[dict[str, object]] = []
    for row in sorted(rows, key=lambda item: item["date"]):
        level *= 1 + float(row["market_return_decimal"])
        if not math.isfinite(level) or level <= 0:
            raise BuildError(f"Cumulative market level became invalid at {row['date']}")
        output.append({"date": row["date"], "value": level, "sourceId": row["source_id"]})
    return output


def _control_at(controls: list[dict[str, str]], decision_date: str) -> dict[str, str] | None:
    for control in controls:
        if control["startDate"] <= decision_date < control["endDate"]:
            return control
    return None


def _source_catalog(
    config: PipelineConfig, market_source_id: str, controls: list[dict[str, str]]
) -> list[dict[str, object]]:
    used = {market_source_id, *(row["sourceId"] for row in controls)}
    records: list[dict[str, object]] = []
    for source in config.sources:
        if source.id in used:
            note = f"{source.role}; {source.authority}"
            if source.captured_locator and source.captured_at and source.captured_sha256:
                note += f"; captured {source.captured_locator} at {source.captured_at}; SHA-256 {source.captured_sha256}"
            if source.locators:
                note += "; evidence locators: " + ", ".join(source.locators)
            if source.captures:
                note += "; evidence captures: " + "; ".join(
                    f"{capture.locator} @ {capture.captured_at} SHA-256 {capture.sha256}"
                    for capture in source.captures
                )
            records.append(
                {
                    "id": source.id,
                    "title": source.title,
                    "locator": source.url,
                    "note": note,
                }
            )
    missing = used - {record["id"] for record in records}
    if missing:
        raise BuildError(
            f"Controls reference source IDs absent from source config: {', '.join(sorted(missing))}"
        )
    return records


def _errors(issues: list[Issue]) -> list[Issue]:
    return [issue for issue in issues if issue.severity == "ERROR"]


def derive_shiller_monthly_total_return(
    rows: list[dict[str, str]],
    first_required_date: date | None = None,
) -> list[dict[str, str]]:
    """Create an explicitly approximate monthly total-return-style series.

    Shiller's ``D`` column is a trailing annual dividend measure, not a
    published monthly total-return index.  Dividing it by twelve makes the
    convention inspectable and avoids relabeling the source as an official S&P
    500 total-return series.
    """
    parsed: list[tuple[date, float, float | None, str]] = []
    months: set[str] = set()
    for row in rows:
        observed = row.get("date", "")
        try:
            observed_date = date.fromisoformat(observed)
            price = float(row.get("price", ""))
            dividend_text = row.get("dividend", "").strip()
            dividend = None if not dividend_text else float(dividend_text)
        except ValueError as exc:
            raise BuildError(
                f"Shiller row has an invalid date, price, or dividend at {observed}"
            ) from exc
        if (
            observed_date != _month_end(observed_date.year, observed_date.month)
            or not math.isfinite(price)
            or price <= 0
            or (dividend is not None and (not math.isfinite(dividend) or dividend < 0))
        ):
            raise BuildError(
                f"Shiller row must contain a positive month-end price and non-negative dividend when present at {observed}"
            )
        month = observed_date.strftime("%Y-%m")
        if month in months:
            raise BuildError(f"Duplicate Shiller month {month}")
        months.add(month)
        parsed.append((observed_date, price, dividend, row.get("source_id", "")))
    if len(parsed) < 2:
        raise BuildError("At least two Shiller monthly observations are required")
    parsed.sort(key=lambda item: item[0])
    source_ids = {source_id for _, _, _, source_id in parsed}
    if source_ids != {"SHILLER_001"}:
        raise BuildError("Shiller return reconstruction must contain only source ID SHILLER_001")
    last_complete_december = max(
        (
            observed_date
            for observed_date, _, dividend, _ in parsed
            if observed_date.month == 12 and dividend is not None
        ),
        default=None,
    )
    if last_complete_december is None and first_required_date is not None:
        raise BuildError("Shiller source has no December observation with a published dividend")
    last_usable_date = last_complete_december or parsed[-1][0]
    usable = [
        item
        for item in parsed
        if item[0] <= last_usable_date
        and (first_required_date is None or item[0] >= first_required_date)
    ]
    if len(usable) < 2:
        raise BuildError(
            "Shiller source has too few complete monthly observations for the requested window"
        )
    missing_dividend = [
        observed_date.isoformat() for observed_date, _, dividend, _ in usable if dividend is None
    ]
    if missing_dividend:
        raise BuildError(
            "Shiller source is missing a dividend within the requested reconstruction window: "
            + ", ".join(missing_dividend[:5])
        )
    output: list[dict[str, str]] = []
    for prior, current in pairwise(usable):
        previous_date, previous_price, _, _ = prior
        observed_date, price, dividend, source_id = current
        if (observed_date.year, observed_date.month) != (
            previous_date.year + (previous_date.month // 12),
            (previous_date.month % 12) + 1,
        ):
            raise BuildError(
                f"Missing Shiller month between {previous_date.isoformat()} and {observed_date.isoformat()}"
            )
        if dividend is None:  # Covered above; narrows the type for static readers.
            raise BuildError(f"Shiller dividend unavailable at {observed_date.isoformat()}")
        monthly_return = (price + dividend / 12.0) / previous_price - 1.0
        if not math.isfinite(monthly_return) or monthly_return <= -1.0:
            raise BuildError(f"Invalid reconstructed Shiller return at {observed_date.isoformat()}")
        output.append(
            {
                "date": observed_date.isoformat(),
                "market_return_decimal": format(monthly_return, ".12g"),
                "source_id": source_id,
            }
        )
    return output


def build_inherited_fundamentals(
    config: PipelineConfig,
    shiller_path: Path,
    policy_path: Path,
    annual_end_year: int,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Join only the pre-declared, frozen CAPE and policy-rate observations.

    The PRES contract permits one source ID and one observation/availability date
    per fundamental record. The emitted composite record therefore uses the later
    CAPE observation date and preserves the individual field dates in the audit.
    """
    shiller = {row["date"]: row for row in read_csv(shiller_path)}
    policy = {row["date"]: row for row in read_csv(policy_path)}
    rows: list[dict[str, object]] = []
    unavailable_after_start: list[int] = []
    started = False
    for year in range(config.annual_start_year, annual_end_year + 1):
        source_year = year - 1
        cape_date = _month_end(source_year, 10).isoformat()
        cape_available = _month_end(source_year, 11).isoformat()
        policy_date = date(source_year, 10, 1).isoformat()
        policy_available = date(source_year, 11, 1).isoformat()
        cape_text = shiller.get(cape_date, {}).get("cape", "").strip()
        policy_text = policy.get(policy_date, {}).get("policy_rate_percent", "").strip()
        if not cape_text or not policy_text:
            if started:
                unavailable_after_start.append(year)
            continue
        try:
            cape, policy_rate = float(cape_text), float(policy_text)
        except ValueError as exc:
            raise BuildError(
                f"Inherited-condition input is not numeric for annual return {year}"
            ) from exc
        if not math.isfinite(cape) or not math.isfinite(policy_rate):
            raise BuildError(f"Inherited-condition input is not finite for annual return {year}")
        started = True
        rows.append(
            {
                "year": year,
                "fundamental": {
                    "date": cape_date,
                    "availableDate": cape_available,
                    "valuation": cape,
                    "valuationDefinition": INHERITED_VALUATION_DEFINITION,
                    "policyRatePct": policy_rate,
                    "sourceId": INHERITED_FUNDAMENTALS_SOURCE_ID,
                },
                "valuation_observation_date": cape_date,
                "valuation_assumed_available_date": cape_available,
                "policy_observation_date": policy_date,
                "policy_assumed_available_date": policy_available,
            }
        )
    if not rows:
        raise BuildError("No joined inherited-condition observations are available")
    if unavailable_after_start:
        raise BuildError(
            "Inherited-condition coverage becomes incomplete after the first eligible annual return: "
            + ", ".join(map(str, unavailable_after_start[:10]))
        )
    first_year = int(rows[0]["year"])
    expected = list(range(first_year, annual_end_year + 1))
    observed = [int(row["year"]) for row in rows]
    if observed != expected:
        raise BuildError("Inherited-condition join is not a contiguous annual sample")
    fundamentals = [dict(row["fundamental"]) for row in rows]
    audit = {
        "schema_version": 1,
        "kind": "FROZEN_INHERITED_CONDITIONS_JOIN",
        "first_annual_year": first_year,
        "last_annual_year": annual_end_year,
        "observations": len(fundamentals),
        "valuation_definition": INHERITED_VALUATION_DEFINITION,
        "source_ids": ["SHILLER_001", "FEDFUNDS_001"],
        "timing": {
            "decision_date": "December 31 of the preceding year",
            "valuation_observation": "October month-end of the preceding year",
            "valuation_assumed_available": "November month-end of the preceding year",
            "policy_observation": "October 1 of the preceding year",
            "policy_assumed_available": "November 1 of the preceding year",
            "availability_note": "Operational anti-look-ahead convention; not verified real-time vintage history.",
        },
        "rows": rows,
    }
    return fundamentals, audit


def inherited_fundamentals_source(shiller_path: Path, policy_path: Path) -> dict[str, object]:
    return {
        "id": INHERITED_FUNDAMENTALS_SOURCE_ID,
        "title": "Frozen Shiller CAPE and FRED effective-funds-rate join",
        "locator": None,
        "note": (
            f"Composite at-origin record: Shiller CAPE normalized SHA-256 {sha256_file(shiller_path)}; "
            f"FRED FEDFUNDS normalized SHA-256 {sha256_file(policy_path)}. "
            "See inherited-conditions protocol for observation and assumed-availability dates."
        ),
    }


def build_annual_dataset(
    config: PipelineConfig,
    market_path: Path,
    controls_path: Path,
    decision_policy: Literal["window_start", "calendar_year_start"],
    reconstruction: str = "crsp_market_proxy",
    fundamentals: list[dict[str, object]] | None = None,
    additional_sources: list[dict[str, object]] | None = None,
    additional_inputs: dict[str, object] | None = None,
    dataset_id_suffix: str = "",
    title_suffix: str = "",
) -> tuple[dict[str, object], dict[str, object]]:
    try:
        definition = RECONSTRUCTIONS[reconstruction]
    except KeyError as exc:
        raise BuildError(f"Unknown reconstruction {reconstruction!r}") from exc
    market = read_csv(market_path)
    controls = read_csv(controls_path)
    issues = (
        check_monthly_returns(market)
        + check_annual_coverage(market, config.annual_start_year - 1, config.annual_end_year)
        + check_controls(controls)
    )
    for index, control in enumerate(controls, 2):
        source_id = control.get("sourceId", "")
        if source_id not in config.manual_control_source_ids:
            issues.append(
                Issue(
                    "controls.source_role",
                    "ERROR",
                    "sourceId is not registered as manual reviewed political-control evidence",
                    f"row {index}",
                )
            )
    errors = _errors(issues)
    if errors:
        message = "; ".join(f"{item.check_id}: {item.message}" for item in errors[:5])
        raise BuildError(f"Cannot build research dataset: {message}")
    market_source_ids = {str(row.get("source_id", "")) for row in market}
    if len(market_source_ids) != 1:
        raise BuildError("Monthly market table must contain one declared source ID")
    market_source_id = next(iter(market_source_ids))
    if market_source_id != definition.expected_source_id:
        raise BuildError(
            f"Reconstruction {definition.key} requires market source {definition.expected_source_id}, "
            f"not {market_source_id or 'a blank source ID'}"
        )
    levels = _monthly_levels(market)
    level_dates = {row["date"] for row in levels}
    windows: list[dict[str, object]] = []
    required_decisions: list[str] = []
    for year in range(config.annual_start_year, config.annual_end_year + 1):
        start = date(year - 1, 12, 31).isoformat()
        end = date(year, 12, 31).isoformat()
        if start not in level_dates or end not in level_dates:
            raise BuildError(
                f"Required exact December endpoint unavailable for annual return {year}"
            )
        decision = start if decision_policy == "window_start" else date(year, 1, 1).isoformat()
        required_decisions.append(decision)
        windows.append(
            {
                "id": f"annual-{year}",
                "label": str(year),
                "kind": "CALENDAR_YEAR",
                "startDate": start,
                "endDate": end,
                "decisionDate": decision,
                "complete": True,
            }
        )
    missing_controls = [
        decision for decision in required_decisions if _control_at(controls, decision) is None
    ]
    if missing_controls:
        raise BuildError(
            f"No reviewed political control interval covers decision date(s): {', '.join(missing_controls[:10])}"
        )
    inputs: dict[str, object] = {
        "market_sha256": sha256_file(market_path),
        "controls_sha256": sha256_file(controls_path),
        "config_sha256": sha256_file(config.path),
    }
    parse_status_path = market_path.parent / "parse_status.json"
    raw_manifest_path = market_path.parent.parent / "raw" / "manifest.json"
    if parse_status_path.exists():
        inputs["market_parse_status_sha256"] = sha256_file(parse_status_path)
    if raw_manifest_path.exists():
        inputs["raw_fetch_manifest_sha256"] = sha256_file(raw_manifest_path)
    controls_manifest_path = controls_path.with_suffix(".manifest.json")
    if controls_manifest_path.exists():
        controls_manifest = read_json(controls_manifest_path, {})
        inputs["controls_promotion_manifest_sha256"] = sha256_file(controls_manifest_path)
        if isinstance(controls_manifest, dict):
            for key in ("review_workbook", "review_workbook_sha256"):
                if controls_manifest.get(key):
                    inputs[key] = controls_manifest[key]
    if additional_inputs:
        inputs.update(additional_inputs)
    sources = _source_catalog(config, market_source_id, controls)
    for source in additional_sources or []:
        source_id = str(source.get("id", ""))
        if not source_id or any(record["id"] == source_id for record in sources):
            raise BuildError(
                f"Additional source has a missing or duplicate ID: {source_id or 'blank'}"
            )
        sources.append(source)
    dataset = {
        "schemaVersion": 1,
        "id": f"{definition.dataset_id_prefix}-{config.annual_start_year}-{config.annual_end_year}{dataset_id_suffix}",
        "title": f"{definition.title_prefix}, {config.annual_start_year}–{config.annual_end_year}{title_suffix}",
        "kind": "USER_SUPPLIED",
        "currency": config.currency,
        "nominalReal": "NOMINAL",
        "returnBasis": definition.return_basis,
        "sources": sources,
        "levels": levels,
        "controls": controls,
        "windows": windows,
        "fundamentals": fundamentals or [],
    }
    manifest = {
        "schema_version": 1,
        "dataset_kind": definition.dataset_kind,
        "reconstruction": definition.key,
        "market_source_id": market_source_id,
        "construction": definition.construction,
        "exact_replication": False,
        "reason": "The seed article's annual S&P 500 inputs and political assignment convention are unavailable.",
        "decision_policy": decision_policy,
        "inputs": inputs,
        "dataset_sha256": __import__("hashlib")
        .sha256(canonical_json(dataset).encode())
        .hexdigest(),
        "quality_issues": [issue.as_dict() for issue in issues],
    }
    return dataset, manifest


def _annual_return(levels: dict[str, float], window: dict[str, object]) -> float:
    start = str(window["startDate"])
    end = str(window["endDate"])
    try:
        return levels[end] / levels[start] - 1.0
    except KeyError as exc:
        raise BuildError(f"Dataset has no level for comparison endpoint {exc.args[0]}") from exc


def _pearson(first: list[float], second: list[float]) -> float | None:
    if len(first) < 2 or len(first) != len(second):
        return None
    first_mean = sum(first) / len(first)
    second_mean = sum(second) / len(second)
    numerator = sum(
        (left - first_mean) * (right - second_mean)
        for left, right in zip(first, second, strict=False)
    )
    left_scale = math.sqrt(sum((left - first_mean) ** 2 for left in first))
    right_scale = math.sqrt(sum((right - second_mean) ** 2 for right in second))
    if not left_scale or not right_scale:
        return None
    return numerator / (left_scale * right_scale)


def _comparison_summary(rows: list[dict[str, object]]) -> dict[str, float | int | None]:
    primary = [float(row["crspMarketProxyReturnDecimal"]) for row in rows]
    robustness = [float(row["shillerSpCompositeApproximationReturnDecimal"]) for row in rows]
    difference = [float(row["differenceDecimal"]) for row in rows]
    if not primary:
        return {
            "observations": 0,
            "crsp_market_proxy_mean": None,
            "shiller_approximation_mean": None,
            "mean_difference": None,
            "median_difference": None,
            "pearson_correlation": None,
        }
    return {
        "observations": len(rows),
        "crsp_market_proxy_mean": sum(primary) / len(primary),
        "shiller_approximation_mean": sum(robustness) / len(robustness),
        "mean_difference": sum(difference) / len(difference),
        "median_difference": median(difference),
        "pearson_correlation": _pearson(primary, robustness),
    }


def build_public_series_comparison(
    config: PipelineConfig,
    crsp_market_path: Path,
    shiller_context_path: Path,
    controls_path: Path,
    decision_policy: Literal["window_start", "calendar_year_start"],
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    list[dict[str, str]],
    dict[str, object],
]:
    """Build both public series and an aligned, descriptive comparison."""
    first_context_date = date(config.annual_start_year - 2, 12, 1)
    shiller_context = read_csv(shiller_context_path)
    shiller_returns = derive_shiller_monthly_total_return(
        shiller_context, first_required_date=first_context_date
    )
    available_end = date.fromisoformat(shiller_returns[-1]["date"])
    if available_end.month != 12:
        raise BuildError("Shiller reconstruction must end at a complete December observation")
    robustness_config = replace(config, annual_end_year=available_end.year)
    primary, primary_manifest = build_annual_dataset(
        config, crsp_market_path, controls_path, decision_policy, reconstruction="crsp_market_proxy"
    )
    # The donor wrote an intermediate beside the caller's input, potentially
    # overwriting it. Keep the derived bytes private and return them to the
    # caller for deliberate publication instead.
    with TemporaryDirectory(prefix="presidential-shiller-") as temporary:
        normalized = Path(temporary) / "normalized"
        normalized.mkdir()
        shiller_return_path = normalized / "shiller_total_return_monthly.csv"
        write_monthly_returns(shiller_return_path, shiller_returns)
        robustness, robustness_manifest = build_annual_dataset(
            robustness_config,
            shiller_return_path,
            controls_path,
            decision_policy,
            reconstruction="shiller_sp_composite_approximation",
        )
    # Preserve the donor's evidenced parse/capture hashes from the actual source
    # directory rather than mistaking the temporary intermediate for an archive.
    source_metadata = {
        "market_parse_status_sha256": shiller_context_path.parent / "parse_status.json",
        "raw_fetch_manifest_sha256": shiller_context_path.parent.parent / "raw" / "manifest.json",
    }
    for key, path in source_metadata.items():
        if path.exists():
            robustness_manifest["inputs"][key] = sha256_file(path)
    primary_levels = {str(row["date"]): float(row["value"]) for row in primary["levels"]}  # type: ignore[index]
    robustness_levels = {str(row["date"]): float(row["value"]) for row in robustness["levels"]}  # type: ignore[index]
    controls = [dict(row) for row in primary["controls"]]  # type: ignore[index]
    rows: list[dict[str, object]] = []
    primary_windows = primary["windows"]  # type: ignore[index]
    robustness_windows = robustness["windows"]  # type: ignore[index]
    if list(primary_windows[: len(robustness_windows)]) != list(robustness_windows):
        raise BuildError("Public-series reconstructions produced incompatible annual windows")
    for primary_window, robustness_window in zip(primary_windows, robustness_windows, strict=False):
        if primary_window["id"] != robustness_window["id"]:  # type: ignore[index]
            raise BuildError("Public-series reconstructions produced different annual windows")
        primary_return = _annual_return(primary_levels, primary_window)  # type: ignore[arg-type]
        robustness_return = _annual_return(robustness_levels, robustness_window)  # type: ignore[arg-type]
        control = _control_at(controls, str(primary_window["decisionDate"]))  # type: ignore[arg-type]
        if control is None:
            raise BuildError(f"No control for comparison window {primary_window['id']}")
        rows.append(
            {
                "year": int(str(primary_window["label"])),
                "decisionDate": primary_window["decisionDate"],
                "crspMarketProxyReturnDecimal": primary_return,
                "shillerSpCompositeApproximationReturnDecimal": robustness_return,
                "differenceDecimal": robustness_return - primary_return,
                "presidentParty": control["presidentParty"],
                "houseControl": control["houseControl"],
                "senateControl": control["senateControl"],
                "controlSourceId": control["sourceId"],
            }
        )
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        key = f"{row['presidentParty']} president / {row['houseControl']} House / {row['senateControl']} Senate"
        grouped.setdefault(key, []).append(row)
    context_dates = [row.get("date", "") for row in shiller_context]
    missing_dividend_dates = [
        row.get("date", "")
        for row in shiller_context
        if not row.get("dividend", "").strip() and row.get("date", "") > available_end.isoformat()
    ]
    comparison = {
        "schema_version": 1,
        "kind": "PUBLIC_SERIES_ROBUSTNESS_COMPARISON",
        "methodology_id": "PID-SERIES-001",
        "exact_replication": False,
        "decision_policy": decision_policy,
        "primary": {"reconstruction": "crsp_market_proxy", "source_id": "FF_MKT_001"},
        "robustness": {
            "reconstruction": "shiller_sp_composite_approximation",
            "source_id": "SHILLER_001",
        },
        "overlap": {
            "first_annual_year": config.annual_start_year,
            "last_annual_year": available_end.year,
            "primary_available_through": config.annual_end_year,
            "robustness_available_through": available_end.year,
        },
        "robustness_source_completeness": {
            "last_shiller_context_date": max(context_dates),
            "last_complete_dividend_december": available_end.isoformat(),
            "excluded_incomplete_dividend_months": missing_dividend_dates,
        },
        "summary": _comparison_summary(rows),
        "by_political_configuration": [
            {"configuration": key, **_comparison_summary(grouped[key])} for key in sorted(grouped)
        ],
        "annual_rows": rows,
    }
    return primary, primary_manifest, robustness, robustness_manifest, shiller_returns, comparison


def write_dataset(output: Path, dataset: dict[str, object], manifest: dict[str, object]) -> None:
    write_json(output, dataset)
    write_json(output.with_suffix(".manifest.json"), manifest)


def write_monthly_returns(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "market_return_decimal", "source_id"])
        writer.writeheader()
        writer.writerows(rows)


def write_public_series_comparison(output_directory: Path, comparison: dict[str, object]) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)
    write_json(output_directory / "public-series-comparison.json", comparison)
    fields = [
        "year",
        "decisionDate",
        "crspMarketProxyReturnDecimal",
        "shillerSpCompositeApproximationReturnDecimal",
        "differenceDecimal",
        "presidentParty",
        "houseControl",
        "senateControl",
        "controlSourceId",
    ]
    with (output_directory / "public-series-comparison.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(comparison["annual_rows"])  # type: ignore[arg-type]


def write_inherited_conditions_ledger(
    output_directory: Path,
    comparison: dict[str, object],
    fundamental_audit: dict[str, object],
) -> None:
    """Write the inspectable paired-return ledger for the frozen extension."""
    audit_rows = {int(row["year"]): row for row in fundamental_audit["rows"]}  # type: ignore[index]
    rows: list[dict[str, object]] = []
    for comparison_row in comparison["annual_rows"]:  # type: ignore[index]
        year = int(comparison_row["year"])
        audit_row = audit_rows.get(year)
        if audit_row is None:
            continue
        fundamental = audit_row["fundamental"]
        house = str(comparison_row["houseControl"])
        senate = str(comparison_row["senateControl"])
        congress = f"{house} Congress" if house == senate else "Split Congress"
        rows.append(
            {
                "year": year,
                "decisionDate": comparison_row["decisionDate"],
                "politicalConfiguration": f"{comparison_row['presidentParty']} president · {congress}",
                "presidentParty": comparison_row["presidentParty"],
                "houseControl": house,
                "senateControl": senate,
                "cape": fundamental["valuation"],
                "capeObservationDate": audit_row["valuation_observation_date"],
                "capeAssumedAvailableDate": audit_row["valuation_assumed_available_date"],
                "policyRatePct": fundamental["policyRatePct"],
                "policyObservationDate": audit_row["policy_observation_date"],
                "policyAssumedAvailableDate": audit_row["policy_assumed_available_date"],
                "crspMarketProxyReturnDecimal": comparison_row["crspMarketProxyReturnDecimal"],
                "shillerSpCompositeApproximationReturnDecimal": comparison_row[
                    "shillerSpCompositeApproximationReturnDecimal"
                ],
                "seriesDifferenceDecimal": comparison_row["differenceDecimal"],
            }
        )
    if len(rows) != int(fundamental_audit["observations"]):
        raise BuildError(
            "Inherited-condition ledger rows do not reconcile to the joined fundamental sample"
        )
    output_directory.mkdir(parents=True, exist_ok=True)
    fields = [
        "year",
        "decisionDate",
        "politicalConfiguration",
        "presidentParty",
        "houseControl",
        "senateControl",
        "cape",
        "capeObservationDate",
        "capeAssumedAvailableDate",
        "policyRatePct",
        "policyObservationDate",
        "policyAssumedAvailableDate",
        "crspMarketProxyReturnDecimal",
        "shillerSpCompositeApproximationReturnDecimal",
        "seriesDifferenceDecimal",
    ]
    with (output_directory / "inherited-conditions-ledger.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _presidential_term_start_year(year: int) -> int | None:
    """Return the regular post-Twentieth-Amendment four-year term start.

    The cycle view is intentionally separate from the primary ``window_start``
    convention. It associates a calendar-year return with the president in
    office on that return year's December 31, then labels the year within the
    constitutional four-year cycle. The 1936 return predates the first
    January-20 term in the analysis sample and is excluded rather than forced
    into this convention.
    """
    if year < 1937:
        return None
    return year - ((year - 1) % 4)


def _cycle_summary(records: list[dict[str, object]], return_field: str) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int], list[dict[str, object]]] = {}
    for record in records:
        key = (str(record["endOfYearPresidentParty"]), int(record["termCycleYear"]))
        grouped.setdefault(key, []).append(record)
    output: list[dict[str, object]] = []
    for (party, cycle_year), rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda row: int(row["year"]))
        values = [float(row[return_field]) for row in ordered]
        average = sum(values) / len(values)
        dispersion = (
            math.sqrt(sum((value - average) ** 2 for value in values) / (len(values) - 1))
            if len(values) > 1
            else None
        )
        best = max(ordered, key=lambda row: (float(row[return_field]), -int(row["year"])))
        worst = min(ordered, key=lambda row: (float(row[return_field]), int(row["year"])))
        output.append(
            {
                "presidentParty": party,
                "termCycleYear": cycle_year,
                "observations": len(ordered),
                "administrations": len({str(row["endOfYearPresidentId"]) for row in ordered}),
                "meanReturnDecimal": average,
                "medianReturnDecimal": median(values),
                "sampleStandardDeviationDecimal": dispersion,
                "minimumReturnDecimal": min(values),
                "maximumReturnDecimal": max(values),
                "best": {
                    "year": best["year"],
                    "president": best["endOfYearPresidentName"],
                    "returnDecimal": best[return_field],
                },
                "worst": {
                    "year": worst["year"],
                    "president": worst["endOfYearPresidentName"],
                    "returnDecimal": worst[return_field],
                },
                "inaugurationTransitionWindows": sum(
                    bool(row["inaugurationTransition"]) for row in ordered
                ),
                "presidentChangedWithinWindow": sum(
                    bool(row["presidentChangedWithinWindow"]) for row in ordered
                ),
            }
        )
    return output


def build_presidential_cycle_analysis(
    comparison: dict[str, object],
    controls: list[dict[str, str]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Derive two display treatments for the same four-year cycle observations.

    Both views use the president in office at the calendar-year return's
    December 31 and a regular four-year cycle that begins in years ending in 1,
    5, and 9. ``inaugurationTransition`` provides the second display treatment:
    it flags every first-cycle-year return because the new regular term begins
    on January 20. A separate flag preserves midyear successions.
    Neither treatment replaces the primary start-of-window political grouping.
    """
    ledger: list[dict[str, object]] = []
    excluded_pre_1937: list[int] = []
    for comparison_row in comparison["annual_rows"]:  # type: ignore[index]
        year = int(comparison_row["year"])
        term_start_year = _presidential_term_start_year(year)
        if term_start_year is None:
            excluded_pre_1937.append(year)
            continue
        start_control = _control_at(controls, str(comparison_row["decisionDate"]))
        end_control = _control_at(controls, f"{year:04d}-12-31")
        if start_control is None or end_control is None:
            raise BuildError(
                f"No reviewed control interval covers the cycle-analysis endpoints for {year}"
            )
        ledger.append(
            {
                "year": year,
                "termStartYear": term_start_year,
                "termCycleYear": year - term_start_year + 1,
                "inaugurationTransition": year == term_start_year,
                "presidentChangedWithinWindow": start_control["presidentId"]
                != end_control["presidentId"],
                "endOfYearPresidentId": end_control["presidentId"],
                "endOfYearPresidentName": end_control["presidentName"],
                "endOfYearPresidentParty": end_control["presidentParty"],
                "crspMarketProxyReturnDecimal": comparison_row["crspMarketProxyReturnDecimal"],
                "shillerSpCompositeApproximationReturnDecimal": comparison_row[
                    "shillerSpCompositeApproximationReturnDecimal"
                ],
            }
        )
    if not ledger:
        raise BuildError("No annual returns are eligible for the presidential-cycle analysis")
    summary = {
        "schema_version": 1,
        "kind": "PRESIDENTIAL_TERM_CYCLE_DESCRIPTIVE",
        "assignment": {
            "primary_view": "All four calendar return years are assigned to the president in office on that return year's December 31.",
            "transition_flag_view": "The same observations are shown with every first-cycle-year return flagged as an inauguration transition, because the new regular term begins on January 20.",
            "succession_note": "A separate flag identifies windows in which the president changed between the annual window start and end, including midterm successions.",
            "not_primary_political_assignment": "This end-of-year cycle convention is a supplementary descriptive view and does not replace the frozen window_start political configuration analysis.",
        },
        "first_annual_year": int(ledger[0]["year"]),
        "last_annual_year": int(ledger[-1]["year"]),
        "observations": len(ledger),
        "excludedPre1937Years": excluded_pre_1937,
        "inaugurationTransitionWindows": sum(bool(row["inaugurationTransition"]) for row in ledger),
        "presidentChangedWithinWindow": sum(
            bool(row["presidentChangedWithinWindow"]) for row in ledger
        ),
        "series": [
            {
                "id": "crsp_market_proxy",
                "returnField": "crspMarketProxyReturnDecimal",
                "byPartyAndCycle": _cycle_summary(ledger, "crspMarketProxyReturnDecimal"),
            },
            {
                "id": "shiller_sp_composite_approximation",
                "returnField": "shillerSpCompositeApproximationReturnDecimal",
                "byPartyAndCycle": _cycle_summary(
                    ledger, "shillerSpCompositeApproximationReturnDecimal"
                ),
            },
        ],
    }
    return ledger, summary


def write_presidential_cycle_analysis(
    output_directory: Path,
    ledger: list[dict[str, object]],
    summary: dict[str, object],
) -> None:
    """Write the paired return ledger and source-independent descriptive summary."""
    output_directory.mkdir(parents=True, exist_ok=True)
    fields = [
        "year",
        "termStartYear",
        "termCycleYear",
        "inaugurationTransition",
        "presidentChangedWithinWindow",
        "endOfYearPresidentId",
        "endOfYearPresidentName",
        "endOfYearPresidentParty",
        "crspMarketProxyReturnDecimal",
        "shillerSpCompositeApproximationReturnDecimal",
    ]
    with (output_directory / "presidential-cycle-ledger.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(ledger)
    write_json(output_directory / "presidential-cycle-summary.json", summary)


def write_controls_template(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "id",
        "startDate",
        "endDate",
        "presidentId",
        "presidentName",
        "presidentParty",
        "houseControl",
        "senateControl",
        "sourceId",
    ]
    if path.exists():
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
