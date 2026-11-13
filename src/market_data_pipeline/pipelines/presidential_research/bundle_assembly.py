"""Assemble all research views from explicitly selected, admitted captures.

This step builds in memory and private temporary files; it does not publish.
"""

import csv
import hashlib
import re
import tempfile
from dataclasses import replace
from pathlib import Path

from .assemble import (
    build_annual_dataset,
    build_inherited_fundamentals,
    build_presidential_cycle_analysis,
    build_public_series_comparison,
    inherited_fundamentals_source,
    write_inherited_conditions_ledger,
    write_monthly_returns,
    write_presidential_cycle_analysis,
    write_public_series_comparison,
)
from .config import load_config
from .dataset import parse_dataset_bytes
from .frozen import PROFILE, validate_frozen_inputs
from .provenance import canonical_json, write_json
from .quality import read_csv
from .review import load_review_version
from .storage import load_capture
from .warehouse import admitted_capture_facts

_REQUIRED = {"FF_MKT_001", "SHILLER_001", "FEDFUNDS_001"}


def _csv(path, fields, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row.get(field) for field in fields} for row in rows)


def _digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_full_bundle(root, selected_captures, review_id, *, profile_id):
    """Require one admitted capture per source and a matching reviewed control set."""
    if not isinstance(selected_captures, dict) or set(selected_captures) != _REQUIRED:
        raise ValueError("Select exactly the three required source captures.")
    if not isinstance(profile_id, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,79}", profile_id):
        raise ValueError("Invalid profile.")
    review, controls, controls_manifest = load_review_version(root, review_id)
    receipts, records, fact_inventory, config_bytes = {}, {}, {}, None
    for source_id in sorted(_REQUIRED):
        receipt, facts = admitted_capture_facts(root, source_id, selected_captures[source_id])
        _, _, captured_config = load_capture(root, source_id, selected_captures[source_id])
        if config_bytes is None:
            config_bytes = captured_config
        elif config_bytes != captured_config:
            raise ValueError("Selected source configurations do not match.")
        receipts[source_id], records[source_id] = receipt, [fact["values"] for fact in facts]
        versions = [
            {key: fact[key] for key in ("fact_type", "fact_key", "version_id")} for fact in facts
        ]
        fact_inventory[source_id] = {
            "capture_id": receipt["capture_id"],
            "capture_sequence": receipt["capture_sequence"],
            "count": len(versions),
            "versions_sha256": _digest(canonical_json(versions)),
        }
    input_kinds = {receipt["input_kind"] for receipt in receipts.values()}
    if len(input_kinds) != 1:
        raise ValueError("Selected input kinds do not match.")
    input_kind = next(iter(input_kinds))
    fixture = input_kind == "SYNTHETIC_FIXTURE"
    if fixture != (review["evidence_kind"] == "SYNTHETIC_FIXTURE"):
        raise ValueError("Selected input and review classifications do not match.")
    if profile_id == PROFILE:
        validate_frozen_inputs(root, receipts, review)
    elif (fixture and not profile_id.startswith("fixture-")) or (
        not fixture and not profile_id.startswith("local-")
    ):
        raise ValueError("profile is not admitted for these selected inputs.")
    if review["config_sha256"] != next(iter(receipts.values()))["config_sha256"]:
        raise ValueError("Review and source configuration digests do not match.")
    with tempfile.TemporaryDirectory(prefix="presidential-complete-") as temporary:
        work = Path(temporary)
        config_path = work / "sources.toml"
        config_path.write_bytes(config_bytes)
        config = load_config(config_path)
        market, shiller, policy = work / "market.csv", work / "shiller.csv", work / "policy.csv"
        controls_path = work / "controls.csv"
        controls_path.write_bytes(controls)
        write_json(work / "controls.manifest.json", controls_manifest)
        _csv(market, ["date", "market_return_decimal", "source_id"], records["FF_MKT_001"])
        _csv(shiller, ["date", "price", "dividend", "cape", "source_id"], records["SHILLER_001"])
        policy_rows = [
            row for row in records["FEDFUNDS_001"] if row["policy_rate_percent"] is not None
        ]
        _csv(policy, ["date", "policy_rate_percent", "source_id"], policy_rows)
        primary, primary_manifest, robustness, robustness_manifest, derived, comparison = (
            build_public_series_comparison(config, market, shiller, controls_path, "window_start")
        )
        overlap_end = int(comparison["overlap"]["last_annual_year"])
        if overlap_end > config.annual_end_year:
            raise ValueError("Common sample exceeds the selected profile.")
        common, common_manifest = build_annual_dataset(
            replace(config, annual_end_year=overlap_end), market, controls_path, "window_start"
        )
        fundamentals, audit = build_inherited_fundamentals(config, shiller, policy, overlap_end)
        audit_path = work / "inherited-conditions-join-audit.json"
        write_json(audit_path, audit)
        inherited_config = replace(
            config, annual_start_year=int(audit["first_annual_year"]), annual_end_year=overlap_end
        )
        inherited_source = inherited_fundamentals_source(shiller, policy)
        inherited_inputs = {
            "inherited_conditions_protocol": "fictional-fixture"
            if fixture
            else "local-inherited-conditions",
            "inherited_conditions_join_audit_sha256": hashlib.sha256(
                audit_path.read_bytes()
            ).hexdigest(),
            "shiller_normalized_sha256": hashlib.sha256(shiller.read_bytes()).hexdigest(),
            "policy_rate_normalized_sha256": hashlib.sha256(policy.read_bytes()).hexdigest(),
        }
        derived_path = work / "shiller_total_return_monthly.csv"
        write_monthly_returns(derived_path, derived)
        primary_inherited, primary_inherited_manifest = build_annual_dataset(
            inherited_config,
            market,
            controls_path,
            "window_start",
            fundamentals=fundamentals,
            additional_sources=[inherited_source],
            additional_inputs=inherited_inputs,
            dataset_id_suffix="-inherited-conditions",
            title_suffix=" with frozen inherited conditions"
            if profile_id == PROFILE
            else " with inherited conditions",
        )
        robustness_inherited, robustness_inherited_manifest = build_annual_dataset(
            inherited_config,
            derived_path,
            controls_path,
            "window_start",
            reconstruction="shiller_sp_composite_approximation",
            fundamentals=fundamentals,
            additional_sources=[inherited_source],
            additional_inputs=inherited_inputs,
            dataset_id_suffix="-inherited-conditions",
            title_suffix=" with frozen inherited conditions"
            if profile_id == PROFILE
            else " with inherited conditions",
        )
        if common["windows"] != robustness["windows"]:
            raise ValueError("Common-sample windows do not match.")
        if (
            primary_inherited["windows"] != robustness_inherited["windows"]
            or primary_inherited["fundamentals"] != robustness_inherited["fundamentals"]
        ):
            raise ValueError("Inherited-condition samples do not match.")
        if len(comparison["annual_rows"]) != len(common["windows"]):
            raise ValueError("Comparison and common-sample counts do not match.")
        datasets = {
            "crsp-market-proxy-annual.json": (primary, primary_manifest),
            "crsp-market-proxy-common-sample-annual.json": (common, common_manifest),
            "shiller-sp-composite-annual.json": (robustness, robustness_manifest),
            "crsp-market-proxy-inherited-conditions-annual.json": (
                primary_inherited,
                primary_inherited_manifest,
            ),
            "shiller-sp-composite-inherited-conditions-annual.json": (
                robustness_inherited,
                robustness_inherited_manifest,
            ),
        }
        if profile_id == PROFILE:
            expected = (88, 87, 87, 68, 68)
            if tuple(len(dataset["windows"]) for dataset, _ in datasets.values()) != expected:
                raise ValueError("Frozen dataset sample counts do not match profile.")
        for dataset, manifest in datasets.values():
            if fixture:
                dataset["title"] = "Fictional fixture — " + dataset["title"]
                for source in dataset["sources"]:
                    source.update(
                        title="Fictional fixture: " + source["id"],
                        locator=None,
                        note="Invented test inputs; no real source observations.",
                    )
                manifest["dataset_kind"] = "SYNTHETIC_FIXTURE"
                manifest["construction"] = "Fictional inputs; " + manifest["construction"]
                manifest["reason"] = "Invented test data; not historical evidence."
            manifest["input_kind"] = input_kind
            manifest["dataset_sha256"] = _digest(canonical_json(dataset))
            parse_dataset_bytes(canonical_json(dataset).encode())
        write_public_series_comparison(work, comparison)
        write_inherited_conditions_ledger(work, comparison, audit)
        cycle_ledger, cycle_summary = build_presidential_cycle_analysis(
            comparison, read_csv(controls_path)
        )
        write_presidential_cycle_analysis(work, cycle_ledger, cycle_summary)
        if profile_id == PROFILE and (
            len(cycle_ledger) != 86 or cycle_summary["observations"] != 86
        ):
            raise ValueError("Frozen cycle sample does not match profile.")
        names = (
            "public-series-comparison.json",
            "public-series-comparison.csv",
            "inherited-conditions-join-audit.json",
            "inherited-conditions-ledger.csv",
            "presidential-cycle-ledger.csv",
            "presidential-cycle-summary.json",
            "shiller_total_return_monthly.csv",
        )
        artifacts = {name: (work / name).read_bytes() for name in names}
    return {
        "input_kind": input_kind,
        "profile_id": profile_id,
        "receipts": receipts,
        "fact_inventory": fact_inventory,
        "review": review,
        "datasets": datasets,
        "artifacts": artifacts,
        "comparison": comparison,
        "cycle_summary": cycle_summary,
    }
