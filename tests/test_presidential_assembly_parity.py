"""PR016-R01: byte-stable parity to donor outputs built from invented inputs."""

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import test_presidential_build as fixture_helpers

from market_data_pipeline.pipelines.presidential_research.assemble import (
    build_inherited_fundamentals,
    build_presidential_cycle_analysis,
    build_public_series_comparison,
)
from market_data_pipeline.pipelines.presidential_research.quality import read_csv


def test_synthetic_assembly_matches_frozen_donor_goldens(tmp_path):
    case = fixture_helpers.BuildTests()
    case.setUp()
    market, shiller = tmp_path / "market.csv", tmp_path / "shiller.csv"
    controls, policy = tmp_path / "controls.csv", tmp_path / "policy.csv"
    case.write_market(market)
    case.write_shiller_context(shiller)
    case.write_controls(controls)
    case.write_policy_rates(policy)
    retained = tmp_path / "shiller_total_return_monthly.csv"
    retained.write_text("existing input must not be overwritten\n")
    primary, _, secondary, _, derived, comparison = build_public_series_comparison(
        case.config, market, shiller, controls, "window_start"
    )
    assert retained.read_text() == "existing input must not be overwritten\n"
    fundamentals, audit = build_inherited_fundamentals(
        replace(case.config, annual_end_year=2022), shiller, policy, 2022
    )
    ledger, summary = build_presidential_cycle_analysis(comparison, read_csv(controls))
    outputs = {
        "primary_dataset": primary,
        "shiller_dataset": secondary,
        "shiller_monthly": derived,
        "comparison": comparison,
        "fundamentals": fundamentals,
        "inherited_audit": audit,
        "cycle_ledger": ledger,
        "cycle_summary": summary,
    }
    fixture = json.loads(
        (Path(__file__).parent / "fixtures/presidential_research/assembly_goldens.json").read_text()
    )
    for name, value in outputs.items():
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(encoded.encode()).hexdigest() == fixture["hashes"][name], name
    assert fixture["counts"] == {
        "primary_windows": len(primary["windows"]),
        "shiller_windows": len(secondary["windows"]),
        "comparison": len(comparison["annual_rows"]),
        "fundamentals": len(fundamentals),
        "cycle_ledger": len(ledger),
    }


def test_candidate_controls_never_admit_research_dataset(tmp_path):
    import pytest

    from market_data_pipeline.pipelines.presidential_research.assemble import (
        BuildError,
        build_annual_dataset,
    )

    case = fixture_helpers.BuildTests()
    case.setUp()
    market, controls = tmp_path / "market.csv", tmp_path / "controls.csv"
    case.write_market(market)
    case.write_controls(controls, source_id="HOUSE_GOV_001")
    with pytest.raises(BuildError, match="controls.source_role"):
        build_annual_dataset(case.config, market, controls, "window_start")


def test_shiller_provenance_uses_original_archives_not_private_intermediate(tmp_path):
    case = fixture_helpers.BuildTests()
    case.setUp()
    normalized = tmp_path / "normalized"
    raw = tmp_path / "raw"
    normalized.mkdir()
    raw.mkdir()
    market, shiller = normalized / "market.csv", normalized / "shiller.csv"
    controls = tmp_path / "controls.csv"
    case.write_market(market)
    case.write_shiller_context(shiller)
    case.write_controls(controls)
    status = normalized / "parse_status.json"
    manifest = raw / "manifest.json"
    status.write_text("fixture status")
    manifest.write_text("fixture raw receipt")
    _, _, _, sidecar, _, _ = build_public_series_comparison(
        case.config, market, shiller, controls, "window_start"
    )
    assert (
        sidecar["inputs"]["market_parse_status_sha256"]
        == hashlib.sha256(status.read_bytes()).hexdigest()
    )
    assert (
        sidecar["inputs"]["raw_fetch_manifest_sha256"]
        == hashlib.sha256(manifest.read_bytes()).hexdigest()
    )
    assert not (normalized / "shiller_total_return_monthly.csv").exists()
