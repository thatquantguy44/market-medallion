"""PR016-R01/R02: independent arithmetic and pinned fictional donor fixture."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures/presidential_research/baseline.json"


def test_invented_baseline_digest_and_independent_expected_values():
    fixture = json.loads(FIXTURE.read_text())
    assert (
        hashlib.sha256(fixture["invented_factor_table"].encode()).hexdigest()
        == fixture["invented_factor_table_sha256"]
    )
    assert (Decimal("2.00") + Decimal("0.10")) / 100 == Decimal(
        fixture["expected_factors"][0]["market_return_decimal"]
    )
    assert (Decimal("-1.00") + Decimal("0.05")) / 100 == Decimal(
        fixture["expected_factors"][1]["market_return_decimal"]
    )
    data = fixture["invented_shiller"]
    assert (Decimal(data["price"]) + Decimal(data["annual_dividend"]) / 12) / Decimal(
        data["prior_price"]
    ) - 1 == Decimal(data["expected_monthly_return"])
    assert len(fixture["source_file_sha256"]) == 10


def test_port_matches_donor_golden_factors_and_preserves_catalog_roles():
    import io
    from zipfile import ZipFile

    from market_data_pipeline.pipelines.presidential_research.config import load_config
    from market_data_pipeline.pipelines.presidential_research.parsers import (
        parse_fama_french_monthly_zip,
    )

    fixture = json.loads(FIXTURE.read_text())
    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("fixture.csv", fixture["invented_factor_table"])
    assert (
        parse_fama_french_monthly_zip(buffer.getvalue(), "FF_MKT_001")
        == fixture["expected_factors"]
    )
    config = load_config(Path(__file__).parents[1] / "config/presidential_research/sources.toml")
    assert {source.id for source in config.sources} == {
        "FF_MKT_001",
        "FEDFUNDS_001",
        "SHILLER_001",
        "HOUSE_GOV_001",
        "WIKI_CONGRESS_001",
        "CTRL_074_A",
        "CTRL_GOV_001",
    }
    assert config.manual_control_source_ids == {"CTRL_074_A", "CTRL_GOV_001"}
    assert all(not config.source(source).enabled for source in config.manual_control_source_ids)
    assert config.source("WIKI_CONGRESS_001").role == "political_control_cross_check"
