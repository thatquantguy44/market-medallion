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
