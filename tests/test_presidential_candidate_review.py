"""PR016-R03: candidate review artifacts never become approved controls."""

# ruff: noqa: E501

from datetime import UTC, datetime

import pytest

from market_data_pipeline.pipelines.presidential_research.candidate_review import prepare_candidates
from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture
from market_data_pipeline.pipelines.presidential_research.warehouse import ingest_capture

NOW = datetime(2026, 11, 13, tzinfo=UTC)
CONFIG = load_config("config/presidential_research/sources.toml")
HOUSE = b"""<table><tr><th>Congress</th><th>House</th><th>Senate</th><th>Presidency</th></tr>
<tr><td>74th (1935-1937)</td><td>Democrats</td><td>Tied</td><td>Democrat (Fictional)</td></tr></table>"""
WIKI = b"""<table><tr><th>Congress</th><th>Years</th><th>Total</th><th>Democrats</th><th>Republicans</th><th>Others</th><th>Vacancies</th><th>Total</th><th>Democrats</th><th>Republicans</th><th>Others</th><th>Vacancies</th><th>President</th><th>Trifecta</th></tr>
<tr><td>74th</td><td>1935-1937</td><td>96</td><td>69/68</td><td>25</td><td>2</td><td>&mdash;</td><td>435</td><td>322</td><td>103</td><td>10</td><td>&mdash;</td><td>Fictional</td><td>Yes</td></tr></table>"""


def test_review_candidates_keep_secondary_counts_and_are_never_controls(tmp_path):
    captures = {}
    for source, payload in (("HOUSE_GOV_001", HOUSE), ("WIKI_CONGRESS_001", WIKI)):
        receipt, _ = import_local_capture(
            tmp_path, CONFIG, source, payload, input_kind="SYNTHETIC_FIXTURE", admitted_at=NOW
        )
        assert ingest_capture(tmp_path, source, receipt["capture_id"])["status"] == "PARSED"
        captures[source] = receipt["capture_id"]
    rows, manifest = prepare_candidates(
        tmp_path, captures["HOUSE_GOV_001"], captures["WIKI_CONGRESS_001"]
    )
    assert len(rows) == 1 and rows[0]["congress"] == "74"
    assert rows[0]["senateControlCandidate"] == "TIED"
    assert rows[0]["wikipediaVariationHint"] == "TRUE"
    assert "mid-Congress changes" in rows[0]["reviewRequired"]
    assert "startDate" not in rows[0]
    assert manifest["controls_file_eligible"] is False
    assert manifest["input_kind"] == "SYNTHETIC_FIXTURE"
    with pytest.raises(ValueError):
        prepare_candidates(tmp_path, captures["WIKI_CONGRESS_001"], captures["HOUSE_GOV_001"])
