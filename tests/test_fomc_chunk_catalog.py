"""FOMC-AC07: observed-version selection, profile consistency and complete generations."""

import pytest
from test_fomc_text_catalog import catalog_input as catalog_input

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.catalog import build_catalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunk_catalog import build_generation
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunks import chunk_text

__all__ = ["catalog_input"]


@pytest.fixture
def generation_input(catalog_input):
    inventory, records = catalog_input
    base = build_catalog(
        inventory, records, inventory_run_id="00000000-0000-0000-0000-000000000099"
    )
    results = {r["extraction"]["extraction_id"]: r["extraction"] for r in records}
    sets = [
        chunk_text(results[v["extraction_ids"][0]], document_version_id=v["version_id"])
        for r in base["representations"]
        for v in r["versions"]
    ]
    return base, sets, results


def test_complete_generation_preserves_observed_versions_gaps_and_current_selection(
    generation_input,
):
    base, sets, results = generation_input
    value = build_generation(base, sets)
    assert len(value["selections"]) == 3
    assert sum(s["current"] for s in value["selections"]) == 1
    assert value["upstream_gaps"] == base["gaps"]
    assert value["coverage"]["complete_selected_unit_coverage"]
    assert build_generation(base, list(reversed(sets))) == value


def test_missing_or_duplicate_observed_version_cannot_publish(generation_input):
    base, sets, results = generation_input
    with pytest.raises(ValueError, match="missing"):
        build_generation(base, sets[:-1])
    with pytest.raises(ValueError, match="one selected"):
        build_generation(base, sets + [sets[0]])


def test_mixed_profiles_and_wrong_extraction_are_refused(generation_input):
    base, sets, results = generation_input
    changed = chunk_text(
        results[sets[0]["extraction_id"]],
        document_version_id=sets[0]["document_version_id"],
        max_tokens=25,
        overlap_tokens=3,
    )
    with pytest.raises(ValueError, match="one chunker profile"):
        build_generation(base, [changed] + sets[1:])
