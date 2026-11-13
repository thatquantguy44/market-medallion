"""SSV-AC10: a bounded, verified source projection from one SQLite snapshot."""

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_gold_query import load as load
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import FIELDS
from market_data_pipeline.pipelines.finra_short_sale_volume.io.snapshot import collect

__all__ = ["manifest", "load"]


def snapshot(root, manifest, **kwargs):
    args = dict(
        approved_use=fixture_grant(),
        operation="fixture",
        trade_date="2026-11-09",
        scopes=["nyse_trf"],
    )
    args.update(kwargs)
    return collect(root, manifest, **args)


def test_ssv_ac10_projection_preserves_typed_history_research_audit_and_lineage(
    tmp_path, manifest, load
):
    load("2026-11-06", values=[("SYN", "20", "0", "100")])
    load()
    latest = load(values=[("SYN", "50", "5", "100")])
    before = {
        str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
    }
    result = snapshot(tmp_path, manifest)
    tables = result["tables"]
    assert set(tables) == set(FIELDS)
    assert {n: len(v) for n, v in tables.items()} == dict(
        silver=3, gold=1, research=1, coverage=2, audit=3, captures=3, membership=3
    )
    assert (
        tables["gold"][0]["short_volume"] == "50"
        and tables["gold"][0]["observed_capture_id"] == latest["capture_id"]
    )
    assert tables["research"][0]["short_share_change"] == "0.3"
    assert tables["research"][0]["calendar_day_gap"] == 3
    assert result["research"]["prior_trade_date"] == "2026-11-06"
    assert {
        str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
    } == before


def test_ssv_ac10_projection_cutoff_preserves_original_version_membership(tmp_path, manifest, load):
    old = load()
    load(values=[("SYN", "50", "5", "100")])
    result = snapshot(tmp_path, manifest, as_of=NOW, as_of_sequence=old["capture_sequence"])
    assert len(result["tables"]["silver"]) == len(result["tables"]["captures"]) == 1
    assert result["tables"]["gold"][0]["short_volume"] == "40"


def test_ssv_ac10_projection_retains_removal_and_valid_empty_without_zero_rows(
    tmp_path, manifest, load
):
    load()
    load(values=[])
    result = snapshot(tmp_path, manifest)
    assert (
        len(result["tables"]["silver"]) == 2 and result["tables"]["silver"][-1]["removed"] is True
    )
    assert result["tables"]["gold"] == result["tables"]["research"] == []
    assert result["tables"]["coverage"][0]["committed_rows"] == 0
    assert result["research"]["status"] == "VALID_EMPTY"


def test_ssv_ac10_incomplete_population_refuses_before_publication_artifacts(
    tmp_path, manifest, load
):
    load()
    with pytest.raises(ValueError, match="complete current"):
        snapshot(tmp_path, manifest, scopes=["listed_trfs"])
    assert not (tmp_path / "parquet").exists()


@pytest.mark.parametrize("limits", [{"max_rows": 1}, {"max_bytes": 10}])
def test_ssv_ac10_projection_preflight_bounds_before_document_loading(
    tmp_path, manifest, load, monkeypatch, limits
):
    load()
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse

    monkeypatch.setattr(
        warehouse, "version_document", lambda *a: pytest.fail("oversized state loaded")
    )
    with pytest.raises(ValueError, match="budget"):
        snapshot(tmp_path, manifest, **limits)


def test_ssv_ac10_projection_excludes_other_dates_and_facilities(tmp_path, manifest, load):
    load("2020-01-01")
    load(facility="nasdaq_carteret_trf")
    load()
    result = snapshot(tmp_path, manifest)
    assert len(result["tables"]["captures"]) == 1
    assert all(v["facility_id"] == "nyse_trf" for v in result["tables"]["silver"])
