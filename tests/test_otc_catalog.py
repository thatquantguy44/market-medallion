"""OTC-AC02: disjoint detail populations and explicit finite Monday partitions."""

from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import (
    Partition,
    profile_for,
    select_partitions,
)
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import (
    load_manifest,
    manifest_from_dict,
)

MANIFEST = Path(__file__).resolve().parents[1] / "manifests/finra-otc-transparency.yml"


def selection(week="2026-10-19", product="ATS_WEEKLY_DETAIL", tier="T1"):
    return {"reporting_week_start": week, "product": product, "tier": tier}


def test_all_six_products_and_tiers_have_distinct_scopes_and_truthful_status():
    manifest = load_manifest(MANIFEST)
    assert {(p.product, p.tier) for p in manifest.profiles} == {
        ("ATS_WEEKLY_DETAIL", "T1"),
        ("ATS_WEEKLY_DETAIL", "T2"),
        ("ATS_WEEKLY_DETAIL", "OTCE"),
        ("NON_ATS_WEEKLY_DETAIL", "T1"),
        ("NON_ATS_WEEKLY_DETAIL", "T2"),
        ("NON_ATS_WEEKLY_DETAIL", "OTCE"),
    }
    assert len({p.scope_id for p in manifest.profiles}) == 6
    assert all(p.conformance == "NOT_CHECKED" for p in manifest.profiles)
    assert [p.expected_delay_weeks for p in manifest.profiles if p.tier == "T1"] == [2, 2]
    assert all(p.expected_delay_weeks == 4 for p in manifest.profiles if p.tier != "T1")
    implemented = [p for p in manifest.profiles if p.parser_status == "INVENTED_FIXTURE_ONLY"]
    assert len(implemented) == 6


def test_explicit_multiweek_partition_selection_preserves_population_identity():
    values = [selection(), selection("2026-10-26"), selection(product="NON_ATS_WEEKLY_DETAIL")]
    partitions = select_partitions(values)
    assert len(partitions) == 3
    assert partitions[0].reporting_week_start == date(2026, 10, 19)
    assert partitions[0] != partitions[1] != partitions[2]
    assert partitions[0].scope_id != partitions[2].scope_id
    assert values[0] == selection()


@pytest.mark.parametrize(
    "product", ["ATS_W_SMBL", "ATS_W_FIRM", "MONTHLY", "BLOCKS", "TOTALS", "ATS", None, []]
)
def test_summary_monthly_block_or_unknown_products_cannot_join_detail(product):
    with pytest.raises(ValueError):
        select_partitions([selection(product=product)])


@pytest.mark.parametrize("tier", ["NMS", "OTC", "ALL", "T3", None, []])
def test_unknown_or_overlapping_tier_aliases_are_rejected(tier):
    with pytest.raises(ValueError):
        profile_for("ATS_WEEKLY_DETAIL", tier)


@pytest.mark.parametrize(
    "week",
    ["2026-10-20", "20261019", "2026-2-02", "2026-02-30", "2026-10-19T00:00:00Z", None, 20261019],
)
def test_partition_weeks_are_literal_valid_monday_dates(week):
    with pytest.raises(ValueError):
        select_partitions([selection(week)])


@pytest.mark.parametrize("values", [[], None, "all", {}, [None], [dict(selection(), extra=True)]])
def test_selection_is_explicit_nonempty_and_structurally_strict(values):
    with pytest.raises(ValueError):
        select_partitions(values)


def test_duplicate_or_overlapping_partitions_fail_instead_of_double_counting():
    with pytest.raises(ValueError, match="duplicate"):
        select_partitions([selection(), deepcopy(selection())])
    with pytest.raises(ValueError):
        select_partitions([selection(), selection("2026-10-26")], max_partitions=1)
    assert len(select_partitions([selection()], max_partitions=1)) == 1


@pytest.mark.parametrize("limit", [0, -1, True, float("inf"), "12", 313])
def test_partition_budget_is_a_finite_bounded_integer(limit):
    with pytest.raises(ValueError):
        select_partitions([selection()], max_partitions=limit)


def test_partition_constructor_cannot_bypass_date_or_population_validation():
    for week in (datetime(2026, 10, 19), date(2026, 10, 20), "2026-10-19"):
        with pytest.raises(ValueError):
            Partition(week, "ATS_WEEKLY_DETAIL", "T1")
    with pytest.raises(ValueError):
        Partition(date(2026, 10, 19), "BLOCKS", "T1")


@pytest.mark.parametrize(
    "mutation",
    ["version", "extra", "missing", "duplicate", "delay", "boolean", "conformance", "schema"],
)
def test_manifest_rejects_unreviewed_catalog_or_scope_changes(mutation):
    value = yaml.safe_load(MANIFEST.read_text())
    if mutation == "version":
        value["catalog_version"] = "unreviewed/2"
    elif mutation == "extra":
        value["allow_live"] = True
    elif mutation == "missing":
        value["profiles"].pop()
    elif mutation == "duplicate":
        value["profiles"][-1] = deepcopy(value["profiles"][0])
    else:
        field, replacement = {
            "delay": ("expected_delay_weeks", 1),
            "boolean": ("expected_delay_weeks", True),
            "conformance": ("conformance", "VERIFIED"),
            "schema": ("schema_profile", "guessed/2"),
        }[mutation]
        value["profiles"][0][field] = replacement
    with pytest.raises(ValueError):
        manifest_from_dict(value)


def test_duplicate_yaml_keys_are_rejected(tmp_path):
    path = tmp_path / "duplicate.yml"
    path.write_text(MANIFEST.read_text() + "\nsource: another\n")
    with pytest.raises(ValueError):
        load_manifest(path)


def test_every_manifest_profile_retains_its_dated_source_review():
    manifest = load_manifest(MANIFEST)
    for profile in manifest.profiles:
        assert profile.reviewed_on == "2026-11-15"
        assert profile.evidence == "specs/spec013/source_review.md"
    value = yaml.safe_load(MANIFEST.read_text())
    value["profiles"][0]["reviewed_on"] = "2026-11-14"
    with pytest.raises(ValueError):
        manifest_from_dict(value)
