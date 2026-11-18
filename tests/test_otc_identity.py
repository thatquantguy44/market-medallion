"""OTC-AC04: evidence-scoped keys, no invented firm identity or name-based joins."""

from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import Partition, profile_for
from market_data_pipeline.pipelines.finra_otc_transparency.data.identity import (
    participant_identity,
    require_unique_participants,
)
from market_data_pipeline.pipelines.finra_otc_transparency.data.weekly import parse_weekly

WEEK = Partition(date(2026, 10, 19), "ATS_WEEKLY_DETAIL", "T1")
NON_ATS = Partition(date(2026, 10, 19), "NON_ATS_WEEKLY_DETAIL", "T1")


def identify(**kwargs):
    values = dict(
        partition=WEEK,
        schema_profile=profile_for(WEEK.product, WEEK.tier).schema_profile,
        route="invented_fixture",
        participant_name_raw="EXAMPLE ATS",
        mpid=None,
        firm_crd=None,
        attribution="REPORTED",
    )
    values.update(kwargs)
    return participant_identity(**values)


def test_supplied_ids_survive_name_changes_without_inferred_global_firm_match():
    first = identify(mpid="0007", firm_crd="00123")
    renamed = identify(mpid="0007", firm_crd="00123", participant_name_raw="RENAMED EXAMPLE ATS")
    next_week = identify(
        mpid="0007", partition=Partition(date(2026, 10, 26), WEEK.product, WEEK.tier)
    )
    assert first.identity_basis == "PROVIDER_ID"
    assert first.identifier_kind == "MPID"
    assert (first.mpid, first.firm_crd) == ("0007", "00123")
    assert first.participant_key == renamed.participant_key == next_week.participant_key
    assert first.participant_name_raw != renamed.participant_name_raw
    assert first != renamed
    assert first.participant_key.startswith("finra-otc-participant/1:")
    assert identify(mpid="0007") == identify(mpid="0007")


def test_crd_only_is_typed_and_cannot_collide_with_equal_mpid_text():
    crd = identify(firm_crd="0007", participant_name_raw=None)
    assert crd.identity_basis == "PROVIDER_ID" and crd.identifier_kind == "CRD"
    assert crd.firm_crd == "0007" and crd.mpid is None
    assert crd.participant_key != identify(mpid="0007").participant_key
    assert crd.participant_key != identify(firm_crd="7").participant_key


def test_same_name_different_ids_is_never_merged():
    first, second = identify(mpid="AAAA"), identify(mpid="BBBB")
    assert first.participant_name_raw == second.participant_name_raw
    assert first.participant_key != second.participant_key
    require_unique_participants([("ZXQ.A", first), ("ZXQ.A", second)])


def test_name_only_renames_and_reused_names_do_not_establish_firm_continuity():
    original = identify()
    renamed = identify(participant_name_raw="DIFFERENT EXAMPLE")
    next_week = identify(partition=Partition(date(2026, 10, 26), WEEK.product, WEEK.tier))
    assert original.identity_basis == "SOURCE_ROW_LABEL"
    assert original.identifier_kind is None and original.mpid is None and original.firm_crd is None
    assert len({original.participant_key, renamed.participant_key, next_week.participant_key}) == 3
    assert identify(participant_name_raw="EXAMPLE ATS ").participant_key != original.participant_key


def test_grouping_needs_explicit_source_evidence_and_never_fabricates_an_id():
    schema = profile_for(NON_ATS.product, NON_ATS.tier).schema_profile
    name_only = identify(
        partition=NON_ATS, schema_profile=schema, participant_name_raw="De Minimis Firms"
    )
    group = identify(
        partition=NON_ATS,
        schema_profile=schema,
        participant_name_raw="De Minimis Firms",
        attribution="AGGREGATED",
    )
    assert name_only.identity_basis == "SOURCE_ROW_LABEL"
    assert group.identity_basis == "AGGREGATED"
    assert group.mpid is None and group.firm_crd is None and group.identifier_kind is None
    assert group.participant_key != name_only.participant_key
    later = identify(
        partition=Partition(date(2026, 10, 26), NON_ATS.product, NON_ATS.tier),
        schema_profile=schema,
        participant_name_raw="De Minimis Firms",
        attribution="AGGREGATED",
    )
    assert later.participant_key != group.participant_key


@pytest.mark.parametrize("explicit", ["REPORTED", "UNKNOWN"])
def test_missing_attribution_is_unknown_unkeyable_and_not_a_shared_fake_firm(explicit):
    unknown = identify(participant_name_raw=None, attribution=explicit)
    assert unknown.identity_basis == "UNKNOWN" and unknown.participant_key is None
    with pytest.raises(ValueError, match="unknown"):
        unknown.require_key()
    with pytest.raises(ValueError, match="unknown"):
        require_unique_participants([("ZXQ.A", unknown)])


@pytest.mark.parametrize("attribution", ["AGGREGATED", "UNKNOWN"])
@pytest.mark.parametrize("identifier", ["mpid", "firm_crd"])
def test_grouped_or_unknown_rows_cannot_claim_conflicting_firm_ids(attribution, identifier):
    with pytest.raises(ValueError):
        identify(attribution=attribution, **{identifier: "0007"})


@pytest.mark.parametrize(
    "changes",
    [
        {"mpid": 7},
        {"mpid": ""},
        {"mpid": "A B"},
        {"mpid": " A"},
        {"firm_crd": 123},
        {"firm_crd": "12.0"},
        {"firm_crd": "-1"},
        {"participant_name_raw": ""},
        {"participant_name_raw": "bad\nname"},
        {"participant_name_raw": "bad\ud800name"},
        {"attribution": "guessed"},
        {"route": "mock"},
        {"schema_profile": "unreviewed/2"},
        {"partition": None},
        {"attribution": "AGGREGATED", "participant_name_raw": None},
    ],
)
def test_ambiguous_malformed_or_unscoped_evidence_fails_loudly(changes):
    with pytest.raises(ValueError):
        identify(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"route": "approved_local_file"},
        {
            "partition": Partition(date(2026, 10, 19), "ATS_WEEKLY_DETAIL", "T2"),
            "schema_profile": profile_for("ATS_WEEKLY_DETAIL", "T2").schema_profile,
        },
        {
            "partition": NON_ATS,
            "schema_profile": profile_for(NON_ATS.product, NON_ATS.tier).schema_profile,
        },
    ],
)
def test_source_keys_cannot_collide_across_routes_or_populations(changes):
    assert identify(mpid="0007").participant_key != identify(mpid="0007", **changes).participant_key


@pytest.mark.parametrize("kwargs", [{}, {"mpid": "0007"}, {"attribution": "AGGREGATED"}])
def test_ambiguous_duplicate_labels_or_ids_quarantine_the_partition(kwargs):
    first = identify(**kwargs)
    with pytest.raises(ValueError, match="duplicate"):
        require_unique_participants([("ZXQ.A", first), ("ZXQ.A", first)])
    require_unique_participants([("ZXQ.A", first), ("ZZZ$B", first)])


def test_conflicting_names_under_same_source_id_remain_ambiguous_within_one_partition():
    with pytest.raises(ValueError, match="duplicate"):
        require_unique_participants(
            [
                ("ZXQ.A", identify(mpid="0007")),
                ("ZXQ.A", identify(mpid="0007", participant_name_raw="OTHER")),
            ]
        )


def test_mixed_identity_contexts_cannot_be_treated_as_one_complete_population():
    first = identify(mpid="0007")
    for other in (
        identify(mpid="0008", route="approved_local_file"),
        identify(mpid="0008", partition=Partition(date(2026, 10, 26), WEEK.product, WEEK.tier)),
    ):
        with pytest.raises(ValueError, match="context"):
            require_unique_participants([("ZXQ.A", first), ("ZZZ$B", other)])


def test_weekly_fixture_exposes_key_basis_and_preserves_raw_identifiers():
    payload = (
        Path(__file__).parent / "fixtures/finra_otc_transparency/ats_t1_weekly.txt"
    ).read_bytes()
    result = parse_weekly(
        payload, partition=WEEK, schema_profile=profile_for(WEEK.product, WEEK.tier).schema_profile
    )
    assert all(row.identity.identity_basis == "PROVIDER_ID" for row in result.rows)
    assert result.rows[1].identity.mpid == result.rows[1].mpid == "0007"
    assert result.rows[0].identity.participant_key == result.rows[2].identity.participant_key
    changed = payload.replace(b"EXAMPLE ALPHA ATS", b"RENAMED EXAMPLE")
    renamed = parse_weekly(changed, partition=WEEK, schema_profile=result.schema_profile)
    assert renamed.rows[0].identity.participant_key == result.rows[0].identity.participant_key
    assert renamed.rows[0].participant_name_raw == "RENAMED EXAMPLE"
    assert renamed.payload_sha256 != result.payload_sha256
