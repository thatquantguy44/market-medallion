import json
from datetime import UTC, date, datetime
from decimal import Decimal

from market_data_pipeline.pipelines.reg_sho_thresholds.identity import (
    load_mappings,
    resolve_identity,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.features import (
    coverage_qualified_counts,
    prior_balance_changes,
)


def test_dated_identity_mapping_respects_effective_and_knowledge_dates(tmp_path):
    path = tmp_path / "mapping.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "security-identity-mapping/1",
                "mappings": [
                    {
                        "namespace": "nasdaq-symbol",
                        "raw_identifier": "ALFA",
                        "target_security_id": "security-old",
                        "effective_from": "2026-01-01",
                        "effective_to": "2026-06-30",
                        "mapping_version": "map-1",
                        "evidence": "reviewed-reference-1",
                        "first_seen_at": "2026-07-01T12:00:00Z",
                        "status": "RESOLVED",
                    },
                    {
                        "namespace": "nasdaq-symbol",
                        "raw_identifier": "ALFA",
                        "target_security_id": "security-new",
                        "effective_from": "2026-07-01",
                        "effective_to": None,
                        "mapping_version": "map-2",
                        "evidence": "reviewed-reference-2",
                        "first_seen_at": "2026-08-01T12:00:00Z",
                        "status": "RESOLVED",
                    },
                ],
            }
        )
    )
    mappings = load_mappings(path)
    early_cutoff = datetime(2026, 7, 15, tzinfo=UTC)
    assert (
        resolve_identity(
            mappings,
            namespace="nasdaq-symbol",
            raw_identifier="ALFA",
            economic_date=date(2026, 7, 10),
            knowledge_cutoff=early_cutoff,
        )["match_status"]
        == "UNRESOLVED"
    )
    resolved = resolve_identity(
        mappings,
        namespace="nasdaq-symbol",
        raw_identifier="ALFA",
        economic_date=date(2026, 7, 10),
        knowledge_cutoff=datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert resolved["target_security_id"] == "security-new"


def test_ftd_features_are_balance_changes_and_only_count_complete_dates():
    rows = [
        {
            "cusip": "000000001",
            "settlement_date": date(2026, 9, 1),
            "quantity_fails": Decimal("100"),
            "row_state": "REPORTED",
        },
        {
            "cusip": "000000001",
            "settlement_date": date(2026, 9, 3),
            "quantity_fails": Decimal("70"),
            "row_state": "REPORTED",
        },
    ]
    changes = prior_balance_changes(rows)
    assert changes[1]["balance_change"] == Decimal("-30")
    assert changes[1]["observation_gap_days"] == 2
    assert changes[1]["interpretation"] == "OUTSTANDING_BALANCE_CHANGE_NOT_NEW_FAILS"
    assert coverage_qualified_counts(rows, {date(2026, 9, 1)}) == {"000000001": 1}


def test_s11_a07_overlapping_and_unknown_identifiers_do_not_choose_a_join_target():
    common = {
        "namespace": "nasdaq-symbol",
        "raw_identifier": "ALFA",
        "effective_from": date(2026, 1, 1),
        "effective_to": None,
        "first_seen_at": datetime(2026, 1, 2, tzinfo=UTC),
        "status": "RESOLVED",
    }
    mappings = [
        {**common, "target_security_id": "security-one", "mapping_version": "v1"},
        {**common, "target_security_id": "security-two", "mapping_version": "v2"},
    ]
    overlap = resolve_identity(
        mappings,
        namespace="nasdaq-symbol",
        raw_identifier="ALFA",
        economic_date=date(2026, 6, 1),
        knowledge_cutoff=datetime(2026, 6, 2, tzinfo=UTC),
    )
    assert overlap == {
        "match_status": "AMBIGUOUS",
        "target_security_id": None,
        "mapping_version": None,
    }
    unknown = resolve_identity(
        mappings,
        namespace="cusip",
        raw_identifier="000000001",
        economic_date=date(2026, 6, 1),
        knowledge_cutoff=datetime(2026, 6, 2, tzinfo=UTC),
    )
    assert unknown == {
        "match_status": "UNRESOLVED",
        "target_security_id": None,
        "mapping_version": None,
    }


def test_s11_a08_explicit_calendar_skips_holiday_without_inventing_observations():
    from market_data_pipeline.pipelines.reg_sho_thresholds.episodes import derive_episodes

    # Invented bounded calendar: July 3 is excluded, followed by a weekend.
    dates = [date(2026, 7, day) for day in (2, 6, 7)]
    scope = {"provider": "nasdaq", "market": "NASDAQ"}
    membership = [
        {
            **scope,
            "raw_symbol": "ALFA",
            "rule_family": "REG_SHO_203",
            "row_state": "REPORTED",
            "list_date": day,
        }
        for day in dates[:2]
    ]
    coverage = [
        {
            **scope,
            "list_date": day,
            "coverage_state": "COMPLETE" if day != dates[-1] else "COMPLETE_EMPTY",
        }
        for day in dates
    ]
    (episode,) = derive_episodes(membership, coverage, dates)
    assert episode["observed_present_count"] == 2
    assert episode["start_date"] == dates[0] and episode["end_date"] == dates[1]
    assert episode["left_censored"] and episode["exit_confirmed"]
    assert not episode["gap_before"] and not episode["gap_after"]
    assert not episode["right_censored"]
