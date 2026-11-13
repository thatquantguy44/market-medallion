"""SSV-AC09/C08: scope completeness, temporal membership and coherent research reads."""

import json
from datetime import timedelta

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_parser import manifest as manifest
from test_ssv_parser import receipt

from market_data_pipeline.pipelines.finra_short_sale_volume.pipeline import run
from market_data_pipeline.pipelines.finra_short_sale_volume.research import research_gold

__all__ = ["manifest"]
NY = "nyse_trf"
NQ = "nasdaq_carteret_trf"
NC = "nasdaq_chicago_trf"


@pytest.fixture
def load(tmp_path, manifest):
    counter = 0

    def add(day="2026-11-09", facility=NY, values=None, total=None, clock=None):
        nonlocal counter
        selected = next(f for f in manifest.facilities if f.id == facility)
        values = [("SYN", "40", "5", "100")] if values is None else values
        bodies = []
        for symbol, short, exempt, volume in values:
            body = {
                "tradeReportDate": day,
                "securitiesInformationProcessorSymbolIdentifier": symbol,
                "shortParQuantity": short,
                "shortExemptParQuantity": exempt,
                "totalParQuantity": volume,
                "marketCode": selected.market_code,
                "reportingFacilityCode": selected.reporting_facility_code,
            }
            text = json.dumps(body)
            for key, value in [
                ("shortParQuantity", short),
                ("shortExemptParQuantity", exempt),
                ("totalParQuantity", volume),
            ]:
                text = text.replace(
                    json.dumps(key) + ": " + json.dumps(value), json.dumps(key) + ": " + value
                )
            bodies.append(text)
        payload = ("[" + ",".join(bodies) + "]").encode()
        observed = clock if clock is not None else NOW + timedelta(seconds=counter)
        counter += 1
        return run(
            tmp_path,
            payload,
            manifest,
            receipt(
                payload,
                total=len(values) if total is None else total,
                day=day,
                market=selected.market_code,
            ),
            approved_use=fixture_grant(),
            operation="fixture",
            clock=lambda: observed,
        )

    return add


def query(root, manifest, **kwargs):
    args = {
        "trade_date": "2026-11-09",
        "scopes": [NY],
        "approved_use": fixture_grant(),
        "operation": "fixture",
    }
    args.update(kwargs)
    return research_gold(root, manifest, **args)


def test_ssv_ac09_individual_prior_date_and_calendar_gap(tmp_path, manifest, load):
    prior = load("2026-11-06", values=[("SYN", "20", "5", "100")])
    current = load()
    result = query(tmp_path, manifest)
    assert result["prior_trade_date"] == "2026-11-06" and result["calendar_day_gap"] == 3
    assert result["gap_basis"] == "CALENDAR_DAYS"
    row = result["rows"][0]
    assert row["short_share"] == "0.4" and row["prior_short_share"] == "0.2"
    assert row["short_share_change"] == "0.2" and row["short_exempt_share_change"] == "0.00"
    assert result["current_partitions"][0]["capture_id"] == current["capture_id"]
    assert result["prior_partitions"][0]["capture_id"] == prior["capture_id"]
    assert (
        row["components"][0]["version_id"]
        and row["components"][0]["capture_id"] == current["capture_id"]
    )
    assert (
        result["knowledge_basis"] == "CURRENT_VINTAGE" and result["clock_basis"] == "LOCAL_CAPTURE"
    )


def test_ssv_ac09_disjoint_aggregate_has_explicit_population_and_denominator(
    tmp_path, manifest, load
):
    load()
    load(facility=NQ, values=[("SYN", "9", "1", "10")])
    result = query(tmp_path, manifest, scopes=[NY, NQ])
    assert result["facility_ids"] == [NQ, NY] and result["status"] == "COMPLETE"
    row = result["rows"][0]
    assert row["short_volume"] == "49" and row["total_volume"] == "110"
    assert row["short_share"] == "0.4454545454545454545454545454545455"
    assert len(row["components"]) == 2 and result["quantity_unit"] == "shares"
    assert result == query(tmp_path, manifest, scopes=[NQ, NY])


def test_ssv_ac09_missing_facility_does_not_renormalize_known_rows(tmp_path, manifest, load):
    load()
    result = query(tmp_path, manifest, scopes=["listed_trfs"])
    assert result["status"] == "INCOMPLETE_POPULATION"
    assert result["missing_facilities"] == [NQ, NC]
    row = result["rows"][0]
    assert row["status"] == "INCOMPLETE_POPULATION"
    assert row["short_volume"] is row["total_volume"] is row["short_share"] is None


def test_ssv_ac09_complete_empty_member_does_not_supply_synthetic_zero(tmp_path, manifest, load):
    load()
    load(facility=NQ, values=[])
    result = query(tmp_path, manifest, scopes=[NY, NQ])
    assert result["status"] == "COMPLETE" and result["missing_facilities"] == []
    row = result["rows"][0]
    assert row["status"] == "NO_REPORTED_ROW" and row["short_volume"] is None
    assert row["missing_row_facilities"] == [NQ]


def test_ssv_ac09_different_symbols_and_case_are_not_crosswalked(tmp_path, manifest, load):
    load(values=[("SYN", "40", "0", "100")])
    load(facility=NQ, values=[("syn", "9", "0", "10")])
    result = query(tmp_path, manifest, scopes=[NY, NQ])
    assert [r["symbol_raw"] for r in result["rows"]] == ["SYN", "syn"]
    assert all(
        r["status"] == "NO_REPORTED_ROW" and r["total_volume"] is None for r in result["rows"]
    )


def test_ssv_ac09_zero_denominator_has_explicit_null_reason(tmp_path, manifest, load):
    load(values=[("SYN", "0", "0", "0")])
    row = query(tmp_path, manifest)["rows"][0]
    assert (
        row["short_volume"] == "0"
        and row["short_share"] is None
        and row["null_reason"] == "ZERO_TOTAL"
    )
    assert row["short_share_change"] is None


def test_ssv_ac09_prior_search_skips_only_incomplete_dates_for_the_same_set(
    tmp_path, manifest, load
):
    load("2026-11-06")
    load("2026-11-06", NQ)
    load("2026-11-07")
    load("2026-11-08", total=20)
    load()
    load(facility=NQ)
    result = query(tmp_path, manifest, scopes=[NY, NQ])
    assert result["prior_trade_date"] == "2026-11-06" and result["calendar_day_gap"] == 3
    assert result["incomplete_prior_dates"] == ["2026-11-08", "2026-11-07"]


def test_ssv_ac09_empty_prior_date_is_not_skipped_to_an_older_symbol_value(
    tmp_path, manifest, load
):
    load("2026-11-06")
    load("2026-11-08", values=[])
    load()
    result = query(tmp_path, manifest)
    assert result["prior_trade_date"] == "2026-11-08"
    row = result["rows"][0]
    assert row["prior_status"] == "NO_REPORTED_ROW" and row["prior_short_share"] is None
    assert row["short_share_change"] is None


def test_ssv_ac09_zero_prior_ratio_does_not_mean_missing_prior(tmp_path, manifest, load):
    load("2026-11-06", values=[("SYN", "0", "0", "100")])
    load()
    row = query(tmp_path, manifest)["rows"][0]
    assert row["prior_short_share"] == "0" and row["short_share_change"] == "0.4"


def test_ssv_ac09_zero_prior_denominator_withholds_change(tmp_path, manifest, load):
    load("2026-11-06", values=[("SYN", "0", "0", "0")])
    load()
    row = query(tmp_path, manifest)["rows"][0]
    assert row["prior_null_reason"] == "ZERO_TOTAL" and row["short_share_change"] is None


def test_ssv_ac09_revision_of_prior_partition_obeys_equal_time_sequence_cutoff(
    tmp_path, manifest, load
):
    load("2026-11-06", clock=NOW)
    current = load(values=[("SYN", "50", "5", "100")], clock=NOW)
    load("2026-11-06", values=[("SYN", "25", "5", "100")], clock=NOW)
    old = query(tmp_path, manifest, as_of=NOW, as_of_sequence=current["capture_sequence"])
    latest = query(tmp_path, manifest, as_of=NOW)
    assert old["rows"][0]["short_share_change"] == "0.1"
    assert latest["rows"][0]["short_share_change"] == "0.25"


def test_ssv_ac09_removed_current_row_is_absent_and_old_cutoff_keeps_membership(
    tmp_path, manifest, load
):
    first = load(clock=NOW)
    load(values=[], clock=NOW)
    assert query(tmp_path, manifest)["status"] == "VALID_EMPTY"
    assert query(tmp_path, manifest)["rows"] == []
    assert (
        len(query(tmp_path, manifest, as_of=NOW, as_of_sequence=first["capture_sequence"])["rows"])
        == 1
    )


def test_ssv_ac09_future_capture_of_old_trade_date_is_not_leaked_into_prior(
    tmp_path, manifest, load
):
    load()
    load("2026-11-06")
    assert query(tmp_path, manifest, as_of=NOW)["prior_trade_date"] is None
    assert query(tmp_path, manifest)["prior_trade_date"] == "2026-11-06"


def test_ssv_ac09_lookback_is_explicit_and_inclusive(tmp_path, manifest, load):
    load("2026-11-06")
    load()
    assert query(tmp_path, manifest, lookback_days=2)["prior_trade_date"] is None
    assert query(tmp_path, manifest, lookback_days=3)["prior_trade_date"] == "2026-11-06"


@pytest.mark.parametrize(
    "scopes", [[], [NY, NY], ["listed_trfs", NY], ["consolidated_trf_adf"], ["orf"], "nyse_trf"]
)
def test_ssv_ac09_unreviewed_overlapping_or_missing_scopes_refuse_before_io(
    tmp_path, manifest, scopes
):
    root = tmp_path / "absent"
    with pytest.raises(ValueError):
        query(root, manifest, scopes=scopes)
    assert not root.exists()


@pytest.mark.parametrize(
    "change",
    [
        {"lookback_days": 0},
        {"lookback_days": True},
        {"lookback_days": 3661},
        {"max_symbols": 0},
        {"max_symbols": 250001},
        {"as_of_sequence": 1},
        {"as_of": NOW.replace(tzinfo=None)},
    ],
)
def test_ssv_ac09_bad_bounds_and_cutoffs_refuse_before_io(tmp_path, manifest, change):
    root = tmp_path / "absent"
    with pytest.raises(ValueError):
        query(root, manifest, **change)
    assert not root.exists()


def test_ssv_ac09_admission_precedes_scope_and_warehouse_access(tmp_path, manifest):
    root = tmp_path / "absent"
    with pytest.raises(PermissionError):
        query(root, manifest, operation="transform")
    assert not root.exists()


def test_ssv_ac09_symbol_budget_is_enforced_without_truncation(tmp_path, manifest, load):
    load(values=[("A", "1", "0", "2"), ("B", "1", "0", "2")])
    with pytest.raises(ValueError, match="symbol budget"):
        query(tmp_path, manifest, max_symbols=1)


def test_ssv_ac09_research_reads_leave_raw_and_warehouse_bytes_unchanged(tmp_path, manifest, load):
    load("2026-11-06")
    load()

    def files():
        return {
            str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
        }

    before = files()
    query(tmp_path, manifest)
    assert files() == before


def test_ssv_ac09_all_facilities_share_one_sqlite_snapshot(tmp_path, manifest, load, monkeypatch):
    import sqlite3

    from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse

    load()
    load(facility=NQ, values=[("SYN", "9", "1", "10")])
    # WAL in this disposable test lets a writer commit while the read is paused.
    # The production/default fixture path remains read-only and uses no new setting.
    with sqlite3.connect(tmp_path / "warehouse/ssv.sqlite") as db:
        assert db.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    original = warehouse.partition_snapshot
    changed = False

    def interleave(*args, **kwargs):
        nonlocal changed
        saved = original(*args, **kwargs)
        if not changed:
            changed = True
            load(values=[("SYN", "99", "5", "100")])
        return saved

    monkeypatch.setattr(warehouse, "partition_snapshot", interleave)
    during = query(tmp_path, manifest, scopes=[NY, NQ])
    after = query(tmp_path, manifest, scopes=[NY, NQ])
    assert during["rows"][0]["short_volume"] == "49"
    assert after["rows"][0]["short_volume"] == "108"
