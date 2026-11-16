"""OTC-AC06: all six invented profiles; no real schema conformance."""

from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import PROFILES, Partition
from market_data_pipeline.pipelines.finra_otc_transparency.data.weekly import parse_weekly

FIXTURE = Path(__file__).parent / "fixtures/finra_otc_transparency/ats_t1_weekly.txt"
NON_ATS = (FIXTURE.parent / "non_ats_t1_invented.txt").read_bytes()


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_all_six_explicit_profiles_parse_exact_counts_and_scoped_identities(profile):
    partition = Partition(date(2026, 10, 19), profile.product, profile.tier)
    raw = FIXTURE.read_bytes() if profile.product == "ATS_WEEKLY_DETAIL" else NON_ATS
    raw = raw.replace(b"\nT1|", b"\n" + profile.tier.encode() + b"|")
    result = parse_weekly(raw, partition=partition, schema_profile=profile.schema_profile)
    assert len(result.rows) == 3
    assert result.completeness == "UNVERIFIED"
    assert all(row.identity.partition == partition for row in result.rows)
    if profile.product == "NON_ATS_WEEKLY_DETAIL":
        assert [r.identity.identity_basis for r in result.rows] == [
            "PROVIDER_ID",
            "SOURCE_ROW_LABEL",
            "AGGREGATED",
        ]
        assert result.rows[0].firm_crd == "00123"
        assert result.rows[0].weekly_shares == 12345678901234567890
        assert result.rows[0].weekly_trades_text == "00042"
        assert all(r.mpid is None for r in result.rows)


@pytest.mark.parametrize("mutation", ["unknown", "group_id", "duplicate", "wrong_header"])
def test_non_ats_synthetic_grammar_rejects_ambiguous_identity(mutation):
    profile = next(p for p in PROFILES if p.product == "NON_ATS_WEEKLY_DETAIL" and p.tier == "T1")
    raw = NON_ATS
    if mutation == "unknown":
        raw = raw.replace(b"||REPORTED", b"||UNKNOWN")
    elif mutation == "group_id":
        raw = raw.replace(b"||AGGREGATED", b"|00012|AGGREGATED")
    elif mutation == "duplicate":
        raw += raw.splitlines()[2] + b"\n"
    else:
        raw = raw.replace(b"|attribution", b"|guessedProviderAttribution")
    with pytest.raises(ValueError):
        parse_weekly(
            raw,
            partition=Partition(date(2026, 10, 19), profile.product, profile.tier),
            schema_profile=profile.schema_profile,
        )


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_each_fixture_profile_roundtrips_bronze_with_typed_rows(tmp_path, profile):
    from uuid import uuid4

    from test_otc_bronze import MANIFEST, USE

    from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import (
        Part,
        archive_capture,
        validate_capture,
    )

    partition = Partition(date(2026, 10, 19), profile.product, profile.tier)
    raw = FIXTURE.read_bytes() if profile.product == "ATS_WEEKLY_DETAIL" else NON_ATS
    raw = raw.replace(b"\nT1|", b"\n" + profile.tier.encode() + b"|")
    receipt = archive_capture(
        tmp_path / "store",
        partition=partition,
        manifest=MANIFEST,
        approved_use=USE,
        run_id=str(uuid4()),
        expected_parts=["file"],
        parts=[Part("file", raw)],
    )
    result = validate_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    assert result.outcome["status"] == "PARSED"
    assert result.parsed.rows[0].partition == partition
    assert len(receipt["schema"]["columns"]) in (8, 10)


def test_historical_t05_ats_capture_and_outcome_remain_idempotently_readable(tmp_path):
    from test_otc_bronze import USE, archive, reseal, state

    from market_data_pipeline.pipelines.finra_otc_transparency.catalog import legacy_manifest
    from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import validate_capture
    from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import schema_snapshot

    root = tmp_path / "legacy"
    receipt = archive(root)
    receipt["manifest"] = legacy_manifest()
    receipt["schema"] = schema_snapshot(
        Partition(date(2026, 10, 19), "ATS_WEEKLY_DETAIL", "T1"), receipt["manifest"]
    )
    reseal(root / "bronze/captures" / receipt["capture_id"] / "receipt.json", receipt)
    first = validate_capture(root, receipt["capture_id"], approved_use=USE)
    assert first.outcome["status"] == "PARSED"
    before = state(root)
    assert validate_capture(root, receipt["capture_id"], approved_use=USE).outcome == first.outcome
    assert state(root) == before
