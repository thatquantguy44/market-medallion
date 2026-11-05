"""Reviewed completeness assertions are exact-file scoped and never inferred."""

import json
from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.sec_fails_to_deliver.completeness import parse_evidence
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import load_manifest

MANIFEST = load_manifest(Path("manifests/sec-fails-to-deliver.yml"))


def evidence_value(raw_hash="a" * 64):
    return {
        "schema_version": "sec-ftd-completeness/1",
        "source": "sec",
        "dataset": "sec-fails-to-deliver",
        "reporting_regime": "semi-monthly-2009+",
        "period_start": "2026-10-01",
        "period_end": "2026-10-15",
        "member_name": "cnsfails202610a.txt",
        "raw_sha256": raw_hash,
        "expected_settlement_dates": ["2026-10-01", "2026-10-15"],
        "source_reference": "invented-source-fixture/1",
        "calendar_reference": "invented-calendar/1",
        "review_reference": "invented-review-only/1",
    }


def test_evidence_matches_exact_archive_dates_and_manifest():
    value = evidence_value()
    evidence = parse_evidence(json.dumps(value).encode(), MANIFEST)
    evidence.validate(raw_sha256="a" * 64, observed_dates=(date(2026, 10, 1), date(2026, 10, 15)))
    with pytest.raises(ValueError):
        evidence.validate(
            raw_sha256="b" * 64, observed_dates=(date(2026, 10, 1), date(2026, 10, 15))
        )
    with pytest.raises(ValueError):
        evidence.validate(raw_sha256="a" * 64, observed_dates=(date(2026, 10, 1),))


@pytest.mark.parametrize(
    "updates",
    [
        {"raw_sha256": ""},
        {"dataset": "other"},
        {"review_reference": ""},
        {"period_start": "2026-10-02"},
        {"member_name": "other.txt"},
        {"expected_settlement_dates": []},
        {"expected_settlement_dates": ["2026-10-01", "2026-10-01"]},
        {"expected_settlement_dates": ["2026-10-15", "2026-10-01"]},
        {"expected_settlement_dates": ["2026-09-30"]},
        {"extra": True},
    ],
)
def test_incomplete_ambiguous_or_unscoped_evidence_is_rejected(updates):
    with pytest.raises(ValueError):
        parse_evidence(json.dumps(evidence_value() | updates).encode(), MANIFEST)


def test_duplicate_evidence_keys_and_oversized_evidence_are_rejected():
    with pytest.raises(ValueError):
        parse_evidence(b'{"schema_version":"x","schema_version":"y"}', MANIFEST)
    with pytest.raises(ValueError):
        parse_evidence(b"x" * 65537, MANIFEST)
