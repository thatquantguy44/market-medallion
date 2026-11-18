"""OTC-AC03: invented ATS/T1 file syntax; no actual coverage or completeness claim."""

import hashlib
import socket
from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import Partition
from market_data_pipeline.pipelines.finra_otc_transparency.data.weekly import parse_weekly
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import (
    load_manifest,
    require_approved_use,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/finra_otc_transparency"
SCHEMA = "finra-otc-ats-weekly-pipe-fixture/1"
WEEK = Partition(date(2026, 10, 19), "ATS_WEEKLY_DETAIL", "T1")
VALID = (FIXTURES / "ats_t1_weekly.txt").read_bytes()


def parse(payload=VALID, **kwargs):
    return parse_weekly(payload, partition=WEEK, schema_profile=SCHEMA, **kwargs)


def changed_cell(column, value):
    lines = VALID.decode().splitlines()
    cells = lines[1].split("|")
    cells[column] = value
    lines[1] = "|".join(cells)
    return ("\n".join(lines) + "\n").encode()


def test_manifest_selected_fixture_preserves_exact_values_context_and_hash():
    manifest = load_manifest(ROOT / "manifests/finra-otc-transparency.yml")
    profile = next(
        p for p in manifest.profiles if (p.product, p.tier) == ("ATS_WEEKLY_DETAIL", "T1")
    )
    result = parse_weekly(VALID, partition=WEEK, schema_profile=profile.schema_profile)
    assert result.partition == WEEK
    assert result.partition.scope_id == "finra-otc:weekly:ATS_WEEKLY_DETAIL:T1"
    assert result.schema_profile == SCHEMA
    assert result.payload_sha256 == hashlib.sha256(VALID).hexdigest()
    assert result.outcome == "PARSED"
    assert result.completeness == "UNVERIFIED"
    assert result.context_source == "EXPLICIT_SELECTION"
    first, second, zero = result.rows
    assert (first.symbol_raw, first.issue_name_raw, first.participant_name_raw, first.mpid) == (
        "ZXQ.A",
        "Invented Café Class A",
        "EXAMPLE ALPHA ATS",
        "A0A1",
    )
    assert first.weekly_shares == 9007199254740993
    assert first.weekly_shares_text == "9007199254740993"
    assert first.weekly_trades == 42
    assert first.weekly_trades_text == "42"
    assert second.mpid == "0007"
    assert second.weekly_shares == 12345678901234567890
    assert zero.symbol_raw == "ZZZ$B"
    assert (zero.weekly_shares, zero.weekly_trades) == (0, 0)
    assert all(r.partition == WEEK and r.tier_raw == "T1" for r in result.rows)
    assert first.provider_update_date == date(2026, 11, 9)
    assert first.provider_initial_publication_date is None
    assert first.firm_crd is None


def test_header_only_is_parsed_empty_without_proving_provider_completeness():
    result = parse((FIXTURES / "ats_t1_empty.txt").read_bytes())
    assert result.rows == ()
    assert result.outcome == "PARSED_EMPTY"
    assert result.completeness == "UNVERIFIED"


@pytest.mark.parametrize("column", [5, 7])
@pytest.mark.parametrize("token", ["", "-1", "1.0", "1e3", "1,000", " 1", "+1", "１２", "9" * 39])
def test_malformed_missing_or_out_of_profile_counts_are_not_coerced(column, token):
    with pytest.raises(ValueError):
        parse(changed_cell(column, token))


@pytest.mark.parametrize(
    "token", ["", "20261109", "2026-11-31", "2026-11-09T00:00:00Z", "11/09/2026"]
)
def test_provider_date_is_strict_date_only(token):
    with pytest.raises(ValueError):
        parse(changed_cell(6, token))


@pytest.mark.parametrize(
    "column,value",
    [(0, "T2"), (0, "t1"), (1, ""), (2, ""), (3, "  "), (4, ""), (4, "AB\tC"), (3, "bad\x7flabel")],
)
def test_mismatched_tier_or_missing_controlled_text_is_rejected(column, value):
    with pytest.raises(ValueError):
        parse(changed_cell(column, value))


def test_unknown_reordered_duplicate_or_missing_headers_are_rejected():
    with pytest.raises(ValueError):
        parse((FIXTURES / "ats_t1_changed_schema.txt").read_bytes())
    header, rows = VALID.split(b"\n", 1)
    columns = header.split(b"|")
    changed = [columns[1], columns[0], *columns[2:]]
    for bad in (
        b"|".join(changed),
        b"|".join([columns[0], *columns[:-1]]),
        b"|".join(columns[:-1]),
    ):
        with pytest.raises(ValueError):
            parse(bad + b"\n" + rows)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"<html>Access denied</html>",
        b"[]",
        b"\xff",
        b"\xef\xbb\xbf" + VALID,
        VALID + b"\n",
        VALID + b"\x00",
    ],
)
def test_missing_html_json_encoding_bom_and_blank_records_fail_loudly(payload):
    with pytest.raises(ValueError):
        parse(payload)


def test_wrong_column_counts_and_broken_csv_quoting_are_rejected():
    lines = VALID.splitlines()
    for bad in (lines[1] + b"|extra", lines[1].rsplit(b"|", 1)[0], b'T1|"unterminated'):
        with pytest.raises(ValueError):
            parse(b"\n".join([lines[0], bad]) + b"\n")


def test_duplicate_natural_key_conflicting_name_or_value_rejects_whole_partition():
    for extra in (
        VALID.splitlines()[1],
        changed_cell(3, "RENAMED EXAMPLE").splitlines()[1],
        changed_cell(5, "7").splitlines()[1],
    ):
        with pytest.raises(ValueError, match="duplicate"):
            parse(VALID + extra + b"\n")


def test_raw_count_text_and_quoted_delimiter_are_preserved():
    result = parse(changed_cell(5, "00042"))
    assert result.rows[0].weekly_shares == 42
    assert result.rows[0].weekly_shares_text == "00042"
    result = parse(changed_cell(2, '"Invented | Company"'))
    assert result.rows[0].issue_name_raw == "Invented | Company"
    assert parse(VALID.replace(b"\n", b"\r\n")).rows == parse().rows


def test_profile_and_population_are_explicit_not_guessed_from_bytes():
    with pytest.raises(ValueError):
        parse_weekly(VALID, partition=WEEK, schema_profile="api-json/1")
    for product, tier in (("NON_ATS_WEEKLY_DETAIL", "T1"), ("ATS_WEEKLY_DETAIL", "T2")):
        with pytest.raises(ValueError):
            parse_weekly(
                VALID, partition=Partition(date(2026, 10, 19), product, tier), schema_profile=SCHEMA
            )
    with pytest.raises(ValueError):
        parse_weekly(VALID, partition=None, schema_profile=SCHEMA)


def test_byte_and_row_limits_are_enforced_at_the_boundary():
    assert len(parse(max_bytes=len(VALID), max_rows=3).rows) == 3
    with pytest.raises(ValueError):
        parse(max_bytes=len(VALID) - 1)
    with pytest.raises(ValueError):
        parse(max_rows=2)


@pytest.mark.parametrize("field", ["max_bytes", "max_rows"])
@pytest.mark.parametrize("limit", [0, -1, True, float("inf"), "100"])
def test_resource_limits_are_positive_finite_integers(field, limit):
    with pytest.raises(ValueError):
        parse(**{field: limit})


@pytest.mark.parametrize("payload", [None, VALID.decode(), bytearray(VALID)])
def test_parser_requires_exact_source_bytes(payload):
    with pytest.raises(ValueError):
        parse(payload)


def test_fixture_gate_and_pure_parser_open_no_file_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("pure OTC validation attempted I/O")

    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    require_approved_use(
        {
            "dataset": "finra-otc-transparency",
            "purpose": "personal_noncommercial_internal_research",
            "route": "invented_fixture",
            "authorization": "pending",
            "permission_reference": None,
            "credential_type": None,
            "provider_dataset": None,
            "attribution": None,
            "operations": ["fixture"],
        },
        operation="fixture",
    )
    assert len(parse().rows) == 3


@pytest.mark.parametrize(
    "column,value", [(1, " ZXQ.A"), (1, "ZXQ.A "), (4, " A0A1"), (4, "A0 A1"), (4, "A0A1\u00a0")]
)
def test_identifier_whitespace_cannot_hide_a_duplicate_population_key(column, value):
    with pytest.raises(ValueError):
        parse(changed_cell(column, value))
