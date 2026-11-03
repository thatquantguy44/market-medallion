"""S11-A01/A02: invented documentary-profile inputs; no provider network."""

import copy
import io
import stat
import warnings
import zipfile
from dataclasses import replace
from datetime import date
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.data import parse_archive, parse_pipe
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import (
    load_manifest,
    manifest_from_dict,
)

FIXTURE = Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes()
MANIFEST = Path("manifests/sec-fails-to-deliver.yml")


def bundle(payload=FIXTURE, name="cnsfails202610a.txt", *, compression=zipfile.ZIP_DEFLATED):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        archive.writestr(name, payload)
    return buffer.getvalue()


def test_s11_a01_preserves_identifiers_decimal_precision_large_counts_and_missing_price():
    manifest = load_manifest(MANIFEST)
    rows = parse_pipe(FIXTURE, manifest)
    assert len(rows) == 2
    assert rows[0].settlement_date == date(2026, 10, 1)
    assert rows[0].cusip == "000000001"
    assert rows[0].symbol_raw == "FIX.A"
    assert rows[0].quantity_fails == 125
    assert str(rows[0].prior_close_price) == "12.3400"
    assert rows[0].prior_close_price == Decimal("12.34")
    assert rows[0].source_row_number == 2
    assert rows[1].quantity_fails == 2**70
    assert rows[1].prior_close_price is None
    assert rows[1].price_missing_reason == "SOURCE_PRICE_UNAVAILABLE"


def test_storage_lineage_preserves_original_numeric_tokens_before_projection():
    payload = FIXTURE.replace(b"|125|", b"|000125|").replace(b"|12.3400", b"|00012.3400")
    rows = parse_pipe(payload, load_manifest(MANIFEST))
    assert rows[0].quantity_fails_raw == "000125"
    assert rows[0].prior_close_price_raw == "00012.3400"
    assert rows[0].quantity_fails == 125
    assert rows[1].prior_close_price_raw == "."


def test_archive_is_read_in_memory_and_returns_auditable_payload_and_member_hashes(tmp_path):
    payload = bundle()
    parsed = parse_archive(payload, load_manifest(MANIFEST))
    assert parsed.observations == parse_pipe(FIXTURE, load_manifest(MANIFEST))
    assert parsed.raw_sha256 == sha256(payload).hexdigest()
    assert parsed.member_sha256 == sha256(FIXTURE).hexdigest()
    assert parsed.member_name == "cnsfails202610a.txt"
    assert parsed.observed_dates == (date(2026, 10, 1), date(2026, 10, 15))
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("compression", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_documentary_profile_accepts_supported_zip_methods_and_crlf(compression):
    payload = FIXTURE.replace(b"\n", b"\r\n")
    assert (
        len(
            parse_archive(
                bundle(payload, compression=compression), load_manifest(MANIFEST)
            ).observations
        )
        == 2
    )


@pytest.mark.parametrize(
    ("before", "after", "message"),
    [
        (b"20261001", b"20260230", "date"),
        (b"20261001", b"20260930", "partition"),
        (b"000000001", b"00000001", "CUSIP"),
        (b"125|", b"-1|", "quantity"),
        (b"125|", b"1.5|", "quantity"),
        (b"12.3400", b"NaN", "price"),
        (b"12.3400", b"Infinity", "price"),
        (b"12.3400", b"-1", "price"),
        (b"12.3400", b"1e2", "price"),
        (b"12.3400", b"", "price"),
        (b"SETTLEMENT DATE", b"TRADE DATE", "header"),
    ],
)
def test_s11_a01_rejects_bad_source_tokens_without_repair(before, after, message):
    with pytest.raises(ValueError, match=message):
        parse_pipe(FIXTURE.replace(before, after, 1), load_manifest(MANIFEST))


@pytest.mark.parametrize("payload", [b"", FIXTURE.splitlines(keepends=True)[0]])
def test_empty_or_header_only_input_is_not_certified_as_a_complete_partition(payload):
    with pytest.raises(ValueError):
        parse_pipe(payload, load_manifest(MANIFEST))


def test_rejects_duplicate_keys_even_when_payloads_are_identical():
    with pytest.raises(ValueError, match="duplicate"):
        parse_pipe(
            FIXTURE.replace(
                b"Trailer record", FIXTURE.splitlines(keepends=True)[1] + b"Trailer record"
            ),
            load_manifest(MANIFEST),
        )


@pytest.mark.parametrize(
    "payload", [FIXTURE + b"\n", FIXTURE[:-1], FIXTURE.replace(b"12.3400", b"12|34")]
)
def test_rejects_blank_unterminated_or_wrong_width_records(payload):
    with pytest.raises(ValueError):
        parse_pipe(payload, load_manifest(MANIFEST))


@pytest.mark.parametrize(
    "name",
    ["../cnsfails202610a.txt", "/cnsfails202610a.txt", "C:\\cnsfails202610a.txt", "other.txt"],
)
def test_s11_a02_rejects_unsafe_or_unexpected_member_names(name):
    with pytest.raises(ValueError, match="member"):
        parse_archive(bundle(name=name), load_manifest(MANIFEST))


def test_rejects_duplicate_or_extra_archive_members():
    for name in ("cnsfails202610a.txt", "extra.txt"):
        buffer = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("cnsfails202610a.txt", FIXTURE)
                archive.writestr(name, FIXTURE)
        with pytest.raises(ValueError, match="member"):
            parse_archive(buffer.getvalue(), load_manifest(MANIFEST))


def test_s11_a02_rejects_symlink_members_without_extracting():
    info = zipfile.ZipInfo("cnsfails202610a.txt")
    info.create_system = 3
    info.external_attr = (stat.S_IFLNK | 0o777) << 16
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(info, b"/private/file")
    with pytest.raises(ValueError, match="member"):
        parse_archive(buffer.getvalue(), load_manifest(MANIFEST))


@pytest.mark.parametrize(
    "payload", [b"<html>Access denied</html>", bundle()[:-10], b"PK\x03\x04invalid"]
)
def test_s11_a02_rejects_challenges_and_corrupt_archives(payload):
    with pytest.raises(ValueError, match="archive"):
        parse_archive(payload, load_manifest(MANIFEST))


def test_stored_member_crc_failure_is_rejected():
    payload = bundle(compression=zipfile.ZIP_STORED)
    payload = payload.replace(b"INVENTED", b"CORRUPTD", 1)
    with pytest.raises(ValueError, match="archive"):
        parse_archive(payload, load_manifest(MANIFEST))


@pytest.mark.parametrize(
    "limit",
    [{"archive_bytes": 20}, {"expanded_bytes": 20}, {"rows": 1}, {"record_bytes": 20}],
)
def test_s11_a02_enforces_each_configured_resource_limit(limit):
    value = yaml.safe_load(MANIFEST.read_text())
    value["limits"].update(limit)
    with pytest.raises(ValueError, match="limit"):
        parse_archive(bundle(), manifest_from_dict(value))


@pytest.mark.parametrize(
    "override",
    [
        {"unknown": "field"},
        {"source": "finra"},
        {"dataset": "other"},
        {"schema_version": "unknown"},
        {"format": "csv"},
        {"reporting_regime": "all-history"},
        {"encoding": "utf-8-sig"},
        {"member_name": "../source.txt"},
        {"partition": {"start": "2026-10-02", "end": "2026-10-15"}},
        {"partition": {"start": "2026-10-01", "end": "2026-10-31"}},
        {"partition": {"start": "2009-06-01", "end": "2009-06-15"}},
        {"partition": {"start": "2026-02-30", "end": "2026-02-30"}},
        {"limits": {"archive_bytes": True}},
        {"limits": {"expanded_bytes": 0}},
        {"access": {"automation": "enabled"}},
        {"access": {"source_url": "https://example.com/data"}},
    ],
)
def test_manifest_rejects_unknown_schema_scope_and_network_configuration(override):
    value = yaml.safe_load(MANIFEST.read_text())
    for key, item in override.items():
        if isinstance(item, dict):
            value[key] = {**value.get(key, {}), **copy.deepcopy(item)}
        else:
            value[key] = item
    with pytest.raises(ValueError):
        manifest_from_dict(value)


def test_manifest_rejects_duplicate_yaml_keys(tmp_path):
    path = tmp_path / "duplicate.yml"
    path.write_text(MANIFEST.read_text() + "source: sec\n")
    with pytest.raises(ValueError, match="duplicate"):
        load_manifest(path)


def test_limits_are_validated_when_manifest_is_constructed_directly():
    with pytest.raises(ValueError, match="limit"):
        replace(load_manifest(MANIFEST), max_rows=0)


def test_half_two_can_end_on_leap_day():
    value = yaml.safe_load(MANIFEST.read_text())
    value["partition"] = {"start": "2024-02-16", "end": "2024-02-29"}
    manifest = manifest_from_dict(value)
    assert manifest.period_end == date(2024, 2, 29)


def test_unsupported_local_zip_header_flags_produce_a_validation_error():
    payload = bytearray(bundle())
    payload[6:8] = (0x40).to_bytes(2, "little")
    with pytest.raises(ValueError, match="archive"):
        parse_archive(bytes(payload), load_manifest(MANIFEST))


def test_unhashable_yaml_key_is_rejected_as_invalid_configuration(tmp_path):
    path = tmp_path / "invalid-key.yml"
    path.write_text("? [unhashable, key]\n: value\n")
    with pytest.raises(ValueError, match="YAML"):
        load_manifest(path)


@pytest.mark.parametrize(
    "payload",
    [
        b"\n".join(FIXTURE.splitlines()[:-2]) + b"\n",
        b"\n".join(FIXTURE.splitlines()[:-1]) + b"\n",
        FIXTURE.replace(b"Trailer record count 2", b"Trailer record count 3"),
        FIXTURE.replace(b"1180591620717411303549", b"1180591620717411303550"),
        FIXTURE.replace(b"Trailer record count 2", b"Trailer record count -2"),
        FIXTURE + b"Trailer record count 2\n",
        FIXTURE + FIXTURE.splitlines(keepends=True)[1],
        FIXTURE.replace(b"Trailer record count 2\n", b""),
        FIXTURE.replace(b"Trailer record count 2", b"Trailer total quantity of shares 2"),
    ],
)
def test_rejects_missing_mismatched_malformed_or_nonterminal_trailers(payload):
    with pytest.raises(ValueError, match="trailer"):
        parse_pipe(payload, load_manifest(MANIFEST))


def test_source_trailers_do_not_consume_fact_row_budget():
    rows = parse_pipe(FIXTURE, replace(load_manifest(MANIFEST), max_rows=2))
    assert len(rows) == 2


def test_explicit_observed_september_bounds_accept_day_15_in_second_archive():
    value = yaml.safe_load(MANIFEST.read_text())
    value["partition"] = {"start": "2026-09-15", "end": "2026-09-30"}
    value["member_name"] = "cnsfails202609b.txt"
    payload = FIXTURE.replace(b"20261001", b"20260915").replace(b"20261015", b"20260930")
    rows = parse_archive(bundle(payload, name=value["member_name"]), manifest_from_dict(value))
    assert rows.observed_dates == (date(2026, 9, 15), date(2026, 9, 30))


def test_explicit_observed_september_first_archive_ends_day_14():
    manifest = replace(
        load_manifest(MANIFEST), period_start=date(2026, 9, 1), period_end=date(2026, 9, 14)
    )
    payload = FIXTURE.replace(b"20261001", b"20260901").replace(b"20261015", b"20260914")
    assert len(parse_pipe(payload, manifest)) == 2
    with pytest.raises(ValueError, match="partition"):
        parse_pipe(payload.replace(b"20260914", b"20260915"), manifest)
