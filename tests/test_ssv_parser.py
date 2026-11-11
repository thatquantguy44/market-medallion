"""SSV-AC03: pure parser; every fixture/value here is invented, never FINRA history."""

import hashlib
import json
import socket
from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import parse_page
from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import load_manifest

FIXTURES = Path("tests/fixtures/finra_short_sale_volume")
ENDPOINT = "https://api.finra.org/data/group/otcMarket/name/regShoDaily"


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/finra-short-sale-volume.yml"))


def wire(rows):
    return json.dumps(rows, separators=(",", ":")).encode()


def invented_row():
    return json.loads((FIXTURES / "invented-valid.json").read_bytes())[0]


def receipt(payload, *, total=2, offset=0, limit=2, status=200, market="N", day="2026-11-09"):
    return {
        "url": ENDPOINT,
        "method": "POST",
        "request_data_version": 1,
        "request_payload": {
            "limit": limit,
            "offset": offset,
            "compareFilters": [
                {"compareType": "EQUAL", "fieldName": "tradeReportDate", "fieldValue": day},
                {"compareType": "EQUAL", "fieldName": "marketCode", "fieldValue": market},
            ],
        },
        "http_status": status,
        "headers": {
            "Content-Type": "application/json",
            "Data-Version": "1",
            "Record-Total": str(total),
            "Record-Offset": str(offset),
            "Record-Limit": str(limit),
        },
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def test_ssv_ac03_invented_fixture_preserves_identifiers_exact_counts_and_raw_digest(manifest):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    page = parse_page(payload, manifest, receipt(payload))
    assert isinstance(page.rows, tuple) and page.partition_complete
    assert page.trade_date == date(2026, 11, 9) and page.facility_id == "nyse_trf"
    assert page.schema_id == "finra-reg-sho-api/1" and page.data_version == 1
    assert page.payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert [(r.symbol_raw, r.reporting_facility_code, r.market_raw) for r in page.rows] == [
        ("000SYN.A", "NYTRF", "N"),
        ("tSt/P-A", "NYTRF", "N"),
    ]
    assert (
        page.rows[0].short_volume,
        page.rows[0].short_exempt_volume,
        page.rows[0].total_volume,
    ) == (40, 5, 100)
    assert page.rows[1].short_volume == 2**80 + 1
    assert page.rows[1].total_volume == 2**81 + 2
    assert type(page.rows[1].short_volume) is Decimal
    assert payload == (FIXTURES / "invented-valid.json").read_bytes()
    with pytest.raises(FrozenInstanceError):
        page.rows[0].symbol_raw = "changed"


def test_ssv_ac03_integral_decimal_and_exponent_tokens_are_exact_under_low_decimal_precision(
    manifest,
):
    payload = (
        wire([invented_row()])
        .replace(b'"shortParQuantity":40', b'"shortParQuantity":9007199254740993.0')
        .replace(b'"shortExemptParQuantity":5', b'"shortExemptParQuantity":5e0')
        .replace(b'"totalParQuantity":100', b'"totalParQuantity":18014398509481986')
    )
    with localcontext() as context:
        context.prec = 2
        row = parse_page(payload, manifest, receipt(payload, total=1)).rows[0]
    assert row.short_volume == 9007199254740993 and row.short_exempt_volume == 5
    assert row.total_volume == 18014398509481986


@pytest.mark.parametrize(
    "token",
    [
        b"true",
        b"null",
        b'"40"',
        b"NaN",
        b"Infinity",
        b"-Infinity",
        b"-1",
        b"[]",
        b"{}",
        b"1e1000000",
        b"1e-1000000",
    ],
)
def test_ssv_ac03_invalid_counts_and_exponent_expansion_are_rejected(manifest, token):
    payload = wire([invented_row()]).replace(
        b'"shortParQuantity":40', b'"shortParQuantity":' + token
    )
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload, total=1))


@pytest.mark.parametrize(
    "changes",
    [
        {"shortExemptParQuantity": 41},
        {"shortParQuantity": 101},
        {"shortExemptParQuantity": -1},
        {"totalParQuantity": -1},
        {"tradeReportDate": "2026-02-30"},
        {"tradeReportDate": "20261109"},
        {"tradeReportDate": "2026-11-10"},
        {"reportingFacilityCode": "NQTRF"},
        {"marketCode": "N,Q"},
        {"unreviewedField": 1},
    ],
)
def test_ssv_ac03_invalid_rows_and_scope_drift_are_rejected(manifest, changes):
    payload = wire([{**invented_row(), **changes}])
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload, total=1))


@pytest.mark.parametrize("symbol", ["", " ", " SYN.A", "SYN.A ", "SYN\nA", "SYN\x00A", None, 10])
def test_ssv_ac03_invalid_symbols_are_rejected_without_cleanup(manifest, symbol):
    row = invented_row()
    row["securitiesInformationProcessorSymbolIdentifier"] = symbol
    payload = wire([row])
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload, total=1))


def test_ssv_ac03_printable_internal_spaces_and_source_order_are_preserved(manifest):
    row = invented_row()
    row["securitiesInformationProcessorSymbolIdentifier"] = "SYN A"
    payload = wire([row])
    assert parse_page(payload, manifest, receipt(payload, total=1)).rows[0].symbol_raw == "SYN A"


@pytest.mark.parametrize(
    "payload",
    [b"", b" ", b"null", b"{}", b"42", b"[", b"[{}", b"[1]", b"<html>login</html>", b"\xff"],
)
def test_ssv_ac03_invalid_json_and_non_array_payloads_are_rejected(manifest, payload):
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload))


def test_ssv_ac03_duplicate_keys_and_duplicate_observations_are_rejected(manifest):
    row = invented_row()
    duplicate_key = wire([row]).replace(
        b'"shortParQuantity":40', b'"shortParQuantity":40,"shortParQuantity":41'
    )
    for payload in (duplicate_key, wire([row, row])):
        with pytest.raises(ValueError):
            parse_page(payload, manifest, receipt(payload))


def test_ssv_ac03_missing_fields_and_wrong_utf_encodings_are_rejected(manifest):
    row = invented_row()
    del row["shortExemptParQuantity"]
    valid = wire([invented_row()])
    for payload in (wire([row]), valid.decode().encode("utf-16"), b"\xef\xbb\xbf" + valid):
        with pytest.raises(ValueError):
            parse_page(payload, manifest, receipt(payload, total=1))


def test_ssv_ac03_empty_json_and_no_content_have_explicit_complete_empty_state(manifest):
    for payload, status in (((FIXTURES / "invented-empty.json").read_bytes(), 200), (b"", 204)):
        info = receipt(payload, total=0, status=status)
        if status == 204:
            del info["headers"]["Content-Type"]
        page = parse_page(payload, manifest, info)
        assert page.rows == () and page.partition_complete and page.record_total == 0


def test_ssv_ac03_partial_short_and_trailing_pages_never_claim_a_complete_partition(manifest):
    full = (FIXTURES / "invented-valid.json").read_bytes()
    short = wire([invented_row()])
    cases = [
        (full, receipt(full, total=10)),
        (short, receipt(short, total=10)),
        (full, receipt(full, total=10, offset=8)),
        (b"[]", receipt(b"[]", total=10, offset=10)),
    ]
    for payload, info in cases:
        page = parse_page(payload, manifest, info)
        assert not page.partition_complete
    with pytest.raises(ValueError):
        parse_page(b"[]", manifest, receipt(b"[]", total=10))


def test_ssv_ac03_parser_has_no_io_and_does_not_assign_or_reinterpret_clocks(manifest, monkeypatch):
    row = invented_row()
    row["tradeReportDate"] = "2020-11-09"
    payload = wire([row])
    info = receipt(payload, total=1, day="2020-11-09")
    info["captured_at"] = "2026-11-10T00:00:00Z"

    def unexpected_io(*args, **kwargs):
        pytest.fail("pure parser attempted I/O")

    monkeypatch.setattr(Path, "open", unexpected_io)
    monkeypatch.setattr(socket, "create_connection", unexpected_io)
    page = parse_page(payload, manifest, info)
    assert page.rows[0].trade_date == date(2020, 11, 9)
    assert not hasattr(page.rows[0], "first_seen_at")
    assert not hasattr(page.rows[0], "provider_published_at")


@pytest.mark.parametrize(
    "path,value",
    [
        (("url",), ENDPOINT + "Mock"),
        (("url",), ENDPOINT.replace("api.finra.org", "example.invalid")),
        (("method",), "GET"),
        (("request_data_version",), 2),
        (("request_data_version",), True),
        (("http_status",), 403),
        (("http_status",), True),
        (("bytes",), 0),
        (("bytes",), True),
        (("sha256",), "0" * 64),
        (("request_payload",), None),
        (("request_payload", "limit"), 0),
        (("request_payload", "limit"), True),
        (("request_payload", "offset"), -1),
        (("request_payload", "async"), True),
        (("request_payload", "unreviewedFilter"), []),
        (("request_payload", "fields"), ["shortParQuantity"]),
        (("request_payload", "compareFilters", 0, "compareType"), "GREATER"),
        (("request_payload", "compareFilters", 0, "fieldValue"), "20261109"),
        (("request_payload", "compareFilters", 1, "fieldValue"), "D"),
        (("headers", "Data-Version"), "2"),
        (("headers", "data-version"), "1"),
        (("headers", "Record-Total"), "1"),
        (("headers", "Record-Total"), "-1"),
        (("headers", "Record-Total"), "2.0"),
        (("headers", "Record-Offset"), "1"),
        (("headers", "Record-Limit"), "1"),
        (("headers", "Record-Max-Limit"), "1"),
        (("headers", "Total-Records-On-Page"), "1"),
        (("headers", "Content-Type"), "text/html"),
        (("headers", "Content-Type"), "application/json; charset=latin-1"),
        (("headers", "Content-Encoding"), "gzip"),
        (("headers", "Content-Length"), "1"),
    ],
)
def test_ssv_ac03_receipt_scope_transport_integrity_and_pagination_mismatches_are_rejected(
    manifest, path, value
):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    info = receipt(payload)
    parent = info
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    with pytest.raises(ValueError):
        parse_page(payload, manifest, info)


@pytest.mark.parametrize(
    "name",
    ["url", "request_payload", "request_data_version", "http_status", "headers", "bytes", "sha256"],
)
def test_ssv_ac03_required_receipt_fields_cannot_be_omitted(manifest, name):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    info = receipt(payload)
    del info[name]
    with pytest.raises(ValueError):
        parse_page(payload, manifest, info)


@pytest.mark.parametrize(
    "name", ["Content-Type", "Data-Version", "Record-Total", "Record-Offset", "Record-Limit"]
)
def test_ssv_ac03_required_response_evidence_cannot_be_omitted(manifest, name):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    info = receipt(payload)
    del info["headers"][name]
    with pytest.raises(ValueError):
        parse_page(payload, manifest, info)


def test_ssv_ac03_duplicate_scope_filters_and_overlapping_query_shapes_are_rejected(manifest):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    for filters in (
        [],
        receipt(payload)["request_payload"]["compareFilters"] * 2,
        [receipt(payload)["request_payload"]["compareFilters"][0]] * 2,
    ):
        info = receipt(payload)
        info["request_payload"]["compareFilters"] = filters
        with pytest.raises(ValueError):
            parse_page(payload, manifest, info)


def test_ssv_ac03_full_projection_json_charset_and_platform_limit_clamp_are_supported(manifest):
    payload = wire([invented_row()])
    info = receipt(payload, total=1)
    info["request_payload"]["fields"] = list(invented_row())
    info["request_payload"]["async"] = False
    info["headers"].update(
        {
            "Content-Type": 'application/json; charset="UTF-8"',
            "Record-Max-Limit": "1",
            "Record-Limit": "1",
            "Total-Records-On-Page": "1",
            "Content-Length": str(len(payload)),
        }
    )
    info["headers"] = {k.lower(): v for k, v in info["headers"].items()}
    page = parse_page(payload, manifest, info)
    assert page.limit == 1 and page.partition_complete and len(page.rows) == 1


@pytest.mark.parametrize("payload,total,offset", [(b"[]", 0, 0), (b"", 2, 0), (b"", 0, 1)])
def test_ssv_ac03_no_content_requires_zero_bytes_zero_total_and_zero_offset(
    manifest, payload, total, offset
):
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload, total=total, status=204, offset=offset))


@pytest.mark.parametrize(
    "options", [{"max_bytes": 1}, {"max_rows": 1}, {"max_bytes": True}, {"max_rows": 0}]
)
def test_ssv_ac03_explicit_resource_budgets_are_enforced(manifest, options):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload), **options)


def test_ssv_ac03_receipt_and_raw_payload_types_are_explicit(manifest):
    payload = b"[]"
    info = receipt(payload, total=0)
    with pytest.raises(ValueError):
        parse_page("[]", manifest, info)
    with pytest.raises(ValueError):
        parse_page(payload, manifest, None)
    with pytest.raises(ValueError):
        parse_page(payload, manifest, {**info, "headers": []})


def test_ssv_ac03_deep_nesting_and_diagnostics_cannot_expose_source_values(manifest):
    payload = b"[" * 2000 + b"]" * 2000
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload))
    marker = "invented-private-marker"
    payload = wire([{**invented_row(), "shortParQuantity": marker}])
    with pytest.raises(ValueError) as failure:
        parse_page(payload, manifest, receipt(payload, total=1))
    assert marker not in str(failure.value)


@pytest.mark.parametrize(
    "facility_id,code,market",
    [
        ("nasdaq_carteret_trf", "NQTRF", "Q"),
        ("nasdaq_chicago_trf", "NCTRF", "B"),
        ("nyse_trf", "NYTRF", "N"),
    ],
)
def test_ssv_ac03_each_reviewed_facility_is_bound_to_its_selected_manifest_scope(
    manifest, facility_id, code, market
):
    from dataclasses import replace

    payload = wire([{**invented_row(), "reportingFacilityCode": code, "marketCode": market}])
    info = receipt(payload, total=1, market=market)
    info["request_payload"]["compareFilters"].reverse()
    page = parse_page(payload, manifest, info)
    assert page.facility_id == facility_id and page.rows[0].source_row == 1
    unselected = replace(
        manifest, facilities=tuple(f for f in manifest.facilities if f.id != facility_id)
    )
    with pytest.raises(ValueError):
        parse_page(payload, unselected, info)


def test_ssv_ac03_numeric_budget_boundaries_are_exact_and_context_is_unchanged(manifest):
    base = wire([invented_row()])
    for number in (b"9" * 4096, b"1e4095"):
        payload = base.replace(b'"shortParQuantity":40', b'"shortParQuantity":' + number).replace(
            b'"totalParQuantity":100', b'"totalParQuantity":' + number
        )
        with localcontext() as context:
            context.prec = 2
            context.clear_flags()
            flags = dict(context.flags)
            row = parse_page(payload, manifest, receipt(payload, total=1)).rows[0]
            assert dict(context.flags) == flags and context.prec == 2
        assert row.short_volume == row.total_volume and row.total_volume > 10**4095 - 1
    for number in (b"9" * 4097, b"1e4096", b"1e999999999999999999999999"):
        payload = base.replace(b'"shortParQuantity":40', b'"shortParQuantity":' + number)
        with pytest.raises(ValueError):
            parse_page(payload, manifest, receipt(payload, total=1))


def test_ssv_ac03_row_count_cannot_exceed_requested_limit(manifest):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    with pytest.raises(ValueError):
        parse_page(payload, manifest, receipt(payload, limit=1))


def test_ssv_ac03_parser_does_not_mutate_request_response_or_raw_inputs(manifest):
    from copy import deepcopy

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    info = receipt(payload)
    before = deepcopy(info)
    parse_page(payload, manifest, info)
    assert info == before
    assert payload == (FIXTURES / "invented-valid.json").read_bytes()


def test_ssv_ac03_fractional_shares_are_lossless_and_not_rounded_to_whole_shares(manifest):
    payload = (FIXTURES / "invented-fractional.json").read_bytes()
    with localcontext() as context:
        context.prec = 2
        context.clear_flags()
        row = parse_page(payload, manifest, receipt(payload, total=1)).rows[0]
        assert not any(context.flags.values())
    assert row.short_volume == Decimal("12.345678")
    assert row.short_exempt_volume == Decimal("0.000001")
    assert row.total_volume == Decimal("24.691356")
    assert all(
        type(v) is Decimal for v in (row.short_volume, row.short_exempt_volume, row.total_volume)
    )
    assert row.short_volume.as_tuple().exponent == -6


def test_ssv_ac03_json_number_scale_is_preserved_without_inventing_an_api_width(manifest):
    payload = wire([{**invented_row(), "shortExemptParQuantity": 0}]).replace(
        b'"shortParQuantity":40', b'"shortParQuantity":40.1250000'
    )
    value = parse_page(payload, manifest, receipt(payload, total=1)).rows[0].short_volume
    assert value == Decimal("40.125") and value.as_tuple().exponent == -7
