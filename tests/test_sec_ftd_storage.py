"""S11-T18 / A14,A15 type-contract evidence, not live backend parity."""

import io
import sqlite3
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.data import parse_pipe
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import load_manifest
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import storage_schemas

SETTINGS = load_config(Path("config/sec_fails_to_deliver/pipeline.example.yml")).storage
CLOCK = datetime(2026, 11, 3, 7, 0, 0, 123456, tzinfo=UTC)


def identity():
    return dict(source="sec", dataset="sec-fails-to-deliver", reporting_regime="semi-monthly-2009+")


def fact():
    return identity() | dict(
        settlement_date=date(2026, 9, 15),
        cusip="000000001",
        symbol_raw="FIX.A",
        description_raw="INVENTED",
        quantity_fails_text="000125",
        quantity_fails=Decimal(125),
        prior_close_price_text="00012.3400",
        prior_close_price=Decimal("12.3400"),
        price_missing_reason=None,
        price_basis="PROVIDER_PREVIOUS_CLOSE",
        row_state="REPORTED",
        partition_id="202609b",
        raw_member="cnsfails202609b.txt",
        source_row_number=2,
        version_id="version-one",
        content_hash="a" * 64,
        raw_sha256="b" * 64,
        member_sha256="c" * 64,
        run_id="run-one",
        capture_sequence=1,
        first_seen_at=CLOCK,
        retrieved_at=CLOCK,
        provider_published_at=None,
        source_updated_at=None,
        parser_version="sec-ftd-six-column/2",
    )


def coverage():
    return identity() | dict(
        partition_id="202609b",
        period_start=date(2026, 9, 15),
        period_end=date(2026, 9, 30),
        coverage_state="PARTIAL",
        expected_date_count=None,
        observed_date_count=1,
        received_row_count=1,
        trailer_record_count=1,
        trailer_quantity_text="125",
        validation_state="MATCHED",
        version_id="coverage-one",
        run_id="run-one",
        capture_sequence=1,
        first_seen_at=CLOCK,
        raw_sha256="b" * 64,
    )


def audit():
    return identity() | dict(
        run_id="run-one",
        capture_sequence=1,
        captured_at=CLOCK,
        completed_at=None,
        status="STARTED",
        row_count=0,
        raw_sha256=None,
        error_code=None,
    )


def publication():
    return identity() | dict(
        snapshot_id="snapshot-one",
        run_id="run-one",
        status="STAGED",
        generated_at=CLOCK,
        published_at=None,
        manifest_sha256="a" * 64,
        snapshot_path="parquet/snapshots/one",
    )


@pytest.mark.parametrize(
    "name,make",
    [("facts", fact), ("coverage", coverage), ("audit", audit), ("publications", publication)],
)
def test_all_ftd_records_round_trip_sqlite_parquet_and_postgres_bindings(name, make):
    schema = storage_schemas(SETTINGS)[name]
    row = make()
    encoded = schema.encode(row, "sqlite")
    with sqlite3.connect(":memory:") as connection:
        types = schema.sql_types("sqlite")
        connection.execute(
            "CREATE TABLE records (" + ",".join(f"{k} {v}" for k, v in types.items()) + ")"
        )
        connection.execute(
            "INSERT INTO records VALUES (" + ",".join("?" for _ in types) + ")",
            tuple(encoded.values()),
        )
        result = dict(
            zip(types, connection.execute("SELECT * FROM records").fetchone(), strict=True)
        )
    assert schema.decode(result, "sqlite") == schema.normalize(row)
    assert schema.decode(schema.encode(row, "postgres"), "postgres") == schema.normalize(row)
    output = io.BytesIO()
    pq.write_table(schema.arrow_table([row]), output)
    assert schema.read_arrow(pq.read_table(pa.BufferReader(output.getvalue()))) == [
        schema.normalize(row)
    ]


def test_ftd_exact_tokens_large_count_missing_price_and_configured_types():
    schema = storage_schemas(SETTINGS)["facts"]
    value = fact() | dict(quantity_fails=Decimal(2**70), quantity_fails_text=str(2**70))
    restored = schema.read_arrow(schema.arrow_table([value]))[0]
    assert restored["cusip"] == "000000001"
    assert restored["quantity_fails"] == 2**70
    assert restored["prior_close_price_text"] == "00012.3400"
    missing = fact() | dict(
        prior_close_price=None,
        prior_close_price_text=".",
        price_missing_reason="SOURCE_PRICE_UNAVAILABLE",
    )
    assert schema.normalize(missing)["prior_close_price"] is None
    configured = storage_schemas(
        replace(SETTINGS, quantity_precision=24, price_precision=20, price_scale=6)
    )["facts"]
    assert configured.arrow_schema().field("prior_close_price").type == pa.decimal128(20, 6)
    assert configured.sql_types("postgres")["quantity_fails"] == "NUMERIC"
    assert configured.contract_id != schema.contract_id


@pytest.mark.parametrize(
    "updates",
    [
        {"cusip": "123"},
        {"source": "other"},
        {"quantity_fails": Decimal(-1)},
        {"quantity_fails_text": "126"},
        {"prior_close_price_text": "12.35"},
        {"prior_close_price": None},
        {"price_missing_reason": "UNKNOWN"},
        {"capture_sequence": 0},
        {"source_row_number": 0},
        {"row_state": "UNKNOWN"},
        {"raw_sha256": "not-a-hash"},
        {"raw_member": "../bad.txt"},
    ],
)
def test_inconsistent_fact_identity_and_tokens_fail(updates):
    with pytest.raises(ValueError):
        storage_schemas(SETTINGS)["facts"].arrow_table([fact() | updates])


def test_withdrawal_keeps_key_and_lineage_but_has_no_manufactured_observation():
    row = fact() | dict(
        row_state="WITHDRAWN",
        symbol_raw=None,
        description_raw=None,
        quantity_fails=None,
        quantity_fails_text=None,
        prior_close_price=None,
        prior_close_price_text=None,
        price_missing_reason=None,
        source_row_number=None,
    )
    restored = storage_schemas(SETTINGS)["facts"].normalize(row)
    assert restored["cusip"] == "000000001"
    assert restored["quantity_fails"] is None


def test_parser_tokens_can_fill_typed_fields_without_reconstructing_raw_text():
    payload = Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes()
    observed = parse_pipe(payload, load_manifest(Path("manifests/sec-fails-to-deliver.yml")))[1]
    row = fact() | dict(
        settlement_date=observed.settlement_date,
        cusip=observed.cusip,
        symbol_raw=observed.symbol_raw,
        description_raw=observed.description_raw,
        quantity_fails=Decimal(observed.quantity_fails),
        quantity_fails_text=observed.quantity_fails_raw,
        prior_close_price=observed.prior_close_price,
        prior_close_price_text=observed.prior_close_price_raw,
        price_missing_reason=observed.price_missing_reason,
        source_row_number=observed.source_row_number,
    )
    assert storage_schemas(SETTINGS)["facts"].normalize(row)["quantity_fails"] == 2**70


def test_core_and_type_contract_imports_require_neither_arrow_nor_spark():
    code = """
import sys
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import storage_schemas
from market_data_pipeline.pipelines.reg_sho_thresholds.storage import (
    storage_schemas as threshold_schemas,
)
assert 'pyarrow' not in sys.modules
assert 'pyspark' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True)


@pytest.mark.parametrize(
    "name,row",
    [
        ("coverage", coverage() | {"coverage_state": "COMPLETE", "trailer_quantity_text": None}),
        ("audit", audit() | {"status": "SUCCEEDED"}),
        ("publications", publication() | {"snapshot_path": "../escape"}),
        (
            "publications",
            publication() | {"status": "PUBLISHED", "published_at": CLOCK.replace(year=2025)},
        ),
    ],
)
def test_incomplete_success_and_unsafe_publication_metadata_are_rejected(name, row):
    with pytest.raises(ValueError):
        storage_schemas(SETTINGS)[name].normalize(row)
