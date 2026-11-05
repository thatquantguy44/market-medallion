"""S11-T18 normalized threshold types; invented data, no exchange adapter."""

import io
import sqlite3
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_pipeline.pipelines.reg_sho_thresholds.storage import storage_schemas

CLOCK = datetime(2026, 11, 3, 8, tzinfo=UTC)


def scope():
    return dict(dataset="reg-sho-thresholds", provider="nasdaq", market="INVENTED_MARKET")


def membership():
    return scope() | dict(
        list_date=date(2026, 9, 15),
        raw_symbol="FIX.A",
        rule_family="REG_SHO",
        source_issue_id="000000001",
        security_name_raw="INVENTED",
        flags_raw_json='{"source_flag":"Y"}',
        row_state="REPORTED",
        source_row_number=2,
        partition_id="invented-list",
        version_id="member-one",
        content_hash="a" * 64,
        raw_sha256="b" * 64,
        run_id="run-one",
        capture_sequence=1,
        first_seen_at=CLOCK,
        retrieved_at=CLOCK,
        provider_published_at=None,
    )


def coverage():
    return scope() | dict(
        list_date=date(2026, 9, 15),
        requested_date=date(2026, 9, 15),
        source_date=date(2026, 9, 15),
        source_created_at_raw="20260915203000",
        source_created_at=None,
        calendar_version="unverified",
        partition_id="invented-list",
        coverage_state="COMPLETE_EMPTY",
        validation_state="VALIDATED",
        expected_row_count=None,
        received_row_count=0,
        version_id="coverage-one",
        raw_sha256="b" * 64,
        run_id="run-one",
        capture_sequence=1,
        first_seen_at=CLOCK,
    )


def audit():
    return scope() | dict(
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
    return dict(
        dataset="reg-sho-thresholds",
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
    [
        ("membership", membership),
        ("coverage", coverage),
        ("audit", audit),
        ("publications", publication),
    ],
)
def test_threshold_records_round_trip_with_independent_scope_and_nullable_clocks(name, make):
    schema = storage_schemas()[name]
    row = make()
    bindings = schema.encode(row, "sqlite")
    with sqlite3.connect(":memory:") as connection:
        types = schema.sql_types("sqlite")
        connection.execute(
            "CREATE TABLE records (" + ",".join(f"{k} {v}" for k, v in types.items()) + ")"
        )
        connection.execute(
            "INSERT INTO records VALUES (" + ",".join("?" for _ in types) + ")",
            tuple(bindings.values()),
        )
        restored = dict(
            zip(types, connection.execute("SELECT * FROM records").fetchone(), strict=True)
        )
    assert schema.decode(restored, "sqlite") == schema.normalize(row)
    assert schema.decode(schema.encode(row, "postgres"), "postgres") == schema.normalize(row)
    output = io.BytesIO()
    pq.write_table(schema.arrow_table([row]), output)
    assert schema.read_arrow(pq.read_table(pa.BufferReader(output.getvalue()))) == [
        schema.normalize(row)
    ]
    assert all(f["type"] != "double" for f in schema.spark_schema_json()["fields"])


@pytest.mark.parametrize(
    "updates",
    [
        {"provider": "combined"},
        {"market": ""},
        {"raw_symbol": ""},
        {"dataset": "sec-fails-to-deliver"},
        {"source_issue_id": 1},
        {"flags_raw_json": "not json"},
        {"flags_raw_json": "[]"},
        {"flags_raw_json": '{"flag":"Y","flag":"N"}'},
        {"capture_sequence": 0},
        {"source_row_number": None},
    ],
)
def test_invalid_threshold_membership_cannot_pass_serialization(updates):
    with pytest.raises(ValueError):
        storage_schemas()["membership"].arrow_table([membership() | updates])


@pytest.mark.parametrize(
    "updates",
    [
        {"coverage_state": "COMPLETE", "received_row_count": 0},
        {"coverage_state": "COMPLETE_EMPTY", "received_row_count": 1},
        {"source_date": date(2026, 9, 14)},
        {"validation_state": "NOT_CHECKED"},
        {"expected_row_count": 2},
        {"received_row_count": -1},
    ],
)
def test_complete_empty_cannot_hide_missing_wrong_date_or_unvalidated_lists(updates):
    with pytest.raises(ValueError):
        storage_schemas()["coverage"].normalize(coverage() | updates)


def test_failed_or_missing_coverage_is_distinct_from_valid_empty():
    schema = storage_schemas()["coverage"]
    failed = coverage() | dict(
        coverage_state="FAILED",
        source_date=None,
        source_created_at_raw=None,
        validation_state="REJECTED",
        raw_sha256=None,
    )
    assert schema.normalize(failed)["coverage_state"] == "FAILED"
    assert schema.normalize(coverage())["coverage_state"] == "COMPLETE_EMPTY"
    withdrawn = membership() | dict(
        row_state="WITHDRAWN",
        security_name_raw=None,
        source_issue_id=None,
        flags_raw_json=None,
        source_row_number=None,
    )
    assert storage_schemas()["membership"].normalize(withdrawn)["raw_symbol"] == "FIX.A"


def test_new_storage_version_requires_reviewed_contract():
    with pytest.raises(ValueError):
        storage_schemas(schema_version="unknown")


@pytest.mark.parametrize(
    "name,row",
    [
        ("audit", audit() | {"status": "SUCCEEDED"}),
        ("publications", publication() | {"snapshot_path": "/tmp/escape"}),
        (
            "publications",
            publication() | {"status": "PUBLISHED", "published_at": CLOCK.replace(year=2025)},
        ),
    ],
)
def test_threshold_success_and_publication_metadata_require_consistent_evidence(name, row):
    with pytest.raises(ValueError):
        storage_schemas()[name].normalize(row)
