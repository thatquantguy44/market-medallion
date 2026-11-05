"""Exact, source-independent field serialization; real SQLite and Parquet I/O."""

import io
import sqlite3
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal, localcontext

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data_pipeline.shared.storage_types import Column, RecordSchema


def schema(precision=38, scale=12):
    return RecordSchema(
        "invented/1",
        "fixture-contract",
        (
            Column("cusip", "string"),
            Column("shares", "decimal", precision=38, scale=0),
            Column("price", "decimal", nullable=True, precision=precision, scale=scale),
            Column("settled", "date"),
            Column("seen", "timestamp"),
            Column("published", "timestamp", nullable=True),
            Column("sequence", "int64"),
        ),
    )


def record():
    return dict(
        cusip="000000001",
        shares=Decimal(2**70),
        price=Decimal("12.3400"),
        settled=date(2026, 9, 15),
        seen=datetime(2026, 11, 3, 1, 2, 3, 456789, tzinfo=timezone(timedelta(hours=-5))),
        published=None,
        sequence=7,
    )


def test_sqlite_and_native_postgres_bindings_round_trip_without_float():
    contract = schema()
    row = record()
    encoded = contract.encode(row, "sqlite")
    with sqlite3.connect(":memory:") as connection:
        columns = contract.sql_types("sqlite")
        connection.execute(
            "CREATE TABLE facts (" + ",".join(f"{k} {v}" for k, v in columns.items()) + ")"
        )
        connection.execute("INSERT INTO facts VALUES (?,?,?,?,?,?,?)", tuple(encoded.values()))
        result = dict(
            zip(columns, connection.execute("SELECT * FROM facts").fetchone(), strict=True)
        )
    restored = contract.decode(result, "sqlite")
    assert restored == contract.normalize(row)
    assert restored["shares"] == 2**70
    assert restored["cusip"] == "000000001"
    assert restored["seen"].hour == 6
    assert restored["seen"].tzinfo is UTC
    assert restored["published"] is None
    native = contract.encode(row, "postgres")
    assert type(native["shares"]) is Decimal
    assert type(native["settled"]) is date
    assert native["seen"].tzinfo is UTC
    assert contract.decode(native, "postgres") == restored


def test_explicit_arrow_parquet_round_trip_and_spark_types():
    contract = schema()
    table = contract.arrow_table([record()])
    output = io.BytesIO()
    pq.write_table(table, output)
    restored = pq.read_table(pa.BufferReader(output.getvalue()))
    assert contract.read_arrow(restored) == [contract.normalize(record())]
    assert table.schema.field("shares").type == pa.decimal128(38, 0)
    assert table.schema.field("seen").type == pa.timestamp("us", tz="UTC")
    fields = {f["name"]: f for f in contract.spark_schema_json()["fields"]}
    assert fields["shares"]["type"] == "decimal(38,0)"
    assert fields["settled"]["type"] == "date"
    assert fields["seen"]["type"] == "timestamp"
    assert fields["published"]["nullable"] is True


@pytest.mark.parametrize("value", ["9" * 38, "0", "1"])
def test_38_digit_integer_boundaries_ignore_decimal_context(value):
    row = record() | {"shares": Decimal(value)}
    with localcontext() as context:
        context.prec = 3
        assert schema().normalize(row)["shares"] == Decimal(value)


@pytest.mark.parametrize(
    "updates",
    [
        {"shares": Decimal("1" + "0" * 38)},
        {"price": Decimal("0.0000000000001")},
        {"price": Decimal("1e26")},
        {"price": Decimal("NaN")},
        {"price": Decimal("Infinity")},
        {"price": 12.34},
        {"sequence": True},
        {"sequence": 2**63},
        {"seen": datetime(2026, 11, 3)},
        {"settled": datetime(2026, 9, 15, tzinfo=UTC)},
        {"cusip": 1},
        {"cusip": None},
        {"extra": "field"},
    ],
)
def test_invalid_or_lossy_values_fail_before_arrow_or_sql(updates):
    with pytest.raises(ValueError):
        schema().arrow_table([record() | updates])


def test_null_price_custom_precision_and_schema_identity():
    contract = schema(8, 2)
    assert contract.normalize(record())["price"] == Decimal("12.34")
    assert contract.normalize(record() | {"price": None})["price"] is None
    wrong = schema().arrow_table([record()])
    with pytest.raises(ValueError, match="schema"):
        contract.read_arrow(wrong)


def test_header_only_arrow_table_keeps_types_without_inferring_empty_coverage():
    contract = schema()
    table = contract.arrow_table([])
    assert table.num_rows == 0
    assert contract.read_arrow(table) == []
    assert table.schema.field("shares").type == pa.decimal128(38, 0)


def test_decimal_price_exact_boundary_and_trailing_zeroes_are_context_independent():
    value = Decimal("9" * 26 + "." + "9" * 12)
    row = record() | {"price": value}
    with localcontext() as context:
        context.prec = 3
        assert schema().normalize(row)["price"] == value
        assert schema(5, 2).normalize(record() | {"price": Decimal("12.34000")})[
            "price"
        ] == Decimal("12.34")


def test_arrow_contract_identity_and_nonnullable_fields_cannot_be_dropped():
    contract = schema()
    table = contract.arrow_table([record()])
    with pytest.raises(ValueError, match="schema"):
        contract.read_arrow(table.replace_schema_metadata({}))
    invalid = pa.Table.from_pylist(
        [contract.normalize(record()) | {"cusip": None}], schema=table.schema
    )
    with pytest.raises(ValueError, match="null"):
        contract.read_arrow(invalid)
