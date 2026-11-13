"""SSV-AC10: versioned exact decimal structs and typed Parquet readback."""

from decimal import Decimal, localcontext

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_ssv_bronze import NOW
from test_ssv_parser import FIXTURES, receipt
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import parse_page
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    decode_decimal,
    encode_decimal,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.versions import (
    observation,
    version,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.parquet import (
    read_table,
    schema,
    write_table,
)

__all__ = ["manifest"]


@pytest.mark.parametrize(
    "text",
    ["0", "-0.000", "12.345678", "1.2300", "1E-4096", "9" * 4096, "1." + "0" * 33 + "E-8192", None],
)
def test_ssv_ac10_decimal_struct_roundtrip_is_exact_and_context_independent(text):
    with localcontext() as context:
        context.prec = 2
        context.capitals = 0
        encoded = encode_decimal(text)
        assert decode_decimal(encoded) == text
    if text is not None:
        assert Decimal(decode_decimal(encoded)).as_tuple() == Decimal(text).as_tuple()
        assert type(encoded["negative"]) is bool
        assert isinstance(encoded["digits"], str)


@pytest.mark.parametrize(
    "value", [1, 1.0, "NaN", "Infinity", " 1", "1e0", "01", "1_0", {"digits": "1"}]
)
def test_ssv_ac10_invalid_decimal_representations_refuse(value):
    with pytest.raises(ValueError):
        encode_decimal(value)


@pytest.mark.parametrize(
    "value",
    [
        {"negative": False, "digits": "01", "exponent": 0},
        {"negative": 0, "digits": "1", "exponent": 0},
        {"negative": False, "digits": "1", "exponent": True},
        {"negative": False, "digits": "1", "exponent": 90000},
        {"negative": False, "digits": "1", "exponent": 0, "secret": "invented"},
    ],
)
def test_ssv_ac10_invalid_decimal_structs_refuse(value):
    with pytest.raises(ValueError):
        decode_decimal(value)


def fact(manifest):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    row = parse_page(payload, manifest, receipt(payload)).rows[0]
    saved = {
        "capture_id": "00000000-0000-0000-0000-000000000001",
        "run_id": "00000000-0000-0000-0000-000000000002",
        "capture_sequence": 1,
        "archived_at": "2026-11-13T15:00:00.000000Z",
        "retrieved_at": None,
        "payload_sha256": "a" * 64,
        "receipt_sha256": "b" * 64,
        "manifest_sha256": "c" * 64,
    }
    return version(observation(row), saved)


def test_ssv_ac10_actual_parquet_types_preserve_source_documents_and_identity(tmp_path, manifest):
    row = fact(manifest)
    row["short_volume"] = "9" * 4096
    row["total_volume"] = "1E+4096"
    path = tmp_path / "silver.parquet"
    saved = write_table(path, "silver", [row])
    assert read_table(path, "silver", saved) == [row]
    table = pq.read_table(path)
    assert pa.types.is_struct(table.schema.field("short_volume").type)
    assert table.schema.field("trade_date").type == pa.date32()
    assert table.schema.field("first_seen_at").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("capture_sequence").type == pa.int64()
    assert table.schema.field("removed").type == pa.bool_()
    assert table.column("first_seen_at")[0].as_py() == NOW
    assert saved["rows"] == 1 and len(saved["schema_sha256"]) == 64
    assert table.schema.equals(schema("silver"), check_metadata=True)


@pytest.mark.parametrize(
    "name", ["silver", "gold", "research", "coverage", "audit", "captures", "membership"]
)
def test_ssv_ac10_empty_parquet_tables_keep_fixed_schema(tmp_path, name):
    path = tmp_path / (name + ".parquet")
    saved = write_table(path, name, [])
    assert read_table(path, name, saved) == []
    assert pq.read_table(path).schema.equals(schema(name), check_metadata=True)


@pytest.mark.parametrize("damage", ["bytes", "count", "schema"])
def test_ssv_ac10_corrupt_or_wrong_schema_parquet_refuses(tmp_path, manifest, damage):
    path = tmp_path / "silver.parquet"
    saved = write_table(path, "silver", [fact(manifest)])
    if damage == "bytes":
        path.write_bytes(b"broken")
    elif damage == "count":
        saved = dict(saved, rows=2)
    else:
        saved = write_table(path.with_name("other.parquet"), "gold", [])
    with pytest.raises(ValueError):
        read_table(path, "silver", saved)


def test_ssv_ac10_invalid_and_oversized_inputs_refuse_before_write(tmp_path, manifest):
    row = fact(manifest)
    for changes, limits in [
        ({"capture_sequence": True}, {}),
        ({"first_seen_at": "2026-11-13T15:00:00"}, {}),
        ({}, {"max_rows": 1}),
    ]:
        path = tmp_path / "refused.parquet"
        rows = [dict(row, **changes)] if changes else [row, row]
        with pytest.raises(ValueError):
            write_table(path, "silver", rows, **limits)
        assert not path.exists()
    with pytest.raises(ValueError):
        write_table(tmp_path / "small.parquet", "silver", [row], max_bytes=10)
    assert not (tmp_path / "small.parquet").exists()
