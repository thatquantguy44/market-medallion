"""SSV-AC10 internal typed adapter; operational callers enforce approved use first."""

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import _day
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    COMPONENT,
    CONTRACT,
    DECIMAL_CODEC,
    DEFAULT_BYTES,
    DEFAULT_ROWS,
    FIELDS,
    MAX_RECORD_BYTES,
    encode_decimal,
    fingerprint,
    limits,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import _clock_text


def _arrow(kind):
    if kind == "decimal":
        return pa.struct(
            [
                pa.field("negative", pa.bool_(), False),
                pa.field("digits", pa.string(), False),
                pa.field("exponent", pa.int32(), False),
            ]
        )
    if kind == "components":
        return pa.list_(
            pa.field(
                "element",
                pa.struct([pa.field(n, _arrow(k), nullable) for n, k, nullable in COMPONENT]),
            )
        )
    return {
        "s": pa.string(),
        "i": pa.int64(),
        "b": pa.bool_(),
        "date": pa.date32(),
        "time": pa.timestamp("us", tz="UTC"),
        "strings": pa.list_(pa.field("element", pa.string())),
    }[kind]


def schema(name):
    checksum = fingerprint(name)
    return pa.schema(
        [pa.field(n, _arrow(k), nullable) for n, k, nullable in FIELDS[name]]
        + [pa.field("record_json", pa.string(), False)],
        metadata={
            b"contract": CONTRACT.encode(),
            b"table": name.encode(),
            b"decimal_codec": DECIMAL_CODEC.encode(),
            b"schema_sha256": checksum.encode(),
        },
    )


def _value(value, kind, nullable):
    if value is None:
        if not nullable:
            raise ValueError("missing FINRA nonnullable snapshot field")
        return None
    if kind == "decimal":
        return encode_decimal(value)
    if kind == "s":
        if not isinstance(value, str) or len(value.encode()) > MAX_RECORD_BYTES:
            raise ValueError("invalid FINRA snapshot text")
    elif kind == "i":
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ValueError("FINRA snapshot integer exceeds int64")
    elif kind == "b":
        if type(value) is not bool:
            raise ValueError("invalid FINRA snapshot boolean")
    elif kind == "date":
        return _day(value)
    elif kind == "time":
        return datetime.fromisoformat(_clock_text(value).replace("Z", "+00:00"))
    elif kind == "strings":
        if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
            raise ValueError("invalid FINRA snapshot string list")
    elif kind == "components":
        if not isinstance(value, list) or len(value) > 3:
            raise ValueError("invalid FINRA snapshot components")
        return [_project(item, COMPONENT) for item in value]
    return value


def _project(record, fields):
    if not isinstance(record, dict) or any(n not in record for n, _, _ in fields):
        raise ValueError("invalid FINRA snapshot record")
    return {n: _value(record[n], k, nullable) for n, k, nullable in fields}


def table(name, rows, *, max_rows=DEFAULT_ROWS, max_bytes=DEFAULT_BYTES):
    limits(max_rows, max_bytes)
    expected = schema(name)
    if not isinstance(rows, list) or len(rows) > max_rows:
        raise ValueError("FINRA snapshot row budget exceeded")
    output = []
    size = 0
    for row in rows:
        raw = encoded(row)
        size += len(raw)
        if len(raw) > MAX_RECORD_BYTES or size > max_bytes:
            raise ValueError("FINRA snapshot byte budget exceeded")
        output.append(dict(_project(row, FIELDS[name]), record_json=raw.decode()))
    result = pa.Table.from_pylist(output, schema=expected)
    if result.nbytes > max_bytes:
        raise ValueError("FINRA typed snapshot byte budget exceeded")
    return result


def file_digest(path, max_bytes):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
        raise ValueError("FINRA snapshot file is missing, unsafe or oversized")
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            checksum.update(block)
    return checksum.hexdigest()


def read_table(path, name, receipt, *, max_rows=DEFAULT_ROWS, max_bytes=DEFAULT_BYTES):
    limits(max_rows, max_bytes)
    try:
        if (
            set(receipt)
            != {"rows", "bytes", "arrow_bytes", "schema_sha256", "file_sha256", "logical_sha256"}
            or any(
                type(receipt[k]) is not int or receipt[k] < 0
                for k in ("rows", "bytes", "arrow_bytes")
            )
            or receipt["rows"] > max_rows
            or max(receipt["bytes"], receipt["arrow_bytes"]) > max_bytes
            or receipt["schema_sha256"] != fingerprint(name)
            or file_digest(path, max_bytes) != receipt["file_sha256"]
            or Path(path).stat().st_size != receipt["bytes"]
        ):
            raise ValueError
        source = pq.ParquetFile(path)
        if source.metadata.num_rows != receipt["rows"] or not source.schema_arrow.equals(
            schema(name), check_metadata=True
        ):
            raise ValueError
        # Bound decompression before allocating Arrow buffers, including record_json.
        uncompressed = sum(
            source.metadata.row_group(group).column(column).total_uncompressed_size
            for group in range(source.metadata.num_row_groups)
            for column in range(source.metadata.num_columns)
        )
        if uncompressed < 0 or uncompressed > max_bytes:
            raise ValueError
        actual = source.read()
        rows = []
        for raw in actual.column("record_json").to_pylist():
            if not isinstance(raw, str) or len(raw.encode()) > MAX_RECORD_BYTES:
                raise ValueError
            row = json.loads(raw)
            if encoded(row).decode() != raw:
                raise ValueError
            rows.append(row)
        expected = table(name, rows, max_rows=max_rows, max_bytes=max_bytes)
        if (
            max(actual.nbytes, expected.nbytes) != receipt["arrow_bytes"]
            or not actual.equals(expected, check_metadata=True)
            or digest(rows) != receipt["logical_sha256"]
        ):
            raise ValueError
        return rows
    except (OSError, ValueError, KeyError, TypeError, pa.ArrowException):
        raise ValueError("FINRA typed Parquet integrity mismatch") from None


def write_table(path, name, rows, *, max_rows=DEFAULT_ROWS, max_bytes=DEFAULT_BYTES):
    values = table(name, rows, max_rows=max_rows, max_bytes=max_bytes)
    path = Path(path)
    created = False
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
        with os.fdopen(fd, "wb") as stream:
            pq.write_table(values, stream, compression="zstd", row_group_size=10000, version="2.6")
            stream.flush()
            os.fsync(stream.fileno())
        if path.stat().st_size > max_bytes:
            raise ValueError("FINRA snapshot file byte budget exceeded")
        receipt = dict(
            rows=len(rows),
            bytes=path.stat().st_size,
            arrow_bytes=max(values.nbytes, pq.ParquetFile(path).read().nbytes),
            schema_sha256=fingerprint(name),
            file_sha256=file_digest(path, max_bytes),
            logical_sha256=digest(rows),
        )
        if read_table(path, name, receipt, max_rows=max_rows, max_bytes=max_bytes) != rows:
            raise ValueError("FINRA snapshot readback mismatch")
        return receipt
    except (OSError, ValueError, pa.ArrowException):
        if created:
            path.unlink(missing_ok=True)
        raise ValueError("FINRA typed Parquet write refused") from None
