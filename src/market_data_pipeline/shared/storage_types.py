"""Small field codecs shared by FTD and threshold contracts; no source semantics.

Arrow is imported only when requested. Spark schemas are explicit StructType JSON;
this module never imports Spark or opens a database connection.
"""

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Context, Decimal, DecimalException, Inexact, InvalidOperation, localcontext


@dataclass(frozen=True)
class Column:
    name: str
    kind: str
    nullable: bool = False
    precision: int | None = None
    scale: int | None = None

    def __post_init__(self):
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", self.name):
            raise ValueError("storage column requires a safe identifier")
        if self.kind not in ("string", "int64", "date", "timestamp", "decimal"):
            raise ValueError("unsupported storage field kind")
        if type(self.nullable) is not bool:
            raise ValueError("storage nullability must be boolean")
        if self.kind == "decimal":
            if (
                type(self.precision) is not int
                or not 1 <= self.precision <= 38
                or type(self.scale) is not int
                or not 0 <= self.scale <= self.precision
            ):
                raise ValueError("invalid portable decimal precision/scale")
        elif self.precision is not None or self.scale is not None:
            raise ValueError("precision/scale apply only to decimal columns")

    def normalize(self, value: object) -> object:
        if value is None:
            if self.nullable:
                return None
            raise ValueError(f"{self.name}: required value is null")
        if self.kind == "string":
            if not isinstance(value, str) or "\x00" in value:
                raise ValueError(f"{self.name}: expected text without NUL")
        elif self.kind == "int64":
            if type(value) is not int or not -(2**63) <= value < 2**63:
                raise ValueError(f"{self.name}: expected signed 64-bit integer")
        elif self.kind == "date":
            if type(value) is not date:
                raise ValueError(f"{self.name}: expected date label")
        elif self.kind == "timestamp":
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError(f"{self.name}: expected timezone-aware datetime")
            value = value.astimezone(UTC)
        elif self.kind == "decimal":
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{self.name}: expected finite exact Decimal")
            _, digits, exponent = value.as_tuple()
            excess = -self.scale - exponent
            if (excess > 0 and any(digits[-excess:])) or (
                not value.is_zero() and value.adjusted() >= self.precision - self.scale
            ):
                raise ValueError(f"{self.name}: decimal range/scale would lose precision")
            try:
                # An independent context prevents the caller's precision from
                # rounding a 38-digit integer or changing storage behavior.
                context = Context(prec=self.precision, traps=[Inexact, InvalidOperation])
                with localcontext(context):
                    value = value.quantize(Decimal((0, (1,), -self.scale)))
            except DecimalException as exc:
                raise ValueError(f"{self.name}: invalid decimal projection") from exc
        return value


@dataclass(frozen=True)
class RecordSchema:
    version: str
    contract_id: str
    columns: tuple[Column, ...]
    validator: Callable[[dict], None] | None = field(default=None, compare=False, repr=False)

    def __post_init__(self):
        if not self.version or not self.contract_id or not self.columns:
            raise ValueError("storage schema requires version, identity and columns")
        if len({c.name for c in self.columns}) != len(self.columns):
            raise ValueError("duplicate storage column")

    def normalize(self, row: Mapping[str, object]) -> dict:
        if not isinstance(row, Mapping) or set(row) != {c.name for c in self.columns}:
            raise ValueError("record fields differ from the storage schema")
        result = {c.name: c.normalize(row[c.name]) for c in self.columns}
        if self.validator:
            self.validator(result)
        return result

    def sql_types(self, backend: str) -> dict[str, str]:
        if backend not in ("sqlite", "postgres"):
            raise ValueError("unsupported SQL backend")
        types = {
            "string": "TEXT",
            "int64": "INTEGER" if backend == "sqlite" else "BIGINT",
            "date": "TEXT" if backend == "sqlite" else "DATE",
            "timestamp": "TEXT" if backend == "sqlite" else "TIMESTAMPTZ",
            "decimal": "TEXT" if backend == "sqlite" else "NUMERIC",
        }
        return {c.name: types[c.kind] for c in self.columns}

    def encode(self, row: Mapping[str, object], backend: str) -> dict:
        self.sql_types(backend)
        values = self.normalize(row)
        if backend == "sqlite":
            for c in self.columns:
                value = values[c.name]
                if value is None:
                    continue
                if c.kind == "timestamp":
                    values[c.name] = value.isoformat(timespec="microseconds")
                elif c.kind == "date":
                    values[c.name] = value.isoformat()
                elif c.kind == "decimal":
                    values[c.name] = format(value, "f")
        return values

    def decode(self, row: Mapping[str, object], backend: str) -> dict:
        self.sql_types(backend)
        if not isinstance(row, Mapping) or set(row) != {c.name for c in self.columns}:
            raise ValueError("record fields differ from the storage schema")
        values = dict(row)
        if backend == "sqlite":
            try:
                for c in self.columns:
                    value = values[c.name]
                    if value is None or c.kind in ("string", "int64"):
                        continue
                    if not isinstance(value, str):
                        raise ValueError("SQLite date/time/decimal fields require text")
                    if c.kind == "date":
                        values[c.name] = date.fromisoformat(value)
                    elif c.kind == "timestamp":
                        values[c.name] = datetime.fromisoformat(value)
                    elif c.kind == "decimal":
                        values[c.name] = Decimal(value)
            except (ValueError, DecimalException) as exc:
                raise ValueError("invalid SQLite serialized value") from exc
        return self.normalize(values)

    def arrow_schema(self):
        import pyarrow as pa

        types = {
            "string": pa.string(),
            "int64": pa.int64(),
            "date": pa.date32(),
            "timestamp": pa.timestamp("us", tz="UTC"),
        }
        return pa.schema(
            [
                pa.field(
                    c.name,
                    pa.decimal128(c.precision, c.scale) if c.kind == "decimal" else types[c.kind],
                    nullable=c.nullable,
                )
                for c in self.columns
            ],
            metadata={"storage_schema": self.version, "storage_contract_id": self.contract_id},
        )

    def arrow_table(self, rows: Iterable[Mapping[str, object]]):
        import pyarrow as pa

        values = [self.normalize(row) for row in rows]
        return pa.Table.from_pylist(values, schema=self.arrow_schema())

    def read_arrow(self, table) -> list[dict]:
        if not table.schema.equals(self.arrow_schema(), check_metadata=True):
            raise ValueError("Arrow schema/contract differs from the storage schema")
        return [self.normalize(row) for row in table.to_pylist()]

    def spark_schema_json(self) -> dict:
        types = {
            "string": "string",
            "int64": "long",
            "date": "date",
            "timestamp": "timestamp",
        }
        return {
            "type": "struct",
            "fields": [
                {
                    "name": c.name,
                    "type": f"decimal({c.precision},{c.scale})"
                    if c.kind == "decimal"
                    else types[c.kind],
                    "nullable": c.nullable,
                    "metadata": {},
                }
                for c in self.columns
            ],
        }
