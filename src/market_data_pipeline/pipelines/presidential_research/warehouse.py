"""Source-owned, offline observed-version SQLite and typed Parquet storage.

This is an admission-time history, not a provider real-time vintage database.
"""

import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import tempfile
from datetime import datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from market_data_pipeline.shared.files import utc_timestamp, writer_lock

from .config import load_config
from .parsers import ParseError, parse_payload
from .storage import _safe_child, load_capture

_METHOD = "presidential-donor/0.1.0"
_SCHEMA = "presidential-facts/1"
_NUMERIC = {"market_return_decimal", "policy_rate_percent", "price", "dividend", "cape"}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _decimal(value):
    if value is None or value == "":
        return None
    try:
        amount = Decimal(value)
        if not amount.is_finite() or amount.copy_abs() >= Decimal(10) ** 20:
            raise ValueError("Decimal is outside the typed storage bounds.")
        with localcontext() as context:
            context.prec = 60
            exact = amount.quantize(Decimal("0.000000000000000001"))
        if exact != amount:
            raise ValueError("Decimal exceeds typed storage precision.")
        return amount
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Decimal cannot be stored exactly in decimal128(38,18).") from exc


def _database(root, *, create):
    silver = _safe_child(Path(root), "silver")
    if create:
        silver.mkdir(mode=0o700, exist_ok=True)
    db = _safe_child(silver, "presidential.sqlite3")
    exists = db.exists()
    if exists and not db.is_file():
        raise ValueError("Invalid presidential warehouse file.")
    if not exists and not create:
        raise ValueError("Presidential warehouse is unavailable.")
    connection = (
        sqlite3.connect(db.as_uri() + ("?mode=rw" if create else "?mode=ro"), uri=True)
        if exists
        else sqlite3.connect(db)
    )
    connection.execute("PRAGMA foreign_keys = ON")
    if exists:
        try:
            row = connection.execute("SELECT value FROM metadata WHERE key = 'schema'").fetchone()
            if row is None or row[0] != _SCHEMA:
                raise ValueError("Unsupported warehouse schema.")
        except (sqlite3.Error, ValueError) as exc:
            connection.close()
            raise ValueError("Invalid presidential warehouse identity.") from exc
        return connection
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS captures (
          capture_id TEXT PRIMARY KEY, capture_sequence INTEGER UNIQUE NOT NULL,
          source_id TEXT NOT NULL, first_seen_at TEXT NOT NULL, retrieved_at TEXT,
          input_kind TEXT NOT NULL, raw_sha256 TEXT NOT NULL, config_sha256 TEXT NOT NULL,
          parser_id TEXT NOT NULL, status TEXT NOT NULL, row_count INTEGER NOT NULL,
          missing_count INTEGER NOT NULL, new_versions INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS facts (
          version_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, fact_type TEXT NOT NULL,
          fact_key TEXT NOT NULL, capture_sequence INTEGER NOT NULL, first_seen_at TEXT NOT NULL,
          retrieved_at TEXT, provider_published_at TEXT, input_kind TEXT NOT NULL,
          raw_sha256 TEXT NOT NULL, capture_id TEXT NOT NULL, content_hash TEXT NOT NULL,
          values_json TEXT NOT NULL, schema_version TEXT NOT NULL, method_version TEXT NOT NULL,
          FOREIGN KEY(capture_id) REFERENCES captures(capture_id)
        );
        CREATE INDEX IF NOT EXISTS fact_history
          ON facts(source_id, fact_type, fact_key, capture_sequence);
        CREATE TABLE IF NOT EXISTS capture_facts (
          capture_id TEXT NOT NULL, fact_type TEXT NOT NULL, fact_key TEXT NOT NULL,
          version_id TEXT NOT NULL, observed_json TEXT NOT NULL,
          PRIMARY KEY(capture_id, fact_type, fact_key),
          FOREIGN KEY(capture_id) REFERENCES captures(capture_id),
          FOREIGN KEY(version_id) REFERENCES facts(version_id)
        );
    """)
    connection.execute("INSERT INTO metadata VALUES ('schema', ?)", (_SCHEMA,))
    connection.commit()
    return connection


def _fred_rows(raw, source_id, parsed):
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    value_columns = [key for key in (reader.fieldnames or []) if key != "observation_date"]
    if len(value_columns) != 1:
        raise ValueError("Invalid policy source columns.")
    normalized = {row["date"]: row for row in parsed}
    rows, seen = [], set()
    for row in reader:
        observed = (row.get("observation_date") or "").strip()
        token = (row.get(value_columns[0]) or "").strip()
        if not observed:
            continue
        try:
            if datetime.strptime(observed, "%Y-%m-%d").date().isoformat() != observed:
                raise ValueError("Invalid policy date.")
        except ValueError as exc:
            raise ValueError("Invalid policy date.") from exc
        if observed in seen:
            raise ValueError("Duplicate policy date.")
        seen.add(observed)
        if token in ("", "."):
            rows.append(
                {
                    "date": observed,
                    "policy_rate_percent": None,
                    "source_id": source_id,
                    "source_token": token,
                }
            )
        else:
            if observed not in normalized:
                raise ValueError("Policy parser and token inventory disagree.")
            if Decimal(token) != Decimal(normalized[observed]["policy_rate_percent"]):
                raise ValueError("Policy normalization would round a source token.")
            rows.append({**normalized[observed], "source_token": token})
    return rows


def _factor_tokens(raw):
    with ZipFile(io.BytesIO(raw)) as archive:
        names = [item for item in archive.namelist() if item.lower().endswith((".csv", ".txt"))]
        if len(names) != 1:
            raise ValueError("Invalid factor archive.")
        lines = archive.read(names[0]).decode("utf-8-sig").splitlines()
    index = next(i for i, line in enumerate(lines) if "Mkt-RF" in line and "RF" in line)
    output = {}
    for row in csv.DictReader(io.StringIO("\n".join(lines[index:]))):
        month = (row.get("") or row.get("Date") or "").strip()
        if not re.fullmatch(r"\d{6}", month):
            continue
        if month in output:
            raise ValueError("Duplicate factor month.")
        output[month] = ((row.get("Mkt-RF") or "").strip(), (row.get("RF") or "").strip())
    return output


def _normalize(parser, raw, source_id):
    rows = parse_payload(parser, raw, source_id)
    if parser == "fred_csv":
        rows = _fred_rows(raw, source_id, rows)
    elif parser == "fama_french_monthly_zip":
        tokens = _factor_tokens(raw)
        enriched = []
        for row in rows:
            market, risk_free = tokens[row["date"][:7].replace("-", "")]
            with localcontext() as context:
                context.prec = 100
                exact_return = (Decimal(market) + Decimal(risk_free)) / Decimal(100)
            if exact_return != Decimal(row["market_return_decimal"]):
                raise ValueError("Market normalization would round source tokens.")
            enriched.append(
                {
                    **row,
                    "construction": "crsp_market_proxy",
                    "market_excess_token": market,
                    "risk_free_token": risk_free,
                }
            )
        rows = enriched
    elif parser == "shiller_xls":
        rows = [{**row, "source_value_origin": "XLS_BINARY_DECODED"} for row in rows]
    output, seen = [], set()
    types = {
        "fred_csv": "POLICY",
        "fama_french_monthly_zip": "MARKET",
        "shiller_xls": "SHILLER_CONTEXT",
        "house_party_government_html": "POLITICAL_CANDIDATE",
        "wikipedia_congress_party_divisions_html": "POLITICAL_CANDIDATE",
    }
    if parser not in types:
        raise ValueError("Unsupported fact parser.")
    for row in rows:
        if row.get("source_id") != source_id:
            raise ValueError("Source identity mismatch.")
        fact_type = types[parser]
        key = row.get("date") if fact_type != "POLITICAL_CANDIDATE" else row.get("congress")
        if fact_type == "MARKET" and isinstance(key, str):
            key = f"{key}|{row['construction']}"
        if not isinstance(key, str) or not key or (fact_type, key) in seen:
            raise ValueError("Duplicate or invalid fact key.")
        seen.add((fact_type, key))
        for field in _NUMERIC:
            if field in row:
                _decimal(row[field])
        for field in ("source_token", "market_excess_token", "risk_free_token"):
            if field in row and row[field] not in ("", "."):
                _decimal(row[field])
        output.append((fact_type, key, row))
    return output


def ingest_capture(root, source_id, capture_id):
    """Reparse and admit an explicitly selected capture in observed sequence order."""
    root = Path(root)
    with writer_lock(root):
        receipt, raw, config_bytes = load_capture(root, source_id, capture_id)
        with tempfile.TemporaryDirectory(prefix="presidential-warehouse-") as tmp:
            config_path = Path(tmp) / "sources.toml"
            config_path.write_bytes(config_bytes)
            source = load_config(config_path).source(source_id)
            if source.parser != receipt["parser_id"]:
                raise ValueError("Capture parser does not match archived configuration.")
        connection = _database(root, create=True)
        try:
            prior = connection.execute(
                "SELECT status, row_count, missing_count, new_versions, capture_sequence, "
                "source_id, first_seen_at, retrieved_at, input_kind, raw_sha256, config_sha256, "
                "parser_id FROM captures WHERE capture_id = ?",
                (capture_id,),
            ).fetchone()
            if prior:
                expected = (
                    receipt["capture_sequence"],
                    source_id,
                    receipt["first_seen_at"],
                    receipt["retrieved_at"],
                    receipt["input_kind"],
                    receipt["sha256"],
                    receipt["config_sha256"],
                    receipt["parser_id"],
                )
                if prior[4:] != expected:
                    raise ValueError("Capture identity conflicts with warehouse audit.")
                return {
                    "status": prior[0],
                    "row_count": prior[1],
                    "missing_count": prior[2],
                    "new_versions": 0,
                    "reused": True,
                }
            latest = connection.execute(
                "SELECT capture_sequence, first_seen_at FROM captures "
                "ORDER BY capture_sequence DESC LIMIT 1"
            ).fetchone()
            if latest and (
                receipt["capture_sequence"] <= latest[0] or receipt["first_seen_at"] < latest[1]
            ):
                raise ValueError("Capture order is ambiguous; ingest in admission sequence.")
            try:
                rows = _normalize(receipt["parser_id"], raw, source_id)
                status = "PARSED"
            except (
                ParseError,
                ValueError,
                UnicodeError,
                BadZipFile,
                csv.Error,
                EOFError,
                OSError,
                KeyError,
                StopIteration,
            ):
                rows, status = [], "REJECTED"
            missing = sum(
                1
                for _, _, row in rows
                if "policy_rate_percent" in row and row["policy_rate_percent"] is None
            )
            new_versions = 0
            with connection:
                connection.execute(
                    "INSERT INTO captures VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        capture_id,
                        receipt["capture_sequence"],
                        source_id,
                        receipt["first_seen_at"],
                        receipt["retrieved_at"],
                        receipt["input_kind"],
                        receipt["sha256"],
                        receipt["config_sha256"],
                        receipt["parser_id"],
                        status,
                        len(rows),
                        missing,
                        0,
                    ),
                )
                for fact_type, key, values in rows:
                    normalized = {
                        k: v
                        for k, v in values.items()
                        if k not in ("source_token", "market_excess_token", "risk_free_token")
                    }
                    content_hash = _hash(
                        {"values": normalized, "input_kind": receipt["input_kind"]}
                    )
                    previous = connection.execute(
                        "SELECT version_id, content_hash FROM facts WHERE source_id=? "
                        "AND fact_type=? AND fact_key=? ORDER BY capture_sequence DESC LIMIT 1",
                        (source_id, fact_type, key),
                    ).fetchone()
                    if previous and previous[1] == content_hash:
                        version_id = previous[0]
                    else:
                        version_id = _hash(
                            {
                                "source_id": source_id,
                                "fact_type": fact_type,
                                "fact_key": key,
                                "capture_sequence": receipt["capture_sequence"],
                                "content_hash": content_hash,
                            }
                        )
                        connection.execute(
                            "INSERT INTO facts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                version_id,
                                source_id,
                                fact_type,
                                key,
                                receipt["capture_sequence"],
                                receipt["first_seen_at"],
                                receipt["retrieved_at"],
                                receipt["provider_published_at"],
                                receipt["input_kind"],
                                receipt["sha256"],
                                capture_id,
                                content_hash,
                                _json(values),
                                _SCHEMA,
                                _METHOD,
                            ),
                        )
                        new_versions += 1
                    connection.execute(
                        "INSERT INTO capture_facts VALUES (?,?,?,?,?)",
                        (capture_id, fact_type, key, version_id, _json(values)),
                    )
                connection.execute(
                    "UPDATE captures SET new_versions=? WHERE capture_id=?",
                    (new_versions, capture_id),
                )
            return {
                "status": status,
                "row_count": len(rows),
                "missing_count": missing,
                "new_versions": new_versions,
                "reused": False,
            }
        finally:
            connection.close()


def observed_as_of(root, cutoff: datetime, *, capture_sequence=None):
    """Select versions known by a UTC cutoff; equal clocks use capture ordering."""
    cutoff_string = utc_timestamp(cutoff)
    if capture_sequence is not None and (type(capture_sequence) is not int or capture_sequence < 1):
        raise ValueError("Invalid capture sequence cutoff.")
    with writer_lock(Path(root)):
        connection = _database(Path(root), create=False)
        try:
            rows = connection.execute(
                """SELECT version_id, source_id, fact_type, fact_key, capture_sequence,
                       first_seen_at, retrieved_at, provider_published_at, input_kind, raw_sha256,
                       capture_id, content_hash, values_json, schema_version, method_version
                FROM (SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY source_id, fact_type, fact_key ORDER BY capture_sequence DESC
                ) AS rank FROM facts WHERE first_seen_at <= ?
                    AND (? IS NULL OR capture_sequence <= ?))
                WHERE rank = 1 ORDER BY source_id, fact_type, fact_key""",
                (cutoff_string, capture_sequence, capture_sequence),
            ).fetchall()
            names = (
                "version_id",
                "source_id",
                "fact_type",
                "fact_key",
                "capture_sequence",
                "first_seen_at",
                "retrieved_at",
                "provider_published_at",
                "input_kind",
                "raw_sha256",
                "capture_id",
                "content_hash",
                "values_json",
                "schema_version",
                "method_version",
            )
            output = []
            checked_captures = {}
            for row in rows:
                item = dict(zip(names, row, strict=True))
                try:
                    item["values"] = json.loads(item.pop("values_json"))
                    values = item["values"]
                    normalized = {
                        k: v
                        for k, v in values.items()
                        if k not in ("source_token", "market_excess_token", "risk_free_token")
                    }
                    content_hash = _hash({"values": normalized, "input_kind": item["input_kind"]})
                    identity = _hash(
                        {
                            "source_id": item["source_id"],
                            "fact_type": item["fact_type"],
                            "fact_key": item["fact_key"],
                            "capture_sequence": item["capture_sequence"],
                            "content_hash": content_hash,
                        }
                    )
                    capture_id = item["capture_id"]
                    if capture_id not in checked_captures:
                        receipt, raw, _ = load_capture(Path(root), item["source_id"], capture_id)
                        reconstructed = {
                            (fact_type, key): row
                            for fact_type, key, row in _normalize(
                                receipt["parser_id"], raw, item["source_id"]
                            )
                        }
                        checked_captures[capture_id] = (receipt, reconstructed)
                    receipt, reconstructed = checked_captures[capture_id]
                    if (
                        item["schema_version"] != _SCHEMA
                        or item["method_version"] != _METHOD
                        or values.get("source_id") != item["source_id"]
                        or reconstructed.get((item["fact_type"], item["fact_key"])) != values
                        or content_hash != item["content_hash"]
                        or identity != item["version_id"]
                        or receipt["capture_sequence"] != item["capture_sequence"]
                        or receipt["first_seen_at"] != item["first_seen_at"]
                        or receipt["retrieved_at"] != item["retrieved_at"]
                        or receipt["provider_published_at"] != item["provider_published_at"]
                        or receipt["input_kind"] != item["input_kind"]
                        or receipt["sha256"] != item["raw_sha256"]
                    ):
                        raise ValueError("Fact and capture do not match.")
                except (
                    BadZipFile,
                    csv.Error,
                    EOFError,
                    KeyError,
                    OSError,
                    StopIteration,
                    TypeError,
                    ValueError,
                ) as exc:
                    raise ValueError("Presidential warehouse integrity check failed.") from exc
                output.append(item)
            return output
        finally:
            connection.close()


def export_parquet(root, destination, cutoff: datetime, *, capture_sequence=None):
    """Export an explicitly selected observed view; never overwrite a file."""
    from datetime import date

    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            ("version_id", pa.string()),
            ("source_id", pa.string()),
            ("fact_type", pa.string()),
            ("fact_key", pa.string()),
            ("observation_date", pa.date32()),
            ("capture_sequence", pa.int64()),
            ("first_seen_at", pa.timestamp("us", tz="UTC")),
            ("retrieved_at", pa.timestamp("us", tz="UTC")),
            ("provider_published_at", pa.timestamp("us", tz="UTC")),
            ("input_kind", pa.string()),
            ("raw_sha256", pa.string()),
            ("capture_id", pa.string()),
            ("content_hash", pa.string()),
            ("numeric_value", pa.decimal128(38, 18)),
            ("price", pa.decimal128(38, 18)),
            ("dividend", pa.decimal128(38, 18)),
            ("cape", pa.decimal128(38, 18)),
            ("source_token", pa.string()),
            ("values_json", pa.string()),
            ("schema_version", pa.string()),
            ("method_version", pa.string()),
        ]
    )
    records = []
    for item in observed_as_of(root, cutoff, capture_sequence=capture_sequence):
        values = item["values"]
        numeric = values.get("policy_rate_percent", values.get("market_return_decimal"))
        record = {
            key: item[key]
            for key in (
                "version_id",
                "source_id",
                "fact_type",
                "fact_key",
                "capture_sequence",
                "input_kind",
                "raw_sha256",
                "capture_id",
                "content_hash",
                "schema_version",
                "method_version",
            )
        }
        record.update(
            observation_date=date.fromisoformat(values["date"]) if "date" in values else None,
            numeric_value=_decimal(numeric),
            price=_decimal(values.get("price")),
            dividend=_decimal(values.get("dividend")),
            cape=_decimal(values.get("cape")),
            source_token=values.get("source_token"),
            values_json=_json(values),
        )
        for key in ("first_seen_at", "retrieved_at", "provider_published_at"):
            record[key] = (
                datetime.fromisoformat(item[key].replace("Z", "+00:00"))
                if item[key] is not None
                else None
            )
        records.append(record)
    table = pa.Table.from_pylist(records, schema=schema)
    target = Path(destination)
    staging = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=".pres-parquet-", dir=target.parent, delete=False
        ) as handle:
            staging = Path(handle.name)
            pq.write_table(table, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(staging, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if staging is not None:
            staging.unlink(missing_ok=True)
    return {
        "path": str(target),
        "rows": len(records),
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
    }
