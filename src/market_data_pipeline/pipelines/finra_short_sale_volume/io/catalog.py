"""SSV-AC10: frozen publication facts and the COMMITTED/PUBLISHED journal.

Callers serialize operations with the pipeline ingestion lock. This catalog is
separate from the operational ingestion database; it owns published snapshots.
"""

import hashlib
import json
import sqlite3
from contextlib import closing, contextmanager
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    CONTRACT,
    DEFAULT_BYTES,
    DEFAULT_ROWS,
    FIELDS,
    MAX_RECORD_BYTES,
    fingerprint,
    limits,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
    _admit,
    _clock_text,
    _identity,
    _json,
    _publish_directory,
    _put,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.warehouse import _schema

MAX_GENERATIONS = 10000
MAX_MANIFEST_BYTES = 1048576
DDL = """
PRAGMA user_version=1;
CREATE TABLE identity(body TEXT NOT NULL);
CREATE TABLE generations(
 generation_id TEXT PRIMARY KEY, ordinal INTEGER NOT NULL UNIQUE CHECK(ordinal>0),
 state TEXT NOT NULL CHECK(state IN ('COMMITTED','PUBLISHED')),
 manifest TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
 previous_id TEXT REFERENCES generations(generation_id),
 generated_at TEXT NOT NULL, published_at TEXT
);
CREATE TABLE snapshot_rows(
 generation_id TEXT NOT NULL REFERENCES generations(generation_id),
 table_name TEXT NOT NULL, row_number INTEGER NOT NULL CHECK(row_number>=0),
 body TEXT NOT NULL, PRIMARY KEY(generation_id,table_name,row_number)
);
"""


def document(text, bound=MAX_MANIFEST_BYTES):
    try:
        if not isinstance(text, str) or len(text.encode()) > bound:
            raise ValueError
        result = json.loads(text)
        if not isinstance(result, dict) or encoded(result).decode() != text:
            raise ValueError
        return result
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("FINRA publication document is invalid") from None


@lru_cache(maxsize=1)
def _expected_schema():
    with closing(sqlite3.connect(":memory:")) as db:
        db.executescript(DDL)
        return _schema(db)


@contextmanager
def connect(root, manifest, *, approved_use, operation, create=False):
    kind = _admit(manifest, approved_use, operation, {"fixture", "transform"})
    root = Path(root)
    source = _json(root / "warehouse.json")
    _identity(source.get("warehouse_id"))
    if any(
        source.get(key) != value
        for key, value in {
            "root_sha256": digest(str(root.resolve())),
            "manifest_sha256": digest(manifest.to_dict()),
            "input_kind": kind,
        }.items()
    ):
        raise ValueError("FINRA publication source binding mismatch")
    expected = dict(
        schema_id=CONTRACT,
        root_sha256=digest(str(root.resolve())),
        manifest_sha256=digest(manifest.to_dict()),
        input_kind=kind,
        source_warehouse_id=source["warehouse_id"],
        schemas_sha256=digest({name: fingerprint(name) for name in FIELDS}),
        ddl_sha256=hashlib.sha256(DDL.encode()).hexdigest(),
    )
    directory = root / "parquet"
    binding = root / "parquet.json"
    if create and not directory.exists() and not directory.is_symlink():
        if binding.exists() or binding.is_symlink():
            raise ValueError("FINRA bound publication catalog is missing")
        identity = dict(expected, catalog_id=str(uuid4()))
        with closing(sqlite3.connect(":memory:")) as db:
            db.executescript(DDL)
            db.execute("INSERT INTO identity VALUES (?)", (encoded(identity).decode(),))
            db.commit()
            _publish_directory(
                root,
                "parquet",
                {
                    "identity.json": encoded(identity),
                    "catalog.sqlite": db.serialize(),
                    ".staging": None,
                    "snapshots": None,
                },
            )
        _put(binding, encoded(identity))
    path = directory / "catalog.sqlite"
    if directory.is_symlink() or path.is_symlink() or not path.is_file():
        raise ValueError("FINRA bound publication catalog is missing or unsafe")
    for name in (".staging", "snapshots"):
        if (directory / name).is_symlink() or not (directory / name).is_dir():
            raise ValueError("FINRA publication directory is unsafe")
    for suffix in ("-journal", "-wal", "-shm"):
        if path.with_name(path.name + suffix).is_symlink():
            raise ValueError("FINRA publication SQLite sidecar is unsafe")
    identity = _json(directory / "identity.json")
    _identity(identity.get("catalog_id"))
    if (
        set(identity) != set(expected) | {"catalog_id"}
        or any(identity[k] != v for k, v in expected.items())
        or _json(binding) != identity
    ):
        raise ValueError("FINRA publication identity mismatch")
    connection = None
    try:
        connection = sqlite3.connect(
            path.resolve().as_uri() + ("?mode=rw" if create else "?mode=ro"),
            uri=True,
            isolation_level=None,
            timeout=10,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        if create:
            connection.execute("PRAGMA synchronous=FULL")
        if (
            connection.execute("PRAGMA quick_check").fetchone()[0] != "ok"
            or connection.execute("PRAGMA user_version").fetchone()[0] != 1
            or _schema(connection) != _expected_schema()
            or connection.execute("PRAGMA foreign_key_check").fetchone() is not None
        ):
            raise ValueError("FINRA publication catalog integrity mismatch")
        stored = connection.execute("SELECT body FROM identity").fetchall()
        if len(stored) != 1 or document(stored[0][0]) != identity:
            raise ValueError("FINRA publication catalog identity mismatch")
        yield connection
    except sqlite3.DatabaseError:
        raise ValueError("FINRA publication catalog is unavailable or corrupt") from None
    finally:
        if connection is not None:
            connection.close()


def get(db, generation_id):
    _identity(generation_id)
    row = db.execute("SELECT * FROM generations WHERE generation_id=?", (generation_id,)).fetchone()
    if row is None:
        return None
    result = dict(row)
    record = document(result["manifest"])
    if (
        digest(record) != result["manifest_sha256"]
        or any(
            record.get(k) != result[k]
            for k in ("generation_id", "ordinal", "previous_id", "generated_at")
        )
        or result["state"] not in {"COMMITTED", "PUBLISHED"}
        or (result["state"] == "PUBLISHED") != (result["published_at"] is not None)
    ):
        raise ValueError("FINRA publication generation metadata mismatch")
    _clock_text(result["generated_at"])
    if result["published_at"] is not None:
        _clock_text(result["published_at"])
    result["manifest"] = record
    return result


def pending(db):
    return [
        get(db, r[0])
        for r in db.execute(
            "SELECT generation_id FROM generations WHERE state='COMMITTED' ORDER BY ordinal"
        )
    ]


def latest(db):
    row = db.execute(
        "SELECT generation_id FROM generations WHERE state='PUBLISHED' "
        "ORDER BY ordinal DESC LIMIT 1"
    ).fetchone()
    return get(db, row[0]) if row else None


def ordinal(db):
    count, highest = db.execute(
        "SELECT COUNT(*),COALESCE(MAX(ordinal),0) FROM generations"
    ).fetchone()
    if count != highest or count >= MAX_GENERATIONS:
        raise ValueError("FINRA publication generation chain is invalid or full")
    return highest + 1


def rows(db, generation_id, *, max_rows=DEFAULT_ROWS, max_bytes=DEFAULT_BYTES):
    limits(max_rows, max_bytes)
    if get(db, generation_id) is None:
        raise ValueError("FINRA publication generation is missing")
    count, size = db.execute(
        "SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM snapshot_rows "
        "WHERE generation_id=?",
        (generation_id,),
    ).fetchone()
    if count > max_rows or size > max_bytes:
        raise ValueError("FINRA publication catalog row/byte budget exceeded")
    result = {name: [] for name in FIELDS}
    for row in db.execute(
        "SELECT table_name,row_number,body FROM snapshot_rows WHERE generation_id=? "
        "ORDER BY table_name,row_number",
        (generation_id,),
    ):
        name, index, body = row
        if name not in result or index != len(result[name]):
            raise ValueError("FINRA publication catalog membership mismatch")
        result[name].append(document(body, MAX_RECORD_BYTES))
    return result


def commit(db, record, tables, *, before_commit=lambda: None):
    _identity(record["generation_id"])
    _clock_text(record["generated_at"])
    document(encoded(record).decode())
    if set(tables) != set(FIELDS) or pending(db):
        raise ValueError("FINRA publication requires seven tables and no pending generation")
    head = latest(db)
    if record["ordinal"] != ordinal(db) or record["previous_id"] != (
        head["generation_id"] if head else None
    ):
        raise ValueError("FINRA publication generation chain conflict")
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(
            "INSERT INTO generations VALUES (?,?, 'COMMITTED',?,?,?,?,NULL)",
            (
                record["generation_id"],
                record["ordinal"],
                encoded(record).decode(),
                digest(record),
                record["previous_id"],
                record["generated_at"],
            ),
        )
        for name, values in tables.items():
            for index, value in enumerate(values):
                body = encoded(value).decode()
                document(body, MAX_RECORD_BYTES)
                db.execute(
                    "INSERT INTO snapshot_rows VALUES (?,?,?,?)",
                    (
                        record["generation_id"],
                        name,
                        index,
                        body,
                    ),
                )
        before_commit()
        db.commit()
    except BaseException:
        db.rollback()
        raise


def mark(db, generation_id, published_at):
    _clock_text(published_at)
    record = get(db, generation_id)
    if record is None:
        raise ValueError("FINRA publication generation is missing")
    if record["state"] == "PUBLISHED":
        return
    head = latest(db)
    if record["previous_id"] != (head["generation_id"] if head else None):
        raise ValueError("FINRA publication completion chain conflict")
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(
            "UPDATE generations SET state='PUBLISHED',published_at=? WHERE generation_id=?",
            (
                published_at,
                generation_id,
            ),
        )
        db.commit()
    except BaseException:
        db.rollback()
        raise
