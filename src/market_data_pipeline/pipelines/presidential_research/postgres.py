"""Explicit native presidential warehouse primary; files remain sealed evidence."""

import hashlib
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from market_data_pipeline.shared.files import writer_lock
from market_data_pipeline.shared.postgres import (
    Connection,
    Settings,
    bind,
    binding,
    connect,
    schema_fingerprint,
)

from . import warehouse

PIPELINE = "presidential_research"
CONTRACT = "presidential-postgres-primary/1"
TABLES = {
    "metadata": "key",
    "captures": "capture_sequence",
    "facts": "version_id",
    "capture_facts": "capture_id,fact_type,fact_key",
    "publication_journal": "snapshot_id",
}
DDL = tuple(
    s.strip().replace("capture_sequence INTEGER", "capture_sequence BIGINT")
    for s in warehouse._DDL.split(";")
    if s.strip()
) + (
    "CREATE TABLE publication_journal (snapshot_id TEXT PRIMARY KEY,status TEXT NOT NULL)",
    "CREATE TABLE primary_identity (singleton BOOLEAN PRIMARY KEY CHECK(singleton),"
    "body TEXT NOT NULL,body_sha256 TEXT NOT NULL)",
)
DDL_SHA = hashlib.sha256("\n".join(DDL).encode()).hexdigest()


def fingerprint(db, name):
    checksum, count = hashlib.sha256(), 0
    for row in db.execute("SELECT * FROM " + name + " ORDER BY " + TABLES[name]):
        checksum.update(warehouse._json(list(row)).encode())
        count += 1
    return dict(count=count, sha256=checksum.hexdigest())


def _identity(db, root, selected, settings):
    import json

    rows = db.execute("SELECT body,body_sha256 FROM primary_identity").fetchall()
    if len(rows) != 1:
        raise ValueError("Presidential PostgreSQL identity is missing or ambiguous")
    try:
        value = json.loads(rows[0][0])
        if (
            set(value)
            != {"format", "store_id", "root_sha256", "ddl_sha256", "schema_sha256", "imports"}
            or value["format"] != CONTRACT
            or rows[0][1] != warehouse._hash(value)
            or value["store_id"] != selected["store_id"]
            or value["root_sha256"] != warehouse._hash(str(Path(root).resolve()))
            or value["ddl_sha256"] != DDL_SHA
            or value["schema_sha256"] != schema_fingerprint(db, settings.schema)
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError):
        raise ValueError("Presidential PostgreSQL identity/schema differs") from None
    if [tuple(r) for r in db.execute("SELECT * FROM metadata")] != [("schema", warehouse._SCHEMA)]:
        raise ValueError("Presidential PostgreSQL logical identity differs")
    return value


class NativeConnection(Connection):
    """Own the shared connection context and retain SQLite's tuple-row API."""

    def __init__(self, db, manager):
        super().__init__(db.raw)
        self.manager = manager

    def execute(self, statement, parameters=()):
        try:
            return super().execute(statement, parameters).raw
        except psycopg.Error:
            raise ValueError("Presidential PostgreSQL statement failed") from None

    def commit(self):
        try:
            super().commit()
        except psycopg.Error:
            self.rollback()
            raise ValueError("Presidential PostgreSQL commit failed") from None

    def close(self):
        self.manager.__exit__(None, None, None)


def open_primary(root, *, write=False):
    selected = binding(root, PIPELINE)
    if selected is None:
        raise ValueError("Presidential PostgreSQL primary is not selected")
    settings = Settings(**selected["postgres"])
    manager = connect(settings, read_only=not write)
    opened = False
    try:
        db = manager.__enter__()
        opened = True
        if not write:
            db.execute("BEGIN")
        _identity(db, root, selected, settings)
        return NativeConnection(db, manager)
    except BaseException as exc:
        if opened:
            manager.__exit__(type(exc), exc, exc.__traceback__)
        if isinstance(exc, psycopg.Error):
            raise ValueError("Presidential PostgreSQL primary is unavailable or invalid") from None
        raise


def _migrate(root, settings, fault):
    if not isinstance(settings, Settings):
        raise ValueError("Presidential migration requires validated PostgreSQL settings")
    selected = binding(root, PIPELINE)
    if selected:
        if selected["postgres"] != asdict(settings):
            raise ValueError("Presidential primary target conflicts")
        db = open_primary(root)
        db.close()
        return dict(status="ALREADY_PRIMARY", warehouse="postgres")
    root = Path(root).resolve(strict=True)
    with writer_lock(root):
        source = warehouse._database(root, create=False)
        try:
            present = {
                r[0] for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            imports = {
                n: fingerprint(source, n)
                if n in present
                else dict(count=0, sha256=hashlib.sha256().hexdigest())
                for n in TABLES
            }
            with connect(settings) as db:
                if db.execute("SELECT to_regclass('primary_identity')").fetchone()[0]:
                    import json

                    row = db.execute("SELECT body FROM primary_identity").fetchone()
                    selected = {"store_id": json.loads(row[0])["store_id"]}
                    value = _identity(db, root, selected, settings)
                    if (
                        value["imports"] != imports
                        or {n: fingerprint(db, n) for n in TABLES} != imports
                    ):
                        raise ValueError("Presidential unbound migration differs from source")
                else:
                    db.execute("BEGIN")
                    try:
                        if db.execute(
                            "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                            "WHERE n.nspname=current_schema() "
                            "AND c.relkind IN ('r','v','m','S','p')"
                        ).fetchone():
                            raise ValueError("Presidential PostgreSQL target must be empty")
                        for statement in DDL:
                            db.execute(statement)
                        for name in TABLES:
                            if name not in present:
                                continue
                            with db.raw.cursor().copy(
                                sql.SQL("COPY {} FROM STDIN").format(sql.Identifier(name))
                            ) as stream:
                                for row in source.execute(
                                    "SELECT * FROM " + name + " ORDER BY " + TABLES[name]
                                ):
                                    stream.write_row(tuple(row))
                        if {n: fingerprint(db, n) for n in TABLES} != imports:
                            raise ValueError("Presidential migration differs from source")
                        value = dict(
                            format=CONTRACT,
                            store_id=str(uuid4()),
                            root_sha256=warehouse._hash(str(root)),
                            ddl_sha256=DDL_SHA,
                            schema_sha256=schema_fingerprint(db, settings.schema),
                            imports=imports,
                        )
                        db.execute(
                            "INSERT INTO primary_identity VALUES(TRUE,?,?)",
                            (warehouse._json(value), warehouse._hash(value)),
                        )
                        fault("before_commit")
                        db.commit()
                    except BaseException:
                        db.rollback()
                        raise
            fault("before_bind")
            bind(root, PIPELINE, settings, value["store_id"])
        finally:
            source.close()
    return dict(status="MIGRATED", warehouse="postgres", tables=imports)


def migrate(root, settings, *, fault=lambda phase: None):
    try:
        return _migrate(root, settings, fault)
    except psycopg.Error:
        raise ValueError(
            "Presidential PostgreSQL migration failed; original store retained"
        ) from None
