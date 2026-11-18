"""PG-AC03/04 OTC native relational primary, preserving the T07 logical engine."""

import hashlib
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

from market_data_pipeline.pipelines.finra_otc_transparency.io import history, warehouse
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import _base, _locked, _root
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest, encoded
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import require_approved_use
from market_data_pipeline.shared.postgres import (
    Connection,
    Settings,
    bind,
    binding,
    connect,
    schema_fingerprint,
)

PIPELINE = "finra_otc_transparency"
CONTRACT = "finra-otc-postgres-primary/1"
TABLES = {
    "otc_metadata": "contract",
    "otc_attempts": "capture_id",
    "otc_publications": "capture_id",
    "otc_rows": "capture_id,symbol_raw,participant_key",
    "otc2_attempts": "capture_id",
    "otc2_versions": "version_id",
    "otc2_snapshots": "capture_id",
    "otc2_members": "capture_id,symbol_raw,participant_key",
    "otc2_migrations": "migration_id",
}
IMMUTABLE = ("otc_metadata", "otc_attempts", "otc_publications", "otc_rows", "otc2_migrations")
DDL = (
    warehouse._SCHEMA
    + history.SCHEMA
    + (
        "CREATE TABLE primary_identity (singleton BOOLEAN PRIMARY KEY CHECK(singleton),"
        "body TEXT NOT NULL,body_sha256 TEXT NOT NULL)",
    )
)
DDL_SHA = hashlib.sha256("\n".join(DDL).encode()).hexdigest()


def fingerprint(db, name):
    count = 0
    checksum = hashlib.sha256()
    for row in db.execute("SELECT * FROM " + name + " ORDER BY " + TABLES[name]):
        checksum.update(encoded(list(row)))
        count += 1
    return {"count": count, "sha256": checksum.hexdigest()}


def _size(raw, schema):
    size = raw.execute(
        "SELECT COALESCE(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=%s AND c.relkind IN ('r','p')",
        (schema,),
    ).fetchone()[0]
    if size > warehouse.MAX_DB_BYTES:
        raise ValueError("OTC PostgreSQL warehouse exceeds its 256 MiB storage ceiling")


class NativeConnection(Connection):
    def __init__(self, raw, schema):
        super().__init__(raw)
        self.schema = schema

    def execute(self, statement, parameters=()):
        if statement.strip().upper() == "COMMIT":
            _size(self.raw, self.schema)
        return super().execute(statement, parameters)

    def commit(self):
        _size(self.raw, self.schema)
        super().commit()


def _metadata(db, root, selected, settings):
    rows = db.execute("SELECT body,body_sha256 FROM primary_identity").fetchall()
    if len(rows) != 1:
        raise ValueError("OTC PostgreSQL identity is missing or ambiguous")
    value = warehouse._record(rows[0][0])
    required = {
        "format",
        "store_id",
        "root_sha256",
        "ddl_sha256",
        "schema_sha256",
        "import_fingerprints",
        "legacy_fingerprints",
    }
    if (
        set(value) != required
        or value.get("format") != CONTRACT
        or rows[0][1] != digest(value)
        or value.get("ddl_sha256") != DDL_SHA
        or value.get("store_id") != selected["store_id"]
        or value.get("root_sha256") != digest(str(Path(root).resolve()))
        or value.get("schema_sha256") != schema_fingerprint(db, settings.schema)
        or not isinstance(value.get("legacy_fingerprints"), dict)
        or set(value["legacy_fingerprints"]) != set(IMMUTABLE)
    ):
        raise ValueError("OTC PostgreSQL primary identity/schema differs")
    logical = db.execute("SELECT contract FROM otc_metadata").fetchall()
    if len(logical) != 1 or logical[0][0] != warehouse.CONTRACT:
        raise ValueError("OTC PostgreSQL logical engine is not v2")
    if {name: fingerprint(db, name) for name in IMMUTABLE} != value["legacy_fingerprints"]:
        raise ValueError("OTC PostgreSQL immutable legacy evidence changed")
    _size(db.raw, settings.schema)
    return value


@contextmanager
def connection(root, *, write=False):
    selected = binding(root, PIPELINE)
    if selected is None:
        raise ValueError("OTC PostgreSQL primary is not selected")
    settings = Settings(**selected["postgres"])
    try:
        with connect(settings, read_only=not write) as db:
            _metadata(db, root, selected, settings)
            yield NativeConnection(db.raw, settings.schema)
    except psycopg.Error:
        raise ValueError("OTC PostgreSQL primary is unavailable or invalid") from None


def _create(db):
    if db.execute(
        "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','v','m','S')"
    ).fetchone():
        raise ValueError("OTC PostgreSQL target must be empty")
    for statement in DDL:
        db.execute(statement)


def _record_identity(db, root, settings, store_id, imports):
    value = dict(
        format=CONTRACT,
        store_id=store_id,
        root_sha256=digest(str(Path(root).resolve())),
        ddl_sha256=DDL_SHA,
        schema_sha256=schema_fingerprint(db, settings.schema),
        import_fingerprints=imports,
        legacy_fingerprints={name: fingerprint(db, name) for name in IMMUTABLE},
    )
    db.execute(
        "INSERT INTO primary_identity VALUES (TRUE,?,?)", (encoded(value).decode(), digest(value))
    )
    _size(db.raw, settings.schema)
    return value


def _existing(db, root, settings):
    row = db.execute("SELECT body FROM primary_identity").fetchone()
    if row is None:
        raise ValueError("OTC PostgreSQL incomplete identity")
    value = warehouse._record(row[0])
    selected = {"store_id": value.get("store_id")}
    return _metadata(db, root, selected, settings)


def _migrate(root, settings, approved_use, fault):
    require_approved_use(approved_use, operation="fixture")
    if not isinstance(settings, Settings):
        raise ValueError("OTC PostgreSQL migration requires validated settings")
    selected = binding(root, PIPELINE)
    if selected:
        if selected["postgres"] != asdict(settings):
            raise ValueError("OTC primary target conflicts")
        with connection(root):
            return {"status": "ALREADY_PRIMARY", "warehouse": "postgres"}
    root = _root(root)
    _base(root)
    with _locked(root), warehouse._connection(root) as source:
        if source.execute("PRAGMA user_version").fetchone()[0] != 2:
            raise ValueError("OTC requires explicit migrate-warehouse before PostgreSQL migration")
        original = {name: fingerprint(source, name) for name in TABLES}
        with connect(settings) as db:
            if db.execute("SELECT to_regclass('primary_identity')").fetchone()[0]:
                value = _existing(db, root, settings)
                if (
                    value["import_fingerprints"] != original
                    or {name: fingerprint(db, name) for name in TABLES} != original
                ):
                    raise ValueError("OTC PostgreSQL incomplete migration differs from source")
            else:
                db.execute("BEGIN IMMEDIATE")
                try:
                    _create(db)
                    for name in TABLES:
                        with db.raw.cursor().copy(
                            sql.SQL("COPY {} FROM STDIN").format(sql.Identifier(name))
                        ) as stream:
                            for row in source.execute(
                                "SELECT * FROM " + name + " ORDER BY " + TABLES[name]
                            ):
                                stream.write_row(tuple(row))
                    if {name: fingerprint(db, name) for name in TABLES} != original:
                        raise ValueError("OTC PostgreSQL migration differs from source")
                    value = _record_identity(db, root, settings, str(uuid4()), original)
                    fault("before_commit")
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
        fault("before_bind")
        bind(root, PIPELINE, settings, value["store_id"])
    return {"status": "MIGRATED", "warehouse": "postgres", "tables": original}


def migrate(root, settings, *, approved_use, fault=lambda phase: None):
    try:
        return _migrate(root, settings, approved_use, fault)
    except psycopg.Error:
        raise ValueError(
            "OTC PostgreSQL migration failed; original store remains retained"
        ) from None


def initialize_empty(root, settings):
    """Administrative storage-only initialization: zero source observations."""
    if not isinstance(settings, Settings):
        raise ValueError("OTC PostgreSQL initialization requires validated settings")
    selected = binding(root, PIPELINE)
    if selected:
        if selected["postgres"] != asdict(settings):
            raise ValueError("OTC primary target conflicts")
        with connection(root):
            return {"status": "ALREADY_PRIMARY", "warehouse": "postgres"}
    root = _root(root, create=True)
    try:
        with _locked(root):
            _base(root, create=True)
            with connect(settings) as db:
                if db.execute("SELECT to_regclass('primary_identity')").fetchone()[0]:
                    value = _existing(db, root, settings)
                    if any(
                        fingerprint(db, name)["count"] for name in TABLES if name != "otc_metadata"
                    ):
                        raise ValueError("OTC empty initialization found observations")
                else:
                    db.execute("BEGIN IMMEDIATE")
                    try:
                        _create(db)
                        db.execute("INSERT INTO otc_metadata VALUES (?)", (warehouse.CONTRACT,))
                        value = _record_identity(db, root, settings, str(uuid4()), {})
                        db.commit()
                    except BaseException:
                        db.rollback()
                        raise
            bind(root, PIPELINE, settings, value["store_id"])
    except psycopg.Error:
        raise ValueError("OTC empty PostgreSQL initialization failed") from None
    return {
        "status": "INITIALIZED",
        "warehouse": "postgres",
        "captures": 0,
        "versions": 0,
        "snapshots": 0,
    }
