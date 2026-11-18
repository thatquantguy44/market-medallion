"""Provider-independent PostgreSQL settings, explicit primary binding and DB-API.

Provider packages own schemas, migrations, admission and publication semantics.
Only positional placeholders and explicit BEGIN spelling are adapted here;
SQLite storage/schema commands are never emulated.
"""

import hashlib
import json
import os
import re
import stat
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import uuid4

FORMAT = "market-medallion-postgres-primary/1"
MARKER = ".postgres-primary.json"


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value):
        raise ValueError("PostgreSQL requires a safe identifier")
    return value


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    database: str
    user: str
    role: str | None
    schema: str
    password_env: str
    sslmode: str = "prefer"
    connect_timeout: int = 10

    def __post_init__(self):
        for value in (self.host, self.database, self.user):
            if not isinstance(value, str) or not value.strip() or "\x00" in value:
                raise ValueError("PostgreSQL host/database/user must be nonempty text")
        for value, maximum in ((self.port, 65535), (self.connect_timeout, 300)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("PostgreSQL port/timeout must be bounded positive integers")
        identifier(self.schema)
        if self.schema in ("public", "information_schema") or self.schema.startswith("pg_"):
            raise ValueError("PostgreSQL primary requires a dedicated provider schema")
        if self.role is not None:
            identifier(self.role)
        if not isinstance(self.password_env, str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*", self.password_env
        ):
            raise ValueError("PostgreSQL password_env must name an environment variable")
        if self.sslmode not in (
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ):
            raise ValueError("Unsupported PostgreSQL sslmode")

    def conninfo(self):
        password = os.environ.get(self.password_env)
        if not password:
            raise ValueError("Set password environment variable " + self.password_env)
        from psycopg.conninfo import make_conninfo

        return make_conninfo(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=password,
            sslmode=self.sslmode,
            connect_timeout=self.connect_timeout,
        )


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate PostgreSQL binding field")
        result[key] = value
    return result


def _root_hash(root):
    return hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()


def binding(root, pipeline):
    identifier(pipeline)
    path = Path(root) / MARKER
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError:
        raise ValueError("PostgreSQL primary binding is unsafe") from None
    try:
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError
        value = json.loads(raw, object_pairs_hook=_pairs)
        if (
            not isinstance(value, dict)
            or set(value) != {"format", "pipeline", "root_sha256", "postgres", "store_id"}
            or value["format"] != FORMAT
            or value["pipeline"] != pipeline
            or value["root_sha256"] != _root_hash(root)
            or not isinstance(value["store_id"], str)
            or not 1 <= len(value["store_id"]) <= 128
        ):
            raise ValueError
        Settings(**value["postgres"])
        return value
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
        raise ValueError("PostgreSQL primary binding is corrupt or mismatched") from None


def bind(root, pipeline, settings, store_id):
    identifier(pipeline)
    if (
        not isinstance(settings, Settings)
        or not isinstance(store_id, str)
        or not 1 <= len(store_id) <= 128
    ):
        raise ValueError("Invalid PostgreSQL primary binding")
    value = dict(
        format=FORMAT,
        pipeline=pipeline,
        root_sha256=_root_hash(root),
        postgres=asdict(settings),
        store_id=store_id,
    )
    current = binding(root, pipeline)
    if current is not None:
        if current != value:
            raise ValueError("PostgreSQL primary is already bound to another target")
        return
    root = Path(root)
    temporary = root / (".postgres-primary-" + uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            os.chmod(temporary, 0o600)
            stream.write((json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode())
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, root / MARKER)
        fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


class Row:
    def __init__(self, names, values):
        self.names, self.values = names, values

    def keys(self):
        return self.names

    def __getitem__(self, key):
        return self.values[self.names.index(key)] if isinstance(key, str) else self.values[key]

    def __iter__(self):
        return iter(self.values)

    def __len__(self):
        return len(self.values)


class Cursor:
    def __init__(self, raw):
        self.raw = raw
        self.names = tuple(c.name for c in raw.description) if raw.description else ()

    def fetchone(self):
        row = self.raw.fetchone()
        return Row(self.names, row) if row is not None else None

    def fetchall(self):
        return [Row(self.names, row) for row in self.raw.fetchall()]

    def __iter__(self):
        return (Row(self.names, row) for row in self.raw)


class Connection:
    backend = "postgres"

    def __init__(self, raw):
        self.raw = raw

    @property
    def in_transaction(self):
        from psycopg.pq import TransactionStatus

        return self.raw.info.transaction_status != TransactionStatus.IDLE

    def execute(self, statement, parameters=()):
        if statement.strip().upper() in ("BEGIN", "BEGIN IMMEDIATE"):
            statement = "BEGIN ISOLATION LEVEL REPEATABLE READ"
        return Cursor(self.raw.execute(statement.replace("?", "%s"), parameters))

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()

    def close(self):
        self.raw.close()

    def __enter__(self):
        if not self.in_transaction:
            self.execute("BEGIN")
        return self

    def __exit__(self, kind, value, traceback):
        self.rollback() if kind else self.commit()


@contextmanager
def connect(settings, *, read_only=False):
    import psycopg
    from psycopg import sql

    raw = psycopg.connect(settings.conninfo(), autocommit=True)
    try:
        if settings.role is not None:
            raw.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(settings.role)))
        if not raw.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname=%s", (settings.schema,)
        ).fetchone():
            raise ValueError("PostgreSQL provider schema does not exist")
        raw.execute(
            sql.SQL("SET search_path TO {},pg_catalog,pg_temp").format(
                sql.Identifier(settings.schema)
            )
        )
        raw.execute("SET lock_timeout='5s'")
        raw.execute("SET statement_timeout='120s'")
        if read_only:
            raw.execute("SET default_transaction_read_only=on")
        else:
            key = int.from_bytes(
                hashlib.sha256((settings.database + ":" + settings.schema).encode()).digest()[:8],
                "big",
                signed=True,
            )
            raw.execute("SELECT pg_advisory_lock(%s)", (key,))
        yield Connection(raw)
    finally:
        raw.close()


def schema_fingerprint(db, schema):
    """Fingerprint native relation/column/constraint/index/view/trigger definitions."""
    statements = (
        "SELECT c.relname,c.relkind,a.attname,a.attnum,"
        "format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,a.attgenerated,"
        "pg_get_expr(d.adbin,d.adrelid) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped "
        "LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum "
        "WHERE n.nspname=? AND c.relkind IN ('r','p','v','m') ORDER BY c.relname,a.attnum",
        "SELECT c.relname,k.conname,k.contype,pg_get_constraintdef(k.oid) "
        "FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=? "
        "ORDER BY c.relname,k.conname",
        "SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=? ORDER BY indexname",
        "SELECT viewname,definition FROM pg_views WHERE schemaname=? ORDER BY viewname",
        "SELECT c.relname,t.tgname,pg_get_triggerdef(t.oid) FROM pg_trigger t "
        "JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=? AND NOT t.tgisinternal ORDER BY c.relname,t.tgname",
    )
    statements += (
        "SELECT sequencename,data_type::text,start_value,min_value,max_value,"
        "increment_by,cycle,cache_size FROM pg_sequences WHERE schemaname=? ORDER BY sequencename",
    )
    shape = [[tuple(row) for row in db.execute(statement, (schema,))] for statement in statements]
    return hashlib.sha256(json.dumps(shape, separators=(",", ":")).encode()).hexdigest()
