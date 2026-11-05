"""Contract-bound SQLite adapter for local SEC FTD ingestion only."""

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import PostgresConfig, StorageConfig
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import IDENTITY, storage_schemas
from market_data_pipeline.pipelines.sec_fails_to_deliver.versions import (
    knowledge_cutoff,
    withdrawals,
)
from market_data_pipeline.shared.files import atomic_json, writer_lock

TABLES = {name: f"sec_ftd_{name}" for name in ("facts", "coverage", "audit", "publications")}
KEYS = {
    "facts": "version_id",
    "coverage": "version_id",
    "audit": "run_id",
    "publications": "snapshot_id",
}


class SQLiteWarehouse:
    """Use as a context manager: the root lock spans capture and DB promotion."""

    def __init__(self, root: Path, settings: StorageConfig, *, read_only=False):
        self.root = Path(root).resolve()
        self.backend = "sqlite"
        self.schemas = storage_schemas(settings)
        self.expected = {
            **IDENTITY,
            "backend": self.backend,
            "storage_contract_id": settings.contract_id,
            "record_schemas": {name: schema.version for name, schema in self.schemas.items()},
        }
        self.connection = None
        self.read_only = read_only

    def __enter__(self):
        self.lock = self._reader_lock() if self.read_only else writer_lock(self.root)
        self.lock.__enter__()
        try:
            self._open()
            return self
        except BaseException:
            if self.connection is not None:
                self.connection.close()
            self.lock.__exit__(None, None, None)
            raise

    def __exit__(self, *args):
        try:
            self.connection.close()
        finally:
            self.lock.__exit__(*args)

    @contextmanager
    def _reader_lock(self):
        import fcntl

        path = self.root / ".writer.lock"
        if path.is_symlink() or not path.is_file():
            raise ValueError("source warehouse requires its original root lock")
        with path.open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _check_identity(self, identity):
        if (
            not isinstance(identity, dict)
            or any(identity.get(k) != v for k, v in self.expected.items())
            or not identity.get("warehouse_id")
        ):
            raise ValueError("root/database belongs to a different warehouse or storage contract")

    def _open(self):
        marker = self.root / ".warehouse.json"
        database = self.root / "warehouse.sqlite3"
        if marker.is_symlink() or database.is_symlink():
            raise ValueError("warehouse artifacts must not be symbolic links")
        existing = marker.exists() or database.exists()
        if existing:
            if not marker.is_file() or not database.is_file():
                raise ValueError(
                    "incomplete root binding; refusing to recreate warehouse artifacts"
                )
            self.identity = json.loads(marker.read_text())
            self._check_identity(self.identity)
        else:
            if self.read_only:
                raise ValueError("source warehouse does not exist")
            if any(p.name != ".writer.lock" for p in self.root.iterdir()):
                raise ValueError("refusing a nonempty unbound pipeline root")
            self.identity = {**self.expected, "warehouse_id": uuid.uuid4().hex}
        self.connection = sqlite3.connect(
            database.as_uri() + "?mode=ro" if self.read_only else database,
            uri=self.read_only,
            timeout=30,
            isolation_level=None,
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA synchronous = FULL")
        if existing:
            stored = self.connection.execute(
                "SELECT value FROM sec_ftd_identity WHERE id=1"
            ).fetchone()
            if stored is None or json.loads(stored["value"]) != self.identity:
                raise ValueError("database identity disagrees with the bound root")
            if self.read_only:
                self.connection.execute("PRAGMA query_only = ON")
                self.connection.execute("BEGIN")
            return
        with self.transaction():
            self.connection.execute(
                "CREATE TABLE sec_ftd_identity "
                "(id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)"
            )
            self.connection.execute(
                "INSERT INTO sec_ftd_identity VALUES (1,?)",
                (json.dumps(self.identity, sort_keys=True),),
            )
            self.connection.execute(
                "CREATE TABLE sec_ftd_sequence "
                "(id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL)"
            )
            self.connection.execute("INSERT INTO sec_ftd_sequence VALUES(1,0)")
            for name, schema in self.schemas.items():
                types = schema.sql_types("sqlite")
                fields = [
                    f"{c.name} {types[c.name]}" + (" NOT NULL" if not c.nullable else "")
                    for c in schema.columns
                ]
                fields.append(f"PRIMARY KEY ({KEYS[name]})")
                if name == "audit":
                    fields.append("UNIQUE (capture_sequence)")
                else:
                    fields.append("FOREIGN KEY (run_id) REFERENCES sec_ftd_audit(run_id)")
                self.connection.execute(f"CREATE TABLE {TABLES[name]} ({','.join(fields)})")
            self.connection.execute(
                "CREATE INDEX sec_ftd_fact_key "
                "ON sec_ftd_facts(settlement_date,cusip,capture_sequence)"
            )
            self.connection.execute(
                "CREATE INDEX sec_ftd_partition ON sec_ftd_coverage(partition_id,capture_sequence)"
            )
            self.connection.execute(
                "CREATE INDEX sec_ftd_fact_run ON sec_ftd_facts(run_id,version_id)"
            )
            columns = ",".join(c.name for c in self.schemas["facts"].columns)
            self.connection.execute(f"""CREATE VIEW sec_ftd_gold AS SELECT {columns} FROM (
                SELECT *,ROW_NUMBER() OVER(
                    PARTITION BY source,dataset,reporting_regime,settlement_date,cusip
                    ORDER BY first_seen_at DESC,capture_sequence DESC) AS rank
                FROM sec_ftd_facts) WHERE rank=1 AND row_state='REPORTED'""")
        atomic_json(marker, self.identity)

    @contextmanager
    def transaction(self):
        if self.read_only:
            raise ValueError("source warehouse is read-only")
        if self.connection.in_transaction:
            raise ValueError("nested warehouse transactions are unsupported")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def next_capture_sequence(self) -> int:
        if not self.connection.in_transaction:
            raise ValueError("capture sequence allocation requires a transaction")
        value = (
            self.connection.execute("SELECT value FROM sec_ftd_sequence WHERE id=1").fetchone()[
                "value"
            ]
            + 1
        )
        if value >= 2**63:
            raise ValueError("capture sequence exhausted")
        self.connection.execute("UPDATE sec_ftd_sequence SET value=? WHERE id=1", (value,))
        return value

    def start_replay_run(self, row):
        """Reserve the original sequence without inventing missing captures."""
        with self.transaction():
            latest = self.connection.execute(
                "SELECT value FROM sec_ftd_sequence WHERE id=1"
            ).fetchone()["value"]
            sequence = row["capture_sequence"]
            if type(sequence) is not int or not latest < sequence < 2**63:
                raise ValueError("replay capture sequence is duplicate or out of order")
            self.connection.execute("UPDATE sec_ftd_sequence SET value=? WHERE id=1", (sequence,))
            self.record_run(row)

    def fact_fingerprint(self, run_id):
        """Stream exact encoded versions; no floating-point or schema inference."""
        digest, count = hashlib.sha256(), 0
        for row in self.connection.execute(
            "SELECT * FROM sec_ftd_facts WHERE run_id=? ORDER BY version_id", (run_id,)
        ):
            normalized = self.schemas["facts"].decode(dict(row), self.backend)
            encoded = self.schemas["facts"].encode(normalized, "sqlite")
            digest.update(json.dumps(encoded, sort_keys=True, separators=(",", ":")).encode())
            digest.update(b"\n")
            count += 1
        return {"count": count, "sha256": digest.hexdigest()}

    def _insert(self, name, row):
        values = self.schemas[name].encode(row, self.backend)
        self.connection.execute(
            f"INSERT INTO {TABLES[name]} ({','.join(values)}) "
            f"VALUES ({','.join('?' for _ in values)})",
            tuple(values.values()),
        )

    def record_run(self, row):
        if not self.connection.in_transaction or row["status"] != "STARTED":
            raise ValueError("start a run inside its allocation transaction")
        value = self.schemas["audit"].encode(row, self.backend)
        latest = self.connection.execute(
            "SELECT captured_at FROM sec_ftd_audit ORDER BY capture_sequence DESC LIMIT 1"
        ).fetchone()
        if latest and value["captured_at"] < latest["captured_at"]:
            raise ValueError("capture clock regressed; ambiguous knowledge ordering")
        self._insert("audit", row)

    def promote_partition(self, facts, coverage, audit, *, evidence=None, capture=None) -> int:
        coverage = self.schemas["coverage"].normalize(coverage)
        audit = self.schemas["audit"].normalize(audit)
        if audit["status"] == "STARTED" or audit["row_count"] != len(facts):
            raise ValueError("promotion requires a terminal audit with matching fact count")
        if (coverage["run_id"], coverage["capture_sequence"]) != (
            audit["run_id"],
            audit["capture_sequence"],
        ):
            raise ValueError("coverage/audit capture lineage disagrees")
        if audit["status"] != "SUCCEEDED" and facts:
            raise ValueError("failed capture cannot promote facts")
        with self.transaction():
            started = self.connection.execute(
                "SELECT * FROM sec_ftd_audit WHERE run_id=?", (audit["run_id"],)
            ).fetchone()
            if (
                started is None
                or started["status"] != "STARTED"
                or started["capture_sequence"] != audit["capture_sequence"]
            ):
                raise ValueError("capture was not started or already finalized")
            inserted = 0
            if audit["status"] == "SUCCEEDED":
                if coverage["validation_state"] != "MATCHED" or coverage[
                    "received_row_count"
                ] != len(facts):
                    raise ValueError("successful capture requires matched row coverage")
                overlap = self.connection.execute(
                    "SELECT 1 FROM sec_ftd_coverage "
                    "WHERE partition_id<>? AND validation_state='MATCHED' "
                    "AND period_start<=? AND period_end>=? LIMIT 1",
                    (
                        coverage["partition_id"],
                        coverage["period_end"].isoformat(),
                        coverage["period_start"].isoformat(),
                    ),
                ).fetchone()
                if overlap:
                    raise ValueError("OVERLAPPING_PARTITION")
                selected, _ = self._selected_query("facts", include_withdrawn=True)
                existing = {}
                for stored in self.connection.execute(
                    f"SELECT * FROM ({selected}) WHERE partition_id=?",
                    (coverage["partition_id"],),
                ):
                    decoded = self.schemas["facts"].decode(dict(stored), self.backend)
                    existing[(decoded["settlement_date"], decoded["cusip"])] = decoded
                keys = set()
                normalized = []
                for row in facts:
                    row = self.schemas["facts"].normalize(row)
                    if (
                        row["row_state"] != "REPORTED"
                        or not coverage["period_start"]
                        <= row["settlement_date"]
                        <= coverage["period_end"]
                        or row["first_seen_at"] != audit["captured_at"]
                        or row["raw_sha256"] != coverage["raw_sha256"]
                        or row["raw_sha256"] != audit["raw_sha256"]
                    ):
                        raise ValueError("reported observation contradicts its capture/scope")
                    normalized.append(row)
                    key = (row["settlement_date"], row["cusip"])
                    if key in keys or (
                        row["run_id"],
                        row["capture_sequence"],
                        row["partition_id"],
                    ) != (audit["run_id"], audit["capture_sequence"], coverage["partition_id"]):
                        raise ValueError("duplicate fact or inconsistent capture lineage")
                    keys.add(key)
                    if key in existing:
                        if (
                            existing[key]["content_hash"] != row["content_hash"]
                            or existing[key]["row_state"] != row["row_state"]
                        ):
                            self._insert("facts", row)
                            inserted += 1
                    else:
                        self._insert("facts", row)
                        inserted += 1
                previous = list(existing.values())
                for row in withdrawals(previous, normalized, coverage, audit, capture, evidence):
                    self._insert("facts", row)
                    inserted += 1
            self._insert("coverage", coverage)
            values = self.schemas["audit"].encode(audit, self.backend)
            self.connection.execute(
                "UPDATE sec_ftd_audit SET "
                + ",".join(f"{k}=?" for k in values)
                + " WHERE run_id=?",
                (*values.values(), audit["run_id"]),
            )
            return inserted

    def _selected_query(self, name, *, as_of=None, as_of_sequence=None, include_withdrawn=False):
        clock = knowledge_cutoff(as_of, as_of_sequence)
        if type(include_withdrawn) is not bool:
            raise ValueError("include_withdrawn must be boolean")
        predicate = ""
        parameters = ()
        if clock is not None:
            predicate = " WHERE first_seen_at<=?"
            parameters = (clock,)
            if as_of_sequence is not None:
                predicate = " WHERE (first_seen_at<? OR (first_seen_at=? AND capture_sequence<=?))"
                parameters = (clock, clock, as_of_sequence)
        columns = ",".join(c.name for c in self.schemas[name].columns)
        keys = (
            "source,dataset,reporting_regime,settlement_date,cusip"
            if name == "facts"
            else "partition_id"
        )
        state = " AND row_state='REPORTED'" if name == "facts" and not include_withdrawn else ""
        return (
            f"SELECT {columns} FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY {keys} "
            f"ORDER BY first_seen_at DESC,capture_sequence DESC) AS rank FROM {TABLES[name]}"
            f"{predicate}) WHERE rank=1{state}",
            parameters,
        )

    def select_facts(
        self,
        *,
        as_of: datetime | None = None,
        as_of_sequence: int | None = None,
        include_withdrawn: bool = False,
        limit: int | None = None,
    ):
        query, parameters = self._selected_query(
            "facts", as_of=as_of, as_of_sequence=as_of_sequence, include_withdrawn=include_withdrawn
        )
        query += " ORDER BY settlement_date,cusip"
        if limit is not None:
            if type(limit) is not int or not 1 <= limit <= 1000:
                raise ValueError("Gold limit must be in [1,1000]")
            query += " LIMIT ?"
            parameters = (*parameters, limit)
        return [
            self.schemas["facts"].decode(dict(row), self.backend)
            for row in self.connection.execute(query, parameters)
        ]

    def select_coverage(self, *, as_of=None, as_of_sequence=None):
        query, parameters = self._selected_query(
            "coverage", as_of=as_of, as_of_sequence=as_of_sequence
        )
        rows = self.connection.execute(query + " ORDER BY partition_id", parameters)
        return [self.schemas["coverage"].decode(dict(row), self.backend) for row in rows]

    def count_facts(self, *, as_of=None, as_of_sequence=None, include_withdrawn=False) -> int:
        query, parameters = self._selected_query(
            "facts", as_of=as_of, as_of_sequence=as_of_sequence, include_withdrawn=include_withdrawn
        )
        return self.connection.execute(
            f"SELECT count(*) AS count FROM ({query})", parameters
        ).fetchone()["count"]

    def select_runs(self):
        return [
            self.schemas["audit"].decode(dict(row), self.backend)
            for row in self.connection.execute(
                "SELECT * FROM sec_ftd_audit ORDER BY capture_sequence"
            )
        ]

    def select_all(self, name):
        if name not in ("facts", "coverage", "audit"):
            raise ValueError("unsupported SEC FTD snapshot record set")
        order = (
            "first_seen_at,capture_sequence,version_id"
            if name != "audit"
            else "capture_sequence,run_id"
        )
        return [
            self.schemas[name].decode(dict(row), self.backend)
            for row in self.connection.execute(f"SELECT * FROM {TABLES[name]} ORDER BY {order}")
        ]

    def record_publication(self, row):
        normalized = self.schemas["publications"].normalize(row)
        if normalized["status"] != "STAGED" or normalized["published_at"] is not None:
            raise ValueError("new SEC FTD publication must begin STAGED")
        with self.transaction():
            run = self.connection.execute(
                "SELECT status FROM sec_ftd_audit WHERE run_id=?", (normalized["run_id"],)
            ).fetchone()
            if run is None or run["status"] == "STARTED":
                raise ValueError("publication requires a terminal captured run")
            self._insert("publications", normalized)

    def transition_publication(self, snapshot_id, status, published_at=None):
        if status not in ("COMMITTED", "PUBLISHED", "ABANDONED"):
            raise ValueError("invalid SEC FTD publication transition")
        with self.transaction():
            stored = self.connection.execute(
                "SELECT * FROM sec_ftd_publications WHERE snapshot_id=?", (snapshot_id,)
            ).fetchone()
            if stored is None:
                raise ValueError("publication snapshot is not registered")
            current = stored["status"]
            allowed = {
                "STAGED": {"COMMITTED", "ABANDONED"},
                "COMMITTED": {"PUBLISHED", "ABANDONED"},
                "PUBLISHED": {"PUBLISHED"},
                "ABANDONED": {"ABANDONED"},
            }
            if status not in allowed[current]:
                raise ValueError("invalid SEC FTD publication state transition")
            row = self.schemas["publications"].decode(dict(stored), self.backend)
            row.update(status=status, published_at=published_at)
            values = self.schemas["publications"].encode(row, self.backend)
            self.connection.execute(
                "UPDATE sec_ftd_publications SET status=?,published_at=? WHERE snapshot_id=?",
                (values["status"], values["published_at"], snapshot_id),
            )

    def pending_publications(self):
        return [
            self.schemas["publications"].decode(dict(row), self.backend)
            for row in self.connection.execute(
                "SELECT * FROM sec_ftd_publications WHERE status IN ('STAGED','COMMITTED')"
            )
        ]

    def select_publications(self):
        return [
            self.schemas["publications"].decode(dict(row), self.backend)
            for row in self.connection.execute(
                "SELECT * FROM sec_ftd_publications ORDER BY generated_at,snapshot_id"
            )
        ]


class _PostgresConnection:
    """Small DB-API compatibility layer for the shared semantic methods above."""

    def __init__(self, raw):
        self.raw = raw

    @property
    def in_transaction(self):
        return self.raw.info.transaction_status.value != 0

    def execute(self, statement, parameters=()):
        return self.raw.execute(statement.replace("?", "%s"), parameters)

    def close(self):
        self.raw.close()


class PostgresWarehouse(SQLiteWarehouse):
    """SEC-owned PostgreSQL adapter with the same normalized warehouse contract."""

    _ADVISORY_LOCK = 721049011

    def __init__(
        self,
        root: Path,
        settings: StorageConfig,
        postgres: PostgresConfig,
        *,
        read_only=False,
    ):
        super().__init__(root, settings, read_only=read_only)
        self.backend = "postgres"
        self.postgres = postgres
        self.created_store = False
        self.expected = {
            **IDENTITY,
            "backend": self.backend,
            "database": postgres.database,
            "schema": postgres.schema,
            "storage_contract_id": settings.contract_id,
            "record_schemas": {name: schema.version for name, schema in self.schemas.items()},
        }

    def __enter__(self):
        # Resolve optional dependency and environment-only secret before the
        # filesystem writer lock creates or changes a configured root.
        self._conninfo = self.postgres.conninfo()
        return super().__enter__()

    def _open(self):
        try:
            import psycopg
            from psycopg import sql
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise ValueError(
                "PostgreSQL support requires the postgres optional dependency"
            ) from exc
        marker = self.root / ".warehouse.json"
        if marker.is_symlink():
            raise ValueError("warehouse marker must not be a symbolic link")
        marker_exists = marker.exists()
        if marker_exists:
            if not marker.is_file():
                raise ValueError(
                    "incomplete root binding; refusing to recreate warehouse artifacts"
                )
            self.identity = json.loads(marker.read_text())
            self._check_identity(self.identity)
        elif self.read_only:
            raise ValueError("source warehouse does not exist")
        elif any(path.name != ".writer.lock" for path in self.root.iterdir()):
            raise ValueError("refusing a nonempty unbound pipeline root")
        raw = psycopg.connect(self._conninfo, autocommit=True, row_factory=dict_row)
        self.connection = _PostgresConnection(raw)
        try:
            if self.postgres.role is not None:
                raw.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(self.postgres.role)))
            exists = raw.execute(
                "SELECT 1 FROM pg_namespace WHERE nspname=%s", (self.postgres.schema,)
            ).fetchone()
            if exists is None:
                raise ValueError("configured PostgreSQL schema does not exist")
            raw.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(self.postgres.schema))
            )
            # Session scope protects writers whose shared database is reached from
            # different hosts where the filesystem flock cannot coordinate them.
            raw.execute("SELECT pg_advisory_lock(%s)", (self._ADVISORY_LOCK,))
            owned = {
                "sec_ftd_identity",
                "sec_ftd_sequence",
                "sec_ftd_facts",
                "sec_ftd_coverage",
                "sec_ftd_audit",
                "sec_ftd_publications",
            }
            present = {
                row["table_name"]
                for row in raw.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema=%s AND table_name=ANY(%s)",
                    (self.postgres.schema, list(owned)),
                )
            }
            if present:
                view = raw.execute(
                    "SELECT 1 FROM information_schema.views "
                    "WHERE table_schema=%s AND table_name='sec_ftd_gold'",
                    (self.postgres.schema,),
                ).fetchone()
                if present != owned or view is None or not marker_exists:
                    raise ValueError(
                        "incomplete root/database binding; refusing to reuse PostgreSQL state"
                    )
                stored = self.connection.execute(
                    "SELECT value FROM sec_ftd_identity WHERE id=1"
                ).fetchone()
                if stored is None or json.loads(stored["value"]) != self.identity:
                    raise ValueError("database identity disagrees with the bound root")
                return
            if marker_exists:
                raise ValueError("bound root has no matching PostgreSQL warehouse")
            self.identity = {**self.expected, "warehouse_id": uuid.uuid4().hex}
            with self.transaction():
                self.connection.execute(
                    "CREATE TABLE sec_ftd_identity "
                    "(id BIGINT PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)"
                )
                self.connection.execute(
                    "INSERT INTO sec_ftd_identity VALUES (1,?)",
                    (json.dumps(self.identity, sort_keys=True),),
                )
                self.connection.execute(
                    "CREATE TABLE sec_ftd_sequence "
                    "(id BIGINT PRIMARY KEY CHECK(id=1), value BIGINT NOT NULL)"
                )
                self.connection.execute("INSERT INTO sec_ftd_sequence VALUES(1,0)")
                for name in ("audit", "facts", "coverage", "publications"):
                    schema = self.schemas[name]
                    fields = []
                    for column in schema.columns:
                        sql_type = schema.sql_types("postgres")[column.name]
                        if column.kind == "decimal":
                            sql_type = f"NUMERIC({column.precision},{column.scale})"
                        fields.append(
                            f"{column.name} {sql_type}"
                            + (" NOT NULL" if not column.nullable else "")
                        )
                    fields.append(f"PRIMARY KEY ({KEYS[name]})")
                    if name == "audit":
                        fields.append("UNIQUE (capture_sequence)")
                    else:
                        fields.append("FOREIGN KEY (run_id) REFERENCES sec_ftd_audit(run_id)")
                    self.connection.execute(f"CREATE TABLE {TABLES[name]} ({','.join(fields)})")
                self.connection.execute(
                    "CREATE INDEX sec_ftd_fact_key "
                    "ON sec_ftd_facts(settlement_date,cusip,capture_sequence)"
                )
                self.connection.execute(
                    "CREATE INDEX sec_ftd_partition "
                    "ON sec_ftd_coverage(partition_id,capture_sequence)"
                )
                self.connection.execute(
                    "CREATE INDEX sec_ftd_fact_run ON sec_ftd_facts(run_id,version_id)"
                )
                columns = ",".join(column.name for column in self.schemas["facts"].columns)
                self.connection.execute(f"""CREATE VIEW sec_ftd_gold AS SELECT {columns} FROM (
                    SELECT *,ROW_NUMBER() OVER(
                        PARTITION BY source,dataset,reporting_regime,settlement_date,cusip
                        ORDER BY first_seen_at DESC,capture_sequence DESC) AS rank
                    FROM sec_ftd_facts) selected
                    WHERE rank=1 AND row_state='REPORTED'""")
            self.created_store = True
            try:
                atomic_json(marker, self.identity)
            except BaseException:
                self._drop_owned_tables(raw)
                self.created_store = False
                raise
        except BaseException:
            raw.close()
            self.connection = None
            raise

    @contextmanager
    def transaction(self):
        if self.read_only:
            raise ValueError("source warehouse is read-only")
        with self.connection.raw.transaction():
            yield

    @staticmethod
    def _drop_owned_tables(raw):
        with raw.transaction():
            raw.execute("DROP VIEW IF EXISTS sec_ftd_gold")
            for table in (
                "sec_ftd_publications",
                "sec_ftd_facts",
                "sec_ftd_coverage",
                "sec_ftd_audit",
                "sec_ftd_sequence",
                "sec_ftd_identity",
            ):
                raw.execute(f"DROP TABLE IF EXISTS {table}")

    def cleanup_created_store(self):
        """Remove only a newly created, identity-matched failed replay target."""
        if not self.created_store:
            return
        marker = self.root / ".warehouse.json"
        if (
            marker.is_symlink()
            or not marker.is_file()
            or json.loads(marker.read_text()) != self.identity
        ):
            raise ValueError("refusing to clean an unverified PostgreSQL replay target")
        import psycopg
        from psycopg import sql
        from psycopg.rows import dict_row

        with psycopg.connect(self._conninfo, autocommit=True, row_factory=dict_row) as raw:
            if self.postgres.role is not None:
                raw.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(self.postgres.role)))
            raw.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(self.postgres.schema))
            )
            raw.execute("SELECT pg_advisory_lock(%s)", (self._ADVISORY_LOCK,))
            stored = raw.execute("SELECT value FROM sec_ftd_identity WHERE id=1").fetchone()
            if stored is None or json.loads(stored["value"]) != self.identity:
                raise ValueError("refusing to clean a changed PostgreSQL replay target")
            self._drop_owned_tables(raw)
        self.created_store = False


def warehouse_for(config, *, root: Path | None = None, read_only=False):
    """Select exactly the configured backend; never fall back between engines."""
    selected_root = config.root if root is None else Path(root)
    if config.warehouse == "sqlite":
        return SQLiteWarehouse(selected_root, config.storage, read_only=read_only)
    if config.warehouse == "postgres":
        return PostgresWarehouse(
            selected_root, config.storage, config.postgres, read_only=read_only
        )
    raise ValueError("unsupported SEC FTD warehouse backend")


def is_database_error(exc: BaseException) -> bool:
    if isinstance(exc, sqlite3.DatabaseError):
        return True
    try:
        import psycopg
    except ImportError:
        return False
    return isinstance(exc, psycopg.Error)
