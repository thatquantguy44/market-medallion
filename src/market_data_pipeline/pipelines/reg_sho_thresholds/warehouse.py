"""Contract-bound SQLite state for independent exchange threshold lists."""

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.storage import storage_schemas
from market_data_pipeline.shared.files import atomic_json, writer_lock

TABLES = {name: f"reg_sho_{name}" for name in storage_schemas()}
KEYS = {
    "membership": "version_id",
    "coverage": "version_id",
    "audit": "run_id",
    "publications": "snapshot_id",
}


class SQLiteWarehouse:
    def __init__(self, root: Path, *, read_only=False):
        self.root = Path(root).resolve()
        self.backend = "sqlite"
        self.schemas = storage_schemas()
        self.expected = {
            "dataset": "reg-sho-thresholds",
            "backend": "sqlite",
            "storage_contract_id": self.schemas["membership"].contract_id,
            "record_schemas": {name: schema.version for name, schema in self.schemas.items()},
        }
        self.read_only = read_only
        self.connection = None

    def __enter__(self):
        self.lock = writer_lock(self.root)
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

    def _open(self):
        marker = self.root / ".warehouse.json"
        database = self.root / "warehouse.sqlite3"
        if marker.is_symlink() or database.is_symlink():
            raise ValueError("threshold warehouse artifacts must not be symbolic links")
        existing = marker.exists() or database.exists()
        if existing:
            if not marker.is_file() or not database.is_file():
                raise ValueError("incomplete threshold root binding")
            self.identity = json.loads(marker.read_text())
            if any(self.identity.get(key) != value for key, value in self.expected.items()):
                raise ValueError("threshold root belongs to a different storage contract")
        else:
            if self.read_only:
                raise ValueError("threshold warehouse does not exist")
            if any(path.name != ".writer.lock" for path in self.root.iterdir()):
                raise ValueError("refusing a nonempty unbound threshold root")
            self.identity = {**self.expected, "warehouse_id": uuid.uuid4().hex}
        self.connection = sqlite3.connect(
            database.as_uri() + "?mode=ro" if self.read_only else database,
            uri=self.read_only,
            timeout=30,
            isolation_level=None,
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA synchronous=FULL")
        if existing:
            stored = self.connection.execute(
                "SELECT value FROM reg_sho_identity WHERE id=1"
            ).fetchone()
            if stored is None or json.loads(stored["value"]) != self.identity:
                raise ValueError("threshold database identity differs from its root")
            if self.read_only:
                self.connection.execute("PRAGMA query_only=ON")
            return
        with self.transaction():
            self.connection.execute(
                "CREATE TABLE reg_sho_identity "
                "(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)"
            )
            self.connection.execute(
                "INSERT INTO reg_sho_identity VALUES(1,?)",
                (json.dumps(self.identity, sort_keys=True),),
            )
            self.connection.execute(
                "CREATE TABLE reg_sho_sequence "
                "(id INTEGER PRIMARY KEY CHECK(id=1),value INTEGER NOT NULL)"
            )
            self.connection.execute("INSERT INTO reg_sho_sequence VALUES(1,0)")
            for name in ("audit", "membership", "coverage", "publications"):
                schema = self.schemas[name]
                types = schema.sql_types("sqlite")
                fields = [
                    f"{column.name} {types[column.name]}"
                    + (" NOT NULL" if not column.nullable else "")
                    for column in schema.columns
                ]
                fields.append(f"PRIMARY KEY ({KEYS[name]})")
                if name == "audit":
                    fields.append("UNIQUE(capture_sequence)")
                else:
                    fields.append("FOREIGN KEY(run_id) REFERENCES reg_sho_audit(run_id)")
                self.connection.execute(f"CREATE TABLE {TABLES[name]} ({','.join(fields)})")
            self.connection.execute(
                "CREATE INDEX reg_sho_member_key ON reg_sho_membership"
                "(provider,market,list_date,raw_symbol,rule_family,capture_sequence)"
            )
            self.connection.execute(
                "CREATE INDEX reg_sho_coverage_key ON reg_sho_coverage"
                "(partition_id,capture_sequence)"
            )
        atomic_json(marker, self.identity)

    @contextmanager
    def transaction(self):
        if self.read_only:
            raise ValueError("threshold warehouse is read-only")
        if self.connection.in_transaction:
            raise ValueError("nested threshold transactions are unsupported")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def _insert(self, name, row):
        values = self.schemas[name].encode(row, self.backend)
        self.connection.execute(
            f"INSERT INTO {TABLES[name]} ({','.join(values)}) "
            f"VALUES ({','.join('?' for _ in values)})",
            tuple(values.values()),
        )

    def next_capture_sequence(self):
        if not self.connection.in_transaction:
            raise ValueError("threshold sequence allocation requires a transaction")
        current = self.connection.execute(
            "SELECT value FROM reg_sho_sequence WHERE id=1"
        ).fetchone()["value"]
        if current >= 2**63 - 1:
            raise ValueError("threshold capture sequence exhausted")
        current += 1
        self.connection.execute("UPDATE reg_sho_sequence SET value=? WHERE id=1", (current,))
        return current

    def record_run(self, row):
        if not self.connection.in_transaction or row.get("status") != "STARTED":
            raise ValueError("threshold run must start in its allocation transaction")
        encoded = self.schemas["audit"].encode(row, self.backend)
        previous = self.connection.execute(
            "SELECT captured_at FROM reg_sho_audit ORDER BY capture_sequence DESC LIMIT 1"
        ).fetchone()
        if previous is not None and encoded["captured_at"] < previous["captured_at"]:
            raise ValueError("threshold capture clock regressed")
        self._insert("audit", row)

    def restore_run(self, audit, coverage, membership):
        """Restore one previously validated terminal run without reinterpreting it."""
        audit = self.schemas["audit"].normalize(audit)
        coverage = self.schemas["coverage"].normalize(coverage)
        membership = [self.schemas["membership"].normalize(row) for row in membership]
        if audit["status"] == "STARTED" or (coverage["run_id"], coverage["capture_sequence"]) != (
            audit["run_id"],
            audit["capture_sequence"],
        ):
            raise ValueError("threshold replay requires terminal matching lineage")
        if any(
            (row["run_id"], row["capture_sequence"]) != (audit["run_id"], audit["capture_sequence"])
            for row in membership
        ):
            raise ValueError("threshold replay membership lineage differs")
        with self.transaction():
            latest = self.connection.execute(
                "SELECT value FROM reg_sho_sequence WHERE id=1"
            ).fetchone()["value"]
            if audit["capture_sequence"] != latest + 1:
                raise ValueError("threshold replay capture sequence is not contiguous")
            self.connection.execute(
                "UPDATE reg_sho_sequence SET value=? WHERE id=1", (audit["capture_sequence"],)
            )
            started = {
                **audit,
                "completed_at": None,
                "status": "STARTED",
                "row_count": 0,
                "raw_sha256": None,
                "error_code": None,
            }
            self._insert("audit", started)
            for row in membership:
                self._insert("membership", row)
            self._insert("coverage", coverage)
            values = self.schemas["audit"].encode(audit, self.backend)
            self.connection.execute(
                "UPDATE reg_sho_audit SET "
                + ",".join(f"{key}=?" for key in values)
                + " WHERE run_id=?",
                (*values.values(), audit["run_id"]),
            )

    def _current_partition(self, partition_id):
        columns = ",".join(column.name for column in self.schemas["membership"].columns)
        rows = self.connection.execute(
            f"SELECT {columns} FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY "
            "provider,market,list_date,raw_symbol,rule_family ORDER BY first_seen_at DESC,"
            "capture_sequence DESC) AS rank FROM reg_sho_membership WHERE partition_id=?) "
            "WHERE rank=1",
            (partition_id,),
        )
        return [self.schemas["membership"].decode(dict(row), self.backend) for row in rows]

    def promote_partition(self, membership, coverage, audit):
        coverage = self.schemas["coverage"].normalize(coverage)
        audit = self.schemas["audit"].normalize(audit)
        if audit["status"] == "STARTED" or audit["row_count"] != len(membership):
            raise ValueError("threshold promotion requires a terminal matching audit")
        if (coverage["run_id"], coverage["capture_sequence"]) != (
            audit["run_id"],
            audit["capture_sequence"],
        ):
            raise ValueError("threshold coverage/audit lineage differs")
        if audit["status"] != "SUCCEEDED" and membership:
            raise ValueError("failed threshold capture cannot promote membership")
        with self.transaction():
            started = self.connection.execute(
                "SELECT * FROM reg_sho_audit WHERE run_id=?", (audit["run_id"],)
            ).fetchone()
            if started is None or started["status"] != "STARTED":
                raise ValueError("threshold capture is not in STARTED state")
            inserted = 0
            if audit["status"] == "SUCCEEDED":
                if coverage["coverage_state"] not in ("COMPLETE", "COMPLETE_EMPTY"):
                    raise ValueError("successful threshold capture requires complete coverage")
                existing = {
                    (row["raw_symbol"], row["rule_family"]): row
                    for row in self._current_partition(coverage["partition_id"])
                }
                current_keys = set()
                for row in membership:
                    normalized = self.schemas["membership"].normalize(row)
                    key = (normalized["raw_symbol"], normalized["rule_family"])
                    if key in current_keys:
                        raise ValueError("duplicate normalized threshold membership")
                    current_keys.add(key)
                    if (
                        normalized["partition_id"] != coverage["partition_id"]
                        or normalized["run_id"] != audit["run_id"]
                        or normalized["capture_sequence"] != audit["capture_sequence"]
                        or normalized["first_seen_at"] != audit["captured_at"]
                        or normalized["raw_sha256"] != coverage["raw_sha256"]
                    ):
                        raise ValueError("threshold membership contradicts capture lineage")
                    previous = existing.get(key)
                    if previous is None or previous["content_hash"] != normalized["content_hash"]:
                        self._insert("membership", normalized)
                        inserted += 1
                for key, previous in existing.items():
                    if previous["row_state"] != "REPORTED" or key in current_keys:
                        continue
                    withdrawn = {
                        **previous,
                        "source_issue_id": None,
                        "security_name_raw": None,
                        "flags_raw_json": None,
                        "row_state": "WITHDRAWN",
                        "source_row_number": None,
                        "version_id": f"{audit['run_id']}:{previous['raw_symbol']}:withdrawn",
                        "content_hash": coverage["raw_sha256"],
                        "raw_sha256": coverage["raw_sha256"],
                        "run_id": audit["run_id"],
                        "capture_sequence": audit["capture_sequence"],
                        "first_seen_at": audit["captured_at"],
                        "retrieved_at": audit["captured_at"],
                        "provider_published_at": None,
                    }
                    self._insert("membership", withdrawn)
                    inserted += 1
            self._insert("coverage", coverage)
            values = self.schemas["audit"].encode(audit, self.backend)
            self.connection.execute(
                "UPDATE reg_sho_audit SET "
                + ",".join(f"{key}=?" for key in values)
                + " WHERE run_id=?",
                (*values.values(), audit["run_id"]),
            )
            return inserted

    @staticmethod
    def _cutoff(as_of, as_of_sequence):
        if as_of is None:
            if as_of_sequence is not None:
                raise ValueError("as_of_sequence requires as_of")
            return "", ()
        if not isinstance(as_of, datetime) or as_of.utcoffset() is None:
            raise ValueError("threshold as_of requires a timezone-aware timestamp")
        clock = as_of.astimezone(UTC).isoformat(timespec="microseconds")
        if as_of_sequence is None:
            return " WHERE first_seen_at<=?", (clock,)
        if type(as_of_sequence) is not int or as_of_sequence < 1:
            raise ValueError("threshold as_of_sequence requires a positive integer")
        return (
            " WHERE (first_seen_at<? OR (first_seen_at=? AND capture_sequence<=?))",
            (clock, clock, as_of_sequence),
        )

    def select_membership(self, *, as_of=None, as_of_sequence=None, include_withdrawn=False):
        predicate, parameters = self._cutoff(as_of, as_of_sequence)
        columns = ",".join(column.name for column in self.schemas["membership"].columns)
        state = "" if include_withdrawn else " AND row_state='REPORTED'"
        query = (
            f"SELECT {columns} FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY provider,market,"
            "list_date,raw_symbol,rule_family ORDER BY first_seen_at DESC,capture_sequence DESC) "
            f"AS rank FROM reg_sho_membership{predicate}) WHERE rank=1{state} "
            "ORDER BY provider,market,list_date,raw_symbol"
        )
        return [
            self.schemas["membership"].decode(dict(row), self.backend)
            for row in self.connection.execute(query, parameters)
        ]

    def select_coverage(self, *, as_of=None, as_of_sequence=None):
        predicate, parameters = self._cutoff(as_of, as_of_sequence)
        columns = ",".join(column.name for column in self.schemas["coverage"].columns)
        query = (
            f"SELECT {columns} FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY partition_id "
            "ORDER BY first_seen_at DESC,capture_sequence DESC) AS rank FROM reg_sho_coverage"
            f"{predicate}) WHERE rank=1 ORDER BY provider,market,list_date"
        )
        return [
            self.schemas["coverage"].decode(dict(row), self.backend)
            for row in self.connection.execute(query, parameters)
        ]

    def select_runs(self):
        return [
            self.schemas["audit"].decode(dict(row), self.backend)
            for row in self.connection.execute(
                "SELECT * FROM reg_sho_audit ORDER BY capture_sequence"
            )
        ]

    def select_all(self, name):
        if name not in ("membership", "coverage", "audit"):
            raise ValueError("unsupported threshold record set")
        order = "capture_sequence,run_id"
        return [
            self.schemas[name].decode(dict(row), self.backend)
            for row in self.connection.execute(f"SELECT * FROM {TABLES[name]} ORDER BY {order}")
        ]


class _PostgresConnection:
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
    """Threshold-owned PostgreSQL adapter; never falls back to SQLite."""

    _ADVISORY_LOCK = 721049012

    def __init__(self, root: Path, postgres, *, read_only=False):
        super().__init__(root, read_only=read_only)
        self.backend = "postgres"
        self.postgres = postgres
        self.created_store = False
        self.expected = {
            "dataset": "reg-sho-thresholds",
            "backend": "postgres",
            "database": postgres.database,
            "schema": postgres.schema,
            "storage_contract_id": self.schemas["membership"].contract_id,
            "record_schemas": {name: schema.version for name, schema in self.schemas.items()},
        }

    def __enter__(self):
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
            raise ValueError("threshold warehouse marker must not be a symbolic link")
        marker_exists = marker.exists()
        if marker_exists:
            if not marker.is_file():
                raise ValueError("incomplete threshold root binding")
            self.identity = json.loads(marker.read_text())
            if any(self.identity.get(key) != value for key, value in self.expected.items()):
                raise ValueError("threshold root belongs to a different storage contract")
        elif self.read_only:
            raise ValueError("threshold warehouse does not exist")
        elif any(path.name != ".writer.lock" for path in self.root.iterdir()):
            raise ValueError("refusing a nonempty unbound threshold root")
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
            raw.execute("SELECT pg_advisory_lock(%s)", (self._ADVISORY_LOCK,))
            owned = {
                "reg_sho_identity",
                "reg_sho_sequence",
                "reg_sho_membership",
                "reg_sho_coverage",
                "reg_sho_audit",
                "reg_sho_publications",
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
                if present != owned or not marker_exists:
                    raise ValueError("incomplete root/database binding; refusing PostgreSQL state")
                stored = self.connection.execute(
                    "SELECT value FROM reg_sho_identity WHERE id=1"
                ).fetchone()
                if stored is None or json.loads(stored["value"]) != self.identity:
                    raise ValueError("threshold database identity differs from its root")
                return
            if marker_exists:
                raise ValueError("bound threshold root has no matching PostgreSQL warehouse")
            self.identity = {**self.expected, "warehouse_id": uuid.uuid4().hex}
            with self.transaction():
                self.connection.execute(
                    "CREATE TABLE reg_sho_identity "
                    "(id BIGINT PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)"
                )
                self.connection.execute(
                    "INSERT INTO reg_sho_identity VALUES(1,?)",
                    (json.dumps(self.identity, sort_keys=True),),
                )
                self.connection.execute(
                    "CREATE TABLE reg_sho_sequence "
                    "(id BIGINT PRIMARY KEY CHECK(id=1),value BIGINT NOT NULL)"
                )
                self.connection.execute("INSERT INTO reg_sho_sequence VALUES(1,0)")
                for name in ("audit", "membership", "coverage", "publications"):
                    schema = self.schemas[name]
                    types = schema.sql_types("postgres")
                    fields = [
                        f"{column.name} {types[column.name]}"
                        + (" NOT NULL" if not column.nullable else "")
                        for column in schema.columns
                    ]
                    fields.append(f"PRIMARY KEY ({KEYS[name]})")
                    if name == "audit":
                        fields.append("UNIQUE(capture_sequence)")
                    else:
                        fields.append("FOREIGN KEY(run_id) REFERENCES reg_sho_audit(run_id)")
                    self.connection.execute(f"CREATE TABLE {TABLES[name]} ({','.join(fields)})")
                self.connection.execute(
                    "CREATE INDEX reg_sho_member_key ON reg_sho_membership"
                    "(provider,market,list_date,raw_symbol,rule_family,capture_sequence)"
                )
                self.connection.execute(
                    "CREATE INDEX reg_sho_coverage_key ON reg_sho_coverage"
                    "(partition_id,capture_sequence)"
                )
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
            raise ValueError("threshold warehouse is read-only")
        with self.connection.raw.transaction():
            yield

    @staticmethod
    def _drop_owned_tables(raw):
        with raw.transaction():
            for table in (
                "reg_sho_publications",
                "reg_sho_membership",
                "reg_sho_coverage",
                "reg_sho_audit",
                "reg_sho_sequence",
                "reg_sho_identity",
            ):
                raw.execute(f"DROP TABLE IF EXISTS {table}")

    def cleanup_created_store(self):
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
            stored = raw.execute("SELECT value FROM reg_sho_identity WHERE id=1").fetchone()
            if stored is None or json.loads(stored["value"]) != self.identity:
                raise ValueError("refusing to clean a changed PostgreSQL replay target")
            self._drop_owned_tables(raw)
        self.created_store = False


def warehouse_for(config, *, root: Path | None = None, read_only=False):
    selected_root = config.root if root is None else Path(root)
    if config.warehouse == "sqlite":
        return SQLiteWarehouse(selected_root, read_only=read_only)
    if config.warehouse == "postgres":
        return PostgresWarehouse(selected_root, config.postgres, read_only=read_only)
    raise ValueError("unsupported threshold warehouse backend")
