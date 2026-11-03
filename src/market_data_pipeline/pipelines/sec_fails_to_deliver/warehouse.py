"""Contract-bound SQLite adapter for local SEC FTD ingestion only."""

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import StorageConfig
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import IDENTITY, storage_schemas
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

    def __init__(self, root: Path, settings: StorageConfig):
        self.root = Path(root).resolve()
        self.schemas = storage_schemas(settings)
        self.expected = {
            **IDENTITY,
            "backend": "sqlite",
            "storage_contract_id": settings.contract_id,
            "record_schemas": {name: schema.version for name, schema in self.schemas.items()},
        }
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
            if any(p.name != ".writer.lock" for p in self.root.iterdir()):
                raise ValueError("refusing a nonempty unbound pipeline root")
            self.identity = {**self.expected, "warehouse_id": uuid.uuid4().hex}
        self.connection = sqlite3.connect(database, timeout=30, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA synchronous = FULL")
        if existing:
            stored = self.connection.execute(
                "SELECT value FROM sec_ftd_identity WHERE id=1"
            ).fetchone()
            if stored is None or json.loads(stored[0]) != self.identity:
                raise ValueError("database identity disagrees with the bound root")
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
            columns = ",".join(c.name for c in self.schemas["facts"].columns)
            self.connection.execute(f"""CREATE VIEW sec_ftd_gold AS SELECT {columns} FROM (
                SELECT *,ROW_NUMBER() OVER(
                    PARTITION BY source,dataset,reporting_regime,settlement_date,cusip
                    ORDER BY first_seen_at DESC,capture_sequence DESC) AS rank
                FROM sec_ftd_facts) WHERE rank=1 AND row_state='REPORTED'""")
        atomic_json(marker, self.identity)

    @contextmanager
    def transaction(self):
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
            self.connection.execute("SELECT value FROM sec_ftd_sequence WHERE id=1").fetchone()[0]
            + 1
        )
        if value >= 2**63:
            raise ValueError("capture sequence exhausted")
        self.connection.execute("UPDATE sec_ftd_sequence SET value=? WHERE id=1", (value,))
        return value

    def _insert(self, name, row):
        values = self.schemas[name].encode(row, "sqlite")
        self.connection.execute(
            f"INSERT INTO {TABLES[name]} ({','.join(values)}) "
            f"VALUES ({','.join('?' for _ in values)})",
            tuple(values.values()),
        )

    def record_run(self, row):
        if not self.connection.in_transaction or row["status"] != "STARTED":
            raise ValueError("start a run inside its allocation transaction")
        value = self.schemas["audit"].encode(row, "sqlite")
        latest = self.connection.execute(
            "SELECT captured_at FROM sec_ftd_audit ORDER BY capture_sequence DESC LIMIT 1"
        ).fetchone()
        if latest and value["captured_at"] < latest[0]:
            raise ValueError("capture clock regressed; ambiguous knowledge ordering")
        self._insert("audit", row)

    def promote_partition(self, facts, coverage, audit) -> int:
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
                previous = self.connection.execute(
                    "SELECT raw_sha256 FROM sec_ftd_coverage "
                    "WHERE partition_id=? AND validation_state='MATCHED' "
                    "ORDER BY capture_sequence DESC LIMIT 1",
                    (coverage["partition_id"],),
                ).fetchone()
                if previous and previous[0] != coverage["raw_sha256"]:
                    raise ValueError("REVISION_REQUIRES_T05")
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
                existing = {
                    (r[0], r[1]): r[2]
                    for r in self.connection.execute(
                        "SELECT settlement_date,cusip,content_hash "
                        "FROM sec_ftd_facts WHERE partition_id=?",
                        (coverage["partition_id"],),
                    )
                }
                keys = set()
                for row in facts:
                    row = self.schemas["facts"].normalize(row)
                    key = (row["settlement_date"].isoformat(), row["cusip"])
                    if key in keys or (
                        row["run_id"],
                        row["capture_sequence"],
                        row["partition_id"],
                    ) != (audit["run_id"], audit["capture_sequence"], coverage["partition_id"]):
                        raise ValueError("duplicate fact or inconsistent capture lineage")
                    keys.add(key)
                    if key in existing:
                        if existing[key] != row["content_hash"]:
                            raise ValueError("REVISION_REQUIRES_T05")
                    else:
                        self._insert("facts", row)
                        inserted += 1
                if previous and set(existing) != keys:
                    raise ValueError("REVISION_REQUIRES_T05")
            self._insert("coverage", coverage)
            values = self.schemas["audit"].encode(audit, "sqlite")
            self.connection.execute(
                "UPDATE sec_ftd_audit SET "
                + ",".join(f"{k}=?" for k in values)
                + " WHERE run_id=?",
                (*values.values(), audit["run_id"]),
            )
            return inserted

    def select_facts(self, *, as_of: datetime | None = None, limit: int | None = None):
        if as_of is not None:
            raise ValueError("observed-as-of selection is delivered in T05")
        query = "SELECT * FROM sec_ftd_gold ORDER BY settlement_date,cusip"
        parameters = ()
        if limit is not None:
            if type(limit) is not int or not 1 <= limit <= 1000:
                raise ValueError("Gold limit must be in [1,1000]")
            query += " LIMIT ?"
            parameters = (limit,)
        return [
            self.schemas["facts"].decode(dict(row), "sqlite")
            for row in self.connection.execute(query, parameters)
        ]

    def select_coverage(self, *, as_of=None):
        if as_of is not None:
            raise ValueError("observed-as-of selection is delivered in T05")
        columns = ",".join(c.name for c in self.schemas["coverage"].columns)
        rows = self.connection.execute(
            f"SELECT {columns} FROM (SELECT *,ROW_NUMBER() OVER("
            "PARTITION BY partition_id ORDER BY first_seen_at DESC,capture_sequence DESC) "
            "AS rank FROM sec_ftd_coverage) WHERE rank=1 ORDER BY partition_id"
        )
        return [self.schemas["coverage"].decode(dict(row), "sqlite") for row in rows]

    def select_runs(self):
        return [
            self.schemas["audit"].decode(dict(row), "sqlite")
            for row in self.connection.execute(
                "SELECT * FROM sec_ftd_audit ORDER BY capture_sequence"
            )
        ]

    def record_publication(self, row):
        raise ValueError("Parquet publication is delivered in T07")

    def pending_publications(self):
        return [
            self.schemas["publications"].decode(dict(row), "sqlite")
            for row in self.connection.execute(
                "SELECT * FROM sec_ftd_publications WHERE status IN ('STAGED','COMMITTED')"
            )
        ]
