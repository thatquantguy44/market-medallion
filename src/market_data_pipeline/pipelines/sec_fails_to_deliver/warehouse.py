"""Contract-bound SQLite adapter for local SEC FTD ingestion only."""

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import StorageConfig
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
        self.schemas = storage_schemas(settings)
        self.expected = {
            **IDENTITY,
            "backend": "sqlite",
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
            if stored is None or json.loads(stored[0]) != self.identity:
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
            self.connection.execute("SELECT value FROM sec_ftd_sequence WHERE id=1").fetchone()[0]
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
            ).fetchone()[0]
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
            digest.update(json.dumps(dict(row), sort_keys=True, separators=(",", ":")).encode())
            digest.update(b"\n")
            count += 1
        return {"count": count, "sha256": digest.hexdigest()}

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
                existing = {
                    (r["settlement_date"], r["cusip"]): r
                    for r in self.connection.execute(
                        f"SELECT * FROM ({selected}) WHERE partition_id=?",
                        (coverage["partition_id"],),
                    )
                }
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
                    key = (row["settlement_date"].isoformat(), row["cusip"])
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
                previous = [
                    self.schemas["facts"].decode(dict(row), "sqlite") for row in existing.values()
                ]
                for row in withdrawals(previous, normalized, coverage, audit, capture, evidence):
                    self._insert("facts", row)
                    inserted += 1
            self._insert("coverage", coverage)
            values = self.schemas["audit"].encode(audit, "sqlite")
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
            self.schemas["facts"].decode(dict(row), "sqlite")
            for row in self.connection.execute(query, parameters)
        ]

    def select_coverage(self, *, as_of=None, as_of_sequence=None):
        query, parameters = self._selected_query(
            "coverage", as_of=as_of, as_of_sequence=as_of_sequence
        )
        rows = self.connection.execute(query + " ORDER BY partition_id", parameters)
        return [self.schemas["coverage"].decode(dict(row), "sqlite") for row in rows]

    def count_facts(self, *, as_of=None, as_of_sequence=None, include_withdrawn=False) -> int:
        query, parameters = self._selected_query(
            "facts", as_of=as_of, as_of_sequence=as_of_sequence, include_withdrawn=include_withdrawn
        )
        return self.connection.execute(f"SELECT count(*) FROM ({query})", parameters).fetchone()[0]

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
