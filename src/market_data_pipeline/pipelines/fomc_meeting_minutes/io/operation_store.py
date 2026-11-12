"""FOMC-AC06: immutable run intents/events/results and explicit local recovery."""

import json
import uuid
from datetime import datetime
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest, encoded
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import _identity, _write
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import CONTRACT as CHUNKS
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import make_directory

CONTRACT = "fomc-operations-postgres/1"
_DDL = [
    """CREATE TABLE {schema}.operation_state (
    singleton boolean PRIMARY KEY CHECK(singleton),contract_id text NOT NULL)""",
    """CREATE TABLE {schema}.operation_runs (
    run_id uuid PRIMARY KEY,started_at timestamptz NOT NULL,
    intent_json jsonb NOT NULL,intent_sha256 text NOT NULL)""",
    """CREATE TABLE {schema}.operation_records (
    run_id uuid NOT NULL REFERENCES {schema}.operation_runs(run_id),
    ordinal integer NOT NULL CHECK(ordinal>=0),kind text NOT NULL
    CHECK(kind IN ('event','result')),record_json jsonb NOT NULL,
    record_sha256 text NOT NULL,PRIMARY KEY(run_id,ordinal))""",
    """CREATE UNIQUE INDEX operation_one_result ON {schema}.operation_records(run_id)
    WHERE kind='result'""",
]


def _file(root, run_id, name):
    return Path(root) / "operations" / _identity(run_id) / name


def _put(path, value):
    make_directory(path.parent)
    try:
        _write(path, encoded(value))
    except FileExistsError:
        if path.read_bytes() != encoded(value):
            raise ValueError("FOMC operation artifact conflict") from None


class OperationStore:
    def __init__(self, store):
        self.store = store

    def pointers(self):
        return {
            key: str(row[0]) if row and row[0] else None
            for key, table, column in [
                ("inventory", "catalog_state", "current_run_id"),
                ("text", "text_state", "current_catalog_id"),
                ("chunk", "chunk_state", "current_catalog_id"),
            ]
            for row in [
                self.store.connection.execute(
                    self.store._query(
                        "SELECT " + column + " FROM {schema}." + table + " WHERE singleton"
                    )
                ).fetchone()
            ]
        }

    def migrate(self):
        store, conn = self.store, self.store.connection
        tables = {"operation_state", "operation_runs", "operation_records"}
        with conn.transaction():
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("market-medallion:" + store.config.schema,),
            )
            existing = {
                r[0]
                for r in conn.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname=%s", (store.config.schema,)
                )
            }
            if "chunk_state" not in existing or conn.execute(
                store._query("SELECT contract_id FROM {schema}.chunk_state")
            ).fetchall() != [(CHUNKS,)]:
                raise ValueError("FOMC operations require the owned chunk migration")
            if existing & tables:
                if existing & tables != tables:
                    raise ValueError("incomplete FOMC operation schema")
                self._owned()
                return
            for ddl in _DDL:
                conn.execute(store._query(ddl))
            conn.execute(
                store._query("INSERT INTO {schema}.operation_state VALUES(true,%s)"), (CONTRACT,)
            )

    def _owned(self):
        if self.store.connection.execute(
            self.store._query("SELECT contract_id FROM {schema}.operation_state")
        ).fetchall() != [(CONTRACT,)]:
            raise ValueError("FOMC operation schema identity mismatch")

    def begin(self, intent, *, root):
        self._owned()
        value = {"schema_id": "fomc-operation-intent/1", "run_id": str(uuid.uuid4()), **intent}
        self._save_intent(value, root)
        return value["run_id"]

    def _save_intent(self, value, root):
        identity = _identity(value["run_id"])
        started = datetime.fromisoformat(value["started_at"].replace("Z", "+00:00"))
        if started.utcoffset() is None:
            raise ValueError("FOMC operation clock requires timezone")
        _put(_file(root, identity, "intent.json"), value)
        store = self.store
        store.connection.execute(
            store._query(
                """INSERT INTO {schema}.operation_runs VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
            ),
            (identity, started, store._Jsonb(value), digest(value)),
        )
        row = store.connection.execute(
            store._query(
                "SELECT intent_json,intent_sha256 FROM {schema}.operation_runs WHERE run_id=%s"
            ),
            (identity,),
        ).fetchone()
        if row != (value, digest(value)):
            raise ValueError("FOMC operation intent mirror mismatch")

    def append(self, run_id, payload, *, root, kind="event"):
        identity = _identity(run_id)
        store = self.store
        if kind not in {"event", "result"}:
            raise ValueError("invalid FOMC operation record kind")
        with store.connection.transaction():
            store.connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("fomc-operation:" + identity,),
            )
            if store.connection.execute(
                store._query(
                    "SELECT 1 FROM {schema}.operation_records WHERE run_id=%s AND kind='result'"
                ),
                (identity,),
            ).fetchone():
                raise ValueError("FOMC operation already has a final result")
            ordinal = store.connection.execute(
                store._query(
                    "SELECT coalesce(max(ordinal)+1,0) FROM {schema}.operation_records"
                    " WHERE run_id=%s"
                ),
                (identity,),
            ).fetchone()[0]
            value = {
                "schema_id": "fomc-operation-record/1",
                "run_id": identity,
                "ordinal": ordinal,
                "kind": kind,
                "payload": payload,
            }
            _put(_file(root, identity, f"{ordinal:06d}.json"), value)
            self._save_record(value)
        return value

    def _save_record(self, value):
        store = self.store
        if value["kind"] == "result" and value["payload"]["status"] == "completed":
            pointers = value["payload"]["pointers_after"]
            for key, table, column in [
                ("inventory", "inventory_runs", "run_id"),
                ("text", "text_catalogs", "catalog_id"),
                ("chunk", "chunk_catalogs", "catalog_id"),
            ]:
                if not store.connection.execute(
                    store._query("SELECT 1 FROM {schema}." + table + " WHERE " + column + "=%s"),
                    (pointers[key],),
                ).fetchone():
                    raise ValueError("FOMC completion result refers to uncommitted catalogs")
        store.connection.execute(
            store._query(
                "INSERT INTO {schema}.operation_records VALUES(%s,%s,%s,%s,%s) ON "
                "CONFLICT DO NOTHING"
            ),
            (value["run_id"], value["ordinal"], value["kind"], store._Jsonb(value), digest(value)),
        )
        row = store.connection.execute(
            store._query(
                "SELECT record_json,record_sha256 FROM {schema}.operation_records "
                "WHERE run_id=%s AND ordinal=%s"
            ),
            (value["run_id"], value["ordinal"]),
        ).fetchone()
        if row != (value, digest(value)):
            raise ValueError("FOMC operation record mirror mismatch")

    def read_run(self, run_id, *, root):
        identity = _identity(run_id)
        store = self.store
        row = store.connection.execute(
            store._query(
                "SELECT intent_json,intent_sha256 FROM {schema}.operation_runs WHERE run_id=%s"
            ),
            (identity,),
        ).fetchone()
        if not row:
            return None
        if (
            row[1] != digest(row[0])
            or json.loads(_file(root, identity, "intent.json").read_bytes()) != row[0]
        ):
            raise ValueError("FOMC operation intent integrity mismatch")
        records = []
        for ordinal, kind, value, sha in store.connection.execute(
            store._query(
                "SELECT ordinal,kind,record_json,record_sha256 "
                "FROM {schema}.operation_records WHERE run_id=%s ORDER BY ordinal"
            ),
            (identity,),
        ):
            if (
                (records and records[-1]["kind"] == "result")
                or value.get("schema_id") != "fomc-operation-record/1"
                or ordinal != len(records)
                or value["ordinal"] != ordinal
                or value["kind"] != kind
                or value["run_id"] != identity
                or digest(value) != sha
                or json.loads(_file(root, identity, f"{ordinal:06d}.json").read_bytes()) != value
            ):
                raise ValueError("FOMC operation record integrity mismatch")
            records.append(value)
        return {
            "intent": row[0],
            "records": records,
            "result": records[-1]["payload"]
            if records and records[-1]["kind"] == "result"
            else None,
        }

    def latest(self, *, root):
        self._owned()
        row = self.store.connection.execute(
            self.store._query(
                "SELECT run_id FROM {schema}.operation_runs "
                "ORDER BY started_at DESC,run_id DESC LIMIT 1"
            )
        ).fetchone()
        return self.read_run(str(row[0]), root=root) if row else None

    def recover(self, run_id, *, root):
        identity = _identity(run_id)
        intent = json.loads(_file(root, identity, "intent.json").read_bytes())
        if intent["run_id"] != identity or intent["schema_id"] != "fomc-operation-intent/1":
            raise ValueError("FOMC operation recovery intent mismatch")
        with self.store.connection.transaction():
            self._save_intent(intent, root)
            finished = False
            for i, path in enumerate(
                sorted(_file(root, identity, "intent.json").parent.glob("[0-9]*.json"))
            ):
                value = json.loads(path.read_bytes())
                if (
                    finished
                    or path.name != f"{i:06d}.json"
                    or value.get("schema_id") != "fomc-operation-record/1"
                    or value["ordinal"] != i
                    or value["run_id"] != identity
                ):
                    raise ValueError("FOMC operation recovery record sequence mismatch")
                self._save_record(value)
                finished = value["kind"] == "result"
        return self.read_run(identity, root=root)
