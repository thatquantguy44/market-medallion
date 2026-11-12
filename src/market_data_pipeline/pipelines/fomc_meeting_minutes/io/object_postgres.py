"""Explicit, transactional migration from per-capture bytes to shared originals."""

import hashlib
from datetime import datetime

CONTRACT = "fomc-bronze-objects-postgres/1"
TABLES = {"bronze_object_state", "bronze_objects", "bronze_capture_objects"}


class PostgresObjectStore:
    def __init__(self, bronze):
        self.bronze, self.store = bronze, bronze.store

    def active(self):
        conn, store = self.store.connection, self.store
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname=%s", (store.config.schema,)
            )
        }
        if not tables & TABLES:
            return False
        if tables & TABLES != TABLES:
            raise ValueError("incomplete FOMC content-object schema")
        if conn.execute(
            store._query("SELECT contract_id FROM {schema}.bronze_object_state")
        ).fetchall() != [(CONTRACT,)]:
            raise ValueError("FOMC content-object identity mismatch")
        return True

    def save_object(self, digest, payload):
        conn, store = self.store.connection, self.store
        conn.execute(
            store._query(
                "INSERT INTO {schema}.bronze_objects (sha256,size_bytes,payload) "
                "VALUES(%s,%s,%s) ON CONFLICT DO NOTHING"
            ),
            (digest, len(payload), payload),
        )
        row = conn.execute(
            store._query("SELECT size_bytes,payload FROM {schema}.bronze_objects WHERE sha256=%s"),
            (digest,),
        ).fetchone()
        if row is None or row[0] != len(payload) or bytes(row[1]) != payload:
            raise ValueError("FOMC content-object hash collision or corruption")

    def associate(self, capture_id, digest):
        conn, store = self.store.connection, self.store
        conn.execute(
            store._query(
                "INSERT INTO {schema}.bronze_capture_objects (capture_id,sha256) "
                "VALUES(%s,%s) ON CONFLICT DO NOTHING"
            ),
            (capture_id, digest),
        )
        row = conn.execute(
            store._query("SELECT sha256 FROM {schema}.bronze_capture_objects WHERE capture_id=%s"),
            (capture_id,),
        ).fetchone()
        if row != (digest,):
            raise ValueError("FOMC capture already references a different content object")

    def migrate(self, *, max_captures=10000):
        if type(max_captures) is not int or max_captures < 1:
            raise ValueError("FOMC object migration requires a positive capture bound")
        conn, store = self.store.connection, self.store
        with conn.transaction():
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("market-medallion:" + store.config.schema,),
            )
            if self.active():
                return
            from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import MIGRATION

            markers = conn.execute(
                store._query("SELECT migration_id FROM {schema}.bronze_schema_migrations")
            ).fetchall()
            if (MIGRATION,) not in markers:
                raise ValueError("FOMC object migration requires owned Bronze storage")
            count = conn.execute(
                store._query("SELECT count(*) FROM {schema}.bronze_captures")
            ).fetchone()[0]
            if count > max_captures:
                raise ValueError("FOMC object migration exceeds its capture bound")
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_object_state (singleton boolean PRIMARY KEY"
                    " CHECK(singleton),contract_id text NOT NULL)"
                )
            )
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_objects (sha256 text PRIMARY KEY,size_bytes"
                    " bigint NOT NULL,payload bytea NOT "
                    "NULL,CHECK(size_bytes=octet_length(payload)))"
                )
            )
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_capture_objects (capture_id uuid PRIMARY "
                    "KEY REFERENCES {schema}.bronze_captures(capture_id),sha256 text NOT NULL"
                    " REFERENCES {schema}.bronze_objects(sha256))"
                )
            )
            for row in conn.execute(
                store._query(
                    "SELECT capture_id,source_url,captured_at,sha256,receipt,payload FROM "
                    "{schema}.bronze_captures ORDER BY capture_id"
                )
            ).fetchall():
                receipt = row[4]
                payload = bytes(row[5]) if row[5] is not None else None
                if (
                    payload is None
                    or hashlib.sha256(payload).hexdigest() != row[3]
                    or receipt.get("sha256") != row[3]
                    or receipt.get("size_bytes") != len(payload)
                    or receipt.get("source_url") != row[1]
                    or receipt.get("capture_id") != str(row[0])
                    or datetime.fromisoformat(receipt["captured_at"]) != row[2]
                ):
                    raise ValueError("FOMC legacy capture hash or lineage mismatch")
                self.save_object(row[3], payload)
                self.associate(row[0], row[3])
            mapped = conn.execute(
                store._query("SELECT count(*) FROM {schema}.bronze_capture_objects")
            ).fetchone()[0]
            if mapped != count:
                raise ValueError("FOMC object migration omitted captures")
            # Clear only the redundant copy after every object and association verifies.
            conn.execute(
                store._query(
                    "ALTER TABLE {schema}.bronze_captures ALTER COLUMN payload DROP NOT NULL"
                )
            )
            conn.execute(store._query("UPDATE {schema}.bronze_captures SET payload=NULL"))
            conn.execute(
                store._query("INSERT INTO {schema}.bronze_object_state VALUES(true,%s)"),
                (CONTRACT,),
            )
