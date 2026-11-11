"""FOMC-AC11 PostgreSQL inventory snapshots, owned schema and atomic current pointer.

These are normalized metadata import receipts, not document-content versions or
proof of a source archive. Real document bytes/text arrive in later slices.
"""

import hashlib
import json
import os
import uuid
from dataclasses import asdict

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import (
    PostgresConfig,
    postgres_from_dict,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import Inventory
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import (
    Manifest,
    manifest_from_dict,
)

CONTRACT = "fomc-inventory-postgres/1"
_DDL = (
    """CREATE TABLE IF NOT EXISTS {schema}.inventory_runs (
    run_id uuid PRIMARY KEY, sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    input_kind text NOT NULL CHECK (input_kind IN ('invented_fixture','normalized_inventory')),
    manifest_sha256 text NOT NULL, inventory_sha256 text NOT NULL,
    manifest_json jsonb NOT NULL, inventory_json jsonb NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS {schema}.inventory_meetings (
    run_id uuid NOT NULL REFERENCES {schema}.inventory_runs(run_id), meeting_id text NOT NULL,
    committee_body text NOT NULL,event_kind text NOT NULL,meeting_dates date[] NOT NULL,
    session_label_raw text,PRIMARY KEY(run_id,meeting_id))""",
    """CREATE TABLE IF NOT EXISTS {schema}.inventory_documents (
    run_id uuid NOT NULL REFERENCES {schema}.inventory_runs(run_id),document_id text NOT NULL,
    source_url text NOT NULL,document_kind text NOT NULL,representation text NOT NULL,
    PRIMARY KEY(run_id,document_id),UNIQUE(run_id,source_url))""",
    """CREATE TABLE IF NOT EXISTS {schema}.inventory_pages (
    run_id uuid NOT NULL REFERENCES {schema}.inventory_runs(run_id),source_page_url text NOT NULL,
    PRIMARY KEY(run_id,source_page_url))""",
    """CREATE TABLE IF NOT EXISTS {schema}.inventory_entries (
    run_id uuid NOT NULL REFERENCES {schema}.inventory_runs(run_id),ordinal integer NOT NULL,
    meeting_id text NOT NULL,document_id text,source_page_url text NOT NULL,
    meeting_label_raw text NOT NULL,minutes_status text NOT NULL,
    document_label_raw text,href_raw text,provider_release_date date,
    PRIMARY KEY(run_id,ordinal),
    FOREIGN KEY(run_id,meeting_id) REFERENCES {schema}.inventory_meetings(run_id,meeting_id),
    FOREIGN KEY(run_id,document_id) REFERENCES {schema}.inventory_documents(run_id,document_id),
    FOREIGN KEY(run_id,source_page_url)
    REFERENCES {schema}.inventory_pages(run_id,source_page_url))""",
    """CREATE TABLE IF NOT EXISTS {schema}.catalog_state (
    singleton boolean PRIMARY KEY CHECK(singleton),contract_id text NOT NULL,
    current_run_id uuid REFERENCES {schema}.inventory_runs(run_id))""",
)


def _hash(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


def _run_id(value: str | None) -> str:
    try:
        return str(uuid.UUID(value)) if value is not None else str(uuid.uuid4())
    except (ValueError, TypeError, AttributeError):
        raise ValueError("FOMC run_id requires UUID text") from None


class PostgresInventoryStore:
    def __init__(self, config: PostgresConfig):
        self.config = postgres_from_dict(asdict(config))
        self.connection = None

    def __enter__(self):
        try:
            import psycopg
            from psycopg import sql
            from psycopg.types.json import Jsonb
        except ImportError:
            raise RuntimeError(
                "install the postgres optional dependency for FOMC storage"
            ) from None
        password = os.environ.get(self.config.password_env)
        if not password:
            raise ValueError("FOMC PostgreSQL password environment variable is not set")
        self._sql, self._Jsonb = sql, Jsonb
        try:
            self.connection = psycopg.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.database,
                user=self.config.user,
                password=password,
                sslmode=self.config.sslmode,
                connect_timeout=self.config.connect_timeout,
                autocommit=True,
            )
        except psycopg.Error:
            raise RuntimeError("FOMC PostgreSQL connection failed") from None
        try:
            if self.config.role:
                self.connection.execute(
                    sql.SQL("SET ROLE {}").format(sql.Identifier(self.config.role))
                )
            self.connection.execute("SET TIME ZONE 'UTC'")
        except Exception:
            self.connection.close()
            raise
        return self

    def __exit__(self, *_args):
        self.connection.close()

    def _query(self, text):
        return self._sql.SQL(text).format(schema=self._sql.Identifier(self.config.schema))

    def initialize(self) -> None:
        """Create only the FOMC namespace; refuse existing tables without our identity marker."""
        with self.connection.transaction():
            self.connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("market-medallion:" + self.config.schema,),
            )
            self.connection.execute(self._query("CREATE SCHEMA IF NOT EXISTS {schema}"))
            tables = {
                row[0]
                for row in self.connection.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname=%s", (self.config.schema,)
                )
            }
            if tables:
                if "catalog_state" not in tables:
                    raise ValueError("existing FOMC namespace lacks the inventory ownership marker")
                marker = self.connection.execute(
                    self._query("SELECT contract_id FROM {schema}.catalog_state")
                ).fetchall()
                if marker != [(CONTRACT,)]:
                    raise ValueError("FOMC PostgreSQL catalog identity mismatch")
            for statement in _DDL:
                self.connection.execute(self._query(statement))
            self.connection.execute(
                self._query("""INSERT INTO {schema}.catalog_state
                (singleton,contract_id,current_run_id) VALUES(true,%s,NULL)
                ON CONFLICT(singleton) DO NOTHING"""),
                (CONTRACT,),
            )

    def save_inventory(
        self,
        inventory: Inventory,
        manifest: Manifest,
        *,
        input_kind: str,
        run_id: str | None = None,
    ) -> dict:
        """Commit one metadata import atomically; identical run retries never move the pointer."""
        if not isinstance(inventory, Inventory) or not isinstance(manifest, Manifest):
            raise ValueError("FOMC storage requires typed inventory and manifest")
        manifest_from_dict(manifest.to_dict())
        if inventory.schema_id != manifest.schema_id or input_kind not in (
            "invented_fixture",
            "normalized_inventory",
        ):
            raise ValueError("FOMC inventory schema or input kind mismatch")
        identity = _run_id(run_id)
        payload = inventory.to_dict()
        manifest_data = manifest.to_dict()
        payload_hash, manifest_hash = _hash(payload), _hash(manifest_data)
        with self.connection.transaction():
            state = self.connection.execute(
                self._query("""SELECT contract_id FROM {schema}.catalog_state
                WHERE singleton=true FOR UPDATE""")
            ).fetchone()
            if state != (CONTRACT,):
                raise ValueError("FOMC PostgreSQL catalog identity mismatch")
            existing = self.connection.execute(
                self._query("""SELECT inventory_sha256,manifest_sha256,
                input_kind,recorded_at FROM {schema}.inventory_runs WHERE run_id=%s"""),
                (identity,),
            ).fetchone()
            if existing:
                if existing[:3] != (payload_hash, manifest_hash, input_kind):
                    raise ValueError("FOMC run_id already identifies different input")
                return {"run_id": identity, "inserted": False, "recorded_at": existing[3]}
            recorded = self.connection.execute(
                self._query("""INSERT INTO {schema}.inventory_runs
                (run_id,input_kind,manifest_sha256,inventory_sha256,manifest_json,inventory_json)
                VALUES(%s,%s,%s,%s,%s,%s) RETURNING recorded_at"""),
                (
                    identity,
                    input_kind,
                    manifest_hash,
                    payload_hash,
                    self._Jsonb(manifest_data),
                    self._Jsonb(payload),
                ),
            ).fetchone()[0]
            with self.connection.cursor() as cur:
                cur.executemany(
                    self._query(
                        "INSERT INTO {schema}.inventory_meetings VALUES(%s,%s,%s,%s,%s,%s)"
                    ),
                    [
                        (
                            identity,
                            m.meeting_id,
                            m.committee_body,
                            m.event_kind,
                            list(m.meeting_dates),
                            m.session_label_raw,
                        )
                        for m in inventory.meetings
                    ],
                )
                cur.executemany(
                    self._query("INSERT INTO {schema}.inventory_documents VALUES(%s,%s,%s,%s,%s)"),
                    [
                        (identity, d.document_id, d.source_url, d.document_kind, d.representation)
                        for d in inventory.documents
                    ],
                )
                cur.executemany(
                    self._query("INSERT INTO {schema}.inventory_pages VALUES(%s,%s)"),
                    [(identity, page) for page in inventory.source_pages],
                )
                cur.executemany(
                    self._query(
                        "INSERT INTO {schema}.inventory_entries "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    ),
                    [
                        (
                            identity,
                            n,
                            e.meeting_id,
                            e.document_id,
                            e.source_page_url,
                            e.meeting_label_raw,
                            e.minutes_status,
                            e.document_label_raw,
                            e.href_raw,
                            e.provider_release_date,
                        )
                        for n, e in enumerate(inventory.entries)
                    ],
                )
            self.connection.execute(
                self._query(
                    "UPDATE {schema}.catalog_state SET current_run_id=%s WHERE singleton=true"
                ),
                (identity,),
            )
        return {"run_id": identity, "inserted": True, "recorded_at": recorded}

    def read_inventory(self, run_id: str | None = None) -> dict | None:
        if run_id is None:
            row = self.connection.execute(
                self._query("""SELECT r.inventory_json,r.inventory_sha256
                FROM {schema}.inventory_runs r
                JOIN {schema}.catalog_state s ON s.current_run_id=r.run_id
                WHERE s.singleton=true""")
            ).fetchone()
        else:
            row = self.connection.execute(
                self._query(
                    "SELECT inventory_json,inventory_sha256 "
                    "FROM {schema}.inventory_runs WHERE run_id=%s"
                ),
                (_run_id(run_id),),
            ).fetchone()
        if row and _hash(row[0]) != row[1]:
            raise ValueError("FOMC inventory snapshot hash mismatch")
        return row[0] if row else None

    def run_count(self) -> int:
        return self.connection.execute(
            self._query("SELECT count(*) FROM {schema}.inventory_runs")
        ).fetchone()[0]
