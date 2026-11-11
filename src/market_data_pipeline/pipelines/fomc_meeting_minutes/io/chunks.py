"""FOMC-AC12: explicit owned chunk migration and immutable full-text row mirrors."""

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunks import chunk_text, validate
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import (
    CONTRACT as TEXT_CONTRACT,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import (
    artifact_path,
    read_artifact,
    write_artifact,
)

CONTRACT = "fomc-chunks-postgres/1"
_DDL = [
    """CREATE TABLE {schema}.chunk_sets (set_id text PRIMARY KEY,
    extraction_id text NOT NULL REFERENCES {schema}.text_extractions(extraction_id),
    document_version_id text NOT NULL,profile_sha256 text NOT NULL,result_json jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp())""",
    """CREATE TABLE {schema}.chunks (set_id text NOT NULL REFERENCES {schema}.chunk_sets(set_id),
    ordinal integer NOT NULL,chunk_id text NOT NULL UNIQUE,chunk_text text NOT NULL,
    eligible boolean NOT NULL,chunk_json jsonb NOT NULL,PRIMARY KEY(set_id,ordinal))""",
    """CREATE INDEX ON {schema}.chunks(set_id,eligible,ordinal)""",
    """CREATE TABLE {schema}.chunk_catalogs (catalog_id text PRIMARY KEY,
    text_catalog_id text NOT NULL REFERENCES {schema}.text_catalogs(catalog_id),
    profile_sha256 text NOT NULL,catalog_json jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp())""",
    """CREATE TABLE {schema}.chunk_state (singleton boolean PRIMARY KEY CHECK(singleton),
    contract_id text NOT NULL,
    current_catalog_id text REFERENCES {schema}.chunk_catalogs(catalog_id))""",
]


class PostgresChunkStore:
    def __init__(self, text):
        self.text, self.store = text, text.store

    def migrate(self):
        store, conn = self.store, self.store.connection
        tables = {"chunk_sets", "chunks", "chunk_catalogs", "chunk_state"}
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
            if "text_state" not in existing:
                raise ValueError("FOMC chunks require the owned text migration")
            if conn.execute(
                store._query("SELECT contract_id FROM {schema}.text_state")
            ).fetchall() != [(TEXT_CONTRACT,)]:
                raise ValueError("FOMC chunk text dependency identity mismatch")
            if existing & tables:
                if existing & tables != tables:
                    raise ValueError("incomplete FOMC chunk schema")
                if conn.execute(
                    store._query("SELECT contract_id FROM {schema}.chunk_state")
                ).fetchall() != [(CONTRACT,)]:
                    raise ValueError("FOMC chunk schema identity mismatch")
                return
            for ddl in _DDL:
                conn.execute(store._query(ddl))
            conn.execute(
                store._query("INSERT INTO {schema}.chunk_state VALUES(true,%s,NULL)"), (CONTRACT,)
            )

    def make_set(self, extraction_id, document_version_id, *, root, **options):
        extraction = self.text.read_extraction(extraction_id, root=root)
        value = chunk_text(extraction, document_version_id=document_version_id, **options)
        return self.save_set(value, root=root)

    def save_set(self, value, *, root):
        extraction = self.text.read_extraction(value["extraction_id"], root=root)
        validate(value, extraction)
        identity = value["set_id"]
        write_artifact(root, "chunks", identity, value)
        store, conn = self.store, self.store.connection
        with conn.transaction():
            if conn.execute(
                store._query("SELECT contract_id FROM {schema}.chunk_state")
            ).fetchall() != [(CONTRACT,)]:
                raise ValueError("FOMC chunk schema identity mismatch")
            conn.execute(
                store._query("""INSERT INTO {schema}.chunk_sets
                (set_id,extraction_id,document_version_id,profile_sha256,result_json)
                VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""),
                (
                    identity,
                    value["extraction_id"],
                    value["document_version_id"],
                    value["profile_sha256"],
                    store._Jsonb(value),
                ),
            )
            for row in value["chunks"]:
                conn.execute(
                    store._query("""INSERT INTO {schema}.chunks
                    (set_id,ordinal,chunk_id,chunk_text,eligible,chunk_json)
                    VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(set_id,ordinal) DO NOTHING"""),
                    (
                        identity,
                        row["ordinal"],
                        row["chunk_id"],
                        row["text"],
                        row["eligible"],
                        store._Jsonb(row),
                    ),
                )
            self.read_set(identity, root=root)
        return value

    def read_set(self, identity, *, root):
        artifact_path(root, "chunks", identity)
        store, conn = self.store, self.store.connection
        row = conn.execute(
            store._query("""SELECT extraction_id,document_version_id,
            profile_sha256,result_json FROM {schema}.chunk_sets WHERE set_id=%s"""),
            (identity,),
        ).fetchone()
        if row is None:
            raise ValueError("FOMC chunk database mirror is missing")
        value = row[3]
        validate(value, self.text.read_extraction(value["extraction_id"], root=root))
        if value["set_id"] != identity or row[:3] != (
            value["extraction_id"],
            value["document_version_id"],
            value["profile_sha256"],
        ):
            raise ValueError("FOMC chunk set identity mismatch")
        if read_artifact(root, "chunks", identity) != value:
            raise ValueError("FOMC chunk filesystem mirror mismatch")
        rows = conn.execute(
            store._query("""SELECT ordinal,chunk_id,chunk_text,eligible,chunk_json
            FROM {schema}.chunks WHERE set_id=%s ORDER BY ordinal"""),
            (identity,),
        ).fetchall()
        if rows != [
            (r["ordinal"], r["chunk_id"], r["text"], r["eligible"], r) for r in value["chunks"]
        ]:
            raise ValueError("FOMC chunk row mirror mismatch")
        return value
