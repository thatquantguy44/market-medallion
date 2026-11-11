"""FOMC-AC12: explicit owned migration and immutable verified text mirrors."""

import json
import re
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.dependencies import (
    discover_html_dependencies,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import (
    encoded,
    extract_text,
    validate,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import _write
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
    PostgresDependencyStore,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import make_directory

CONTRACT = "fomc-text-postgres/1"
_DDL = [
    """CREATE TABLE {schema}.text_extractions (extraction_id text PRIMARY KEY,
    source_sha256 text NOT NULL,profile_sha256 text NOT NULL,text_sha256 text NOT NULL,
    full_text text NOT NULL,result_json jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp())""",
    """CREATE TABLE {schema}.text_units (extraction_id text NOT NULL REFERENCES
    {schema}.text_extractions(extraction_id),ordinal integer NOT NULL,unit_json jsonb NOT NULL,
    PRIMARY KEY(extraction_id,ordinal))""",
    """CREATE TABLE {schema}.text_capture_links (capture_id uuid NOT NULL REFERENCES
    {schema}.bronze_captures(capture_id),extraction_id text NOT NULL REFERENCES
    {schema}.text_extractions(extraction_id),PRIMARY KEY(capture_id,extraction_id))""",
    """CREATE TABLE {schema}.text_catalogs (catalog_id text PRIMARY KEY,
    inventory_run_id uuid NOT NULL REFERENCES {schema}.inventory_runs(run_id),
    catalog_json jsonb NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp())""",
    """CREATE TABLE {schema}.text_state (singleton boolean PRIMARY KEY CHECK(singleton),
    contract_id text NOT NULL,
    current_catalog_id text REFERENCES {schema}.text_catalogs(catalog_id))""",
]


def artifact_path(root, kind, identity):
    if (
        kind not in {"extractions", "catalogs", "chunks", "chunk_catalogs"}
        or not isinstance(identity, str)
        or not re.fullmatch("[0-9a-f]{64}", identity)
    ):
        raise ValueError("FOMC text artifact identity mismatch")
    return Path(root) / "text" / kind / (identity + ".json")


def write_artifact(root, kind, identity, value):
    path = artifact_path(root, kind, identity)
    make_directory(path.parent)
    body = encoded(value)
    try:
        _write(path, body)
    except FileExistsError:
        if path.read_bytes() != body:
            raise ValueError("FOMC immutable text artifact conflict") from None
    return path


def read_artifact(root, kind, identity):
    try:
        return json.loads(artifact_path(root, kind, identity).read_bytes())
    except (OSError, ValueError, UnicodeError):
        raise ValueError("FOMC text filesystem mirror is missing or corrupt") from None


class PostgresTextStore:
    def __init__(self, bronze):
        self.bronze, self.store = bronze, bronze.store
        self.dependencies = PostgresDependencyStore(bronze)

    def migrate(self):
        store, conn = self.store, self.store.connection
        tables = {
            "text_extractions",
            "text_units",
            "text_capture_links",
            "text_catalogs",
            "text_state",
        }
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
            if not {"bronze_captures", "bronze_dependency_state"}.issubset(existing):
                raise ValueError("FOMC text migration requires owned Bronze/dependency tables")
            marker = conn.execute(
                store._query("SELECT contract_id FROM {schema}.bronze_dependency_state")
            ).fetchall()
            from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
                CONTRACT as DEPENDENCIES,
            )

            if marker != [(DEPENDENCIES,)]:
                raise ValueError("FOMC text migration dependency identity mismatch")
            if existing & tables:
                if existing & tables != tables:
                    raise ValueError("incomplete FOMC text schema")
                if conn.execute(
                    store._query("SELECT contract_id FROM {schema}.text_state")
                ).fetchall() != [(CONTRACT,)]:
                    raise ValueError("FOMC text schema identity mismatch")
                return
            for ddl in _DDL:
                conn.execute(store._query(ddl))
            conn.execute(
                store._query("INSERT INTO {schema}.text_state VALUES(true,%s,NULL)"), (CONTRACT,)
            )

    def extract_capture(self, capture_id, manifest, *, root, **limits):
        receipt, payload = self.dependencies._verified(capture_id, manifest, root)
        if receipt["kind"] != "document":
            raise ValueError("FOMC extraction requires a document capture")
        result = extract_text(payload, receipt["source_url"], manifest, **limits)
        if result["status"] != "failed":
            self.verify_dependencies(receipt, payload, manifest, root=root)
        return self.save_result(capture_id, result, manifest, root=root)

    def verify_dependencies(self, receipt, payload, manifest, *, root):
        if not receipt["source_url"].lower().endswith(".pdf"):
            dependencies = discover_html_dependencies(payload, receipt["source_url"], manifest)
            if dependencies.dependencies:
                checks = self.store.connection.execute(
                    self.store._query(
                        "SELECT check_id FROM {schema}.bronze_dependency_checks "
                        "WHERE parent_capture_id=%s"
                    ),
                    (receipt["capture_id"],),
                ).fetchall()
                if not checks:
                    raise ValueError("FOMC HTML text requires verified dependency originals")
                for (check_id,) in checks:
                    evidence = self.dependencies.read_check(check_id)
                    for child in evidence["dependencies"]:
                        self.dependencies._verified(child["child_capture_id"], manifest, root)

    def save_result(self, capture_id, result, manifest, *, root):
        result = validate(result)
        receipt, _ = self.dependencies._verified(capture_id, manifest, root)
        if (
            receipt["kind"] != "document"
            or receipt["sha256"] != result["source_sha256"]
            or receipt["source_url"] != result["source_url"]
        ):
            raise ValueError("FOMC extraction does not match original capture")
        identity = result["extraction_id"]
        write_artifact(root, "extractions", identity, result)
        store, conn = self.store, self.store.connection
        with conn.transaction():
            conn.execute(
                store._query("""INSERT INTO {schema}.text_extractions
                (extraction_id,source_sha256,profile_sha256,text_sha256,full_text,result_json)
                VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""),
                (
                    identity,
                    result["source_sha256"],
                    result["profile_sha256"],
                    result["text_sha256"],
                    result["full_text"],
                    store._Jsonb(result),
                ),
            )
            for unit in result["units"]:
                conn.execute(
                    store._query(
                        "INSERT INTO {schema}.text_units VALUES(%s,%s,%s) ON CONFLICT DO NOTHING"
                    ),
                    (identity, unit["ordinal"], store._Jsonb(unit)),
                )
            self.read_extraction(identity, root=root)
            conn.execute(
                store._query(
                    "INSERT INTO {schema}.text_capture_links VALUES(%s,%s) ON CONFLICT DO NOTHING"
                ),
                (capture_id, identity),
            )
        return result

    def read_extraction(self, identity, *, root):
        store, conn = self.store, self.store.connection
        artifact_path(root, "extractions", identity)
        row = conn.execute(
            store._query(
                "SELECT source_sha256,profile_sha256,text_sha256,full_text,result_json "
                "FROM {schema}.text_extractions WHERE extraction_id=%s"
            ),
            (identity,),
        ).fetchone()
        if row is None:
            raise ValueError("FOMC text database mirror is missing")
        value = validate(row[4])
        if value["extraction_id"] != identity:
            raise ValueError("FOMC text extraction identity mismatch")
        if row[:4] != (
            value["source_sha256"],
            value["profile_sha256"],
            value["text_sha256"],
            value["full_text"],
        ):
            raise ValueError("FOMC text database mirror mismatch")
        if read_artifact(root, "extractions", identity) != value:
            raise ValueError("FOMC text filesystem mirror mismatch")
        units = conn.execute(
            store._query(
                "SELECT ordinal,unit_json FROM {schema}.text_units "
                "WHERE extraction_id=%s ORDER BY ordinal"
            ),
            (identity,),
        ).fetchall()
        if units != [(u["ordinal"], u) for u in value["units"]]:
            raise ValueError("FOMC text unit mirror mismatch")
        return value
