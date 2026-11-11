"""Explicit dependency migration and byte-verified, immutable direct-link checks."""

import hashlib
from dataclasses import asdict
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.dependencies import (
    discover_html_dependencies,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
    MIGRATION,
    _identity,
    _json,
    read_capture,
)

CONTRACT = "fomc-bronze-dependencies-postgres/1"


class PostgresDependencyStore:
    def __init__(self, bronze):
        self.bronze, self.store = bronze, bronze.store

    def migrate(self):
        store, conn = self.store, self.store.connection
        tables = {"bronze_dependency_state", "bronze_dependency_checks", "bronze_dependencies"}
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
            if not {"bronze_captures", "bronze_schema_migrations"}.issubset(existing):
                raise ValueError("FOMC dependency storage requires the owned Bronze migration")
            migrations = {
                r[0]
                for r in conn.execute(
                    store._query("SELECT migration_id FROM {schema}.bronze_schema_migrations")
                )
            }
            if MIGRATION not in migrations:
                raise ValueError("FOMC dependency storage requires the owned Bronze migration")
            if existing & tables:
                if existing & tables != tables:
                    raise ValueError("incomplete FOMC dependency schema")
                marker = conn.execute(
                    store._query("SELECT contract_id FROM {schema}.bronze_dependency_state")
                ).fetchall()
                if marker != [(CONTRACT,)]:
                    raise ValueError("FOMC dependency schema identity mismatch")
                return
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_dependency_state (singleton boolean PRIMARY"
                    " KEY CHECK(singleton),contract_id text NOT NULL)"
                )
            )
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_dependency_checks (check_id text PRIMARY "
                    "KEY,parent_capture_id uuid NOT NULL REFERENCES "
                    "{schema}.bronze_captures(capture_id),evidence jsonb NOT NULL)"
                )
            )
            conn.execute(
                store._query(
                    "CREATE INDEX ON {schema}.bronze_dependency_checks (parent_capture_id)"
                )
            )
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_dependencies (check_id text NOT NULL "
                    "REFERENCES {schema}.bronze_dependency_checks(check_id),ordinal integer "
                    "NOT NULL,child_capture_id uuid NOT NULL REFERENCES "
                    "{schema}.bronze_captures(capture_id),relation text NOT NULL,href_raw "
                    "text NOT NULL,label_raw text NOT NULL,PRIMARY KEY(check_id,ordinal))"
                )
            )
            conn.execute(
                store._query("CREATE INDEX ON {schema}.bronze_dependencies (child_capture_id)")
            )
            conn.execute(
                store._query("INSERT INTO {schema}.bronze_dependency_state VALUES(true,%s)"),
                (CONTRACT,),
            )

    def _verified(self, capture_id, manifest, root):
        receipt, payload = self.bronze.read_capture(capture_id)
        marker = Path(root) / "captures" / _identity(capture_id) / "committed.json"
        if not marker.is_file() or read_capture(root, capture_id) != (receipt, payload):
            raise ValueError("FOMC dependencies require matching committed filesystem evidence")
        if (
            not receipt.get("accepted")
            or receipt.get("manifest_sha256")
            != hashlib.sha256(_json(manifest.to_dict())).hexdigest()
        ):
            raise ValueError("FOMC dependencies require accepted matching manifest captures")
        return receipt, payload

    def commit(self, parent_capture_id, child_capture_ids, manifest, *, root):
        if (
            not isinstance(child_capture_ids, (list, tuple))
            or len(child_capture_ids) > 1000
            or len(set(child_capture_ids)) != len(child_capture_ids)
        ):
            raise ValueError("FOMC dependency capture set must contain distinct bounded IDs")
        receipt, payload = self._verified(parent_capture_id, manifest, root)
        if receipt["kind"] != "document":
            raise ValueError("FOMC dependency parent must be an HTML document")
        result = discover_html_dependencies(payload, receipt["source_url"], manifest)
        if result.issues:
            raise ValueError("FOMC dependencies have unresolved discovery issues")
        children = {}
        for identity in child_capture_ids:
            child, body = self._verified(identity, manifest, root)
            if child["source_url"] in children:
                raise ValueError("FOMC dependency check requires one capture per exact URL")
            children[child["source_url"]] = (identity, child, body)
        if set(children) != {d.source_url for d in result.dependencies}:
            raise ValueError("FOMC captures do not match expected dependencies")
        links = []
        for dependency in result.dependencies:
            identity, child, body = children[dependency.source_url]
            if child["kind"] != dependency.kind:
                raise ValueError("FOMC dependency capture kind mismatch")
            if dependency.kind == "asset" and not body.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ValueError("FOMC dependency image lacks the reviewed PNG signature")
            if dependency.kind == "document":
                if dependency.source_url.lower().endswith(".pdf"):
                    if not body.startswith(b"%PDF-") or not body.rstrip().endswith(b"%%EOF"):
                        raise ValueError("FOMC dependency lacks the expected PDF container framing")
                else:
                    child_discovery = discover_html_dependencies(
                        body, child["source_url"], manifest
                    )
                    if child_discovery.issues:
                        raise ValueError("FOMC child HTML has unresolved dependency issues")
            links.append(
                {**asdict(dependency), "child_capture_id": identity, "sha256": child["sha256"]}
            )
        evidence = {
            "schema_id": "fomc-bronze-dependency-check/1",
            "coverage_kind": "direct_html_dependencies",
            "parent_capture_id": parent_capture_id,
            "parent_sha256": receipt["sha256"],
            "discovery_profile_id": result.profile_id,
            "dependencies": links,
            "embedded_references": list(result.embedded_references),
        }
        check_id = hashlib.sha256(_json(evidence)).hexdigest()
        with self.store.connection.transaction():
            self.store.connection.execute(
                self.store._query(
                    "INSERT INTO {schema}.bronze_dependency_checks "
                    "(check_id,parent_capture_id,evidence) VALUES(%s,%s,%s) ON CONFLICT DO "
                    "NOTHING"
                ),
                (check_id, parent_capture_id, self.store._Jsonb(evidence)),
            )
            for ordinal, link in enumerate(links):
                self.store.connection.execute(
                    self.store._query(
                        "INSERT INTO {schema}.bronze_dependencies "
                        "(check_id,ordinal,child_capture_id,relation,href_raw,label_raw) "
                        "VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                    ),
                    (
                        check_id,
                        ordinal,
                        link["child_capture_id"],
                        link["relation"],
                        link["href_raw"],
                        link["label_raw"],
                    ),
                )
            if self.read_check(check_id) != evidence:
                raise ValueError("FOMC dependency check has conflicting evidence")
        return check_id

    def read_check(self, check_id):
        row = self.store.connection.execute(
            self.store._query(
                "SELECT parent_capture_id,evidence FROM {schema}.bronze_dependency_checks"
                " WHERE check_id=%s"
            ),
            (check_id,),
        ).fetchone()
        if row is None:
            return None
        evidence = row[1]
        if hashlib.sha256(_json(evidence)).hexdigest() != check_id or str(row[0]) != evidence.get(
            "parent_capture_id"
        ):
            raise ValueError("FOMC dependency evidence hash mismatch")
        links = self.store.connection.execute(
            self.store._query(
                "SELECT ordinal,child_capture_id,relation,href_raw,label_raw FROM "
                "{schema}.bronze_dependencies WHERE check_id=%s ORDER BY ordinal"
            ),
            (check_id,),
        ).fetchall()
        expected = [
            (i, d["child_capture_id"], d["relation"], d["href_raw"], d["label_raw"])
            for i, d in enumerate(evidence["dependencies"])
        ]
        if [(r[0], str(r[1]), *r[2:]) for r in links] != expected:
            raise ValueError("FOMC dependency link evidence mismatch")
        return evidence

    def meeting_associations(self, capture_id):
        """Resolve inherited inventory associations, including bundled and nested addenda."""
        self.bronze.read_capture(capture_id)
        rows = self.store.connection.execute(
            self.store._query(
                "WITH RECURSIVE parents(capture_id) AS (SELECT %s::uuid UNION "
                "SELECT c.parent_capture_id FROM {schema}.bronze_dependency_checks c "
                "JOIN {schema}.bronze_dependencies d ON d.check_id=c.check_id "
                "JOIN parents p ON p.capture_id=d.child_capture_id) "
                "SELECT DISTINCT l.run_id,l.document_id,e.meeting_id,e.source_page_url "
                "FROM parents p JOIN {schema}.bronze_document_links l ON l.capture_id=p.capture_id "
                "JOIN {schema}.inventory_entries e ON e.run_id=l.run_id AND "
                "e.document_id=l.document_id "
                "ORDER BY l.run_id,l.document_id,e.meeting_id,e.source_page_url"
            ),
            (_identity(capture_id),),
        ).fetchall()
        return [
            dict(
                zip(
                    ("run_id", "document_id", "meeting_id", "source_page_url"),
                    (str(r[0]), *r[1:]),
                    strict=True,
                )
            )
            for r in rows
        ]
