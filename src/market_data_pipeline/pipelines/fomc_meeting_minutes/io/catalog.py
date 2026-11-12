"""FOMC-AC05: verified generation reads and atomic PostgreSQL catalog selection."""

from contextlib import nullcontext
from datetime import datetime

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.catalog import (
    build_catalog,
    validate_catalog,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import (
    CONTRACT,
    artifact_path,
    read_artifact,
    write_artifact,
)
from market_data_pipeline.shared.files import writer_lock


class PostgresTextCatalog:
    def __init__(self, text):
        self.text, self.store = text, text.store

    def _records(self, bindings, run_id, manifest, root):
        result = []
        for binding in bindings:
            capture_id, extraction_id = binding["capture_id"], binding["extraction_id"]
            receipt, payload = self.text.dependencies._verified(capture_id, manifest, root)
            extraction = self.text.read_extraction(extraction_id, root=root)
            if extraction["status"] != "failed":
                self.text.verify_dependencies(receipt, payload, manifest, root=root)
            if not self.store.connection.execute(
                self.store._query(
                    "SELECT 1 FROM {schema}.text_capture_links "
                    "WHERE capture_id=%s AND extraction_id=%s"
                ),
                (capture_id, extraction_id),
            ).fetchone():
                raise ValueError("FOMC catalog requires committed text/capture links")
            meetings = sorted(
                {
                    r["meeting_id"]
                    for r in self.text.dependencies.meeting_associations(capture_id)
                    if r["run_id"] == run_id
                }
            )
            if "meeting_ids" in binding and binding["meeting_ids"] != meetings:
                raise ValueError("FOMC catalog inherited meeting associations mismatch")
            result.append({"receipt": receipt, "extraction": extraction, "meeting_ids": meetings})
        return result

    def publish(
        self,
        bindings,
        manifest,
        *,
        root,
        inventory_run_id,
        start_year=1990,
        as_of=None,
        _locked=False,
    ):
        if not isinstance(bindings, (list, tuple)) or not 1 <= len(bindings) <= 10000:
            raise ValueError("FOMC catalog requires a bounded explicit extraction set")
        store, conn = self.store, self.store.connection
        with nullcontext() if _locked else writer_lock(root), conn.transaction():
            state = conn.execute(
                store._query(
                    "SELECT contract_id,current_catalog_id FROM {schema}.text_state FOR UPDATE"
                )
            ).fetchone()
            if not state or state[0] != CONTRACT:
                raise ValueError("FOMC text catalog identity mismatch")
            inventory = store.read_inventory(inventory_run_id)
            if inventory is None:
                raise ValueError("FOMC catalog requires an owned inventory snapshot")
            records = self._records(bindings, inventory_run_id, manifest, root)
            value = build_catalog(
                inventory,
                records,
                inventory_run_id=inventory_run_id,
                start_year=start_year,
                as_of=as_of,
            )
            if not value["capture_ids"] or any(
                e["status"] == "failed"
                for rep in value["representations"]
                for v in rep["versions"]
                for e in v["extractions"]
            ):
                raise ValueError("FOMC failed extraction cannot publish a successful catalog")
            identity = value["catalog_id"]
            write_artifact(root, "catalogs", identity, value)
            existing = conn.execute(
                store._query("SELECT catalog_json FROM {schema}.text_catalogs WHERE catalog_id=%s"),
                (identity,),
            ).fetchone()
            if existing:
                if self.read(manifest, root=root, catalog_id=identity) != value:
                    raise ValueError("FOMC catalog retry mismatch")
                return value
            conn.execute(
                store._query(
                    "INSERT INTO {schema}.text_catalogs(catalog_id,inventory_run_id,catalog_json) "
                    "VALUES(%s,%s,%s)"
                ),
                (identity, inventory_run_id, store._Jsonb(value)),
            )
            self.read(manifest, root=root, catalog_id=identity)
            conn.execute(
                store._query(
                    "UPDATE {schema}.text_state SET current_catalog_id=%s WHERE singleton"
                ),
                (identity,),
            )
            return value

    def read(self, manifest, *, root, catalog_id=None):
        store, conn = self.store, self.store.connection
        if catalog_id is None:
            state = conn.execute(
                store._query(
                    "SELECT contract_id,current_catalog_id FROM {schema}.text_state WHERE singleton"
                )
            ).fetchone()
            if not state or state[0] != CONTRACT:
                raise ValueError("FOMC text catalog identity mismatch")
            catalog_id = state[1]
            if catalog_id is None:
                return None
        artifact_path(root, "catalogs", catalog_id)
        row = conn.execute(
            store._query(
                "SELECT catalog_json,inventory_run_id FROM {schema}.text_catalogs "
                "WHERE catalog_id=%s"
            ),
            (catalog_id,),
        ).fetchone()
        if row is None:
            raise ValueError("FOMC catalog database mirror is missing")
        value = validate_catalog(row[0])
        if value["catalog_id"] != catalog_id or str(row[1]) != value["inventory_run_id"]:
            raise ValueError("FOMC catalog identity mismatch")
        if read_artifact(root, "catalogs", catalog_id) != value:
            raise ValueError("FOMC catalog filesystem mirror mismatch")
        inventory = store.read_inventory(value["inventory_run_id"])
        records = self._records(value["bindings"], value["inventory_run_id"], manifest, root)
        rebuilt = build_catalog(
            inventory,
            records,
            inventory_run_id=value["inventory_run_id"],
            start_year=value["scope_start_year"],
            as_of=datetime.fromisoformat(value["as_of"].replace("Z", "+00:00"))
            if value["as_of"]
            else None,
        )
        if rebuilt != value:
            raise ValueError("FOMC catalog original evidence mismatch")
        return value
