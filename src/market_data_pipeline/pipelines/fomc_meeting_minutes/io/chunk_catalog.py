"""FOMC-AC07/12: atomic chunk-generation selection and citation-aware reads."""

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunk_catalog import (
    build_generation,
    validate_generation,
    versions,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.catalog import PostgresTextCatalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import CONTRACT
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import (
    artifact_path,
    read_artifact,
    write_artifact,
)
from market_data_pipeline.shared.files import writer_lock


class PostgresChunkCatalog:
    def __init__(self, chunks):
        self.chunks, self.store = chunks, chunks.store
        self.text_catalog = PostgresTextCatalog(chunks.text)

    def publish(self, manifest, *, root, text_catalog_id=None, selections=None, **options):
        base = self.text_catalog.read(manifest, root=root, catalog_id=text_catalog_id)
        if base is None:
            raise ValueError("FOMC chunks require a committed text catalog")
        expected = versions(base)
        if selections is not None and set(selections) != set(expected):
            raise ValueError(
                "FOMC explicit extraction selections must cover every observed version"
            )
        sets = []
        for identity, (_, version) in sorted(expected.items()):
            if selections is None:
                if len(version["extraction_ids"]) != 1:
                    raise ValueError("FOMC multiple extraction profiles require explicit selection")
                extraction_id = version["extraction_ids"][0]
            else:
                extraction_id = selections[identity]
                if extraction_id not in version["extraction_ids"]:
                    raise ValueError("FOMC selected extraction does not belong to source version")
            sets.append(self.chunks.make_set(extraction_id, identity, root=root, **options))
        return self.commit_sets(
            [s["set_id"] for s in sets],
            manifest,
            root=root,
            text_catalog_id=base["catalog_id"],
            require_current_text=text_catalog_id is None,
        )

    def commit_sets(self, set_ids, manifest, *, root, text_catalog_id, require_current_text=False):
        if not isinstance(set_ids, (list, tuple)) or not 1 <= len(set_ids) <= 10000:
            raise ValueError("FOMC chunk generation requires bounded explicit set IDs")
        store, conn = self.store, self.store.connection
        with writer_lock(root), conn.transaction():
            state = conn.execute(
                store._query(
                    "SELECT contract_id,current_catalog_id FROM {schema}.chunk_state FOR UPDATE"
                )
            ).fetchone()
            if not state or state[0] != CONTRACT:
                raise ValueError("FOMC chunk catalog identity mismatch")
            if require_current_text:
                current = conn.execute(
                    store._query(
                        "SELECT current_catalog_id FROM {schema}.text_state "
                        "WHERE singleton FOR SHARE"
                    )
                ).fetchone()
                if current != (text_catalog_id,):
                    raise ValueError("FOMC text catalog changed during chunk generation; retry")
            base = self.text_catalog.read(manifest, root=root, catalog_id=text_catalog_id)
            sets = [self.chunks.read_set(i, root=root) for i in set_ids]
            value = build_generation(base, sets)
            identity = value["catalog_id"]
            write_artifact(root, "chunk_catalogs", identity, value)
            existing = conn.execute(
                store._query("SELECT 1 FROM {schema}.chunk_catalogs WHERE catalog_id=%s"),
                (identity,),
            ).fetchone()
            if existing:
                if self.read(manifest, root=root, catalog_id=identity) != value:
                    raise ValueError("FOMC chunk generation retry mismatch")
                return value
            conn.execute(
                store._query("""INSERT INTO {schema}.chunk_catalogs
                (catalog_id,text_catalog_id,profile_sha256,catalog_json) VALUES(%s,%s,%s,%s)"""),
                (identity, text_catalog_id, value["profile_sha256"], store._Jsonb(value)),
            )
            self.read(manifest, root=root, catalog_id=identity)
            conn.execute(
                store._query(
                    "UPDATE {schema}.chunk_state SET current_catalog_id=%s WHERE singleton"
                ),
                (identity,),
            )
            return value

    def read(self, manifest, *, root, catalog_id=None):
        store, conn = self.store, self.store.connection
        if catalog_id is None:
            state = conn.execute(
                store._query(
                    "SELECT contract_id,current_catalog_id FROM {schema}.chunk_state "
                    "WHERE singleton"
                )
            ).fetchone()
            if not state or state[0] != CONTRACT:
                raise ValueError("FOMC chunk catalog identity mismatch")
            catalog_id = state[1]
            if catalog_id is None:
                return None
        artifact_path(root, "chunk_catalogs", catalog_id)
        row = conn.execute(
            store._query(
                "SELECT text_catalog_id,profile_sha256,catalog_json "
                "FROM {schema}.chunk_catalogs WHERE catalog_id=%s"
            ),
            (catalog_id,),
        ).fetchone()
        if row is None:
            raise ValueError("FOMC chunk catalog database mirror is missing")
        value = validate_generation(row[2])
        if value["catalog_id"] != catalog_id or row[:2] != (
            value["text_catalog_id"],
            value["profile_sha256"],
        ):
            raise ValueError("FOMC chunk generation identity mismatch")
        if read_artifact(root, "chunk_catalogs", catalog_id) != value:
            raise ValueError("FOMC chunk catalog filesystem mirror mismatch")
        base = self.text_catalog.read(manifest, root=root, catalog_id=value["text_catalog_id"])
        sets = [self.chunks.read_set(s["set_id"], root=root) for s in value["selections"]]
        if build_generation(base, sets) != value:
            raise ValueError("FOMC chunk generation input/coverage mismatch")
        return value

    def read_chunks(
        self, manifest, *, root, catalog_id=None, eligible_only=True, current_only=True
    ):
        if type(eligible_only) is not bool or type(current_only) is not bool:
            raise ValueError("FOMC chunk eligibility/version filters require booleans")
        value = self.read(manifest, root=root, catalog_id=catalog_id)
        if value is None:
            return []
        rows = []
        for selection in value["selections"]:
            if current_only and not selection["current"]:
                continue
            result = self.chunks.read_set(selection["set_id"], root=root)
            for chunk in result["chunks"]:
                if eligible_only and not chunk["eligible"]:
                    continue
                rows.append(
                    {
                        **chunk,
                        "meeting_ids": selection["meeting_ids"],
                        "capture_ids": selection["capture_ids"],
                        "first_seen_at": selection["first_seen_at"],
                        "chunk_catalog_id": value["catalog_id"],
                        "text_catalog_id": value["text_catalog_id"],
                    }
                )
        return rows
