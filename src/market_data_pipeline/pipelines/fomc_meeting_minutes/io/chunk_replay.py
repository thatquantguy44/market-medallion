"""FOMC-AC12: empty-target chunk replay using retained source/text/chunk artifacts."""

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import CONTRACT
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text_replay import replay_text_catalog


def replay_chunk_catalog(source, target, manifest, *, source_root, target_root, catalog_id=None):
    """No HTTP, extraction, tokenization or chunk generation. Keep all source clocks."""
    store = target.store
    marker = store.connection.execute(
        store._query("SELECT contract_id,current_catalog_id FROM {schema}.chunk_state")
    ).fetchall()
    if marker != [(CONTRACT, None)] or any(
        store.connection.execute(store._query("SELECT count(*) FROM {schema}." + table)).fetchone()[
            0
        ]
        for table in ("chunk_sets", "chunks", "chunk_catalogs")
    ):
        raise ValueError("FOMC chunk replay requires an initialized empty target")
    value = source.read(manifest, root=source_root, catalog_id=catalog_id)
    if value is None:
        raise ValueError("FOMC chunk replay requires a committed source generation")
    replay_text_catalog(
        source.text_catalog,
        target.text_catalog,
        manifest,
        source_root=source_root,
        target_root=target_root,
        catalog_id=value["text_catalog_id"],
    )
    for selection in value["selections"]:
        result = source.chunks.read_set(selection["set_id"], root=source_root)
        target.chunks.save_set(result, root=target_root)
    replayed = target.commit_sets(
        [s["set_id"] for s in value["selections"]],
        manifest,
        root=target_root,
        text_catalog_id=value["text_catalog_id"],
    )
    if replayed != value:
        raise ValueError("FOMC chunk replay generation mismatch")
    return replayed
