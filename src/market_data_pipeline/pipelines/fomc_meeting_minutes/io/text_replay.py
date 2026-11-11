"""FOMC-AC05/12: empty-target replay of verified stored outputs; no extraction/HTTP."""

import os
from datetime import date, datetime
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import (
    DocumentRef,
    Inventory,
    InventoryEntry,
    MeetingRef,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
    _write,
    commit_capture,
    read_capture,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import (
    make_directory,
    store_payload,
)


def _inventory(value):
    return Inventory(
        value["schema_id"],
        tuple(
            MeetingRef(
                **{**m, "meeting_dates": tuple(date.fromisoformat(d) for d in m["meeting_dates"])}
            )
            for m in value["meetings"]
        ),
        tuple(DocumentRef(**d) for d in value["documents"]),
        tuple(
            InventoryEntry(
                **{
                    **e,
                    "provider_release_date": date.fromisoformat(e["provider_release_date"])
                    if e["provider_release_date"]
                    else None,
                }
            )
            for e in value["entries"]
        ),
        tuple(value["source_pages"]),
    )


def replay_text_catalog(source, target, manifest, *, source_root, target_root, catalog_id=None):
    """Caller supplies an initialized, isolated empty target FOMC store/root.

    Preserve all retained source capture receipts and dependency evidence, including
    nonselected legacy evidence, while replaying the explicitly selected catalog.
    """
    source_root, target_root = Path(source_root).resolve(), Path(target_root).resolve()
    source_target = tuple(
        getattr(source.store.config, k) for k in ("host", "port", "database", "schema")
    )
    replay_target = tuple(
        getattr(target.store.config, k) for k in ("host", "port", "database", "schema")
    )
    if (
        source_root == target_root
        or source_target == replay_target
        or target_root.is_relative_to(source_root)
        or source_root.is_relative_to(target_root)
    ):
        raise ValueError("FOMC replay requires independent empty target")
    if target_root.exists() and any(target_root.iterdir()):
        raise ValueError("FOMC replay target root must be empty")
    s, t = source.store, target.store
    if (
        t.run_count()
        or t.connection.execute(
            t._query("SELECT count(*) FROM {schema}.bronze_captures")
        ).fetchone()[0]
    ):
        raise ValueError("FOMC replay target database must be empty")
    value = source.read(manifest, root=source_root, catalog_id=catalog_id)
    if value is None:
        raise ValueError("FOMC replay requires a committed source catalog")
    run_id = value["inventory_run_id"]
    row = s.connection.execute(
        s._query("SELECT input_kind FROM {schema}.inventory_runs WHERE run_id=%s"), (run_id,)
    ).fetchone()
    t.save_inventory(_inventory(value["inventory"]), manifest, input_kind=row[0], run_id=run_id)
    for (capture_id,) in s.connection.execute(
        s._query("SELECT capture_id FROM {schema}.bronze_captures ORDER BY capture_id")
    ):
        capture_id = str(capture_id)
        receipt, payload = source.text.bronze.read_capture(capture_id)
        if (
            read_capture(source_root, capture_id) != (receipt, payload)
            or not (source_root / "captures" / capture_id / "committed.json").is_file()
        ):
            raise ValueError("FOMC replay original mirror mismatch")
        directory = target_root / "captures" / capture_id
        make_directory(directory)
        _write(
            directory / "receipt.json",
            (source_root / "captures" / capture_id / "receipt.json").read_bytes(),
        )
        os.link(store_payload(target_root, payload), directory / "payload.bin")
        commit_capture(target_root, capture_id, target.text.bronze)
    inventory_links = s.connection.execute(
        s._query("SELECT capture_id FROM {schema}.bronze_inventory_links WHERE run_id=%s"),
        (run_id,),
    ).fetchall()
    for (capture_id,) in inventory_links:
        target.text.bronze.link_inventory(str(capture_id), run_id)
    discovery = source.text.bronze.read_inventory_discovery(run_id)
    if discovery is not None:
        from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest

        t.connection.execute(
            t._query("INSERT INTO {schema}.bronze_inventory_discovery VALUES(%s,%s,%s)"),
            (run_id, t._Jsonb(discovery), digest(discovery)),
        )
    links = s.connection.execute(
        s._query(
            "SELECT capture_id,document_id FROM {schema}.bronze_document_links WHERE run_id=%s"
        ),
        (run_id,),
    ).fetchall()
    for capture_id, document_id in links:
        target.text.bronze.link_document(str(capture_id), run_id, document_id)
    for (check_id,) in s.connection.execute(
        s._query("SELECT check_id FROM {schema}.bronze_dependency_checks ORDER BY check_id")
    ):
        check = source.text.dependencies.read_check(check_id)
        replayed = target.text.dependencies.commit(
            check["parent_capture_id"],
            [d["child_capture_id"] for d in check["dependencies"]],
            manifest,
            root=target_root,
        )
        if replayed != check_id:
            raise ValueError("FOMC replay dependency evidence mismatch")
    for binding in value["bindings"]:
        result = source.text.read_extraction(binding["extraction_id"], root=source_root)
        target.text.save_result(binding["capture_id"], result, manifest, root=target_root)
    replayed = target.publish(
        value["bindings"],
        manifest,
        root=target_root,
        inventory_run_id=run_id,
        start_year=value["scope_start_year"],
        as_of=datetime.fromisoformat(value["as_of"].replace("Z", "+00:00"))
        if value["as_of"]
        else None,
    )
    if replayed != value:
        raise ValueError("FOMC replay catalog mismatch")
    return replayed
