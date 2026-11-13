"""FOMC-QAC01/04: owned verified snapshots and immutable offline report output."""

from contextlib import contextmanager
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality import (
    build_report,
    validate_report,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import encoded
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
    PostgresBronzeStore,
    _write,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.catalog import PostgresTextCatalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunk_catalog import (
    PostgresChunkCatalog,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest


@contextmanager
def source_snapshot(store):
    lock = "fomc-operation:" + store.config.schema
    conn = store.connection
    if not conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (lock,)).fetchone()[
        0
    ]:
        raise ValueError("another FOMC operation is already running")
    try:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            yield
    finally:
        conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (lock,))


def read_quality_inputs(cfg, store, *, report=None):
    """Read verified inputs within source_snapshot; an old report pins old generations."""
    pointers = (
        OperationStore(store).pointers()
        if report is None
        else {
            "inventory": report["inventory_run_id"],
            "text": report["text_catalog_id"],
            "chunk": report["chunk_catalog_id"],
        }
    )
    if not all(pointers.values()):
        raise ValueError("FOMC quality requires committed inventory/text/chunk catalogs")
    manifest = load_manifest(cfg.manifest)
    text = PostgresTextStore(PostgresBronzeStore(store))
    chunks = PostgresChunkStore(text)
    generation = PostgresChunkCatalog(chunks).read(
        manifest, root=cfg.root, catalog_id=pointers["chunk"]
    )
    if (
        generation["text_catalog_id"] != pointers["text"]
        or generation["inventory_run_id"] != pointers["inventory"]
    ):
        raise ValueError("FOMC quality catalog pointers are incoherent")
    base = PostgresTextCatalog(text).read(manifest, root=cfg.root, catalog_id=pointers["text"])
    inventory = store.read_inventory(pointers["inventory"])
    extractions = {
        s["extraction_id"]: text.read_extraction(s["extraction_id"], root=cfg.root)
        for s in generation["selections"]
    }
    sets = [chunks.read_set(s["set_id"], root=cfg.root) for s in generation["selections"]]
    if report is None and OperationStore(store).pointers() != pointers:
        raise ValueError("FOMC quality catalog pointers changed during read")
    return inventory, base, generation, extractions, sets


def quality_report(cfg, *, inventory_store):
    with source_snapshot(inventory_store):
        return build_report(*read_quality_inputs(cfg, inventory_store))


def write_quality_bytes(cfg, path, body):
    """Exclusive content write outside canonical inputs; identical retries are safe."""
    path = Path(path).resolve()
    protected = [
        Path(cfg.root).resolve() / name
        for name in (
            "captures",
            "objects",
            "text",
            "operations",
            "attempts",
            "attempt_outcomes",
            "backups",
        )
    ]
    if path == Path(cfg.root).resolve() or any(path.is_relative_to(p) for p in protected):
        raise ValueError("FOMC quality output overlaps canonical source state")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not isinstance(body, bytes):
        raise ValueError("FOMC quality output requires bytes")
    try:
        _write(path, body)
    except FileExistsError:
        if path.read_bytes() != body:
            raise ValueError("FOMC quality output conflict") from None
    return path


def write_quality_artifact(cfg, path, value):
    return write_quality_bytes(cfg, path, encoded(value))


def write_report(cfg, path, value):
    return write_quality_artifact(cfg, path, validate_report(value))
