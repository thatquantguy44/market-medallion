"""SSV-AC10/C07: explicit local typed publication with crash recovery.

Only current.json selects a generation. COMMITTED catalog facts must be finished
from their staged bytes before another generation is collected. No HTTP or
operational schema migration occurs here.
"""

import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    CONTRACT,
    DEFAULT_BYTES,
    DEFAULT_ROWS,
    FIELDS,
    fingerprint,
    limits,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io import catalog, parquet, snapshot
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
    _admit,
    _clock,
    _identity,
    _json,
    _publish_directory,
    _read,
    _sync,
    _write,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.warehouse import ingestion_lock
from market_data_pipeline.pipelines.finra_short_sale_volume.research import validated_request

FILES = {"owner.json", "manifest.json"} | {name + ".parquet" for name in FIELDS}
RECORD_KEYS = {
    "contract",
    "generation_id",
    "ordinal",
    "previous_id",
    "generated_at",
    "request",
    "research",
    "source_warehouse_id",
    "source_state_sha256",
    "files",
    "schemas",
}


def _gate(root, manifest, approved_use, operation, max_rows, max_bytes):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    limits(max_rows, max_bytes)
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("FINRA publication requires an existing safe source root")
    return root


def _owner(root, generation_id):
    return dict(
        contract=CONTRACT,
        generation_id=_identity(generation_id),
        catalog_id=_json(root / "parquet/identity.json")["catalog_id"],
    )


def _pointer(record):
    return dict(
        contract=CONTRACT,
        generation_id=record["generation_id"],
        ordinal=record["ordinal"],
        manifest_sha256=record["manifest_sha256"],
    )


def _head(root, db):
    pending = catalog.pending(db)
    if len(pending) > 1:
        raise ValueError("FINRA publication has multiple pending generations")
    latest = catalog.latest(db)
    path = root / "parquet/current.json"
    if not path.exists() and not path.is_symlink():
        if latest:
            raise ValueError("FINRA published pointer is missing")
        return None
    pointer = _json(path)
    generation_id = _identity(pointer.get("generation_id"))
    record = catalog.get(db, generation_id)
    if record is None or pointer != _pointer(record):
        raise ValueError("FINRA publication pointer does not match the catalog")
    if record["state"] == "PUBLISHED":
        if latest is None or record["generation_id"] != latest["generation_id"]:
            raise ValueError("FINRA publication pointer regressed")
    elif (
        not pending
        or record != pending[0]
        or record["previous_id"] != (latest["generation_id"] if latest else None)
    ):
        raise ValueError("FINRA publication pointer has an invalid pending generation")
    return record


def _verify(root, db, record, directory, max_rows, max_bytes):
    """Compare every typed file to immutable catalog facts; never recollect source."""
    try:
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError
        if {p.name for p in directory.iterdir()} != FILES:
            raise ValueError
        if _json(directory / "owner.json") != _owner(root, record["generation_id"]):
            raise ValueError
        manifest = json.loads(_read(directory / "manifest.json", catalog.MAX_MANIFEST_BYTES))
        if encoded(manifest) != _read(directory / "manifest.json", catalog.MAX_MANIFEST_BYTES):
            raise ValueError
        if manifest != record["manifest"] or digest(manifest) != record["manifest_sha256"]:
            raise ValueError
        if (
            set(manifest) != RECORD_KEYS
            or manifest["contract"] != CONTRACT
            or manifest["schemas"] != {n: fingerprint(n) for n in FIELDS}
            or manifest["source_warehouse_id"]
            != _json(root / "parquet.json")["source_warehouse_id"]
            or set(manifest["files"]) != set(FIELDS)
        ):
            raise ValueError
        receipts = manifest["files"]
        for key in ("rows", "bytes", "arrow_bytes"):
            values = [receipts[n][key] for n in FIELDS]
            if any(type(v) is not int or v < 0 for v in values):
                raise ValueError
            if sum(values) > (max_rows if key == "rows" else max_bytes):
                raise ValueError("FINRA publication row/byte budget exceeded")
        frozen = catalog.rows(db, record["generation_id"], max_rows=max_rows, max_bytes=max_bytes)
        tables = {
            name: parquet.read_table(
                directory / (name + ".parquet"),
                name,
                receipts[name],
                max_rows=max_rows,
                max_bytes=max_bytes,
            )
            for name in FIELDS
        }
        if tables != frozen:
            raise ValueError
        projected = dict(
            request=manifest["request"],
            research=manifest["research"],
            source_warehouse_id=manifest["source_warehouse_id"],
            tables=tables,
        )
        if digest(projected) != manifest["source_state_sha256"]:
            raise ValueError
        return tables
    except (KeyError, TypeError, UnicodeError, RecursionError):
        raise ValueError(
            "FINRA publication artifacts do not match their frozen generation"
        ) from None
    except ValueError as error:
        if "budget" in str(error):
            raise
        raise ValueError(
            "FINRA publication artifacts do not match their frozen generation"
        ) from None


def _summary(db, record):
    return dict(
        contract=CONTRACT,
        generation_id=record["generation_id"],
        ordinal=record["ordinal"],
        state=record["state"],
        published_at=record["published_at"],
        recovery_required=bool(catalog.pending(db)),
        request=record["manifest"]["request"],
        research=record["manifest"]["research"],
        row_counts={n: v["rows"] for n, v in record["manifest"]["files"].items()},
        manifest_sha256=record["manifest_sha256"],
    )


def _select(root, record):
    path = root / "parquet/current.json"
    stage = path.parent / (".pointer-" + uuid4().hex)
    try:
        _write(stage, encoded(_pointer(record)))
        os.replace(stage, path)
        _sync(path.parent)
    finally:
        stage.unlink(missing_ok=True)


def _orphans(root, db):
    base = root / "parquet/.staging"
    directories = list(base.iterdir())
    if len(directories) > catalog.MAX_GENERATIONS:
        raise ValueError("FINRA publication staging history exceeds its bound")
    for directory in directories:
        generation_id = _identity(directory.name)
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError("FINRA publication staging directory is unsafe")
        if _json(directory / "owner.json") != _owner(root, generation_id):
            raise ValueError("FINRA publication staging owner mismatch")
        if catalog.get(db, generation_id) is not None:
            continue
        children = list(directory.iterdir())
        if any(p.name not in FILES or p.is_symlink() or not p.is_file() for p in children):
            raise ValueError("FINRA uncommitted stage contains unexpected artifacts")
        shutil.rmtree(directory)
        _sync(base)


def _finish(root, db, record, max_rows, max_bytes, clock, fault):
    generation_id = record["generation_id"]
    stage = root / "parquet/.staging" / generation_id
    final = root / "parquet/snapshots" / generation_id
    if final.exists() or final.is_symlink():
        if stage.exists() or stage.is_symlink():
            raise ValueError("FINRA publication has conflicting staged and final artifacts")
        _verify(root, db, record, final, max_rows, max_bytes)
    else:
        _verify(root, db, record, stage, max_rows, max_bytes)
        os.rename(stage, final)
        _sync(final.parent)
        _sync(stage.parent)
        _verify(root, db, record, final, max_rows, max_bytes)
    fault("finalized")
    _select(root, record)
    fault("pointer")
    fault("before_mark")
    catalog.mark(db, generation_id, _clock(clock()))
    fault("published")
    return catalog.get(db, generation_id)


def _recover(root, db, max_rows, max_bytes, clock, fault):
    head = _head(root, db)
    if head:
        _verify(
            root, db, head, root / "parquet/snapshots" / head["generation_id"], max_rows, max_bytes
        )
    _orphans(root, db)
    pending = catalog.pending(db)
    if pending:
        latest = catalog.latest(db)
        if pending[0]["previous_id"] != (latest["generation_id"] if latest else None):
            raise ValueError("FINRA pending publication chain mismatch")
        head = _finish(root, db, pending[0], max_rows, max_bytes, clock, fault)
    return head


def publish(
    root,
    manifest,
    *,
    approved_use,
    operation,
    trade_date=None,
    scopes=None,
    as_of=None,
    as_of_sequence=None,
    lookback_days=366,
    max_symbols=15000,
    max_rows=DEFAULT_ROWS,
    max_bytes=DEFAULT_BYTES,
    generation_id=None,
    clock=lambda: datetime.now(UTC),
    _fault=lambda at: None,
):
    root = _gate(root, manifest, approved_use, operation, max_rows, max_bytes)
    # Validate the entire request before recovery or filesystem changes.
    request = dict(
        trade_date=trade_date,
        scopes=scopes,
        as_of=as_of,
        as_of_sequence=as_of_sequence,
        lookback_days=lookback_days,
        max_symbols=max_symbols,
    )
    validated_request(manifest, **request)
    if generation_id is not None:
        _identity(generation_id)
    shared = dict(approved_use=approved_use, operation=operation)
    with ingestion_lock(root):
        # The source is verified before initializing the publication catalog.
        if not (root / "parquet").exists() and not (root / "parquet").is_symlink():
            projected = snapshot.collect(
                root, manifest, **shared, **request, max_rows=max_rows, max_bytes=max_bytes
            )
        else:
            projected = None
        with catalog.connect(root, manifest, **shared, create=True) as db:
            head = _recover(root, db, max_rows, max_bytes, clock, _fault)
            if projected is None:
                projected = snapshot.collect(
                    root, manifest, **shared, **request, max_rows=max_rows, max_bytes=max_bytes
                )
            existing = catalog.get(db, generation_id) if generation_id is not None else head
            if (
                existing
                and existing["manifest"]["source_state_sha256"] == projected["state_sha256"]
            ):
                if head is None or existing["generation_id"] != head["generation_id"]:
                    raise ValueError("FINRA publication cannot repoint an older generation")
                return _summary(db, existing)
            if existing and generation_id is not None:
                raise ValueError("FINRA publication generation ID conflicts with frozen input")
            generation_id = generation_id or str(uuid4())
            stage = root / "parquet/.staging" / generation_id
            if (root / "parquet/snapshots" / generation_id).exists():
                raise ValueError("FINRA publication final generation already exists")
            _publish_directory(
                stage.parent, generation_id, {"owner.json": encoded(_owner(root, generation_id))}
            )
            try:
                receipts = {
                    name: parquet.write_table(
                        stage / (name + ".parquet"),
                        name,
                        values,
                        max_rows=max_rows,
                        max_bytes=max_bytes,
                    )
                    for name, values in projected["tables"].items()
                }
                for key in ("bytes", "arrow_bytes"):
                    if sum(r[key] for r in receipts.values()) > max_bytes:
                        raise ValueError("FINRA publication file/Arrow byte budget exceeded")
                record = dict(
                    contract=CONTRACT,
                    generation_id=generation_id,
                    ordinal=catalog.ordinal(db),
                    previous_id=head["generation_id"] if head else None,
                    generated_at=_clock(clock()),
                    request=projected["request"],
                    research=projected["research"],
                    source_warehouse_id=projected["source_warehouse_id"],
                    source_state_sha256=projected["state_sha256"],
                    files=receipts,
                    schemas={n: fingerprint(n) for n in FIELDS},
                )
                catalog.document(encoded(record).decode())
                _write(stage / "manifest.json", encoded(record))
                _sync(stage)
                _fault("staged")
                catalog.commit(
                    db, record, projected["tables"], before_commit=lambda: _fault("before_commit")
                )
                _fault("committed")
                committed = catalog.get(db, generation_id)
                _verify(root, db, committed, stage, max_rows, max_bytes)
                result = _finish(root, db, committed, max_rows, max_bytes, clock, _fault)
                return _summary(db, result)
            finally:
                # Query durable state: a commit may succeed before its caller fails.
                if catalog.get(db, generation_id) is None and stage.exists():
                    _orphans(root, db)


def read(
    root,
    manifest,
    *,
    approved_use,
    operation,
    table=None,
    max_rows=DEFAULT_ROWS,
    max_bytes=DEFAULT_BYTES,
):
    root = _gate(root, manifest, approved_use, operation, max_rows, max_bytes)
    if table is not None and table not in FIELDS:
        raise ValueError("unknown FINRA snapshot table")
    with ingestion_lock(root):
        with catalog.connect(root, manifest, approved_use=approved_use, operation=operation) as db:
            head = _head(root, db)
            if head is None:
                raise ValueError("FINRA has no published snapshot")
            tables = _verify(
                root,
                db,
                head,
                root / "parquet/snapshots" / head["generation_id"],
                max_rows,
                max_bytes,
            )
            return dict(_summary(db, head), tables={table: tables[table]} if table else tables)


def recover(
    root,
    manifest,
    *,
    approved_use,
    operation,
    max_rows=DEFAULT_ROWS,
    max_bytes=DEFAULT_BYTES,
    clock=lambda: datetime.now(UTC),
    _fault=lambda at: None,
):
    root = _gate(root, manifest, approved_use, operation, max_rows, max_bytes)
    with ingestion_lock(root):
        with catalog.connect(
            root, manifest, approved_use=approved_use, operation=operation, create=True
        ) as db:
            head = _recover(root, db, max_rows, max_bytes, clock, _fault)
            return (
                _summary(db, head)
                if head
                else dict(state="NO_PUBLICATION", recovery_required=False)
            )
