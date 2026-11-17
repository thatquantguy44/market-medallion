"""OTC-T06 source-owned SQLite: initial immutable fixture populations only.

T07 owns replacement/revision history. One transaction couples rows, coverage,
selection and terminal success; a separate PENDING attempt survives rollback.
"""

import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from urllib.parse import quote

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import (
    Partition,
    select_partitions,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import _base, _locked, _root
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest, encoded

LEGACY_CONTRACT = "finra-otc-sqlite/1"
CONTRACT = "finra-otc-sqlite/2"
APPLICATION_ID = 0x4F544331
MAX_ROWS = 10000
MAX_DB_BYTES = 256 * 1024 * 1024
_SCHEMA = (
    "CREATE TABLE otc_metadata (contract TEXT NOT NULL)",
    """CREATE TABLE otc_attempts (
       capture_id TEXT PRIMARY KEY, capture_sequence INTEGER NOT NULL UNIQUE,
       week TEXT NOT NULL, product TEXT NOT NULL, tier TEXT NOT NULL,
       status TEXT NOT NULL, reason TEXT, context_json TEXT NOT NULL,
       context_sha256 TEXT NOT NULL)""",
    """CREATE TABLE otc_publications (
       capture_id TEXT PRIMARY KEY REFERENCES otc_attempts(capture_id),
       week TEXT NOT NULL, product TEXT NOT NULL, tier TEXT NOT NULL,
       coverage_json TEXT NOT NULL, rows_sha256 TEXT NOT NULL, publication_sha256 TEXT NOT NULL,
       UNIQUE(week,product,tier))""",
    """CREATE TABLE otc_rows (
       capture_id TEXT NOT NULL REFERENCES otc_attempts(capture_id),
       symbol_raw TEXT NOT NULL, participant_key TEXT NOT NULL, row_json TEXT NOT NULL,
       PRIMARY KEY(capture_id,symbol_raw,participant_key))""",
    "CREATE INDEX otc_attempt_partition ON otc_attempts(week,product,tier,capture_sequence)",
)


def _commit(db):
    db.execute("COMMIT")


def _safe_database(path, *, empty=False):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_DB_BYTES:
                raise ValueError
            header = stream.read(100)
            if not empty and (
                header[:16] != b"SQLite format 3\x00" or header[18:20] != b"\x01\x01"
            ):
                raise ValueError
        for suffix in ("-journal", "-wal", "-shm"):
            extra = path.with_name(path.name + suffix)
            if extra.is_symlink() or (extra.exists() and not extra.is_file()):
                raise ValueError
            if suffix != "-journal" and extra.exists():
                raise ValueError
    except (OSError, ValueError):
        raise ValueError(
            "OTC warehouse is missing, unsafe, oversized or not rollback-journal SQLite"
        ) from None


@contextmanager
def _connection(root, *, write=False, create=False, allow_legacy=False):
    path = root / "otc.sqlite3"
    created = False
    if write and create and not path.exists() and not path.is_symlink():
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        created = True
    _safe_database(path, empty=created)
    db = sqlite3.connect(
        "file:" + quote(str(path), safe="/") + ("?mode=rw" if write else "?mode=ro"),
        uri=True,
        isolation_level=None,
        timeout=5,
    )
    try:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA trusted_schema=OFF")
        if write:
            db.execute("PRAGMA synchronous=EXTRA")
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
            db.execute(f"PRAGMA max_page_count={MAX_DB_BYTES // page_size}")
        if created:
            try:
                db.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA:
                    db.execute(statement)
                from market_data_pipeline.pipelines.finra_otc_transparency.io.history import SCHEMA

                for statement in SCHEMA:
                    db.execute(statement)
                db.execute("INSERT INTO otc_metadata VALUES (?)", (CONTRACT,))
                db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                db.execute("PRAGMA user_version=2")
                db.execute("COMMIT")
            except BaseException:
                db.close()
                path.unlink()
                raise
        version = db.execute("PRAGMA user_version").fetchone()[0]
        contract = [r[0] for r in db.execute("SELECT contract FROM otc_metadata")]
        if db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID or (
            version,
            contract,
        ) not in ((1, [LEGACY_CONTRACT]), (2, [CONTRACT])):
            raise ValueError("OTC warehouse storage identity mismatch")
        if write and version == 1 and not allow_legacy:
            raise ValueError("OTC warehouse requires explicit migrate-warehouse before writing")
        if version == 2:
            from market_data_pipeline.pipelines.finra_otc_transparency.io.migration import (
                verify_legacy,
            )

            verify_legacy(db)
        if not write:
            db.execute("PRAGMA query_only=ON")
        yield db
    finally:
        db.close()


def _record(raw, limit=65536):
    try:
        if not isinstance(raw, str) or len(raw.encode("utf-8")) > limit:
            raise ValueError
        value = json.loads(raw)
        if not isinstance(value, dict) or encoded(value) != raw.encode("utf-8"):
            raise ValueError
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ValueError("OTC warehouse JSON is corrupt, noncanonical or oversized") from None
    return value


def _attempt(row):
    if row is None:
        return None
    value = _record(row["context_json"])
    if (
        row["context_sha256"] != digest(value)
        or value.get("capture_id") != row["capture_id"]
        or value.get("capture_sequence") != row["capture_sequence"]
        or ("captured_at" in row.keys() and value.get("captured_at") != row["captured_at"])
        or (value.get("reporting_week_start"), value.get("product"), value.get("tier"))
        != (row["week"], row["product"], row["tier"])
    ):
        raise ValueError("OTC warehouse attempt context mismatch")
    return {**value, "status": row["status"], "reason": row["reason"]}


def require_current(root):
    from pathlib import Path

    root = Path(root)
    if (root / "otc.sqlite3").exists() or (root / "otc.sqlite3").is_symlink():
        _base(_root(root))
        with _connection(root) as db:
            if db.execute("PRAGMA user_version").fetchone()[0] != 2:
                raise ValueError("OTC warehouse requires explicit migrate-warehouse before writing")


def persist(root, result, *, status, reason, coverage, rows):
    from market_data_pipeline.pipelines.finra_otc_transparency.io import history

    root = _root(root)
    _base(root)
    if result.receipt["admission"]["route"] != "invented_fixture" or len(rows) > MAX_ROWS:
        raise ValueError("OTC panel accepts at most 10000 invented rows")
    if len(encoded(rows)) > 64 * 1024 * 1024:
        raise ValueError("OTC normalized publication exceeds 64 MiB")
    with _locked(root), _connection(root, write=True, create=True) as db:
        return history.persist(
            db, result, status=status, reason=reason, coverage=coverage, rows=rows
        )


def selection(partitions):
    if not isinstance(partitions, (list, tuple)) or any(
        not isinstance(p, Partition) for p in partitions
    ):
        raise ValueError("OTC inspect requires explicit validated partitions")
    return select_partitions(
        [
            {
                "reporting_week_start": p.reporting_week_start.isoformat(),
                "product": p.product,
                "tier": p.tier,
            }
            for p in partitions
        ]
    )


def inspect(root, *, partitions, max_rows=MAX_ROWS, as_of=None, as_of_sequence=None):
    from market_data_pipeline.pipelines.finra_otc_transparency.io import history_read, legacy

    if type(max_rows) is not int or not 1 <= max_rows <= MAX_ROWS:
        raise ValueError("OTC panel read limit must be in 1..10000")
    selected = selection(partitions)
    cutoff = history_read.cutoff(as_of, as_of_sequence)
    root = _root(root)
    _base(root)
    with _connection(root) as db:
        db.execute("BEGIN")
        if db.execute("PRAGMA user_version").fetchone()[0] == 1:
            if as_of is not None or as_of_sequence is not None:
                raise ValueError("OTC historical inspection requires explicit migration")
            return legacy.inspect(db, selected, max_rows)
        return history_read.inspect(db, selected, max_rows, cutoff)
