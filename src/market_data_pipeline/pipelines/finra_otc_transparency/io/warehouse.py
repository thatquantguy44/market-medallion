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
from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import ORIGIN, PANEL
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import _base, _locked, _root
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest, encoded

CONTRACT = "finra-otc-sqlite/1"
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
def _connection(root, *, write=False):
    path = root / "otc.sqlite3"
    created = False
    if write and not path.exists() and not path.is_symlink():
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
                db.execute("INSERT INTO otc_metadata VALUES (?)", (CONTRACT,))
                db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                db.execute("PRAGMA user_version=1")
                db.execute("COMMIT")
            except BaseException:
                db.close()
                path.unlink()
                raise
        if (
            db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
            or db.execute("PRAGMA user_version").fetchone()[0] != 1
            or [r[0] for r in db.execute("SELECT contract FROM otc_metadata")] != [CONTRACT]
        ):
            raise ValueError("OTC warehouse storage identity mismatch")
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
        or (value.get("reporting_week_start"), value.get("product"), value.get("tier"))
        != (row["week"], row["product"], row["tier"])
    ):
        raise ValueError("OTC warehouse attempt context mismatch")
    return {**value, "status": row["status"], "reason": row["reason"]}


def persist(root, result, *, status, reason, coverage, rows):
    """Private normalized boundary, called only after verified fixture admission."""
    receipt = result.receipt
    root = _root(root)
    _base(root)
    if receipt["admission"]["route"] != "invented_fixture" or len(rows) > MAX_ROWS:
        raise ValueError("OTC local panel accepts at most 10000 invented fixture rows")
    rows = sorted(rows, key=lambda r: (r["symbol_raw"], r["participant_key"]))
    if len(encoded(rows)) > 64 * 1024 * 1024:
        raise ValueError("OTC normalized fixture exceeds its 64 MiB publication bound")
    scope = receipt["partition"]
    key = (scope["reporting_week_start"], scope["product"], scope["tier"])
    context = {
        **scope,
        "capture_id": receipt["capture_id"],
        "run_id": receipt["run_id"],
        "capture_sequence": receipt["capture_sequence"],
        "captured_at": receipt["captured_at"],
        "receipt_sha256": receipt["receipt_sha256"],
        "origin": ORIGIN,
        "row_count": result.outcome["row_count"],
        "schema_profile": receipt["schema"]["schema_profile"],
    }
    capture_id = receipt["capture_id"]
    with _locked(root), _connection(root, write=True) as db:
        old = _attempt(
            db.execute("SELECT * FROM otc_attempts WHERE capture_id=?", (capture_id,)).fetchone()
        )
        if old is not None:
            if old["receipt_sha256"] != receipt["receipt_sha256"]:
                raise ValueError("OTC immutable attempt identity conflict")
            if old["status"] != "PENDING":
                if old["status"] == "PUBLISHED":
                    panel = inspect(root, partitions=select_partitions([scope]))
                    selected = panel["partitions"][0]["publication"]
                    if selected is None or selected["capture_id"] != capture_id:
                        raise ValueError("OTC successful attempt has no verified publication")
                return old
        else:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO otc_attempts VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    capture_id,
                    receipt["capture_sequence"],
                    *key,
                    "PENDING",
                    None,
                    encoded(context).decode(),
                    digest(context),
                ),
            )
            _commit(db)
        try:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute(
                "SELECT capture_id FROM otc_publications WHERE week=? AND product=? AND tier=?", key
            ).fetchone()
            if status == "PUBLISHED" and previous is not None:
                status, reason = "REPLACEMENT_DEFERRED", "T07_REQUIRED"
            if status == "PUBLISHED":
                if coverage is None:
                    raise ValueError("OTC publication requires explicit fixture coverage")
                db.executemany(
                    "INSERT INTO otc_rows VALUES (?,?,?,?)",
                    [
                        (capture_id, r["symbol_raw"], r["participant_key"], encoded(r).decode())
                        for r in rows
                    ],
                )
                db.execute(
                    "INSERT INTO otc_publications VALUES (?,?,?,?,?,?,?)",
                    (
                        capture_id,
                        *key,
                        encoded(coverage).decode(),
                        digest(rows),
                        digest(
                            {
                                "capture_id": capture_id,
                                "receipt_sha256": receipt["receipt_sha256"],
                                "coverage": coverage,
                                "rows_sha256": digest(rows),
                            }
                        ),
                    ),
                )
            db.execute(
                "UPDATE otc_attempts SET status=?,reason=? WHERE capture_id=?",
                (status, reason, capture_id),
            )
            _commit(db)
        except Exception:
            if db.in_transaction:
                db.execute("ROLLBACK")
            # A failed terminal write can leave PENDING; it can never create success.
            try:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "UPDATE otc_attempts SET status='FAILED',reason='PERSISTENCE_ERROR' "
                    "WHERE capture_id=? AND status='PENDING'",
                    (capture_id,),
                )
                _commit(db)
            except sqlite3.Error:
                if db.in_transaction:
                    db.execute("ROLLBACK")
            raise
    return {**context, "status": status, "reason": reason}


def inspect(root, *, partitions, max_rows=MAX_ROWS):
    """Read one coherent DB snapshot; no creation, raw parsing, repairs or network."""
    if type(max_rows) is not int or not 1 <= max_rows <= MAX_ROWS:
        raise ValueError("OTC panel read limit must be in 1..10000")
    if not isinstance(partitions, (tuple, list)) or any(
        not isinstance(p, Partition) for p in partitions
    ):
        raise ValueError("OTC inspect requires explicit validated partitions")
    selection = select_partitions(
        [
            {
                "reporting_week_start": p.reporting_week_start.isoformat(),
                "product": p.product,
                "tier": p.tier,
            }
            for p in partitions
        ]
    )
    root = _root(root)
    _base(root)
    items, rows = [], []
    total_bytes = 0
    with _connection(root) as db:
        db.execute("BEGIN")
        for partition in selection:
            scope = {
                "reporting_week_start": partition.reporting_week_start.isoformat(),
                "product": partition.product,
                "tier": partition.tier,
            }
            key = (scope["reporting_week_start"], scope["product"], scope["tier"])
            latest = _attempt(
                db.execute(
                    "SELECT * FROM otc_attempts WHERE week=? AND product=? AND tier=? "
                    "ORDER BY capture_sequence DESC LIMIT 1",
                    key,
                ).fetchone()
            )
            publication = db.execute(
                "SELECT * FROM otc_publications WHERE week=? AND product=? AND tier=?", key
            ).fetchone()
            selected = None
            if publication is not None:
                capture_id = publication["capture_id"]
                attempt = _attempt(
                    db.execute(
                        "SELECT * FROM otc_attempts WHERE capture_id=?", (capture_id,)
                    ).fetchone()
                )
                coverage = _record(publication["coverage_json"])
                records = db.execute(
                    "SELECT row_json FROM otc_rows WHERE capture_id=? "
                    "ORDER BY symbol_raw,participant_key LIMIT ?",
                    (capture_id, max_rows - len(rows) + 1),
                ).fetchall()
                total_bytes += sum(len(r[0].encode("utf-8")) for r in records)
                if total_bytes > 64 * 1024 * 1024:
                    raise ValueError("OTC panel exceeds its 64 MiB normalized read budget")
                population = [_record(r[0], 64 * 1024 * 1024) for r in records]
                if len(rows) + len(population) > max_rows:
                    raise ValueError("OTC panel exceeds the requested row budget")
                if (
                    attempt is None
                    or attempt["status"] != "PUBLISHED"
                    or coverage["row_count"] != len(population)
                    or publication["rows_sha256"] != digest(population)
                    or publication["publication_sha256"]
                    != digest(
                        {
                            "capture_id": capture_id,
                            "receipt_sha256": attempt["receipt_sha256"],
                            "coverage": coverage,
                            "rows_sha256": publication["rows_sha256"],
                        }
                    )
                ):
                    raise ValueError("OTC publication rows, coverage or terminal status mismatch")
                selected = {**attempt, "coverage": coverage}
                rows.extend(population)
            items.append({"partition": scope, "publication": selected, "latest_attempt": latest})
        db.execute("ROLLBACK")
    published = sum(item["publication"] is not None for item in items)
    if not published:
        status = "UNAVAILABLE_FIXTURE"
    elif published < len(items):
        status = "PARTIAL_FIXTURE"
    elif any(
        item["latest_attempt"]["capture_id"] != item["publication"]["capture_id"] for item in items
    ):
        status = "DEGRADED_FIXTURE"
    else:
        status = "COMPLETE_FIXTURE"
    return {
        "format": PANEL,
        "origin": ORIGIN,
        "source_conformance": "NOT_CHECKED",
        "status": status,
        "partitions": items,
        "rows": rows,
    }
