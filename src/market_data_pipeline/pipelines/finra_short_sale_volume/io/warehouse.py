"""SSV-AC06: source-owned SQLite identity, audit, atomic partition publication."""

import fcntl
import hashlib
import json
import os
import sqlite3
from contextlib import closing, contextmanager
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import _day
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.data.versions import (
    TRANSFORM,
    content_hash,
    observation,
    removal,
    version,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
    _admit,
    _identity,
    _json,
    _publish_directory,
    _put,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.schema import CONTRACT, SCHEMA

MAX_VERSIONS = 250000


def scope(manifest, trade_date, facility_id):
    day = _day(trade_date).isoformat()
    if facility_id not in {f.id for f in manifest.facilities}:
        raise ValueError("FINRA facility is outside the selected manifest")
    return day, facility_id


def _document(text):
    if not isinstance(text, str) or len(text.encode()) > 65536:
        raise ValueError("FINRA warehouse document exceeds its bound")
    value = json.loads(text)
    if not isinstance(value, dict) or encoded(value).decode() != text:
        raise ValueError("FINRA warehouse document is corrupt")
    return value


@contextmanager
def ingestion_lock(root):
    path = Path(root) / ".ingest.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


@contextmanager
def connect(root, manifest, *, approved_use, operation, create=False):
    kind = _admit(manifest, approved_use, operation, {"fixture", "transform"})
    root = Path(root)
    directory = root / "warehouse"
    expected = {
        "schema_id": CONTRACT,
        "root_sha256": digest(str(root.resolve())),
        "manifest_sha256": digest(manifest.to_dict()),
        "input_kind": kind,
        "transform": TRANSFORM,
        "ddl_sha256": hashlib.sha256(SCHEMA.encode()).hexdigest(),
    }
    binding = root / "warehouse.json"
    if create and not directory.exists() and not directory.is_symlink():
        if binding.exists() or binding.is_symlink():
            raise ValueError("FINRA bound warehouse is missing")
        identity = dict(expected, warehouse_id=str(uuid4()))
        with closing(sqlite3.connect(":memory:")) as initial:
            initial.executescript(SCHEMA)
            initial.execute("INSERT INTO identity VALUES (?)", (encoded(identity).decode(),))
            initial.commit()
            _publish_directory(
                root,
                "warehouse",
                {"identity.json": encoded(identity), "ssv.sqlite": initial.serialize()},
            )
        _put(binding, encoded(identity))
    db = directory / "ssv.sqlite"
    if directory.is_symlink() or db.is_symlink() or not db.is_file():
        raise ValueError("FINRA bound warehouse is missing or unsafe")
    for suffix in ("-journal", "-wal", "-shm"):
        if db.with_name(db.name + suffix).is_symlink():
            raise ValueError("FINRA SQLite sidecar is unsafe")
    identity = _json(directory / "identity.json")
    _identity(identity.get("warehouse_id"))
    if _json(binding) != identity:
        raise ValueError("FINRA warehouse binding mismatch")
    if (
        identity.get("input_kind") in {"invented_fixture", "provider_api"}
        and identity["input_kind"] != kind
    ):
        raise PermissionError("FINRA fixture/provider warehouse kind cannot be relabelled")
    if set(identity) != set(expected) | {"warehouse_id"} or any(
        identity[k] != v for k, v in expected.items()
    ):
        raise ValueError("FINRA warehouse identity mismatch")
    connection = None
    try:
        connection = sqlite3.connect(
            db.resolve().as_uri() + ("?mode=rw" if create else "?mode=ro"),
            uri=True,
            isolation_level=None,
            timeout=10,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        if create:
            connection.execute("PRAGMA synchronous=FULL")
        if (
            connection.execute("PRAGMA quick_check").fetchone()[0] != "ok"
            or connection.execute("PRAGMA user_version").fetchone()[0] != 1
        ):
            raise ValueError("FINRA warehouse schema or integrity mismatch")
        if (
            _schema(connection) != _expected_schema()
            or connection.execute("PRAGMA foreign_key_check").fetchone() is not None
        ):
            raise ValueError("FINRA warehouse schema or foreign-key mismatch")
        stored = connection.execute("SELECT body FROM identity").fetchall()
        if len(stored) != 1 or _document(stored[0][0]) != identity:
            raise ValueError("FINRA warehouse identity mismatch")
        yield connection
    except sqlite3.DatabaseError:
        raise ValueError("FINRA warehouse database is unavailable or corrupt") from None
    finally:
        if connection is not None:
            connection.close()


def terminal(db, saved, profile):
    result = capture_result(db, saved["capture_id"])
    if result is None:
        return None
    if (
        result["receipt_sha256"] != saved["receipt_sha256"]
        or result["profile"] != profile
        or result["transform"] != TRANSFORM
    ):
        raise ValueError("FINRA terminal capture profile conflict")
    if result["status"] == "APPLIED":
        rows(db, saved["capture_id"])
    return result


def start(db, saved, day, facility, now, profile):
    pending = db.execute(
        "SELECT attempt_id,capture_id FROM attempts WHERE status='STARTED'"
    ).fetchall()
    if any(row["capture_id"] != saved["capture_id"] for row in pending):
        raise RuntimeError("recover the pending FINRA capture before continuing")
    last = db.execute(
        "SELECT archived_at,capture_sequence FROM captures ORDER BY archived_at DESC"
        ",capture_sequence DESC LIMIT 1"
    ).fetchone()
    if last is not None and (saved["archived_at"], saved["capture_sequence"]) <= tuple(last):
        raise ValueError("FINRA capture processing order regressed")
    attempt = str(uuid4())
    db.execute("BEGIN IMMEDIATE")
    try:
        for row in pending:
            db.execute(
                "UPDATE attempts SET status='INTERRUPTED',completed_at=?,error_code='interru"
                "pted' WHERE attempt_id=?",
                (now, row["attempt_id"]),
            )
        db.execute(
            "INSERT INTO attempts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                attempt,
                saved["capture_id"],
                saved["run_id"],
                day,
                facility,
                saved["archived_at"],
                saved["capture_sequence"],
                now,
                None,
                "STARTED",
                None,
                encoded({"parser": profile, "transform": TRANSFORM}).decode(),
            ),
        )
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return attempt


def failed(db, attempt, now):
    db.execute(
        "UPDATE attempts SET status='FAILED',completed_at=?,error_code='execution_fa"
        "iled' WHERE attempt_id=? AND status='STARTED'",
        (now, attempt),
    )


def apply(db, saved, page, parsed, profile, attempt, now, checkpoint):
    day, facility = parsed["trade_date"], parsed["facility_id"]
    status = "REJECTED" if page is None else ("APPLIED" if page.partition_complete else "PARTIAL")
    result = {
        "capture_id": saved["capture_id"],
        "run_id": saved["run_id"],
        "capture_sequence": saved["capture_sequence"],
        "archived_at": saved["archived_at"],
        "receipt_sha256": saved["receipt_sha256"],
        "payload_sha256": saved["payload_sha256"],
        "profile": profile,
        "transform": TRANSFORM,
        "trade_date": day,
        "facility_id": facility,
        "status": status,
        "parsed_rows": len(page.rows) if page else 0,
        "accepted_rows": len(page.rows) if status == "APPLIED" else 0,
        "new_versions": 0,
        "removed_rows": 0,
        "error_code": parsed["error_code"],
        "completed_at": None,
    }
    db.execute("BEGIN IMMEDIATE")
    try:
        if status == "APPLIED":
            previous = db.execute(
                "SELECT capture_id FROM partitions WHERE trade_date=? AND facility_id=?",
                (day, facility),
            ).fetchone()
            old = {item["symbol_raw"]: item for item in rows(db, previous[0])} if previous else {}
            bodies = {row.symbol_raw: observation(row) for row in page.rows}
            removed = set(old) - set(bodies)
            changes = {
                key: body
                for key, body in bodies.items()
                if key not in old or old[key]["value_sha256"] != body["value_sha256"]
            }
            changes.update({key: removal(old[key]) for key in removed})
            if (
                db.execute("SELECT COUNT(*) FROM versions").fetchone()[0] + len(changes)
                > MAX_VERSIONS
            ):
                raise ValueError("FINRA version budget exceeded")
            selected = {key: body["version_id"] for key, body in old.items() if key in bodies}
            for key, body in sorted(changes.items()):
                value = version(body, saved)
                db.execute(
                    "INSERT INTO versions VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        value["version_id"],
                        day,
                        facility,
                        key,
                        saved["archived_at"],
                        saved["capture_sequence"],
                        saved["capture_id"],
                        int(body["removed"]),
                        encoded(value).decode(),
                    ),
                )
                if key not in removed:
                    selected[key] = value["version_id"]
                result["new_versions"] += 1
            result["removed_rows"] = len(removed)
            for row in page.rows:
                db.execute(
                    "INSERT INTO capture_rows VALUES (?,?,?,?)",
                    (saved["capture_id"], row.symbol_raw, selected[row.symbol_raw], row.source_row),
                )
            checkpoint("after_versions")
        result["completed_at"] = now()
        members = [
            list(row)
            for row in db.execute(
                "SELECT symbol_raw,version_id,source_row FROM capture_rows WHERE capture_id=? "
                "ORDER BY symbol_raw",
                (saved["capture_id"],),
            )
        ]
        result["membership_sha256"] = digest(members)
        result["result_sha256"] = digest(result)
        db.execute(
            "INSERT INTO captures VALUES (?,?,?,?,?,?,?)",
            (
                saved["capture_id"],
                saved["capture_sequence"],
                saved["archived_at"],
                day,
                facility,
                status,
                encoded(result).decode(),
            ),
        )
        if status == "APPLIED":
            db.execute(
                "INSERT INTO partitions VALUES (?,?,?) ON CONFLICT(trade_date,facility_id) D"
                "O UPDATE SET capture_id=excluded.capture_id",
                (day, facility, saved["capture_id"]),
            )
        checkpoint("after_membership")
        db.execute(
            "UPDATE attempts SET status=?,completed_at=?,error_code=? WHERE attempt_id=?",
            (status, result["completed_at"], result["error_code"], attempt),
        )
        checkpoint("before_commit")
        db.commit()
    except BaseException:
        db.rollback()
        raise
    checkpoint("after_commit")
    return result


def version_document(row):
    value = _document(row["body"])
    identity = value.pop("version_id", None)
    keys = (
        "trade_date",
        "facility_id",
        "symbol_raw",
        "first_seen_at",
        "capture_sequence",
        "capture_id",
        "removed",
    )
    if (
        identity != row["version_id"]
        or digest(value) != identity
        or any(value.get(k) != row[k] for k in keys)
        or type(value.get("removed")) is not bool
        or value.get("value_sha256") != content_hash(value)
    ):
        raise ValueError("FINRA stored version integrity mismatch")
    value["version_id"] = identity
    return value


def rows(db, capture_id):
    values = []
    members = []
    for row in db.execute(
        "SELECT v.version_id,v.trade_date,v.facility_id,v.symbol_raw,v.first_seen_at,"
        "v.capture_sequence,v.capture_id,v.removed,v.body,m.source_row AS observed_source_row,"
        "m.symbol_raw AS observed_symbol FROM capture_rows m JOIN versions v "
        "ON m.version_id=v.version_id WHERE m.capture_id=? ORDER BY v.symbol_raw",
        (capture_id,),
    ):
        value = version_document(row)
        source_row = row["observed_source_row"]
        if (
            value["removed"]
            or row["observed_symbol"] != value["symbol_raw"]
            or type(source_row) is not int
            or source_row < 1
        ):
            raise ValueError("FINRA partition membership integrity mismatch")
        members.append([value["symbol_raw"], value["version_id"], source_row])
        value["observed_source_row"] = source_row
        values.append(value)
    result = capture_result(db, capture_id)
    if (
        result is None
        or result["accepted_rows"] != len(values)
        or result["membership_sha256"] != digest(members)
    ):
        raise ValueError("FINRA partition membership identity mismatch")
    return values


def _schema(db):
    return [
        tuple(row)
        for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")
    ]


@lru_cache(maxsize=1)
def _expected_schema():
    with closing(sqlite3.connect(":memory:")) as db:
        db.executescript(SCHEMA)
        return _schema(db)


def capture_result(db, capture_id):
    row = db.execute(
        "SELECT capture_id,capture_sequence,archived_at,trade_date,facility_id,status,body "
        "FROM captures WHERE capture_id=?",
        (capture_id,),
    ).fetchone()
    if row is None:
        return None
    value = _document(row["body"])
    checksum = value.pop("result_sha256", None)
    if digest(value) != checksum or any(value[k] != row[k] for k in row.keys() if k != "body"):
        raise ValueError("FINRA terminal capture integrity mismatch")
    value["result_sha256"] = checksum
    return value
