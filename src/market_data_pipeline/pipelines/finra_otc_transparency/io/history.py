"""OTC-AC07 append-only value occurrences and complete capture membership."""

from market_data_pipeline.pipelines.finra_otc_transparency.data.versions import (
    TRANSFORM,
    content,
    make_version,
    tokens,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io import warehouse as store
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest, encoded

MAX_VERSIONS = 100000
SCHEMA = (
    """CREATE TABLE otc2_attempts (
       capture_id TEXT PRIMARY KEY, capture_sequence INTEGER NOT NULL UNIQUE,
       captured_at TEXT NOT NULL,
       week TEXT NOT NULL, product TEXT NOT NULL, tier TEXT NOT NULL,
       status TEXT NOT NULL, reason TEXT, context_json TEXT NOT NULL,
       context_sha256 TEXT NOT NULL)""",
    """CREATE TABLE otc2_versions (
       version_id TEXT PRIMARY KEY, capture_id TEXT NOT NULL REFERENCES otc2_attempts(capture_id),
       capture_sequence INTEGER NOT NULL, week TEXT NOT NULL, product TEXT NOT NULL,
       tier TEXT NOT NULL, symbol_raw TEXT NOT NULL, participant_key TEXT NOT NULL,
       record_json TEXT NOT NULL)""",
    """CREATE TABLE otc2_snapshots (
       capture_id TEXT PRIMARY KEY REFERENCES otc2_attempts(capture_id),
       capture_sequence INTEGER NOT NULL UNIQUE, captured_at TEXT NOT NULL,
       week TEXT NOT NULL, product TEXT NOT NULL, tier TEXT NOT NULL,
       record_json TEXT NOT NULL, record_sha256 TEXT NOT NULL)""",
    """CREATE TABLE otc2_members (
       capture_id TEXT NOT NULL REFERENCES otc2_snapshots(capture_id) DEFERRABLE INITIALLY DEFERRED,
       symbol_raw TEXT NOT NULL, participant_key TEXT NOT NULL,
       version_id TEXT NOT NULL REFERENCES otc2_versions(version_id), tokens_json TEXT NOT NULL,
       PRIMARY KEY(capture_id,symbol_raw,participant_key))""",
    """CREATE TABLE otc2_migrations (migration_id TEXT PRIMARY KEY, record_json TEXT NOT NULL,
       record_sha256 TEXT NOT NULL)""",
    "CREATE INDEX otc2_attempt_scope ON otc2_attempts(week,product,tier,capture_sequence)",
    "CREATE INDEX otc2_snapshot_scope ON otc2_snapshots(week,product,tier,capture_sequence)",
    "CREATE INDEX otc2_version_scope ON otc2_versions"
    "(week,product,tier,symbol_raw,participant_key,capture_sequence)",
)


def context(result):
    r = result.receipt
    return {
        **r["partition"],
        **{
            k: r[k]
            for k in ("capture_id", "run_id", "capture_sequence", "captured_at", "receipt_sha256")
        },
        "origin": "INVENTED_FIXTURE",
        "row_count": result.outcome["row_count"],
        "schema_profile": r["schema"]["schema_profile"],
        "transform_version": TRANSFORM,
    }


def insert_attempt(db, result):
    r = result.receipt
    c = context(result)
    p = r["partition"]
    db.execute(
        "INSERT INTO otc2_attempts VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            r["capture_id"],
            r["capture_sequence"],
            r["captured_at"],
            p["reporting_week_start"],
            p["product"],
            p["tier"],
            "PENDING",
            None,
            encoded(c).decode(),
            digest(c),
        ),
    )


def _heads(db, key):
    from market_data_pipeline.pipelines.finra_otc_transparency.io.history_read import version_record

    records = db.execute(
        """SELECT * FROM (
        SELECT v.*,a.status AS origin_status,a.captured_at AS origin_clock,
        row_number() OVER (PARTITION BY v.symbol_raw,v.participant_key
                           ORDER BY v.capture_sequence DESC) AS rank
        FROM otc2_versions v JOIN otc2_attempts a ON a.capture_id=v.capture_id
        WHERE v.week=? AND v.product=? AND v.tier=?) WHERE rank=1 LIMIT ?""",
        (*key, MAX_VERSIONS + 1),
    ).fetchall()
    if (
        len(records) > MAX_VERSIONS
        or sum(len(r["record_json"].encode()) for r in records) > 64 * 1024 * 1024
    ):
        raise ValueError("OTC version head budget exceeded")
    return {(r["symbol_raw"], r["participant_key"]): version_record(r) for r in records}


def append_snapshot(db, result, coverage, rows):
    from market_data_pipeline.pipelines.finra_otc_transparency.io.history_read import snapshot

    r = result.receipt
    p = r["partition"]
    key = (p["reporting_week_start"], p["product"], p["tier"])
    old = db.execute(
        "SELECT capture_id FROM otc2_snapshots WHERE week=? AND product=? AND tier=? "
        "ORDER BY capture_sequence DESC LIMIT 1",
        key,
    ).fetchone()
    previous, previous_rows = snapshot(db, old[0], store.MAX_ROWS) if old else (None, [])
    if previous and previous["capture_sequence"] >= r["capture_sequence"]:
        return "OUT_OF_ORDER", "NEWER_COMPLETE_CAPTURE"
    heads = _heads(db, key)
    members = []
    changes = []
    rows = sorted(rows, key=lambda row: (row["symbol_raw"], row["participant_key"]))
    current_keys = {(row["symbol_raw"], row["participant_key"]) for row in rows}
    if len(current_keys) != len(rows):
        raise ValueError("OTC duplicate normalized membership")
    for row in rows:
        k = (row["symbol_raw"], row["participant_key"])
        previous_version = heads.get(k)
        version = make_version(r, k, content(row), previous_version)
        if version is not None:
            changes.append(version)
        selected = version if version is not None else previous_version
        members.append(
            {
                "symbol_raw": k[0],
                "participant_key": k[1],
                "version_id": selected["version_id"],
                "tokens": tokens(row),
            }
        )
    missing = {(row["symbol_raw"], row["participant_key"]) for row in previous_rows} - current_keys
    for k in sorted(missing):
        changes.append(make_version(r, k, None, heads[k]))
    count = db.execute("SELECT count(*) FROM otc2_versions").fetchone()[0]
    if count + len(changes) > MAX_VERSIONS:
        raise ValueError("OTC retained version ceiling exceeded")
    for v in changes:
        db.execute(
            "INSERT INTO otc2_versions VALUES (?,?,?,?,?,?,?,?,?)",
            (
                v["version_id"],
                v["capture_id"],
                v["capture_sequence"],
                *key,
                v["symbol_raw"],
                v["participant_key"],
                encoded(v).decode(),
            ),
        )
    for member in members:
        db.execute(
            "INSERT INTO otc2_members VALUES (?,?,?,?,?)",
            (
                r["capture_id"],
                member["symbol_raw"],
                member["participant_key"],
                member["version_id"],
                encoded(member["tokens"]).decode(),
            ),
        )
    record = {
        **context(result),
        "coverage": coverage,
        "members_sha256": digest(members),
        "previous_capture_id": previous["capture_id"] if previous else None,
        "versions_created": len(changes),
        "removed_rows": len(missing),
    }
    db.execute(
        "INSERT INTO otc2_snapshots VALUES (?,?,?,?,?,?,?,?)",
        (
            r["capture_id"],
            r["capture_sequence"],
            r["captured_at"],
            *key,
            encoded(record).decode(),
            digest(record),
        ),
    )
    return "PUBLISHED", None


def persist(db, result, *, status, reason, coverage, rows):
    from market_data_pipeline.pipelines.finra_otc_transparency.io.history_read import snapshot

    r = result.receipt
    capture_id = r["capture_id"]
    old = store._attempt(
        db.execute("SELECT * FROM otc2_attempts WHERE capture_id=?", (capture_id,)).fetchone()
    )
    if old:
        if old["receipt_sha256"] != r["receipt_sha256"]:
            raise ValueError("OTC immutable transformation attempt conflict")
        if old["status"] != "PENDING":
            if old["status"] == "PUBLISHED":
                snapshot(db, capture_id, store.MAX_ROWS)
            return old
    else:
        db.execute("BEGIN IMMEDIATE")
        insert_attempt(db, result)
        store._commit(db)
    try:
        db.execute("BEGIN IMMEDIATE")
        if status == "PUBLISHED":
            if coverage is None:
                raise ValueError("OTC complete snapshot requires coverage")
            status, reason = append_snapshot(db, result, coverage, rows)
        db.execute(
            "UPDATE otc2_attempts SET status=?,reason=? WHERE capture_id=?",
            (status, reason, capture_id),
        )
        store._commit(db)
    except Exception:
        if db.in_transaction:
            db.execute("ROLLBACK")
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE otc2_attempts SET status='FAILED',reason='PERSISTENCE_ERROR' "
                "WHERE capture_id=? AND status='PENDING'",
                (capture_id,),
            )
            store._commit(db)
        except Exception:
            if db.in_transaction:
                db.execute("ROLLBACK")
        raise
    return {**context(result), "status": status, "reason": reason}
