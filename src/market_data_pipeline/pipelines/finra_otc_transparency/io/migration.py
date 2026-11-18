"""Explicit T06->T07 fixture warehouse migration; never rewrite legacy evidence."""

from datetime import UTC, datetime

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import select_partitions
from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import prepare
from market_data_pipeline.pipelines.finra_otc_transparency.data.weekly import parse_weekly
from market_data_pipeline.pipelines.finra_otc_transparency.io import (
    history,
    legacy,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io import (
    warehouse as store,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import (
    ValidationResult,
    read_capture,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest, encoded


def migrate(root, *, approved_use, max_captures=1000):
    if type(max_captures) is not int or not 1 <= max_captures <= 1000:
        raise ValueError("OTC migration capture budget must be in 1..1000")
    root = store._root(root)
    store._base(root)
    with store._locked(root), store._connection(root, write=True, allow_legacy=True) as db:
        if getattr(db, "backend", None) == "postgres":
            from market_data_pipeline.pipelines.finra_otc_transparency.io.postgres import CONTRACT

            return {
                "status": "ALREADY_CURRENT",
                "warehouse_contract": CONTRACT,
                "logical_contract": store.CONTRACT,
            }
        if db.execute("PRAGMA user_version").fetchone()[0] == 2:
            return {"status": "ALREADY_CURRENT", "warehouse_contract": store.CONTRACT}
        db.execute("BEGIN IMMEDIATE")
        try:
            attempts = db.execute(
                "SELECT * FROM otc_attempts ORDER BY capture_sequence LIMIT ?", (max_captures + 1,)
            ).fetchall()
            if len(attempts) > max_captures:
                raise ValueError("OTC migration capture budget exceeded")
            for attempt in attempts:
                store._attempt(attempt)
            if db.execute("SELECT count(*) FROM otc_rows").fetchone()[0] > store.MAX_ROWS:
                raise ValueError("OTC migration row budget exceeded")
            pubs = db.execute(
                "SELECT week,product,tier FROM otc_publications ORDER BY week,product,tier"
            ).fetchall()
            if db.execute("SELECT count(*) FROM otc_attempts WHERE status='PUBLISHED'").fetchone()[
                0
            ] != len(pubs):
                raise ValueError("OTC legacy success/selection mismatch")
            selection = (
                select_partitions(
                    [{"reporting_week_start": p[0], "product": p[1], "tier": p[2]} for p in pubs]
                )
                if pubs
                else ()
            )
            panel = (
                legacy.inspect(db, selection, store.MAX_ROWS)
                if selection
                else {"partitions": [], "rows": []}
            )
            preserved = {
                table: digest(
                    [list(row) for row in db.execute("SELECT * FROM " + table + " ORDER BY 1,2")]
                )
                for table in ("otc_attempts", "otc_publications", "otc_rows")
            }
            for statement in history.SCHEMA:
                db.execute(statement)
            seeded = []
            # v1 has at most one selected capture per partition; seeding does not
            # reconsider deferred/rejected attempts or invent intermediate history.
            for item in panel["partitions"]:
                published = item["publication"]
                capture_id = published["capture_id"]
                capture = read_capture(root, capture_id, approved_use=approved_use)
                receipt = capture.receipt
                (p,) = select_partitions([receipt["partition"]])
                if (
                    receipt["coverage"] != "RECEIVED_ALL"
                    or receipt["admission"]["route"] != "invented_fixture"
                    or set(capture.parts) != {"file"}
                ):
                    raise ValueError("OTC migration requires complete invented source evidence")
                parsed = parse_weekly(
                    capture.parts["file"],
                    partition=p,
                    schema_profile=receipt["schema"]["schema_profile"],
                    max_bytes=receipt["limits"]["max_bytes"],
                    max_rows=store.MAX_ROWS,
                )
                result = ValidationResult(receipt, {"row_count": len(parsed.rows)}, parsed)
                status, reason, coverage, rows = prepare(capture, result)
                old_rows = [row for row in panel["rows"] if row["capture_id"] == capture_id]
                rows = sorted(rows, key=lambda r: (r["symbol_raw"], r["participant_key"]))
                old_context = {
                    k: v for k, v in published.items() if k not in ("coverage", "status", "reason")
                }
                current_context = {
                    k: v for k, v in history.context(result).items() if k != "transform_version"
                }
                if (
                    status != "PUBLISHED"
                    or reason is not None
                    or encoded(rows) != encoded(old_rows)
                    or coverage != published["coverage"]
                    or old_context != current_context
                ):
                    raise ValueError("OTC legacy publication differs from retained source evidence")
                history.insert_attempt(db, result)
                history.append_snapshot(db, result, coverage, rows)
                db.execute(
                    "UPDATE otc2_attempts SET status='PUBLISHED' WHERE capture_id=?", (capture_id,)
                )
                seeded.append(capture_id)
            record = {
                "format": "finra-otc-migration/1",
                "from": store.LEGACY_CONTRACT,
                "to": store.CONTRACT,
                "legacy_table_sha256": preserved,
                "seeded_capture_ids": seeded,
                "migrated_at": datetime.now(UTC).isoformat(),
            }
            db.execute(
                "INSERT INTO otc2_migrations VALUES (?,?,?)",
                ("1-to-2", encoded(record).decode(), digest(record)),
            )
            db.execute("UPDATE otc_metadata SET contract=?", (store.CONTRACT,))
            db.execute("PRAGMA user_version=2")
            store._commit(db)
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
    return {
        "status": "MIGRATED",
        "warehouse_contract": store.CONTRACT,
        "seeded_snapshots": len(seeded),
    }


def verify_legacy(db):
    rows = db.execute("SELECT record_json,record_sha256 FROM otc2_migrations").fetchall()
    if len(rows) > 1:
        raise ValueError("OTC migration inventory is inconsistent")
    for row in rows:
        record = store._record(row[0])
        if row[1] != digest(record):
            raise ValueError("OTC migration evidence seal mismatch")
        for table in ("otc_attempts", "otc_publications", "otc_rows"):
            limit = 10000 if table == "otc_rows" else 1000
            if db.execute("SELECT count(*) FROM " + table).fetchone()[0] > limit:
                raise ValueError("OTC preserved legacy inventory exceeds its bound")
            logical = [list(r) for r in db.execute("SELECT * FROM " + table + " ORDER BY 1,2")]
            if digest(logical) != record["legacy_table_sha256"][table]:
                raise ValueError("OTC preserved legacy table was changed after migration")
