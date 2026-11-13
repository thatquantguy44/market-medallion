"""SSV-AC06: offline admitted input -> immutable Bronze -> audited SQLite Gold."""

from datetime import UTC, datetime
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import _request
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import settings
from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse as store
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
    _admit,
    _clock,
    archive_input,
    parse_capture,
    read_capture,
)


def _operation(manifest, grant, operation):
    _admit(manifest, grant, operation, {"fixture", "local_import"})
    transform = "fixture" if operation == "fixture" else "transform"
    _admit(manifest, grant, transform, {"fixture", "transform"})
    return transform


def run(
    root,
    payload,
    manifest,
    api_receipt,
    *,
    approved_use,
    operation,
    run_id=None,
    capture_id=None,
    clock=None,
    retrieved_at=None,
    max_bytes=20 * 1024 * 1024,
    max_rows=5000,
    fault=None,
):
    transform = _operation(manifest, approved_use, operation)
    saved = archive_input(
        root,
        payload,
        manifest,
        api_receipt,
        approved_use=approved_use,
        operation=operation,
        run_id=run_id or str(uuid4()),
        capture_id=capture_id,
        archived_at=clock() if clock else None,
        retrieved_at=retrieved_at,
        max_bytes=max_bytes,
        max_rows=max_rows,
    )
    return ingest_capture(
        root,
        saved["capture_id"],
        manifest,
        approved_use=approved_use,
        operation=transform,
        max_bytes=max_bytes,
        max_rows=max_rows,
        clock=clock,
        fault=fault,
    )


def ingest_capture(
    root,
    capture_id,
    manifest,
    *,
    approved_use,
    operation,
    max_bytes=20 * 1024 * 1024,
    max_rows=5000,
    clock=None,
    fault=None,
):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    profile = settings(max_bytes, max_rows)
    saved, _ = read_capture(
        root,
        capture_id,
        manifest,
        approved_use=approved_use,
        operation=operation,
        max_bytes=max_bytes,
    )
    day, facility, _, _ = _request(
        saved["api_receipt"], manifest, saved["capture_limits"]["max_rows"]
    )

    def now():
        return _clock(clock() if clock else datetime.now(UTC))

    checkpoint = fault or (lambda stage: None)
    with (
        store.ingestion_lock(root),
        store.connect(
            root, manifest, approved_use=approved_use, operation=operation, create=True
        ) as db,
    ):
        existing = store.terminal(db, saved, profile)
        if existing is not None:
            return existing
        attempt = store.start(db, saved, day.isoformat(), facility.id, now())
        try:
            checkpoint("after_start")
            page, parsed = parse_capture(
                root,
                capture_id,
                manifest,
                approved_use=approved_use,
                operation=operation,
                max_bytes=max_bytes,
                max_rows=max_rows,
            )
            parsed = dict(parsed, trade_date=day.isoformat(), facility_id=facility.id)
            return store.apply(db, saved, page, parsed, profile, attempt, now(), checkpoint)
        except Exception:
            store.failed(db, attempt, now())
            raise RuntimeError("FINRA local execution failed; original capture retained") from None


def read_gold(
    root,
    manifest,
    *,
    approved_use,
    operation,
    trade_date,
    facility_id,
    as_of=None,
    as_of_sequence=None,
):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    day, facility = store.scope(manifest, trade_date, facility_id)
    if as_of is not None or as_of_sequence is not None:
        raise ValueError("cutoff selection requires SSV-T07")
    with store.connect(root, manifest, approved_use=approved_use, operation=operation) as db:
        db.execute("BEGIN")
        selected = db.execute(
            "SELECT capture_id FROM partitions WHERE trade_date=? AND facility_id=?",
            (day, facility),
        ).fetchone()
        capture_id = selected[0] if selected else None
        return {
            "trade_date": day,
            "facility_id": facility,
            "capture_id": capture_id,
            "knowledge_basis": "CURRENT_VINTAGE",
            "clock_basis": "LOCAL_CAPTURE",
            "rows": store.rows(db, capture_id) if capture_id else [],
        }


def audit(root, manifest, *, approved_use, operation, limit=20):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("FINRA audit limit must be 1..1000")
    with store.connect(root, manifest, approved_use=approved_use, operation=operation) as db:
        return [
            dict(row)
            for row in db.execute(
                "SELECT attempt_id,capture_id,run_id,trade_date,facility_id,archived_at,capt"
                "ure_sequence,started_at,completed_at,status,error_code FROM attempts ORDER "
                "BY rowid DESC LIMIT ?",
                (limit,),
            )
        ]


def coverage(root, manifest, *, approved_use, operation, trade_date, facility_id, symbol=None):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    day, facility = store.scope(manifest, trade_date, facility_id)
    with store.connect(root, manifest, approved_use=approved_use, operation=operation) as db:
        db.execute("BEGIN")
        last = db.execute(
            "SELECT capture_id,status FROM attempts WHERE trade_date=? AND facility_id=?"
            " ORDER BY rowid DESC LIMIT 1",
            (day, facility),
        ).fetchone()
        current = db.execute(
            "SELECT capture_id FROM partitions WHERE trade_date=? AND facility_id=?",
            (day, facility),
        ).fetchone()
        committed = current[0] if current else None
        count = db.execute(
            "SELECT COUNT(*) FROM capture_rows WHERE capture_id=?", (committed,)
        ).fetchone()[0]
        states = {
            "APPLIED": "COMPLETE" if count else "VALID_EMPTY",
            "REJECTED": "FAILED",
            "PARTIAL": "PARTIAL",
            "FAILED": "FAILED",
            "STARTED": "PENDING",
            "INTERRUPTED": "PENDING",
        }
        result = {
            "trade_date": day,
            "facility_id": facility,
            "latest_attempt_status": states[last["status"]] if last else "MISSING",
            "latest_attempt_capture_id": last["capture_id"] if last else None,
            "committed_capture_id": committed,
            "committed_rows": count,
        }
        if symbol is not None:
            found = db.execute(
                "SELECT 1 FROM capture_rows WHERE capture_id=? AND symbol_raw=?",
                (committed, symbol),
            ).fetchone()
            result["symbol_status"] = (
                "UNKNOWN" if committed is None else ("REPORTED" if found else "NO_REPORTED_ROW")
            )
        return result
