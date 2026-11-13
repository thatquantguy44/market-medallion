"""SSV-AC10 verified bounded source projection; all tables share one read transaction."""

from pathlib import Path

from market_data_pipeline.pipelines.finra_short_sale_volume import research
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    DEFAULT_BYTES,
    DEFAULT_ROWS,
    limits,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse as store
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import _admit, _json


def predicate(request, alias="", clock="archived_at"):
    prefix = alias + "." if alias else ""
    placeholders = ",".join("?" for _ in request["facility_ids"])
    sql = (
        f"{prefix}facility_id IN ({placeholders}) "
        f"AND {prefix}trade_date>=? AND {prefix}trade_date<=?"
    )
    args = [*request["facility_ids"], request["earliest_trade_date"], request["trade_date"]]
    cutoff = request["as_of"]
    if cutoff is not None:
        sql += f" AND ({prefix}{clock}<? OR ({prefix}{clock}=? AND {prefix}capture_sequence<=?))"
        args.extend(
            [
                cutoff,
                cutoff,
                request["as_of_sequence"] if request["as_of_sequence"] is not None else 2**63 - 1,
            ]
        )
    return sql, args


def collect(
    root,
    manifest,
    *,
    approved_use,
    operation,
    trade_date,
    scopes,
    as_of=None,
    as_of_sequence=None,
    lookback_days=366,
    max_symbols=15000,
    max_rows=DEFAULT_ROWS,
    max_bytes=DEFAULT_BYTES,
):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    limits(max_rows, max_bytes)
    request = research.validated_request(
        manifest,
        trade_date,
        scopes,
        as_of=as_of,
        as_of_sequence=as_of_sequence,
        lookback_days=lookback_days,
        max_symbols=max_symbols,
    )
    root = Path(root)
    with store.connect(root, manifest, approved_use=approved_use, operation=operation) as db:
        db.execute("BEGIN")
        if db.execute("SELECT 1 FROM attempts WHERE status='STARTED' LIMIT 1").fetchone():
            raise ValueError("recover pending FINRA source attempts before publication")
        where, args = predicate(request)
        vwhere, vargs = predicate(request, clock="first_seen_at")
        cwhere, cargs = predicate(request, alias="c")
        counts = 0
        size = 0
        for name, clause, parameters in [
            ("versions", vwhere, vargs),
            ("captures", where, args),
            ("attempts", where, args),
        ]:
            expression = (
                "length(CAST(body AS BLOB))"
                if name != "attempts"
                else "length(CAST(profile AS BLOB))+512"
            )
            row = db.execute(
                f"SELECT COUNT(*),COALESCE(SUM({expression}),0) FROM {name} WHERE " + clause,
                parameters,
            ).fetchone()
            counts += row[0]
            size += row[1]
        membership_count = db.execute(
            "SELECT COUNT(*) FROM capture_rows m JOIN captures c "
            "ON m.capture_id=c.capture_id WHERE " + cwhere,
            cargs,
        ).fetchone()[0]
        if counts + membership_count > max_rows or size + membership_count * 256 > max_bytes:
            raise ValueError("FINRA snapshot source row/byte budget exceeded")
        report = research.report(db, request)
        if report["status"] == "INCOMPLETE_POPULATION":
            raise ValueError("FINRA publication requires complete current partitions")
        silver = [
            store.version_document(row)
            for row in db.execute(
                "SELECT version_id,trade_date,facility_id,symbol_raw,first_seen_at,"
                "capture_sequence,capture_id,removed,body "
                "FROM versions WHERE "
                + vwhere
                + " ORDER BY first_seen_at,capture_sequence,facility_id,symbol_raw",
                vargs,
            )
        ]
        captures = []
        for row in db.execute(
            "SELECT capture_id FROM captures WHERE "
            + where
            + " ORDER BY archived_at,capture_sequence",
            args,
        ):
            captures.append(store.capture_result(db, row[0]))
            store.rows(db, row[0])
        audit = []
        for row in db.execute(
            "SELECT attempt_id,capture_id,run_id,trade_date,facility_id,archived_at,"
            "capture_sequence,started_at,completed_at,status,error_code,profile "
            "FROM attempts WHERE " + where + " ORDER BY archived_at,capture_sequence,rowid",
            args,
        ):
            value = dict(row)
            value["profile"] = store._document(value["profile"])
            audit.append(value)
        membership = [
            dict(row)
            for row in db.execute(
                "SELECT m.capture_id,m.symbol_raw,m.version_id,m.source_row FROM capture_rows m "
                "JOIN captures c ON m.capture_id=c.capture_id WHERE "
                + cwhere
                + " ORDER BY c.archived_at,c.capture_sequence,m.symbol_raw",
                cargs,
            )
        ]
        dates = sorted({r["trade_date"] for r in audit} | {request["trade_date"]})
        coverage = []
        gold = []
        for day in dates:
            for facility in request["facility_ids"]:
                selected = store.partition_snapshot(
                    db,
                    day,
                    facility,
                    request["as_of"],
                    request["as_of_sequence"],
                    row_budget=max_symbols,
                )
                attempts = [
                    r for r in audit if r["trade_date"] == day and r["facility_id"] == facility
                ]
                latest = attempts[-1] if attempts else None
                count = len(selected["rows"]) if selected["capture_id"] else None
                states = {
                    "APPLIED": "COMPLETE" if count else "VALID_EMPTY",
                    "PARTIAL": "PARTIAL",
                    "REJECTED": "FAILED",
                    "FAILED": "FAILED",
                    "INTERRUPTED": "PENDING",
                }
                if latest and latest["status"] not in states:
                    raise ValueError("invalid FINRA source attempt state")
                coverage.append(
                    dict(
                        trade_date=day,
                        facility_id=facility,
                        capture_id=selected["capture_id"],
                        committed_rows=count,
                        latest_attempt_status=states[latest["status"]] if latest else "MISSING",
                        latest_attempt_capture_id=latest["capture_id"] if latest else None,
                    )
                )
                if day == request["trade_date"]:
                    gold.extend(
                        dict(row, observed_capture_id=selected["capture_id"])
                        for row in selected["rows"]
                    )
        header = {k: v for k, v in report.items() if k != "rows"}
        research_rows = [
            dict(
                row,
                trade_date=request["trade_date"],
                facility_ids=request["facility_ids"],
                prior_trade_date=report["prior_trade_date"],
                calendar_day_gap=report["calendar_day_gap"],
                evidence_known_at=report["evidence_known_at"],
            )
            for row in report["rows"]
        ]
        tables = dict(
            silver=silver,
            gold=gold,
            research=research_rows,
            coverage=coverage,
            captures=captures,
            audit=audit,
            membership=membership,
        )
        if (
            sum(len(rows) for rows in tables.values()) > max_rows
            or sum(len(encoded(row)) for rows in tables.values() for row in rows) > max_bytes
        ):
            raise ValueError("FINRA snapshot projection row/byte budget exceeded")
        identity = _json(root / "warehouse/identity.json")
        result = dict(
            request=request,
            research=header,
            source_warehouse_id=identity["warehouse_id"],
            tables=tables,
        )
        result["state_sha256"] = digest(result)
        return result
