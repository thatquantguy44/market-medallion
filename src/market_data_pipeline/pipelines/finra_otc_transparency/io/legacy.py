"""Frozen T06 read protocol; legacy tables are never rewritten by T07."""

from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import ORIGIN, PANEL
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import digest


def inspect(db, selection, max_rows):
    from market_data_pipeline.pipelines.finra_otc_transparency.io.warehouse import _attempt, _record

    items, rows = [], []
    total_bytes = 0
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
