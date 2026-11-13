"""SSV-AC09: read-only matched-population research and local prior-date comparisons."""

from datetime import timedelta
from decimal import Decimal

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import _day
from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import (
    RATIO_METHOD,
    decimal_text,
    matched_metrics,
    ratio_change,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import (
    quantity_from_text,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse as store
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import _admit, _clock
from market_data_pipeline.pipelines.finra_short_sale_volume.registry import select_scopes

METHOD = "finra-ssv-research-gold/1"
_QUANTITIES = ("short_volume", "short_exempt_volume", "total_volume")
_RATIOS = ("short_share", "short_exempt_share")


def _partitions(db, day, facilities, cutoff, sequence):
    return {f: store.partition_snapshot(db, day, f, cutoff, sequence) for f in facilities}


def _provenance(partitions):
    return [
        dict(
            facility_id=f,
            **{k: v for k, v in snapshot.items() if k != "rows"},
            status="MISSING"
            if snapshot["capture_id"] is None
            else ("COMPLETE" if snapshot["rows"] else "VALID_EMPTY"),
        )
        for f, snapshot in partitions.items()
    ]


def _metric(symbol, partitions, index):
    missing = [f for f, snapshot in partitions.items() if snapshot["capture_id"] is None]
    absent = [
        f
        for f, snapshot in partitions.items()
        if snapshot["capture_id"] is not None and symbol not in index[f]
    ]
    status = "INCOMPLETE_POPULATION" if missing else ("NO_REPORTED_ROW" if absent else "AVAILABLE")
    result = dict(status=status, missing_row_facilities=absent)
    if status == "AVAILABLE":
        result.update(
            matched_metrics(
                [
                    tuple(quantity_from_text(index[f][symbol][k]) for k in _QUANTITIES)
                    for f in partitions
                ]
            )
        )
    else:
        result.update({k: None for k in _QUANTITIES + _RATIOS})
        result["null_reason"] = status
    result["components"] = []
    for f, snapshot in partitions.items():
        row = index[f].get(symbol)
        result["components"].append(
            dict(
                facility_id=f,
                capture_id=snapshot["capture_id"],
                version_id=row["version_id"] if row else None,
                source_capture_id=row["capture_id"] if row else None,
                source_row=row["observed_source_row"] if row else None,
                status="REPORTED"
                if row
                else ("NO_REPORTED_ROW" if snapshot["capture_id"] else "MISSING_PARTITION"),
                **{k: row[k] if row else None for k in _QUANTITIES},
            )
        )
    return result


def _prior_date(db, day, earliest, facilities, cutoff, sequence):
    placeholders = ",".join("?" for _ in facilities)
    query = (
        "SELECT trade_date,COUNT(DISTINCT CASE WHEN status='APPLIED' THEN facility_id END) "
        f"AS available FROM captures WHERE facility_id IN ({placeholders}) "
        "AND trade_date>=? AND trade_date<? "
    )
    parameters = [*facilities, earliest, day]
    if cutoff is not None:
        query += "AND (archived_at<? OR (archived_at=? AND capture_sequence<=?)) "
        parameters.extend([cutoff, cutoff, sequence if sequence is not None else 2**63 - 1])
    query += "GROUP BY trade_date ORDER BY trade_date DESC"
    skipped = []
    for row in db.execute(query, parameters):
        if row["available"] == len(facilities):
            return row["trade_date"], skipped
        skipped.append(row["trade_date"])
    return None, skipped


def research_gold(
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
):
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    day = _day(trade_date)
    facilities = [f.id for f in select_scopes(scopes)]
    if set(facilities) - {f.id for f in manifest.facilities}:
        raise ValueError("FINRA research population is outside the manifest")
    if type(lookback_days) is not int or not 1 <= lookback_days <= 3660:
        raise ValueError("FINRA research lookback requires 1..3660 calendar days")
    if type(max_symbols) is not int or not 1 <= max_symbols <= 250000:
        raise ValueError("FINRA research symbol budget requires 1..250000")
    cutoff = _clock(as_of) if as_of is not None else None
    if as_of_sequence is not None and (
        cutoff is None or type(as_of_sequence) is not int or not 1 <= as_of_sequence < 2**63
    ):
        raise ValueError("FINRA sequence cutoff requires an aware time and positive integer")
    try:
        earliest = (day - timedelta(days=lookback_days)).isoformat()
    except OverflowError:
        raise ValueError("FINRA research lookback exceeds date range") from None
    with store.connect(root, manifest, approved_use=approved_use, operation=operation) as db:
        db.execute("BEGIN")
        current = _partitions(db, day.isoformat(), facilities, cutoff, as_of_sequence)
        prior_day, skipped = _prior_date(
            db, day.isoformat(), earliest, facilities, cutoff, as_of_sequence
        )
        prior = _partitions(db, prior_day, facilities, cutoff, as_of_sequence) if prior_day else {}
        current_index = {f: {r["symbol_raw"]: r for r in p["rows"]} for f, p in current.items()}
        prior_index = {f: {r["symbol_raw"]: r for r in p["rows"]} for f, p in prior.items()}
        symbols = sorted({symbol for rows in current_index.values() for symbol in rows})
        if len(symbols) > max_symbols:
            raise ValueError("FINRA research symbol budget exceeded")
        rows = []
        for symbol in symbols:
            row = dict(symbol_raw=symbol, **_metric(symbol, current, current_index))
            old = _metric(symbol, prior, prior_index) if prior_day else None
            row["prior_status"] = old["status"] if old else "NO_PRIOR_COMPLETE_DATE"
            row["prior_null_reason"] = old["null_reason"] if old else "NO_PRIOR_COMPLETE_DATE"
            row["prior_components"] = old["components"] if old else []
            for key in _RATIOS:
                value = row[key]
                previous = old[key] if old else None
                row["prior_" + key] = previous
                row[key + "_change"] = decimal_text(
                    ratio_change(
                        Decimal(value) if value is not None else None,
                        Decimal(previous) if previous is not None else None,
                    )
                )
            rows.append(row)
        missing = [f for f, p in current.items() if p["capture_id"] is None]
        clocks = [
            p["archived_at"]
            for p in list(current.values()) + list(prior.values())
            if p["archived_at"]
        ]
        return dict(
            method=METHOD,
            source="finra",
            dataset="finra-short-sale-volume",
            trade_date=day.isoformat(),
            facility_ids=facilities,
            quantity_unit="shares",
            ratio_method=RATIO_METHOD,
            ratio_unit="1",
            knowledge_basis="CURRENT_VINTAGE",
            clock_basis="LOCAL_CAPTURE",
            population_basis="LATEST_COMPLETE_PARTITION_PER_FACILITY_AT_CUTOFF",
            status="INCOMPLETE_POPULATION" if missing else ("COMPLETE" if rows else "VALID_EMPTY"),
            as_of=cutoff,
            as_of_sequence=as_of_sequence,
            evidence_known_at=max(clocks, default=None),
            lookback_days=lookback_days,
            prior_trade_date=prior_day,
            calendar_day_gap=(day - _day(prior_day)).days if prior_day else None,
            gap_basis="CALENDAR_DAYS",
            incomplete_prior_dates=skipped,
            missing_facilities=missing,
            current_partitions=_provenance(current),
            prior_partitions=_provenance(prior),
            rows=rows,
        )
