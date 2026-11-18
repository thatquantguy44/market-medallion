"""OTC-AC07 bounded coherent current/as-of membership and immutable history reads."""

from datetime import datetime

from market_data_pipeline.pipelines.finra_otc_transparency.data.versions import (
    LINEAGE_FIELDS,
    TRANSFORM,
    tokens,
    verify_version,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io import warehouse as store
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import (
    MAX_SEQUENCES,
    clock,
    digest,
)


def cutoff(as_of=None, sequence=None):
    if sequence is not None and (
        as_of is None or type(sequence) is not int or not 0 <= sequence <= MAX_SEQUENCES
    ):
        raise ValueError("OTC sequence cutoff requires an aware clock and integer in 0..100000")
    if as_of is None:
        return ("9999-12-31T23:59:59.999999Z", MAX_SEQUENCES)
    try:
        stamp = clock(
            datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if isinstance(as_of, str)
            else as_of
        )
    except (ValueError, TypeError, AttributeError):
        raise ValueError("OTC as-of cutoff requires an aware timestamp") from None
    return stamp, sequence if sequence is not None else MAX_SEQUENCES


def version_record(row):
    v = verify_version(store._record(row["record_json"], 64 * 1024 * 1024))
    p = v["partition"]
    if (
        v["version_id"] != row["version_id"]
        or v["capture_id"] != row["capture_id"]
        or v["capture_sequence"] != row["capture_sequence"]
        or (
            p["reporting_week_start"],
            p["product"],
            p["tier"],
            v["symbol_raw"],
            v["participant_key"],
        )
        != (row["week"], row["product"], row["tier"], row["symbol_raw"], row["participant_key"])
        or v["first_seen_at"] != v["captured_at"]
        or ("origin_status" in row.keys() and row["origin_status"] != "PUBLISHED")
        or ("origin_clock" in row.keys() and row["origin_clock"] != v["captured_at"])
    ):
        raise ValueError("OTC immutable version index/lineage mismatch")
    return v


def snapshot(db, capture_id, max_rows):
    row = db.execute("SELECT * FROM otc2_snapshots WHERE capture_id=?", (capture_id,)).fetchone()
    if row is None:
        raise ValueError("OTC successful attempt has no committed snapshot")
    record = store._record(row["record_json"])
    attempt = store._attempt(
        db.execute("SELECT * FROM otc2_attempts WHERE capture_id=?", (capture_id,)).fetchone()
    )
    required = {
        "coverage",
        "members_sha256",
        "previous_capture_id",
        "versions_created",
        "removed_rows",
    }
    if (
        attempt is None
        or set(record) != (set(attempt) - {"status", "reason"}) | required
        or not isinstance(record.get("coverage"), dict)
        or type(record["coverage"].get("row_count")) is not int
        or type(record.get("versions_created")) is not int
        or type(record.get("removed_rows")) is not int
    ):
        raise ValueError("OTC snapshot fields differ from the committed contract")
    if (
        record["capture_id"] != capture_id
        or record["capture_sequence"] != row["capture_sequence"]
        or record["captured_at"] != row["captured_at"]
        or row["record_sha256"] != digest(record)
        or attempt is None
        or attempt["status"] != "PUBLISHED"
        or any(record.get(k) != v for k, v in attempt.items() if k not in ("status", "reason"))
    ):
        raise ValueError("OTC snapshot, terminal attempt or index mismatch")
    members = db.execute(
        """SELECT m.symbol_raw AS member_symbol,m.participant_key AS member_key,
        m.version_id AS member_version,m.tokens_json,v.*,
        a.status AS origin_status,a.captured_at AS origin_clock FROM otc2_members m
        JOIN otc2_versions v ON v.version_id=m.version_id
        JOIN otc2_attempts a ON a.capture_id=v.capture_id WHERE m.capture_id=?
        ORDER BY m.symbol_raw,m.participant_key LIMIT ?""",
        (capture_id, max_rows + 1),
    ).fetchall()
    if len(members) > max_rows:
        raise ValueError("OTC panel exceeds its requested row budget")
    if (
        sum(len(m["record_json"].encode()) + len(m["tokens_json"].encode()) for m in members)
        > 64 * 1024 * 1024
    ):
        raise ValueError("OTC snapshot exceeds its 64 MiB read budget")
    fingerprints = []
    rows = []
    for member in members:
        version = version_record(member)
        original = store._record(member["tokens_json"])
        if (
            version["state"] != "OBSERVED"
            or version["partition"]
            != {"reporting_week_start": row["week"], "product": row["product"], "tier": row["tier"]}
            or (version["symbol_raw"], version["participant_key"])
            != (member["member_symbol"], member["member_key"])
            or version["capture_sequence"] > record["capture_sequence"]
            or version["captured_at"] > record["captured_at"]
        ):
            raise ValueError("OTC membership references an incompatible or future version")
        tokens({**version["content"], **original})
        fingerprints.append(
            {
                "symbol_raw": member["member_symbol"],
                "participant_key": member["member_key"],
                "version_id": member["member_version"],
                "tokens": original,
            }
        )
        rows.append(
            {
                **version["content"],
                **{k: version[k] for k in LINEAGE_FIELDS},
                "first_seen_at": version["first_seen_at"],
                "version_id": version["version_id"],
                "content_sha256": version["content_sha256"],
                **original,
                "membership_capture_id": capture_id,
                "membership_sequence": record["capture_sequence"],
                "membership_captured_at": record["captured_at"],
                "membership_receipt_sha256": record["receipt_sha256"],
            }
        )
    created = db.execute(
        "SELECT count(*) FROM otc2_versions WHERE capture_id=?", (capture_id,)
    ).fetchone()[0]
    if (
        record["coverage"]["row_count"] != len(rows)
        or record["row_count"] != len(rows)
        or record["members_sha256"] != digest(fingerprints)
        or record["versions_created"] != created
    ):
        raise ValueError("OTC snapshot membership, coverage or version count mismatch")
    return {**record, "status": "PUBLISHED", "reason": None}, rows


def _where_cutoff():
    return "(captured_at<? OR (captured_at=? AND capture_sequence<=?))"


def _latest(db, key, bound):
    stamp, seq = bound
    row = db.execute(
        "SELECT * FROM otc2_attempts WHERE week=? AND product=? AND tier=? AND "
        + _where_cutoff()
        + " ORDER BY capture_sequence DESC LIMIT 1",
        (*key, stamp, stamp, seq),
    ).fetchone()
    candidates = [store._attempt(row)] if row else []
    legacy = db.execute(
        "SELECT * FROM otc_attempts WHERE week=? AND product=? AND tier=? "
        "ORDER BY capture_sequence DESC LIMIT 1001",
        key,
    ).fetchall()
    if len(legacy) > 1000:
        raise ValueError("OTC retained legacy attempt budget exceeded")
    for row in legacy:
        value = store._attempt(row)
        if (value["captured_at"], value["capture_sequence"]) <= bound:
            candidates.append(value)
            break
    return max(
        candidates,
        key=lambda v: (v["capture_sequence"], v.get("transform_version") == TRANSFORM),
        default=None,
    )


def inspect(db, selection, max_rows, bound):
    items = []
    rows = []
    stamp, seq = bound
    for p in selection:
        scope = {
            "reporting_week_start": p.reporting_week_start.isoformat(),
            "product": p.product,
            "tier": p.tier,
        }
        key = (scope["reporting_week_start"], scope["product"], scope["tier"])
        selected = db.execute(
            "SELECT capture_id FROM otc2_snapshots WHERE week=? AND product=? AND tier=? AND "
            + _where_cutoff()
            + " ORDER BY capture_sequence DESC LIMIT 1",
            (*key, stamp, stamp, seq),
        ).fetchone()
        success = db.execute(
            "SELECT capture_id FROM otc2_attempts WHERE week=? AND product=? AND tier=? "
            "AND status='PUBLISHED' AND "
            + _where_cutoff()
            + " ORDER BY capture_sequence DESC LIMIT 1",
            (*key, stamp, stamp, seq),
        ).fetchone()
        if (selected is None) != (success is None) or (selected and selected[0] != success[0]):
            raise ValueError("OTC committed snapshot selection is inconsistent")
        publication, population = (
            snapshot(db, selected[0], max_rows - len(rows)) if selected else (None, [])
        )
        rows.extend(population)
        items.append(
            {
                "partition": scope,
                "publication": publication,
                "latest_attempt": _latest(db, key, bound),
            }
        )
    published = sum(i["publication"] is not None for i in items)
    if not published:
        status = "UNAVAILABLE_FIXTURE"
    elif published < len(items):
        status = "PARTIAL_FIXTURE"
    elif any(i["latest_attempt"]["capture_id"] != i["publication"]["capture_id"] for i in items):
        status = "DEGRADED_FIXTURE"
    else:
        status = "COMPLETE_FIXTURE"
    if sum(len(store.encoded(r)) for r in rows) > 64 * 1024 * 1024:
        raise ValueError("OTC panel exceeds 64 MiB")
    return {
        "format": "finra-otc-local-panel/2",
        "origin": "INVENTED_FIXTURE",
        "source_conformance": "NOT_CHECKED",
        "knowledge_basis": "OBSERVED_CAPTURE",
        "status": status,
        "partitions": items,
        "rows": rows,
    }


def history(db, selection, max_versions, bound):
    versions = []
    stamp, seq = bound
    bytes_read = 0
    for p in selection:
        rows = db.execute(
            """SELECT v.*,a.status AS origin_status,a.captured_at AS origin_clock
            FROM otc2_versions v JOIN otc2_attempts a ON a.capture_id=v.capture_id
            WHERE v.week=? AND v.product=? AND v.tier=? AND
            (a.captured_at<? OR (a.captured_at=? AND a.capture_sequence<=?))
            ORDER BY v.capture_sequence,v.symbol_raw,v.participant_key LIMIT ?""",
            (
                p.reporting_week_start.isoformat(),
                p.product,
                p.tier,
                stamp,
                stamp,
                seq,
                max_versions - len(versions) + 1,
            ),
        ).fetchall()
        if len(versions) + len(rows) > max_versions:
            raise ValueError("OTC version read budget exceeded")
        bytes_read += sum(len(r["record_json"].encode()) for r in rows)
        if bytes_read > 64 * 1024 * 1024:
            raise ValueError("OTC history exceeds 64 MiB")
        expected = 0
        snapshots = db.execute(
            "SELECT record_json,record_sha256 FROM otc2_snapshots "
            "WHERE week=? AND product=? AND tier=? AND " + _where_cutoff(),
            (p.reporting_week_start.isoformat(), p.product, p.tier, stamp, stamp, seq),
        )
        for snapshot_record in snapshots:
            bytes_read += len(snapshot_record[0].encode())
            if bytes_read > 64 * 1024 * 1024:
                raise ValueError("OTC history inventory exceeds 64 MiB")
            descriptor = store._record(snapshot_record[0])
            if (
                snapshot_record[1] != digest(descriptor)
                or type(descriptor["versions_created"]) is not int
            ):
                raise ValueError("OTC history snapshot evidence is inconsistent")
            expected += descriptor["versions_created"]
        if expected != len(rows):
            raise ValueError("OTC history is missing immutable occurrences")
        population = [version_record(r) for r in rows]
        heads = {}
        for version in population:
            key = (version["symbol_raw"], version["participant_key"])
            previous = heads.get(key)
            if version["previous_version_id"] != (previous["version_id"] if previous else None) or (
                previous
                and (
                    previous["capture_sequence"] >= version["capture_sequence"]
                    or previous["content_sha256"] == version["content_sha256"]
                )
            ):
                raise ValueError("OTC immutable revision chain is missing or inconsistent")
            heads[key] = version
        versions.extend(population)
    return {"format": "finra-otc-history/1", "origin": "INVENTED_FIXTURE", "versions": versions}
