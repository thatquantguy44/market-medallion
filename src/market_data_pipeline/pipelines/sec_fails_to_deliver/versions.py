"""Source-owned knowledge cutoffs and observed-version helpers; no I/O."""

import hashlib
import json
from datetime import UTC, datetime

from market_data_pipeline.pipelines.sec_fails_to_deliver.completeness import CompletenessEvidence


def knowledge_cutoff(as_of: datetime | None, sequence: int | None = None) -> str | None:
    if sequence is not None and (
        type(sequence) is not int or not 0 <= sequence < 2**63 or as_of is None
    ):
        raise ValueError("sequence cutoff requires an aware timestamp and integer in [0,2^63)")
    if as_of is None:
        return None
    if not isinstance(as_of, datetime) or as_of.utcoffset() is None:
        raise ValueError("as-of cutoff requires a timezone-aware datetime")
    return as_of.astimezone(UTC).isoformat(timespec="microseconds")


def withdrawals(
    previous, incoming, coverage, audit, capture, evidence: CompletenessEvidence | None
):
    """Derive explicit tombstones only from independently validated full scope."""
    if coverage["coverage_state"] not in ("COMPLETE", "COMPLETE_EMPTY"):
        return []
    if evidence is None or capture is None or coverage["coverage_state"] == "COMPLETE_EMPTY":
        raise ValueError(
            "complete SEC replacement requires admitted nonempty scope and capture evidence"
        )
    dates = tuple(sorted({row["settlement_date"] for row in incoming}))
    evidence.validate(raw_sha256=coverage["raw_sha256"], observed_dates=dates)
    if (
        (coverage["period_start"], coverage["period_end"])
        != (evidence.period_start, evidence.period_end)
        or capture["raw_member"] != evidence.member_name
        or coverage["expected_date_count"] != len(evidence.expected_dates)
        or coverage["observed_date_count"] != len(dates)
        or coverage["trailer_record_count"] != len(incoming)
        or coverage["trailer_quantity_text"]
        != str(sum(int(row["quantity_fails"]) for row in incoming))
    ):
        raise ValueError("complete replacement scope/count evidence is inconsistent")
    keys = {(row["settlement_date"], row["cusip"]) for row in incoming}
    result = []
    for old in previous:
        if old["row_state"] == "WITHDRAWN" or (old["settlement_date"], old["cusip"]) in keys:
            continue
        row = dict(old)
        for name in (
            "symbol_raw",
            "description_raw",
            "quantity_fails",
            "quantity_fails_text",
            "prior_close_price",
            "prior_close_price_text",
            "price_missing_reason",
            "source_row_number",
        ):
            row[name] = None
        content = hashlib.sha256(
            json.dumps(
                {
                    "settlement_date": old["settlement_date"].isoformat(),
                    "cusip": old["cusip"],
                    "row_state": "WITHDRAWN",
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        version = hashlib.sha256(
            f"{audit['run_id']}:{old['settlement_date']}:{old['cusip']}:{content}".encode()
        ).hexdigest()
        row.update(
            row_state="WITHDRAWN",
            content_hash=content,
            version_id=version,
            run_id=audit["run_id"],
            capture_sequence=audit["capture_sequence"],
            first_seen_at=audit["captured_at"],
            retrieved_at=audit["captured_at"],
            raw_sha256=coverage["raw_sha256"],
            member_sha256=capture["member_sha256"],
            raw_member=capture["raw_member"],
            parser_version=capture["parser_version"],
            provider_published_at=None,
            source_updated_at=None,
        )
        result.append(row)
    return result
