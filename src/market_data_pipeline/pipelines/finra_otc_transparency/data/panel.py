"""OTC-AC06 local fixture membership evidence and lossless panel records."""

from collections import Counter
from dataclasses import asdict

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import profile_for
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import encoded

POPULATION = "finra-otc-fixture-population/1"
PANEL = "finra-otc-local-panel/1"
ORIGIN = "INVENTED_FIXTURE"
BASES = ("PROVIDER_ID", "SOURCE_ROW_LABEL", "AGGREGATED", "UNKNOWN")


def fixture_assertion(partition, schema_profile, payload_sha256, expected_rows):
    if type(expected_rows) is not int or not 0 <= expected_rows <= 1000000:
        raise ValueError("OTC fixture expected rows must be an integer in 0..1000000")
    return {
        "format": POPULATION,
        "origin": ORIGIN,
        "partition": {
            **asdict(partition),
            "reporting_week_start": partition.reporting_week_start.isoformat(),
        },
        "schema_profile": schema_profile,
        "expected_parts": ["file"],
        "payload_sha256": payload_sha256,
        "expected_rows": expected_rows,
    }


def prepare(capture, result):
    """Received-all and parse success alone never admit local membership."""
    receipt, parsed = result.receipt, result.parsed
    if parsed is None:
        return result.outcome["status"], result.outcome["reason"], None, []
    if capture.discovery is None:
        return "UNVERIFIED", "FIXTURE_ASSERTION_REQUIRED", None, []
    expected = fixture_assertion(
        parsed.partition, parsed.schema_profile, parsed.payload_sha256, len(parsed.rows)
    )
    # Exact canonical bytes distinguish booleans from integer row counts, as well
    # as rejecting extra fields, duplicate keys and altered scope/hash assertions.
    if capture.discovery != encoded(expected):
        return "REJECTED", "FIXTURE_ASSERTION_MISMATCH", None, []
    if receipt["expected_parts"] != ["file"] or set(capture.parts) != {"file"}:
        return "REJECTED", "FIXTURE_ASSERTION_MISMATCH", None, []
    counts = Counter(row.identity.identity_basis for row in parsed.rows)
    profile = profile_for(parsed.partition.product, parsed.partition.tier)
    coverage = {
        "status": "COMPLETE_FIXTURE" if parsed.rows else "COMPLETE_EMPTY_FIXTURE",
        "origin": ORIGIN,
        "source_conformance": "NOT_CHECKED",
        "row_count": len(parsed.rows),
        "attribution_rows": {basis: counts[basis] for basis in BASES},
        "expected_delay_weeks": profile.expected_delay_weeks,
        "expected_parts": receipt["expected_parts"],
        "received_parts": [p["part_id"] for p in receipt["parts"]],
        "fixture_assertion_sha256": receipt["discovery"]["sha256"],
        "bronze_membership": receipt["membership"],
    }
    rows = []
    for row in parsed.rows:
        rows.append(
            {
                **receipt["partition"],
                "scope_id": row.partition.scope_id,
                "schema_profile": parsed.schema_profile,
                "origin": ORIGIN,
                "symbol_raw": row.symbol_raw,
                "issue_name_raw": row.issue_name_raw,
                "tier_raw": row.tier_raw,
                "participant_key": row.identity.require_key(),
                "participant_name_raw": row.participant_name_raw,
                "mpid": row.mpid,
                "firm_crd": row.firm_crd,
                "identity_basis": row.identity.identity_basis,
                "identifier_kind": row.identity.identifier_kind,
                "weekly_shares": str(row.weekly_shares),
                "weekly_trades": str(row.weekly_trades),
                "weekly_shares_text": row.weekly_shares_text,
                "weekly_trades_text": row.weekly_trades_text,
                "provider_update_date": row.provider_update_date.isoformat(),
                "provider_initial_publication_date": (
                    row.provider_initial_publication_date.isoformat()
                    if row.provider_initial_publication_date
                    else None
                ),
                "provider_published_at": receipt["provider_published_at"],
                "captured_at": receipt["captured_at"],
                "first_seen_at": receipt["captured_at"],
                "capture_id": receipt["capture_id"],
                "run_id": receipt["run_id"],
                "capture_sequence": receipt["capture_sequence"],
                "receipt_sha256": receipt["receipt_sha256"],
                "manifest_sha256": receipt["manifest_sha256"],
                "schema_sha256": receipt["schema_sha256"],
            }
        )
    return "PUBLISHED", None, coverage, rows
