"""Offline T17 reconciliation of the authorized ten-file threshold capture.

Inputs and provider rows remain local. The output contains hashes, counts,
clocks and coverage only; it does not establish rights beyond the recorded
operator attestation or certify other markets/history.
"""

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.manifest import load_manifest
from market_data_pipeline.pipelines.reg_sho_thresholds.parser import parse_list
from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import warehouse_for


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def reconcile(config_path):
    config = load_config(config_path)
    require(
        not config.acquisition.enabled and not config.databricks.enabled,
        "T17 review requires acquisition and Databricks disabled",
    )
    require(len(config.partitions) == 10, "T17 requires exactly ten selected lists")
    with warehouse_for(config, read_only=True) as warehouse:
        membership = warehouse.select_membership()
        coverage = warehouse.select_coverage()
        audits = warehouse.select_runs()
    rows_by_key = {
        (row["provider"], row["market"], row["list_date"], row["raw_symbol"]): row
        for row in membership
    }
    require(len(rows_by_key) == len(membership), "duplicate retained threshold membership")
    coverage_by_key = {(row["provider"], row["market"], row["list_date"]): row for row in coverage}
    audit_by_id = {row["run_id"]: row for row in audits}
    reports, total = [], 0
    for selected in config.partitions.values():
        require(
            selected.provider in ("nasdaq", "nyse")
            and selected.market == ("NASDAQ" if selected.provider == "nasdaq" else "NYSE")
            and selected.list_date.year == 2026
            and selected.list_date.month == 10
            and 26 <= selected.list_date.day <= 30,
            "T17 sample selection differs",
        )
        manifest = load_manifest(selected.manifest)
        require(
            not selected.input.is_symlink() and selected.input.stat().st_size <= manifest.max_bytes,
            "threshold local sample is unavailable or oversized",
        )
        payload = selected.input.read_bytes()
        raw_hash = hashlib.sha256(payload).hexdigest()
        parsed = parse_list(payload, manifest, list_date=selected.list_date, market=selected.market)
        lines = payload.decode(manifest.encoding).splitlines()
        require(lines[0].split("|") == list(manifest.header), "independent header differs")
        require(len(lines[1:-1]) == len(parsed.rows), "independent row count differs")
        require(lines[-1] == parsed.source_created_at_raw, "independent trailer differs")
        scope = (selected.provider, selected.market, selected.list_date)
        covered = coverage_by_key[scope]
        audit = audit_by_id[covered["run_id"]]
        require(
            audit["status"] == "SUCCEEDED" and covered["received_row_count"] == len(parsed.rows),
            "retained threshold coverage/audit differs",
        )
        expected_state = "COMPLETE" if parsed.rows else "COMPLETE_EMPTY"
        require(
            covered["coverage_state"] == expected_state, "retained threshold coverage state differs"
        )
        require(
            covered["source_created_at_raw"] == lines[-1], "retained source creation token differs"
        )
        require(covered["raw_sha256"] == raw_hash, "retained threshold digest differs")
        bronze = config.root / "bronze" / covered["run_id"]
        receipt = json.loads((bronze / "receipt.json").read_text())
        require(digest(bronze / "payload.bin") == raw_hash, "threshold Bronze payload differs")
        require(
            receipt["source_url"] == selected.source_url
            and receipt["transport"] == "https"
            and receipt["origin_retrieved_at"] is not None,
            "threshold HTTPS provenance differs",
        )
        for number, (line, row) in enumerate(zip(lines[1:-1], parsed.rows, strict=True), 2):
            fields = line.split("|")
            flags = dict(market_category=fields[2], reg_sho_threshold_flag=fields[3])
            if selected.provider == "nasdaq":
                flags.update(rule_3210=fields[4], filler=fields[5])
            else:
                flags.update(filler_1=fields[4], filler_2=fields[5])
            require(
                (row.raw_symbol, row.security_name_raw, row.market_category, row.source_row_number)
                == (*fields[:3], number)
                and json.loads(row.flags_raw_json) == flags,
                "independent threshold field comparison failed",
            )
            stored = rows_by_key[(*scope, fields[0])]
            require(
                stored["security_name_raw"] == fields[1]
                and json.loads(stored["flags_raw_json"]) == flags
                and stored["source_row_number"] == number
                and stored["raw_sha256"] == raw_hash
                and stored["capture_sequence"] == covered["capture_sequence"]
                and stored["first_seen_at"] == covered["first_seen_at"]
                and stored["provider_published_at"] is None,
                "retained threshold normalized values or clocks differ",
            )
        require(digest(selected.input) == raw_hash, "threshold sample changed during review")
        total += len(parsed.rows)
        reports.append(
            {
                "provider": selected.provider,
                "market": selected.market,
                "list_date": selected.list_date.isoformat(),
                "file": selected.input.name,
                "source_url": selected.source_url,
                "raw_sha256": raw_hash,
                "payload_bytes": len(payload),
                "row_count": len(parsed.rows),
                "coverage_state": expected_state,
                "source_created_at_raw": lines[-1],
                "source_created_at_timezone": "UNKNOWN_NOT_INFERRED",
                "retrieved_at": receipt["origin_retrieved_at"],
                "first_seen_at": covered["first_seen_at"].isoformat(),
                "all_fields_and_bronze_compared": True,
            }
        )
    require(
        total == len(membership) and len(coverage) == len(audits) == 10,
        "retained ten-list counts differ",
    )
    return {
        "schema_version": "spec011-threshold-sample-review/1",
        "verified_at": datetime.now(UTC).isoformat(),
        "files": reports,
        "membership_rows_compared": total,
        "list_count": 10,
        "source_selection": "NASDAQ and NYSE, 2026-10-26 through 2026-10-30 only",
        "provider_requests_during_review": 0,
        "authorization_basis": "USER_ATTESTED_VERBAL_PROVIDER_PERMISSION",
        "permission_independently_verified": False,
        "historical_publication_vintages": "NOT_RECONSTRUCTED",
        "other_markets_and_dates": "NOT_CERTIFIED",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(
        args.output.suffix == ".json"
        and not args.output.resolve().is_relative_to(load_config(args.config).root),
        "review output must not modify the retained warehouse",
    )
    report = reconcile(args.config)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": "VERIFIED",
                "lists": report["list_count"],
                "rows": report["membership_rows_compared"],
                "report": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()
