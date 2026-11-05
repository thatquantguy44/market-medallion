"""Reconcile only the two approved September SEC archives and their retained snapshot.

This offline review prints aggregates and hashes only. It never downloads data,
creates a warehouse, changes knowledge clocks or certifies population coverage.
"""

import argparse
import hashlib
import json
import zipfile
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from market_data_pipeline.pipelines.sec_fails_to_deliver.data import parse_archive
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import load_manifest
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import current_snapshot

PINS = {
    "202609a": "46c12079fe62d84278e9d06f97cfa34eb9ebef5fca709fc856a49e3257ac2f01",
    "202609b": "a89615d5b7fe1e8dd0fc2872406467ac71e499f0d8216ef9c0afb8cc98bdd99e",
}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def source_fields(row):
    return (
        row.settlement_date,
        row.cusip,
        row.symbol_raw,
        row.quantity_fails,
        row.description_raw,
        row.prior_close_price,
        row.price_missing_reason,
        row.source_row_number,
        row.quantity_fails_raw,
        row.prior_close_price_raw,
    )


def reconcile(data_dir, warehouse_root):
    reports, expected = [], {}
    for stamp, pin in PINS.items():
        path = data_dir / f"cnsfails{stamp}.zip"
        manifest_path = Path(f"manifests/sec-fails-to-deliver-{stamp}.yml")
        require(
            not path.is_symlink() and path.stat().st_size <= 8 * 1024 * 1024,
            "approved SEC sample is unavailable or oversized",
        )
        payload = path.read_bytes()
        require(hashlib.sha256(payload).hexdigest() == pin, "approved SEC archive digest changed")
        manifest = load_manifest(manifest_path)
        parsed = parse_archive(payload, manifest)
        with zipfile.ZipFile(path) as archive:
            member = archive.read(parsed.member_name)
        lines = member.decode("ascii").splitlines()
        tokens = [line.split("|") for line in lines[1:-2]]
        require(len(tokens) == len(parsed.observations), "independent SEC row count differs")
        for number, (fields, observation) in enumerate(
            zip(tokens, parsed.observations, strict=True), 2
        ):
            raw_date, cusip, symbol, quantity, description, price = fields
            settled = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:]))
            independent = (
                settled,
                cusip,
                symbol or None,
                int(quantity),
                description or None,
                None if price == "." else Decimal(price),
                "SOURCE_PRICE_UNAVAILABLE" if price == "." else None,
                number,
                quantity,
                price,
            )
            require(
                source_fields(observation) == independent, "independent SEC field comparison failed"
            )
            key = (settled, cusip)
            require(key not in expected, "approved SEC samples contain overlapping natural keys")
            expected[key] = (
                independent,
                parsed.raw_sha256,
                parsed.member_sha256,
                parsed.member_name,
            )
        total = sum(int(fields[3]) for fields in tokens)
        require(
            lines[-2:]
            == [f"Trailer record count {len(tokens)}", f"Trailer total quantity of shares {total}"],
            "independent SEC trailer comparison failed",
        )
        require(digest(path) == pin, "approved SEC file changed during review")
        reports.append(
            {
                "file": path.name,
                "raw_sha256": pin,
                "raw_bytes": len(payload),
                "member_sha256": parsed.member_sha256,
                "member_bytes": len(member),
                "manifest_sha256": digest(manifest_path),
                "row_count": len(tokens),
                "quantity_fails_total": total,
                "all_fields_compared": True,
                "date_counts": dict(sorted(Counter(fields[0] for fields in tokens).items())),
                "missing_prices": sum(fields[5] == "." for fields in tokens),
                "missing_symbols": sum(not fields[2] for fields in tokens),
                "leading_zero_cusip_rows": sum(fields[1].startswith("0") for fields in tokens),
                "source_coverage": "NOT_CERTIFIED",
                "input_provenance": "APPROVED_LOCAL_FILE_ORIGINAL_RETRIEVAL_UNKNOWN",
            }
        )
    snapshot = current_snapshot(warehouse_root)
    manifest = json.loads((snapshot / "manifest.json").read_text())
    protected = [warehouse_root / "parquet/current.json", snapshot / "manifest.json"]
    protected.extend(
        snapshot / f"{name}.parquet" for name in ("facts", "gold", "coverage", "audit")
    )
    before = {str(path): digest(path) for path in protected}
    tables = {}
    for name in ("facts", "gold", "coverage", "audit"):
        path = snapshot / f"{name}.parquet"
        require(digest(path) == manifest["files"][name], "retained snapshot file digest differs")
        tables[name] = pq.read_table(path).to_pylist()
    for name in ("facts", "gold"):
        require(len(tables[name]) == len(expected), "retained SEC snapshot row count differs")
        actual_keys = set()
        for row in tables[name]:
            key = (row["settlement_date"], row["cusip"])
            require(key in expected and key not in actual_keys, "retained SEC snapshot keys differ")
            actual_keys.add(key)
            independent, raw_hash, member_hash, member_name = expected[key]
            actual = (
                row["settlement_date"],
                row["cusip"],
                row["symbol_raw"],
                row["quantity_fails"],
                row["description_raw"],
                row["prior_close_price"],
                row["price_missing_reason"],
                row["source_row_number"],
                row["quantity_fails_text"],
                row["prior_close_price_text"],
            )
            require(
                actual == independent and row["row_state"] == "REPORTED",
                "retained SEC snapshot source values differ",
            )
            require(
                (row["raw_sha256"], row["member_sha256"], row["raw_member"])
                == (raw_hash, member_hash, member_name),
                "retained SEC raw lineage differs",
            )
            require(
                row["provider_published_at"] is row["source_updated_at"] is None,
                "unobserved SEC publication clock was supplied",
            )
    require(
        all(row["coverage_state"] == "PARTIAL" for row in tables["coverage"]),
        "real SEC coverage must remain PARTIAL",
    )
    require(
        all(row["status"] == "SUCCEEDED" for row in tables["audit"]),
        "retained SEC audit has an unsuccessful capture",
    )
    require(
        before == {str(path): digest(path) for path in protected},
        "retained SEC snapshot changed during review",
    )
    return {
        "schema_version": "spec011-sec-sample-review/1",
        "verified_at": datetime.now(UTC).isoformat(),
        "provider_requests": 0,
        "files": reports,
        "total_facts_compared": len(expected),
        "snapshot_id": snapshot.name,
        "snapshot_manifest_sha256": digest(snapshot / "manifest.json"),
        "snapshot_row_counts": {name: len(rows) for name, rows in tables.items()},
        "snapshot_and_sources_unchanged": True,
        "first_seen_min": min(row["first_seen_at"] for row in tables["facts"]).isoformat(),
        "first_seen_max": max(row["first_seen_at"] for row in tables["facts"]).isoformat(),
        "original_publication_and_retrieval": "UNKNOWN_NOT_RECONSTRUCTED",
        "source_population_coverage": "PARTIAL_NOT_CERTIFIED",
        "exchange_conformance": "NOT_ESTABLISHED_BY_THIS_SEC_REVIEW",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/sec_ftd"))
    parser.add_argument(
        "--warehouse-root", type=Path, default=Path("data/sec_fails_to_deliver/t06-verification")
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(
        args.output.suffix == ".json"
        and not args.output.resolve().is_relative_to(args.warehouse_root.resolve()),
        "review output must be JSON outside the retained warehouse",
    )
    report = reconcile(args.data_dir, args.warehouse_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "status": "VERIFIED",
                "facts_compared": report["total_facts_compared"],
                "provider_requests": 0,
                "report": str(args.output),
            }
        )
    )


if __name__ == "__main__":
    main()
