"""OTC-T06 explicit fixture-only local commands. No environment or network access."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from market_data_pipeline.pipelines.finra_otc_transparency import pipeline
from market_data_pipeline.pipelines.finra_otc_transparency.catalog import (
    PROFILES,
    select_partitions,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import _read
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import DEFAULT_BYTES, limits
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import load_manifest


def fixture_use():
    return {
        "dataset": "finra-otc-transparency",
        "purpose": "personal_noncommercial_internal_research",
        "route": "invented_fixture",
        "authorization": "pending",
        "permission_reference": None,
        "credential_type": None,
        "provider_dataset": None,
        "attribution": None,
        "operations": ["fixture"],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="FINRA OTC local invented-fixture panel (no live source)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="Validate the current fixture manifest")
    validate.add_argument("--manifest", type=Path, required=True)
    for name in ("run", "ingest-capture", "inspect", "history", "migrate-warehouse"):
        command = commands.add_parser(name)
        command.add_argument("--root", type=Path, required=True)
        command.add_argument(
            "--invented-fixture",
            action="store_true",
            required=True,
            help="Acknowledge invented data only; this cannot admit real observations",
        )
        if name == "run":
            command.add_argument("--manifest", type=Path, required=True)
            command.add_argument("--input", type=Path, required=True)
            command.add_argument("--week", required=True)
            command.add_argument(
                "--product", choices=sorted({p.product for p in PROFILES}), required=True
            )
            command.add_argument("--tier", choices=("T1", "T2", "OTCE"), required=True)
            command.add_argument(
                "--expected-rows",
                type=int,
                help="Explicit synthetic complete-population assertion; 0 admits fixture empty",
            )
            command.add_argument("--max-bytes", type=int, default=DEFAULT_BYTES)
            command.add_argument("--max-rows", type=int, default=10000)
            command.add_argument("--run-id")
            command.add_argument("--capture-id")
        elif name == "ingest-capture":
            command.add_argument("--capture-id", required=True)
            command.add_argument("--max-bytes", type=int, default=DEFAULT_BYTES)
        elif name == "migrate-warehouse":
            command.add_argument("--max-captures", type=int, default=1000)
        else:
            command.add_argument(
                "--partition",
                action="append",
                required=True,
                help="Explicit WEEK:PRODUCT:TIER; repeat for bounded multi-population selection",
            )
            if name == "history":
                command.add_argument("--max-versions", type=int, default=10000)
            else:
                command.add_argument("--max-rows", type=int, default=10000)
            command.add_argument("--as-of")
            command.add_argument("--as-of-sequence", type=int)
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            manifest = load_manifest(args.manifest)
            value = {
                "status": "VALID",
                "source_conformance": "NOT_CHECKED",
                "profiles": [p.to_dict() for p in manifest.profiles],
            }
        elif args.command == "run":
            limits(args.max_bytes, args.max_rows)
            if args.max_rows > 10000:
                raise ValueError("OTC local panel supports at most 10000 rows")
            manifest = load_manifest(args.manifest)
            (partition,) = select_partitions(
                [{"reporting_week_start": args.week, "product": args.product, "tier": args.tier}]
            )
            value = pipeline.run_fixture(
                args.root,
                payload=_read(args.input, args.max_bytes),
                partition=partition,
                manifest=manifest,
                approved_use=fixture_use(),
                expected_rows=args.expected_rows,
                run_id=args.run_id,
                capture_id=args.capture_id,
                max_bytes=args.max_bytes,
                max_rows=args.max_rows,
            )
        elif args.command == "ingest-capture":
            value = pipeline.publish_capture(
                args.root, args.capture_id, approved_use=fixture_use(), max_bytes=args.max_bytes
            )
        elif args.command == "migrate-warehouse":
            value = pipeline.migrate_warehouse(
                args.root, approved_use=fixture_use(), max_captures=args.max_captures
            )
        else:
            selections = []
            for token in args.partition:
                fields = token.split(":")
                if len(fields) != 3:
                    raise ValueError("OTC inspect partition must be WEEK:PRODUCT:TIER")
                selections.append(
                    dict(zip(("reporting_week_start", "product", "tier"), fields, strict=True))
                )
            selected = select_partitions(selections)
            if args.command == "history":
                value = pipeline.observation_history(
                    args.root,
                    partitions=selected,
                    approved_use=fixture_use(),
                    max_versions=args.max_versions,
                    as_of=args.as_of,
                    as_of_sequence=args.as_of_sequence,
                )
                value["status"] = "HISTORY"
            else:
                value = pipeline.inspect_panel(
                    args.root,
                    partitions=selected,
                    approved_use=fixture_use(),
                    max_rows=args.max_rows,
                    as_of=args.as_of,
                    as_of_sequence=args.as_of_sequence,
                )
    except (ValueError, OSError, sqlite3.Error):
        print(
            json.dumps({"status": "ERROR", "error": "OTC_LOCAL_OPERATION_FAILED"}), file=sys.stderr
        )
        return 1
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))
    return (
        0
        if value["status"]
        in ("VALID", "PUBLISHED", "COMPLETE_FIXTURE", "MIGRATED", "ALREADY_CURRENT", "HISTORY")
        else 2
    )
