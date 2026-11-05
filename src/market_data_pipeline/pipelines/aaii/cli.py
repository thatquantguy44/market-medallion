import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.aaii.config import RuntimeConfig, load_config, resolve_runtime
from market_data_pipeline.pipelines.aaii.io.warehouse import Warehouse
from market_data_pipeline.pipelines.aaii.manifest import load_manifest
from market_data_pipeline.pipelines.aaii.pipeline import export, run
from market_data_pipeline.pipelines.aaii.replay import replay
from market_data_pipeline.shared.files import utc_timestamp


def warehouse_dsn(backend: str) -> str | None:
    return resolve_runtime(warehouse=backend)[2]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="AAII medallion pipeline and recovery")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="validate a manifest without writes")
    validate.add_argument(
        "--manifest", type=Path, default=Path("manifests/aaii-investor-sentiment.yml")
    )
    for command in (
        "run",
        "weekly",
        "backfill",
        "export",
        "gold",
        "replay",
        "check",
        "backup",
        "restore",
        "retain",
    ):
        sub = commands.add_parser(command)
        sub.add_argument("--root", type=Path)
        sub.add_argument("--warehouse", choices=("sqlite", "postgres"))
        sub.add_argument("--config", type=Path)
        if command == "run":
            inputs = sub.add_mutually_exclusive_group(required=True)
            inputs.add_argument("--input", type=Path)
            inputs.add_argument("--live", action="store_true")
        if command in ("run", "weekly", "backfill"):
            sub.add_argument(
                "--manifest",
                type=Path,
                default=Path(
                    "manifests/aaii-investor-sentiment.yml"
                    if command == "run"
                    else "manifests/aaii-investor-sentiment-xls.yml"
                ),
            )
        if command in ("run", "backfill"):
            sub.add_argument("--start", required=command == "backfill")
            sub.add_argument("--end", required=command == "backfill")
        if command == "backfill":
            sub.add_argument("--input", type=Path, required=True)
            sub.add_argument("--quarantine-invalid", action="store_true")
        if command in ("weekly", "gold", "check"):
            sub.add_argument("--as-of", help="timezone-aware ISO timestamp")
        if command == "replay":
            sub.add_argument("--source-root", type=Path, required=True)
        if command == "backup":
            sub.add_argument("--destination", type=Path, required=True)
        if command == "restore":
            sub.add_argument("--backup", type=Path, required=True)
        if command == "retain":
            sub.add_argument("--keep", type=int, default=5)
            sub.add_argument("--apply", action="store_true", help="default is a dry run")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.command == "validate":
            result = load_manifest(args.manifest).to_dict()
        else:
            runtime = load_config(args.config) if args.config else RuntimeConfig()
            root, _, dsn = resolve_runtime(
                config=runtime,
                root=args.root,
                warehouse="sqlite" if args.command == "retain" else args.warehouse,
            )
            if args.command == "run":
                if args.live:
                    from market_data_pipeline.pipelines.aaii.source import run_live

                    result = run_live(
                        args.manifest,
                        root,
                        start=args.start,
                        end=args.end,
                        dsn=dsn,
                        max_attempts=runtime.operations.http_attempts,
                        network=runtime.network,
                    )
                else:
                    result = run(
                        args.input, args.manifest, root, dsn=dsn, start=args.start, end=args.end
                    )
            elif args.command == "weekly":
                from market_data_pipeline.pipelines.aaii.operations import weekly_window
                from market_data_pipeline.pipelines.aaii.source import run_live

                now = (
                    datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
                    if args.as_of
                    else datetime.now(UTC)
                )
                start, end = weekly_window(now, runtime.operations.overlap_days)
                result = run_live(
                    args.manifest,
                    root,
                    start=start,
                    end=end,
                    dsn=dsn,
                    max_attempts=runtime.operations.http_attempts,
                    network=runtime.network,
                )
            elif args.command == "backfill":
                from market_data_pipeline.pipelines.aaii.backfill import backfill

                result = backfill(
                    args.input,
                    args.manifest,
                    root,
                    start=args.start,
                    end=args.end,
                    quarantine=args.quarantine_invalid,
                    dsn=dsn,
                )
            elif args.command == "replay":
                result = replay(args.source_root, root, dsn=dsn)
            elif args.command == "check":
                from market_data_pipeline.pipelines.aaii.operations import health_report

                now = (
                    datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
                    if args.as_of
                    else None
                )
                result = health_report(
                    root, dsn=dsn, now=now, max_age_days=runtime.operations.max_age_days
                )
            elif args.command in ("backup", "restore", "retain"):
                from market_data_pipeline.pipelines.aaii.recovery import (
                    backup,
                    restore,
                    retain_snapshots,
                )

                if args.command == "backup":
                    result = backup(root, args.destination, dsn=dsn)
                elif args.command == "restore":
                    result = restore(args.backup, root, dsn=dsn)
                else:
                    result = retain_snapshots(root, keep=args.keep, apply=args.apply)
            else:
                if not dsn and not (root / "warehouse.sqlite3").exists():
                    raise ValueError("warehouse does not exist; run an import first")
                if args.command == "export":
                    result = {"snapshot": export(root, dsn=dsn)}
                else:
                    cutoff = (
                        utc_timestamp(datetime.fromisoformat(args.as_of.replace("Z", "+00:00")))
                        if args.as_of
                        else None
                    )
                    with Warehouse(root, dsn) as warehouse:
                        result = warehouse.gold(cutoff)
        print(json.dumps(result, indent=2))
        if args.command == "replay" and (result["rejected"] or result.get("partial")):
            return 1
        if args.command == "check" and not result["healthy"]:
            return 1
        if args.command == "backfill" and result["status"] == "partial":
            return 2
        return 0
    except Exception as exc:
        message = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        print(f"error: {message}", file=sys.stderr)
        return 1
