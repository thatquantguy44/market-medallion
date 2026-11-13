"""FOMC-owned operator entrypoints; local commands never select an HTTP route."""

import argparse
import json
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import load_operations
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
    PostgresDependencyStore,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
    PostgresObjectStore,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality import (
    quality_report,
    write_report,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.recovery import recover_attempt
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.operations import (
    health,
    plan,
    run_operation,
    validate_run,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Bounded FOMC archive/text/chunk operations")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in [
        "validate-config",
        "migrate",
        "plan",
        "run",
        "health",
        "quality-report",
        "recover-capture",
        "recover-run",
    ]:
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--operations", type=Path, required=True)
        if name in {"plan", "run"}:
            command.add_argument("--start-year", type=int, required=True)
            command.add_argument("--end-year", type=int, required=True)
            command.add_argument("--mode", choices=["backfill", "refresh"], default="backfill")
        if name == "quality-report":
            command.add_argument("--output", type=Path, required=True)
        if name == "run":
            command.add_argument("--live", action="store_true")
        if name == "recover-capture":
            command.add_argument("--capture-id", required=True)
        if name == "recover-run":
            command.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    live_started = False
    try:
        cfg = load_config(args.config)
        settings = load_operations(args.operations)
        if args.command == "validate-config":
            result = {
                "status": "valid",
                "start_year": cfg.start_year,
                "schema": cfg.postgres.schema,
                "scheduled_live_access": settings.scheduled_live_access,
                "request_performed": False,
            }
        else:
            if args.command == "run":
                validate_run(
                    cfg,
                    settings,
                    start_year=args.start_year,
                    end_year=args.end_year,
                    mode=args.mode,
                    live=args.live,
                )
            with PostgresInventoryStore(cfg.postgres) as store:
                if args.command == "migrate":
                    store.initialize()
                    bronze = PostgresBronzeStore(store)
                    bronze.migrate()
                    PostgresObjectStore(bronze).migrate()
                    PostgresDependencyStore(bronze).migrate()
                    text = PostgresTextStore(bronze)
                    text.migrate()
                    PostgresChunkStore(text).migrate()
                    OperationStore(store).migrate()
                    result = {"status": "migrated", "request_performed": False}
                elif args.command == "plan":
                    result = plan(
                        cfg,
                        settings,
                        inventory_store=store,
                        start_year=args.start_year,
                        end_year=args.end_year,
                        mode=args.mode,
                    )
                elif args.command == "run":
                    live_started = True
                    result = run_operation(
                        cfg,
                        settings,
                        inventory_store=store,
                        start_year=args.start_year,
                        end_year=args.end_year,
                        mode=args.mode,
                        live=args.live,
                    )
                elif args.command == "health":
                    result = health(cfg, settings, inventory_store=store)
                elif args.command == "quality-report":
                    report = quality_report(cfg, inventory_store=store)
                    write_report(cfg, args.output, report)
                    result = {
                        "status": "reported",
                        "report_id": report["report_id"],
                        "totals": report["totals"],
                        "request_performed": False,
                    }
                elif args.command == "recover-capture":
                    result = recover_attempt(cfg.root, args.capture_id, PostgresBronzeStore(store))
                    result = {**result, "request_performed": False}
                else:
                    value = OperationStore(store).recover(args.run_id, root=cfg.root)
                    result = {
                        "status": "recovered" if value["result"] else "interrupted",
                        "run_id": args.run_id,
                        "result": value["result"],
                        "request_performed": False,
                    }
        print(json.dumps(result, sort_keys=True, ensure_ascii=False))
        return 2 if result.get("status") in {"partial", "failed", "degraded", "interrupted"} else 0
    except Exception:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_code": "invalid_input_or_unavailable_state",
                    "request_performed": None if live_started else False,
                }
            )
        )
        return 2
