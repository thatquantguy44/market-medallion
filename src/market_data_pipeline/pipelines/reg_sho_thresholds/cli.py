"""Operate bounded Nasdaq and NYSE Reg SHO threshold-list partitions."""

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import acquire_and_run, gold, run


def _date(value):
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("requires an ISO date") from exc


def _timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("requires an ISO timestamp") from exc
    if result.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp requires a timezone")
    return result


def _json(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError("unsupported threshold JSON value")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in (
        "validate-config",
        "health",
        "plan",
        "run",
        "acquire-run",
        "gold",
        "export-research",
        "replay",
    ):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name in ("run", "acquire-run"):
            command.add_argument("--partition", required=True)
        elif name == "plan":
            command.add_argument("--provider", choices=("nasdaq", "nyse"))
            command.add_argument("--start", type=_date)
            command.add_argument("--end", type=_date)
        elif name == "gold":
            command.add_argument("--as-of", type=_timestamp)
            command.add_argument("--as-of-sequence", type=int)
            command.add_argument("--include-withdrawn", action="store_true")
        elif name == "export-research":
            command.add_argument("--destination", type=Path, required=True)
            command.add_argument("--max-rows", type=int, default=1_000_000)
        elif name == "replay":
            command.add_argument("--source-root", type=Path, required=True)
            command.add_argument("--max-captures", type=int, default=1000)
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "validate-config":
            config = load_config(arguments.config)
            result = {
                "pipeline": "reg-sho-thresholds",
                "config_validation": "PASSED",
                "warehouse": config.warehouse,
                "acquisition_enabled": config.acquisition.enabled,
                "partition_ids": sorted(config.partitions),
                "providers": sorted({item.provider for item in config.partitions.values()}),
                "storage_contract": config.schema_version,
                "request_performed": False,
            }
        elif arguments.command == "health":
            from market_data_pipeline.pipelines.reg_sho_thresholds.operations import health

            result = health(arguments.config)
        elif arguments.command == "plan":
            from market_data_pipeline.pipelines.reg_sho_thresholds.operations import plan

            if arguments.start and arguments.end and arguments.end < arguments.start:
                raise ValueError("plan end date precedes start date")
            result = plan(
                arguments.config,
                provider=arguments.provider,
                start=arguments.start,
                end=arguments.end,
            )
        elif arguments.command == "run":
            result = run(arguments.config, arguments.partition)
        elif arguments.command == "acquire-run":
            result = acquire_and_run(arguments.config, arguments.partition)
        elif arguments.command == "gold":
            result = gold(
                arguments.config,
                as_of=arguments.as_of,
                as_of_sequence=arguments.as_of_sequence,
                include_withdrawn=arguments.include_withdrawn,
            )
        elif arguments.command == "replay":
            from market_data_pipeline.pipelines.reg_sho_thresholds.replay import replay

            result = replay(
                arguments.source_root,
                arguments.config,
                max_captures=arguments.max_captures,
            )
        else:
            from market_data_pipeline.pipelines.reg_sho_thresholds.research_export import (
                export_research_artifact,
            )

            result = export_research_artifact(
                arguments.config, arguments.destination, max_rows=arguments.max_rows
            )
    except (OSError, ValueError, sqlite3.DatabaseError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, default=_json))
    if arguments.command in ("run", "acquire-run"):
        return 0 if result["status"] == "SUCCEEDED" else 1
    return 0
