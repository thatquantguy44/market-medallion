"""Validate, inspect or ingest one explicitly configured local SEC FTD archive."""

import argparse
import json
import re
import sqlite3
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.data import parse_archive
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import (
    SCHEMA_VERSION,
    load_manifest,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import acquire_and_run, gold, run
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import is_database_error


def _json_value(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError("unsupported SEC FTD output type")


def _as_of(value):
    if not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})",
        value,
    ):
        raise argparse.ArgumentTypeError(
            "as-of requires an ISO timestamp with timezone and at most microseconds"
        )
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("invalid as-of timestamp") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "inspect"):
        command = commands.add_parser(name)
        if name == "inspect":
            selection = command.add_mutually_exclusive_group(required=True)
            selection.add_argument("--manifest", type=Path)
            selection.add_argument("--config", type=Path)
            command.add_argument("--input", type=Path)
            command.add_argument("--partition")
        else:
            command.add_argument("--manifest", type=Path, required=True)
    config_command = commands.add_parser("validate-config")
    config_command.add_argument("--config", type=Path, required=True)
    for name in ("run", "acquire-run", "gold", "replay", "publish", "health", "export-research"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name in ("run", "acquire-run"):
            command.add_argument("--partition", required=True)
        elif name == "replay":
            command.add_argument("--source-root", type=Path, required=True)
            command.add_argument("--max-captures", type=int, default=1000)
        elif name == "gold":
            command.add_argument("--limit", type=int, required=True)
            command.add_argument("--as-of", type=_as_of)
            command.add_argument(
                "--as-of-sequence",
                type=int,
                help="inclusive capture-sequence bound at the exact --as-of timestamp",
            )
            command.add_argument("--include-withdrawn", action="store_true")
        elif name == "export-research":
            command.add_argument("--destination", type=Path, required=True)
            command.add_argument("--max-rows", type=int, default=500_000)
    arguments = parser.parse_args(argv)
    if arguments.command == "inspect":
        if arguments.config and (not arguments.partition or arguments.input):
            parser.error("config inspection requires --partition and no --input override")
        if arguments.manifest and (not arguments.input or arguments.partition):
            parser.error("manifest inspection requires --input and no --partition")
    try:
        if arguments.command in (
            "run",
            "acquire-run",
            "gold",
            "replay",
            "publish",
            "health",
            "export-research",
        ):
            if arguments.command == "replay":
                from market_data_pipeline.pipelines.sec_fails_to_deliver.replay import replay

                result = replay(
                    arguments.source_root, arguments.config, max_captures=arguments.max_captures
                )
            elif arguments.command == "run":
                result = run(arguments.config, arguments.partition)
            elif arguments.command == "acquire-run":
                result = acquire_and_run(arguments.config, arguments.partition)
            elif arguments.command == "publish":
                from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import (
                    publish_config,
                )

                result = publish_config(arguments.config)
            elif arguments.command == "health":
                from market_data_pipeline.pipelines.sec_fails_to_deliver.operations import health

                result = health(arguments.config)
            elif arguments.command == "export-research":
                from market_data_pipeline.pipelines.sec_fails_to_deliver.research_export import (
                    export_research_artifact,
                )

                result = export_research_artifact(
                    arguments.config,
                    arguments.destination,
                    max_rows=arguments.max_rows,
                )
            else:
                result = gold(
                    arguments.config,
                    limit=arguments.limit,
                    as_of=arguments.as_of,
                    as_of_sequence=arguments.as_of_sequence,
                    include_withdrawn=arguments.include_withdrawn,
                )
            print(json.dumps(result, sort_keys=True, default=_json_value))
            return (
                0
                if arguments.command in ("gold", "publish", "health", "export-research")
                or result["status"] == "SUCCEEDED"
                else 1
            )
        if arguments.command == "validate-config":
            config = load_config(arguments.config)
            print(
                json.dumps(
                    {
                        "pipeline": "sec-fails-to-deliver",
                        "config_validation": "PASSED",
                        "warehouse": config.warehouse,
                        "databricks_enabled": config.databricks.enabled,
                        "acquisition_enabled": config.acquisition.enabled,
                        "acquisition_configured_partitions": sorted(
                            name for name, item in config.partitions.items() if item.source_url
                        ),
                        "storage_contract_id": config.storage.contract_id,
                        "partition_ids": sorted(config.partitions),
                        "manifests_validated": False,
                        "storage_implemented": True,
                        "delta_implemented": False,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if arguments.command == "inspect" and arguments.config:
            config = load_config(arguments.config)
            if arguments.partition not in config.partitions:
                raise ValueError("partition is not present in the SEC FTD config")
            selected = config.partitions[arguments.partition]
            arguments.manifest, arguments.input = selected.manifest, selected.input
        manifest = load_manifest(arguments.manifest)
        result = {
            "source": "sec",
            "dataset": "sec-fails-to-deliver",
            "schema_version": SCHEMA_VERSION,
            "provider_conformance": "UNVERIFIED",
            "period_start": manifest.period_start.isoformat(),
            "period_end": manifest.period_end.isoformat(),
        }
        if arguments.command == "inspect":
            if not arguments.input.is_file():
                raise OSError("input is not a regular local file")
            with arguments.input.open("rb") as stream:
                payload = stream.read(manifest.max_archive_bytes + 1)
            archive = parse_archive(payload, manifest)
            result.update(
                {
                    "file_validation": "PASSED",
                    "profile_conformance": "MATCHED",
                    "source_coverage": "NOT_CERTIFIED",
                    "row_count": len(archive.observations),
                    "observed_date_count": len(archive.observed_dates),
                    "quantity_fails_total": sum(row.quantity_fails for row in archive.observations),
                    "trailers_validated": True,
                    "raw_sha256": archive.raw_sha256,
                    "member_sha256": archive.member_sha256,
                }
            )
    except sqlite3.DatabaseError:
        print(json.dumps({"error": "local SEC FTD warehouse is unavailable"}), file=sys.stderr)
        return 1
    except OSError:
        print(json.dumps({"error": "local input is unavailable"}), file=sys.stderr)
        return 1
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    except Exception as exc:
        if not is_database_error(exc):
            raise
        print(json.dumps({"error": "configured SEC FTD warehouse is unavailable"}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0
