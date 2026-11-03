"""Validate the offline SEC FTD profile or inspect one bounded local archive."""

import argparse
import json
import sys
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.data import parse_archive
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import (
    SCHEMA_VERSION,
    load_manifest,
)


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
    arguments = parser.parse_args(argv)
    if arguments.command == "inspect":
        if arguments.config and (not arguments.partition or arguments.input):
            parser.error("config inspection requires --partition and no --input override")
        if arguments.manifest and (not arguments.input or arguments.partition):
            parser.error("manifest inspection requires --input and no --partition")
    try:
        if arguments.command == "validate-config":
            config = load_config(arguments.config)
            print(
                json.dumps(
                    {
                        "pipeline": "sec-fails-to-deliver",
                        "config_validation": "PASSED",
                        "warehouse": config.warehouse,
                        "databricks_enabled": config.databricks.enabled,
                        "storage_contract_id": config.storage.contract_id,
                        "partition_ids": sorted(config.partitions),
                        "manifests_validated": False,
                        "storage_implemented": False,
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
    except OSError:
        print(json.dumps({"error": "local input is unavailable"}), file=sys.stderr)
        return 1
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0
