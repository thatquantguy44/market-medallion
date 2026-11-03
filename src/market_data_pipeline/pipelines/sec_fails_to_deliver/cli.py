"""Validate the offline SEC FTD profile or inspect one bounded local archive."""

import argparse
import json
import sys
from pathlib import Path

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
        command.add_argument("--manifest", type=Path, required=True)
        if name == "inspect":
            command.add_argument("--input", type=Path, required=True)
    arguments = parser.parse_args(argv)
    try:
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
