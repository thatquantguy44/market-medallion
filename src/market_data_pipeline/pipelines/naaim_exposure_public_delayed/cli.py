"""NAAIM Exposure Index: 3-month-delayed public data for internal research."""

import argparse
import json
from pathlib import Path

from market_data_pipeline.pipelines.naaim_exposure_public_delayed.operations import health_report
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.pipeline import run_live


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="pipeline-owned output directory")
    parser.add_argument(
        "--permission-reference",
        help="nonsecret reference to NAAIM permission for this live request",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="read local capture and publication health without contacting NAAIM",
    )
    parser.add_argument(
        "--max-capture-age-days",
        type=int,
        default=14,
        help="maximum local-capture age for --check (default: 14)",
    )
    arguments = parser.parse_args(argv)
    if arguments.check:
        result = health_report(
            arguments.root,
            max_capture_age_days=arguments.max_capture_age_days,
        )
        print(json.dumps(result, sort_keys=True))
        return 0 if result["healthy"] else 1
    if not arguments.permission_reference:
        parser.error("--permission-reference is required unless --check is used")
    result = run_live(arguments.root, permission_reference=arguments.permission_reference)
    print(json.dumps(result, sort_keys=True))
    return 0
