"""SSV-AC06 source-owned local commands. No environment loading or live access."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from market_data_pipeline.pipelines.finra_short_sale_volume import pipeline
from market_data_pipeline.pipelines.finra_short_sale_volume.config import load_config


def _read(path, limit):
    with Path(path).open("rb") as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ValueError("FINRA local input exceeds byte budget")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description="FINRA short-sale-volume local pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("validate-config", "run", "ingest-capture", "gold", "coverage", "audit"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True, type=Path)
        if name == "run":
            command.add_argument("--input", required=True, type=Path)
            command.add_argument("--receipt", required=True, type=Path)
            command.add_argument("--capture-id")
            command.add_argument("--run-id")
        if name == "ingest-capture":
            command.add_argument("--capture-id", required=True)
        if name in ("gold", "coverage"):
            command.add_argument("--trade-date", required=True)
            command.add_argument("--facility", required=True)
        if name == "gold":
            command.add_argument("--as-of", type=datetime.fromisoformat)
            command.add_argument("--as-of-sequence", type=int)
        if name == "coverage":
            command.add_argument("--symbol")
        if name == "audit":
            command.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        shared = {"approved_use": config.approved_use, "operation": config.transform_operation}
        if args.command == "validate-config":
            result = {
                "schema_id": "finra-ssv-local-config/1",
                "operation": config.operation,
                "scopes": list(config.manifest.scopes),
                "max_bytes": config.max_bytes,
                "max_rows": config.max_rows,
            }
        elif args.command == "run":
            payload = _read(args.input, config.max_bytes)
            receipt = json.loads(_read(args.receipt, 65536))
            result = pipeline.run(
                config.root,
                payload,
                config.manifest,
                receipt,
                approved_use=config.approved_use,
                operation=config.operation,
                run_id=args.run_id,
                capture_id=args.capture_id,
                max_bytes=config.max_bytes,
                max_rows=config.max_rows,
            )
        elif args.command == "ingest-capture":
            result = pipeline.ingest_capture(
                config.root,
                args.capture_id,
                config.manifest,
                **shared,
                max_bytes=config.max_bytes,
                max_rows=config.max_rows,
            )
        elif args.command == "gold":
            result = pipeline.read_gold(
                config.root,
                config.manifest,
                **shared,
                trade_date=args.trade_date,
                facility_id=args.facility,
                as_of=args.as_of,
                as_of_sequence=args.as_of_sequence,
            )
        elif args.command == "coverage":
            result = pipeline.coverage(
                config.root,
                config.manifest,
                **shared,
                trade_date=args.trade_date,
                facility_id=args.facility,
                symbol=args.symbol,
            )
        else:
            result = pipeline.audit(config.root, config.manifest, **shared, limit=args.limit)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return (
            2 if isinstance(result, dict) and result.get("status") in {"PARTIAL", "REJECTED"} else 0
        )
    except (OSError, ValueError, PermissionError, RuntimeError, RecursionError):
        print(
            "FINRA local command failed; check admitted configuration, input and retaine"
            "d audit evidence",
            file=sys.stderr,
        )
        return 2
