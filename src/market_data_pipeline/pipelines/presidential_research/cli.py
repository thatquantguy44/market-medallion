"""Explicit offline commands for the presidential research pipeline."""

import argparse
import csv
import hashlib
import io
import json
import re
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path

from .backup import backup_selected_snapshot, restore_backup
from .bundle_assembly import build_full_bundle
from .candidate_review import prepare_candidates, validate_control_rows
from .config import load_config
from .operations import research_health
from .pipeline import import_local_capture
from .publication import (
    _bytes,
    _write,
    build_baseline,
    publish_baseline,
    publish_full_bundle,
    read_current,
)
from .replay import replay_selected_snapshot
from .review import load_review_version, promote_review_workbook
from .storage import MAX_CONFIG_BYTES, MAX_RAW_BYTES, _read_bounded, load_capture
from .source import fetch_selected
from .warehouse import ingest_capture

_SOURCE = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")
_ID = re.compile(r"^[0-9a-f]{32}$")


def _now():
    return datetime.now(UTC)


def _config_default():
    return Path(
        str(
            files("market_data_pipeline.pipelines.presidential_research").joinpath(
                "resources/sources.toml"
            )
        )
    )


def _parser():
    parser = argparse.ArgumentParser(
        prog="market-medallion presidential",
        description="Explicit offline presidential research inputs and publication",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def workspace(command):
        command.add_argument("--workspace", required=True, type=Path)

    def config(command):
        command.add_argument("--config", type=Path, default=_config_default())

    init = commands.add_parser("init", help="create an empty workspace directory")
    workspace(init)
    imported = commands.add_parser(
        "import-local", help="archive explicitly classified local source bytes"
    )
    workspace(imported)
    config(imported)
    imported.add_argument("--source", required=True)
    imported.add_argument("--file", required=True, type=Path)
    imported.add_argument(
        "--input-kind", required=True, choices=("CAPTURED_SOURCE", "SYNTHETIC_FIXTURE")
    )
    imported.add_argument("--sha256")
    imported.add_argument("--retrieved-at", help="evidenced ISO timestamp, distinct from admission")
    parse = commands.add_parser("parse", help="admit explicitly selected captured versions")
    workspace(parse)
    config(parse)
    parse.add_argument("--capture", action="append", required=True, help="SOURCE_ID=CAPTURE_ID")
    parse.add_argument("--source", action="append", default=[])
    for name in ("validate", "health"):
        command = commands.add_parser(name, help="report local health without a source request")
        workspace(command)
        config(command)
        command.add_argument("--stale-after-days", type=int, default=30)
        command.add_argument("--expected-end-year", type=int)
    controls = commands.add_parser(
        "validate-controls", help="validate shape and source roles, not approval"
    )
    config(controls)
    controls.add_argument("--controls", required=True, type=Path)
    promoted = commands.add_parser(
        "export-approved-controls",
        help="archive an evidenced approved workbook and export its controls",
    )
    workspace(promoted)
    config(promoted)
    promoted.add_argument("--workbook", required=True, type=Path)
    promoted.add_argument("--controls", required=True, type=Path)
    promoted.add_argument(
        "--evidence-kind", required=True, choices=("REVIEWED_HISTORY", "SYNTHETIC_FIXTURE")
    )
    candidates = commands.add_parser(
        "prepare-control-review", help="prepare review-only candidates"
    )
    workspace(candidates)
    config(candidates)
    candidates.add_argument("--house-capture-id", required=True)
    candidates.add_argument("--wikipedia-capture-id", required=True)
    candidates.add_argument("--output", required=True, type=Path)
    baseline = commands.add_parser(
        "build-annual", help="build one explicit baseline without advancing the pointer"
    )
    workspace(baseline)
    config(baseline)
    baseline.add_argument("--capture-id", required=True)
    baseline.add_argument("--review-id", required=True)
    baseline.add_argument("--controls", required=True, type=Path)
    baseline.add_argument("--output", required=True, type=Path)
    baseline.add_argument("--profile", required=True)
    baseline.add_argument(
        "--decision-policy", required=True, choices=("window_start", "calendar_year_start")
    )
    complete = commands.add_parser(
        "build-public-series-comparison",
        help="build the complete explicit bundle to a new directory",
    )
    workspace(complete)
    config(complete)
    complete.add_argument("--capture", action="append", required=True)
    complete.add_argument("--review-id", required=True)
    complete.add_argument("--controls", required=True, type=Path)
    complete.add_argument("--output-dir", required=True, type=Path)
    complete.add_argument("--profile", required=True)
    complete.add_argument(
        "--decision-policy", required=True, choices=("window_start", "calendar_year_start")
    )
    single = commands.add_parser(
        "publish-baseline", help="atomically select one validated baseline"
    )
    workspace(single)
    single.add_argument("--capture-id", required=True)
    single.add_argument("--review-id", required=True)
    single.add_argument("--profile", required=True)
    publish = commands.add_parser("publish", help="atomically select a complete validated bundle")
    workspace(publish)
    publish.add_argument("--capture", action="append", required=True)
    publish.add_argument("--review-id", required=True)
    publish.add_argument("--profile", required=True)
    check = commands.add_parser("check", help="verify the selected snapshot")
    workspace(check)
    for name in ("replay", "backup", "restore"):
        command = commands.add_parser(name)
        if name == "restore":
            command.add_argument("--archive", required=True, type=Path)
        else:
            workspace(command)
        command.add_argument("--destination", required=True, type=Path)
    fetch = commands.add_parser(
        "fetch", help="source acquisition is blocked until separately reviewed"
    )
    workspace(fetch)
    config(fetch)
    fetch.add_argument("--source", action="append", required=True)
    return parser


def _selection(values):
    output = {}
    for value in values:
        source, delimiter, capture_id = value.partition("=")
        if (
            delimiter != "="
            or not _SOURCE.fullmatch(source)
            or not _ID.fullmatch(capture_id)
            or source in output
        ):
            raise ValueError("Each capture must be a unique SOURCE_ID=CAPTURE_ID selection.")
        output[source] = capture_id
    return output


def _matching_config(config_path, root, selection):
    config = _read_bounded(config_path, MAX_CONFIG_BYTES)
    load_config(config_path)
    for source_id, capture_id in selection.items():
        receipt, _, _ = load_capture(root, source_id, capture_id)
        if hashlib.sha256(config).hexdigest() != receipt["config_sha256"]:
            raise ValueError("Selected capture and configuration digest differ.")


def _matching_controls(root, review_id, path):
    _, controls, _ = load_review_version(root, review_id)
    if _read_bounded(path, 5 * 1024 * 1024) != controls:
        raise ValueError("Controls do not match the selected reviewed version.")


def _write_pair(output, payload, manifest):
    sidecar = output.with_suffix(".manifest.json")
    if (
        output == sidecar
        or output.exists()
        or output.is_symlink()
        or sidecar.exists()
        or sidecar.is_symlink()
    ):
        raise FileExistsError("Output or sidecar already exists.")
    _write(output, payload)
    _write(sidecar, _bytes(manifest))


def _print(value):
    print(json.dumps(value, sort_keys=True, ensure_ascii=True))


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        command = args.command
        if command == "init":
            if args.workspace.is_symlink():
                raise ValueError("Workspace cannot be a symlink.")
            args.workspace.mkdir(parents=True, exist_ok=True)
            _print({"status": "INITIALIZED"})
        elif command == "fetch":
            result = fetch_selected(args.workspace, load_config(args.config), args.source)
            _print(result)
            return 0 if result["status"] == "SUCCEEDED" else 1
        elif command == "import-local":
            config = load_config(args.config)
            retrieved = (
                datetime.fromisoformat(args.retrieved_at.replace("Z", "+00:00"))
                if args.retrieved_at
                else None
            )
            receipt, outcome = import_local_capture(
                args.workspace,
                config,
                args.source,
                _read_bounded(args.file, MAX_RAW_BYTES),
                admitted_at=_now(),
                input_kind=args.input_kind,
                expected_sha256=args.sha256,
                retrieved_at=retrieved,
            )
            _print({"capture": receipt, "parse": outcome})
            return 0 if outcome["status"] == "PARSED" else 1
        elif command == "parse":
            selected = _selection(args.capture)
            if args.source and set(args.source) != set(selected):
                raise ValueError("--source must match the explicitly selected captures.")
            _matching_config(args.config, args.workspace, selected)
            ordered = sorted(
                selected.items(),
                key=lambda item: load_capture(args.workspace, item[0], item[1])[0][
                    "capture_sequence"
                ],
            )
            outcomes = [
                {"source_id": source, **ingest_capture(args.workspace, source, capture_id)}
                for source, capture_id in ordered
            ]
            _print({"captures": outcomes})
            return 0 if all(item["status"] == "PARSED" for item in outcomes) else 1
        elif command in ("validate", "health"):
            load_config(args.config)
            status = research_health(
                args.workspace,
                now=_now(),
                stale_after=timedelta(days=args.stale_after_days),
                expected_end_year=args.expected_end_year,
            )
            _print(status)
            return 0 if status["snapshot"]["status"] == "VALID" else 1
        elif command == "validate-controls":
            content = _read_bounded(args.controls, 5 * 1024 * 1024).decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(content))
            if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
                raise ValueError("Invalid controls header.")
            report = validate_control_rows(load_config(args.config), list(reader))
            _print(report)
            return 0 if report["errors"] == 0 else 1
        elif command == "export-approved-controls":
            sidecar = args.controls.with_suffix(".manifest.json")
            if (
                args.controls.exists()
                or args.controls.is_symlink()
                or sidecar.exists()
                or sidecar.is_symlink()
            ):
                raise FileExistsError("Controls output already exists.")
            receipt = promote_review_workbook(
                args.workspace,
                load_config(args.config),
                args.workbook,
                admitted_at=_now(),
                evidence_kind=args.evidence_kind,
            )
            _, controls, manifest = load_review_version(args.workspace, receipt["review_id"])
            _write_pair(
                args.controls,
                controls,
                {**manifest, "review_id": receipt["review_id"], "input_kind": args.evidence_kind},
            )
            _print({"review": receipt, "output": str(args.controls)})
        elif command == "prepare-control-review":
            selected = {
                "HOUSE_GOV_001": args.house_capture_id,
                "WIKI_CONGRESS_001": args.wikipedia_capture_id,
            }
            _matching_config(args.config, args.workspace, selected)
            rows, manifest = prepare_candidates(
                args.workspace, args.house_capture_id, args.wikipedia_capture_id
            )
            stream = io.StringIO(newline="")
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
            _write_pair(args.output, stream.getvalue().encode(), manifest)
            _print(
                {
                    "candidates": len(rows),
                    "controls_file_eligible": False,
                    "inputKind": manifest["input_kind"],
                }
            )
        elif command == "build-annual":
            if args.decision_policy != "window_start":
                raise ValueError(
                    "calendar_year_start is incompatible with the strict v1 "
                    "at-origin dataset contract."
                )
            _matching_config(args.config, args.workspace, {"FF_MKT_001": args.capture_id})
            _matching_controls(args.workspace, args.review_id, args.controls)
            dataset, manifest, capture, _, _ = build_baseline(
                args.workspace, args.capture_id, args.review_id, profile_id=args.profile
            )
            _write_pair(args.output, _bytes(dataset), manifest)
            _print(
                {
                    "output": str(args.output),
                    "inputKind": capture["input_kind"],
                    "windows": len(dataset["windows"]),
                }
            )
        elif command == "build-public-series-comparison":
            if args.decision_policy != "window_start":
                raise ValueError(
                    "calendar_year_start is incompatible with the strict v1 "
                    "at-origin dataset contract."
                )
            selected = _selection(args.capture)
            _matching_config(args.config, args.workspace, selected)
            _matching_controls(args.workspace, args.review_id, args.controls)
            built = build_full_bundle(
                args.workspace, selected, args.review_id, profile_id=args.profile
            )
            args.output_dir.mkdir(mode=0o700)
            for name, (dataset, manifest) in built["datasets"].items():
                _write_pair(args.output_dir / name, _bytes(dataset), manifest)
            for name, payload in built["artifacts"].items():
                _write(args.output_dir / name, payload)
            _write(
                args.output_dir / "bundle-info.json",
                _bytes(
                    {
                        "inputKind": built["input_kind"],
                        "profileId": args.profile,
                        "reviewId": args.review_id,
                        "factInventory": built["fact_inventory"],
                    }
                ),
            )
            _print(
                {
                    "output": str(args.output_dir),
                    "inputKind": built["input_kind"],
                    "datasets": len(built["datasets"]),
                }
            )
        elif command == "publish-baseline":
            _print(
                publish_baseline(
                    args.workspace,
                    args.capture_id,
                    args.review_id,
                    profile_id=args.profile,
                    created_at=_now(),
                )
            )
        elif command == "publish":
            _print(
                publish_full_bundle(
                    args.workspace,
                    _selection(args.capture),
                    args.review_id,
                    profile_id=args.profile,
                    created_at=_now(),
                )
            )
        elif command == "check":
            current = read_current(args.workspace)
            _print(
                {
                    "snapshotId": current["catalog"]["snapshotId"],
                    "inputKind": current["catalog"]["inputKind"],
                    "datasets": len(current["datasets"]),
                    "artifacts": len(current["artifacts"]),
                }
            )
        elif command == "replay":
            _print(replay_selected_snapshot(args.workspace, args.destination, replayed_at=_now()))
        elif command == "backup":
            _print(backup_selected_snapshot(args.workspace, args.destination, backed_up_at=_now()))
        elif command == "restore":
            _print(restore_backup(args.archive, args.destination, restored_at=_now()))
        return 0
    except (OSError, ValueError, sqlite3.Error, UnicodeError) as exc:
        _print({"error": type(exc).__name__, "message": str(exc)})
        return 2


if __name__ == "__main__":
    sys.exit(main())
