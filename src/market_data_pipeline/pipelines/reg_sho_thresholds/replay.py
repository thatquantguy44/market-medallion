"""Atomic, network-free threshold warehouse reconstruction."""

import hashlib
import json
import os
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import (
    PostgresWarehouse,
    SQLiteWarehouse,
    warehouse_for,
)
from market_data_pipeline.shared.files import atomic_json, utc_timestamp


def _digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def replay(source_root: Path, config_path: Path, *, max_captures=1000):
    if type(max_captures) is not int or not 1 <= max_captures <= 10_000:
        raise ValueError("max_captures requires an integer in [1,10000]")
    source_root = Path(source_root).resolve()
    config = load_config(Path(config_path))
    destination = config.root
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("threshold replay destination must be empty")
    if destination == source_root or destination.is_relative_to(source_root):
        raise ValueError("threshold replay destination must be separate from source")
    stage = destination.with_name(f".{destination.name}.replay.{uuid.uuid4().hex}.tmp")
    if stage.exists():
        raise ValueError("threshold replay staging path already exists")
    try:
        with SQLiteWarehouse(source_root, read_only=True) as source:
            audits = source.select_all("audit")
            coverage = source.select_all("coverage")
            membership = source.select_all("membership")
            if not audits or len(audits) > max_captures:
                raise ValueError("threshold replay source capture count is outside bounds")
            by_coverage = {row["run_id"]: row for row in coverage}
            by_membership = {}
            for row in membership:
                by_membership.setdefault(row["run_id"], []).append(row)
            if set(by_coverage) != {row["run_id"] for row in audits}:
                raise ValueError("threshold replay source coverage inventory is incomplete")
            bronze = source_root / "bronze"
            for audit in audits:
                directory = bronze / audit["run_id"]
                receipt = directory / "receipt.json"
                if directory.is_symlink() or not receipt.is_file():
                    raise ValueError("threshold replay source Bronze inventory is incomplete")
                value = json.loads(receipt.read_text())
                if (
                    value.get("run_id") != audit["run_id"]
                    or value.get("capture_sequence") != audit["capture_sequence"]
                    or value.get("raw_sha256") != audit["raw_sha256"]
                    or value.get("status") != audit["status"]
                ):
                    raise ValueError("threshold replay receipt differs from source audit")
            target_store = warehouse_for(config, root=stage)
            with target_store as target:
                for audit in audits:
                    target.restore_run(
                        audit,
                        by_coverage[audit["run_id"]],
                        by_membership.get(audit["run_id"], []),
                    )
                for name in ("audit", "coverage", "membership"):
                    if target.select_all(name) != source.select_all(name):
                        raise ValueError("threshold replay normalized state differs")
            shutil.copytree(bronze, stage / "bronze")
            inventory = []
            for audit in audits:
                directory = stage / "bronze" / audit["run_id"]
                inventory.append(
                    {
                        "run_id": audit["run_id"],
                        "capture_sequence": audit["capture_sequence"],
                        "receipt_sha256": _digest(directory / "receipt.json"),
                        "payload_sha256": (
                            _digest(directory / "payload.bin")
                            if (directory / "payload.bin").is_file()
                            else None
                        ),
                    }
                )
            atomic_json(
                stage / "replay.json",
                {
                    "schema_version": "reg-sho-replay/1",
                    "completed_at": utc_timestamp(datetime.now(UTC)),
                    "capture_count": len(audits),
                    "membership_version_count": len(membership),
                    "captures": inventory,
                },
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            destination.rmdir()
        os.replace(stage, destination)
    except BaseException:
        if "target_store" in locals() and isinstance(target_store, PostgresWarehouse):
            target_store.cleanup_created_store()
        if stage.exists():
            shutil.rmtree(stage)
        raise
    return {
        "pipeline": "reg-sho-thresholds",
        "status": "SUCCEEDED",
        "capture_count": len(audits),
        "membership_version_count": len(membership),
        "destination": str(destination),
    }
