"""Promote an immutable, independently reviewed controls workbook offline."""

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

from .config import PipelineConfig, load_config
from .review_workbook import MAX_WORKBOOK_BYTES, export_approved_controls
from .storage import MAX_CONFIG_BYTES, _read_bounded, _safe_child


def promote_review_workbook(
    root: Path, config: PipelineConfig, workbook: Path, *, admitted_at: datetime
) -> dict:
    """Copy and validate reviewed evidence into a fresh immutable review version."""
    admitted = utc_timestamp(admitted_at)
    workbook_bytes = _read_bounded(Path(workbook), MAX_WORKBOOK_BYTES)
    config_bytes = _read_bounded(config.path, MAX_CONFIG_BYTES)
    root = Path(root)
    with writer_lock(root):
        review = _safe_child(root, "review")
        review.mkdir(mode=0o700, exist_ok=True)
        review_id = uuid.uuid4().hex
        target = _safe_child(review, review_id)
        if target.exists():
            raise ValueError("Review identity already exists.")
        staging = Path(tempfile.mkdtemp(prefix=".review-", dir=review))
        try:
            workbook_copy = staging / "review.xlsx"
            config_copy = staging / "sources.toml"
            for path, content in ((workbook_copy, workbook_bytes), (config_copy, config_bytes)):
                with path.open("xb") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
            snapshot_config = load_config(config_copy)
            if replace(snapshot_config, path=config.path) != config:
                raise ValueError("Source catalog changed since selection.")
            result = export_approved_controls(
                snapshot_config, workbook_copy, staging / "controls.csv"
            )
            manifest_path = staging / "controls.manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["review_workbook"] = "review.xlsx"
            atomic_json(manifest_path, manifest)
            receipt = {
                "schema": "presidential-review-receipt/1",
                "review_id": review_id,
                "admitted_at": admitted,
                "workbook_sha256": hashlib.sha256(workbook_bytes).hexdigest(),
                "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
                "controls_sha256": hashlib.sha256(
                    (staging / "controls.csv").read_bytes()
                ).hexdigest(),
                "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                "approved_count": result["controls"],
            }
            atomic_json(staging / "receipt.json", receipt)
            os.rename(staging, target)
            directory_fd = os.open(review, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return receipt
        finally:
            if staging.exists():
                shutil.rmtree(staging)


def load_review_version(root: Path, review_id: str) -> tuple[dict, bytes, dict]:
    """Recheck an explicitly selected review version before any downstream build."""
    import re

    if not isinstance(review_id, str) or not re.fullmatch(r"[0-9a-f]{32}", review_id):
        raise ValueError("Invalid review ID.")
    directory = _safe_child(_safe_child(Path(root), "review"), review_id)
    receipt = json.loads(_read_bounded(_safe_child(directory, "receipt.json"), 65536))
    workbook = _read_bounded(_safe_child(directory, "review.xlsx"), MAX_WORKBOOK_BYTES)
    config_bytes = _read_bounded(_safe_child(directory, "sources.toml"), MAX_CONFIG_BYTES)
    controls = _read_bounded(_safe_child(directory, "controls.csv"), 5 * 1024 * 1024)
    manifest_bytes = _read_bounded(
        _safe_child(directory, "controls.manifest.json"), 5 * 1024 * 1024
    )
    manifest = json.loads(manifest_bytes)
    expected_keys = {
        "schema",
        "review_id",
        "admitted_at",
        "workbook_sha256",
        "config_sha256",
        "controls_sha256",
        "manifest_sha256",
        "approved_count",
    }
    if (
        not isinstance(receipt, dict)
        or set(receipt) != expected_keys
        or not isinstance(manifest, dict)
    ):
        raise ValueError("Invalid review receipt.")
    try:
        admitted = receipt["admitted_at"]
        if (
            not isinstance(admitted, str)
            or utc_timestamp(datetime.fromisoformat(admitted.replace("Z", "+00:00"))) != admitted
        ):
            raise ValueError("Invalid review time.")
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid review receipt.") from exc

    def digest(value: bytes) -> str:
        return hashlib.sha256(value).hexdigest()

    if (
        receipt["schema"] != "presidential-review-receipt/1"
        or receipt["review_id"] != review_id
        or type(receipt["approved_count"]) is not int
        or receipt["approved_count"] < 1
        or receipt["workbook_sha256"] != digest(workbook)
        or receipt["config_sha256"] != digest(config_bytes)
        or receipt["controls_sha256"] != digest(controls)
        or receipt["manifest_sha256"] != digest(manifest_bytes)
        or manifest.get("artifact_kind") != "APPROVED_POLITICAL_CONTROLS"
        or manifest.get("review_workbook") != "review.xlsx"
        or manifest.get("approved_count") != receipt["approved_count"]
        or manifest.get("review_workbook_sha256") != receipt["workbook_sha256"]
        or manifest.get("config_sha256") != receipt["config_sha256"]
        or manifest.get("controls_sha256") != receipt["controls_sha256"]
    ):
        raise ValueError("Review integrity check failed.")
    return receipt, controls, manifest
