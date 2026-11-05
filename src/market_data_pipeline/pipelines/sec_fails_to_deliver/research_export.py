"""Publish a bounded, attributed SEC FTD research exchange for QuantMeridian."""

import hashlib
import json
import os
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import current_snapshot
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

SCHEMA_VERSION = "sec-ftd-research/1"
SOURCE_PAGE = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def export_research_artifact(
    config_path: Path,
    destination: Path,
    *,
    max_rows: int = 500_000,
    generated_at: datetime | None = None,
) -> dict:
    """Copy the coherent current Gold/coverage generation behind an atomic pointer."""
    if type(max_rows) is not int or not 1 <= max_rows <= 10_000_000:
        raise ValueError("max_rows requires an integer in [1,10000000]")
    config = load_config(Path(config_path))
    destination = Path(destination).resolve()
    clock = utc_timestamp(generated_at or datetime.now(UTC))
    with writer_lock(config.root):
        snapshot = current_snapshot(config.root)
        try:
            source_manifest = json.loads((snapshot / "manifest.json").read_text())
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            raise ValueError("SEC FTD publication manifest is unavailable") from exc
        observations = snapshot / "gold.parquet"
        coverage = snapshot / "coverage.parquet"
        if any(path.is_symlink() or not path.is_file() for path in (observations, coverage)):
            raise ValueError("SEC FTD publication files are unavailable")
        if _digest(observations) != source_manifest.get("files", {}).get("gold") or _digest(
            coverage
        ) != source_manifest.get("files", {}).get("coverage"):
            raise ValueError("SEC FTD publication file checksum mismatch")
        observation_count = pq.ParquetFile(observations).metadata.num_rows
        coverage_count = pq.ParquetFile(coverage).metadata.num_rows
        if observation_count > max_rows:
            raise ValueError("SEC FTD research export exceeds max_rows")
        export_id = uuid.uuid4().hex
        with writer_lock(destination):
            generations = destination / "generations"
            generations.mkdir(parents=True, exist_ok=True)
            staging = destination / f".{export_id}.tmp"
            final = generations / export_id
            staging.mkdir(exist_ok=False)
            try:
                shutil.copyfile(observations, staging / "observations.parquet")
                shutil.copyfile(coverage, staging / "coverage.parquet")
                manifest = {
                    "schema_version": SCHEMA_VERSION,
                    "export_id": export_id,
                    "generated_at": clock,
                    "source": {
                        "provider": "U.S. Securities and Exchange Commission",
                        "dataset": "sec-fails-to-deliver",
                        "source_page": SOURCE_PAGE,
                        "publication_snapshot_id": source_manifest["snapshot_id"],
                        "publication_generated_at": source_manifest["generated_at"],
                        "knowledge_basis": "CURRENT_VINTAGE",
                    },
                    "semantics": {
                        "unit": "shares outstanding as of settlement date",
                        "price_basis": "provider previous close",
                        "population_coverage": "NOT_CERTIFIED",
                        "interpretation_limits": [
                            "values are outstanding balances, not daily flows",
                            "fail age cannot be inferred",
                            "fails are not necessarily caused by short selling",
                            "missing rows are not zero without complete coverage evidence",
                        ],
                    },
                    "files": {
                        "observations.parquet": {
                            "sha256": _digest(staging / "observations.parquet"),
                            "rows": observation_count,
                        },
                        "coverage.parquet": {
                            "sha256": _digest(staging / "coverage.parquet"),
                            "rows": coverage_count,
                        },
                    },
                }
                atomic_json(staging / "manifest.json", manifest)
                os.replace(staging, final)
                atomic_json(
                    destination / "current.json",
                    {
                        "schema_version": "sec-ftd-research-pointer/1",
                        "export_id": export_id,
                        "generation": f"generations/{export_id}",
                        "manifest_sha256": _digest(final / "manifest.json"),
                    },
                )
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
    return {
        "schema_version": SCHEMA_VERSION,
        "export_id": export_id,
        "destination": str(destination),
        "observation_count": observation_count,
        "coverage_count": coverage_count,
        "knowledge_basis": "CURRENT_VINTAGE",
        "source_coverage": "NOT_CERTIFIED",
    }
