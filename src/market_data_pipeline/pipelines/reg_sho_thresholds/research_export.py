"""Atomic native Reg SHO threshold research exchange for QuantMeridian."""

import hashlib
import os
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.episodes import derive_episodes
from market_data_pipeline.pipelines.reg_sho_thresholds.storage import storage_schemas
from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import warehouse_for
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

SCHEMA_VERSION = "reg-sho-thresholds-research/1"


def _digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while block := stream.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def _episode_table(rows):
    schema = pa.schema(
        [
            pa.field("provider", pa.string(), nullable=False),
            pa.field("market", pa.string(), nullable=False),
            pa.field("raw_symbol", pa.string(), nullable=False),
            pa.field("rule_family", pa.string(), nullable=False),
            pa.field("start_date", pa.date32(), nullable=False),
            pa.field("end_date", pa.date32(), nullable=False),
            pa.field("observed_present_count", pa.int64(), nullable=False),
            *[
                pa.field(name, pa.bool_(), nullable=False)
                for name in (
                    "entry_confirmed",
                    "exit_confirmed",
                    "left_censored",
                    "right_censored",
                    "gap_before",
                    "gap_after",
                )
            ],
        ],
        metadata={"schema_version": "reg-sho-episodes/1"},
    )
    return pa.Table.from_pylist(rows, schema=schema)


def export_research_artifact(
    config_path: Path,
    destination: Path,
    *,
    max_rows=1_000_000,
    generated_at=None,
):
    if type(max_rows) is not int or not 1 <= max_rows <= 10_000_000:
        raise ValueError("max_rows requires an integer in [1,10000000]")
    config = load_config(Path(config_path))
    if not (config.root / ".warehouse.json").is_file():
        raise ValueError("threshold warehouse does not exist")
    with warehouse_for(config, read_only=True) as warehouse:
        membership = warehouse.select_membership(include_withdrawn=False)
        coverage = warehouse.select_coverage()
    if len(membership) > max_rows:
        raise ValueError("threshold research export exceeds max_rows")
    dates = sorted(
        {item.list_date for item in config.partitions.values()}
        | {row["list_date"] for row in coverage}
    )
    episodes = derive_episodes(membership, coverage, dates)
    schemas = storage_schemas()
    destination = Path(destination).resolve()
    export_id = uuid.uuid4().hex
    generated = utc_timestamp(generated_at or datetime.now(UTC))
    with writer_lock(destination):
        generations = destination / "generations"
        generations.mkdir(parents=True, exist_ok=True)
        staging = destination / f".{export_id}.tmp"
        final = generations / export_id
        staging.mkdir(exist_ok=False)
        try:
            pq.write_table(
                schemas["membership"].arrow_table(membership),
                staging / "membership.parquet",
            )
            pq.write_table(schemas["coverage"].arrow_table(coverage), staging / "coverage.parquet")
            pq.write_table(_episode_table(episodes), staging / "episodes.parquet")
            files = {}
            for name, count in (
                ("membership.parquet", len(membership)),
                ("coverage.parquet", len(coverage)),
                ("episodes.parquet", len(episodes)),
            ):
                files[name] = {"sha256": _digest(staging / name), "rows": count}
            scopes = sorted({(row["provider"], row["market"]) for row in coverage})
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "export_id": export_id,
                "generated_at": generated,
                "knowledge_basis": "CURRENT_VINTAGE",
                "calendar_basis": "configured-and-observed-list-dates/1",
                "sources": [
                    {
                        "provider": provider,
                        "market": market,
                        "source_page": (
                            "https://www.nasdaqtrader.com/Trader.aspx?id=RegSHOThreshold"
                            if provider == "nasdaq"
                            else "https://www.nyse.com/regulation/regulation-sho"
                        ),
                    }
                    for provider, market in scopes
                ],
                "semantics": {
                    "unit": "list membership",
                    "missing_list_behavior": "UNKNOWN_NOT_EMPTY",
                    "episode_exit_requires_next_complete_expected_date": True,
                    "identity_basis": "provider-market-native-symbol-rule-family",
                },
                "files": files,
            }
            atomic_json(staging / "manifest.json", manifest)
            os.replace(staging, final)
            atomic_json(
                destination / "current.json",
                {
                    "schema_version": "reg-sho-thresholds-research-pointer/1",
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
        "membership_count": len(membership),
        "coverage_count": len(coverage),
        "episode_count": len(episodes),
        "knowledge_basis": "CURRENT_VINTAGE",
    }
