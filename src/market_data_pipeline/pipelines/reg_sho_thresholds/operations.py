"""No-request threshold health and dry-run execution planning."""

from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.manifest import load_manifest


def health(config_path: Path):
    config = load_config(Path(config_path))
    partitions = []
    for name, selected in sorted(config.partitions.items()):
        manifest = load_manifest(selected.manifest)
        if manifest.provider != selected.provider:
            raise ValueError("threshold partition differs from its manifest")
        partitions.append(
            {
                "partition": name,
                "provider": selected.provider,
                "market": selected.market,
                "list_date": selected.list_date.isoformat(),
                "local_input_available": selected.input.is_file(),
                "acquisition_configured": selected.source_url is not None,
            }
        )
    return {
        "pipeline": "reg-sho-thresholds",
        "status": "READY",
        "request_performed": False,
        "acquisition_enabled": config.acquisition.enabled,
        "warehouse_initialized": (config.root / ".warehouse.json").is_file(),
        "publication_available": (config.root / "research" / "current.json").is_file(),
        "dagster_schedule_default": "STOPPED",
        "partitions": partitions,
    }


def plan(config_path: Path, *, provider=None, start=None, end=None):
    config = load_config(Path(config_path))
    selected = []
    for name, item in sorted(config.partitions.items(), key=lambda pair: pair[1].list_date):
        if provider is not None and item.provider != provider:
            continue
        if start is not None and item.list_date < start:
            continue
        if end is not None and item.list_date > end:
            continue
        selected.append(
            {
                "partition": name,
                "provider": item.provider,
                "market": item.market,
                "list_date": item.list_date.isoformat(),
                "live_acquisition": item.source_url is not None,
            }
        )
    return {
        "pipeline": "reg-sho-thresholds",
        "dry_run": True,
        "request_performed": False,
        "partition_count": len(selected),
        "partitions": selected,
    }
