"""No-request health and execution planning for SEC FTD operators."""

from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import load_manifest


def health(config_path: Path) -> dict:
    """Validate local intent/manifests without HTTP, credentials, or DB connections."""
    config = load_config(Path(config_path))
    partitions = []
    for name, selected in sorted(config.partitions.items()):
        manifest = load_manifest(selected.manifest)
        partitions.append(
            {
                "partition": name,
                "period_start": manifest.period_start.isoformat(),
                "period_end": manifest.period_end.isoformat(),
                "local_input_available": selected.input.is_file(),
                "acquisition_configured": selected.source_url is not None,
            }
        )
    marker = config.root / ".warehouse.json"
    pointer = config.root / "parquet" / "current.json"
    return {
        "pipeline": "sec-fails-to-deliver",
        "status": "READY",
        "request_performed": False,
        "warehouse": config.warehouse,
        "warehouse_initialized": marker.is_file(),
        "publication_available": pointer.is_file(),
        "acquisition_enabled": config.acquisition.enabled,
        "partition_count": len(partitions),
        "partitions": partitions,
        "dagster_schedule_default": "STOPPED",
    }
