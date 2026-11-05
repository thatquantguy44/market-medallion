# Databricks notebook source
# Install this repository's built wheel on the job cluster first. The configured
# root and selected snapshot must be staged under the configured Unity Catalog volume.
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.delta import publish_configured_delta

dbutils.widgets.text("config", "")  # noqa: F821
dbutils.widgets.text("snapshot", "")  # noqa: F821
snapshot = dbutils.widgets.get("snapshot")  # noqa: F821
result = publish_configured_delta(
    spark,  # noqa: F821
    Path(dbutils.widgets.get("config")),  # noqa: F821
    Path(snapshot) if snapshot else None,
)
print(result)
