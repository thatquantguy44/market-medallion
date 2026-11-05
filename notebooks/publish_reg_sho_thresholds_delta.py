# Databricks notebook source
# Install this repository's wheel first. Stage a verified threshold snapshot and
# source identity under the configured Unity Catalog volume; keep autoMerge disabled.
from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.delta import publish_configured_delta

dbutils.widgets.text("config", "")  # noqa: F821
dbutils.widgets.text("snapshot", "")  # noqa: F821
snapshot = dbutils.widgets.get("snapshot")  # noqa: F821
result = publish_configured_delta(
    spark,  # noqa: F821
    Path(dbutils.widgets.get("config")),  # noqa: F821
    Path(snapshot) if snapshot else None,
)
print(result)
