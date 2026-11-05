from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config

EXAMPLE = Path("config/reg_sho_thresholds/pipeline.example.yml")


def _write(tmp_path, mutate=None):
    value = yaml.safe_load(EXAMPLE.read_text())
    value["root"] = str(tmp_path / "root")
    if mutate:
        mutate(value)
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_threshold_example_loads_without_io_or_secrets(tmp_path):
    config = load_config(_write(tmp_path))
    assert not config.acquisition.enabled
    assert config.schema_version == "reg-sho-storage/1"
    assert config.partitions["nasdaq_2026_11_03"].market == "NASDAQ"
    assert config.partitions["nyse_american_2026_11_03"].market == "NYSE American"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(unknown=True),
        lambda value: value.update(warehouse="duckdb"),
        lambda value: value["acquisition"].update(enabled="false"),
        lambda value: value["storage"].update(schema_version="unknown"),
        lambda value: value["partitions"]["nasdaq_2026_11_03"].update(
            source_url="https://example.test/nasdaq.txt"
        ),
        lambda value: value["partitions"]["nyse_american_2026_11_03"].update(
            source_url=(
                "https://www.nyse.com/api/regulatory/threshold-securities/download?"
                "selectedDate=2026-11-02&market=NYSE%20American"
            )
        ),
    ],
)
def test_threshold_config_rejects_unknown_or_mismatched_settings(tmp_path, mutate):
    with pytest.raises(ValueError):
        load_config(_write(tmp_path, mutate))


def test_threshold_postgres_intent_loads_without_reading_password(tmp_path, monkeypatch):
    monkeypatch.delenv("REG_SHO_POSTGRES_PASSWORD", raising=False)
    config = load_config(_write(tmp_path, lambda value: value.update(warehouse="postgres")))
    assert config.warehouse == "postgres"
    assert config.postgres.schema == "reg_sho_thresholds"
    with pytest.raises(ValueError, match="REG_SHO_POSTGRES_PASSWORD"):
        config.postgres.conninfo()


def test_older_threshold_config_defaults_to_disabled_delta(tmp_path):
    path = _write(tmp_path, lambda value: value.pop("databricks"))
    assert not load_config(path).databricks.enabled


@pytest.mark.parametrize(
    "delta",
    [
        None,
        {"enabled": False},
        {
            "enabled": "false",
            "catalog": None,
            "schema": None,
            "staging_volume": None,
            "table_prefix": "reg_sho",
        },
        {
            "enabled": True,
            "catalog": None,
            "schema": None,
            "staging_volume": None,
            "table_prefix": "reg_sho",
        },
        {
            "enabled": True,
            "catalog": "research",
            "schema": "thresholds",
            "staging_volume": "/Volumes/wrong/thresholds/data",
            "table_prefix": "reg_sho",
        },
        {
            "enabled": True,
            "catalog": "research",
            "schema": "thresholds",
            "staging_volume": "/Volumes/research/thresholds/../data",
            "table_prefix": "reg_sho",
        },
    ],
)
def test_delta_config_rejects_incomplete_or_unsafe_targets(tmp_path, delta):
    with pytest.raises(ValueError, match="databricks|Databricks"):
        load_config(_write(tmp_path, lambda value: value.update(databricks=delta)))


def test_explicit_delta_target_configuration_is_offline(tmp_path):
    config = load_config(
        _write(
            tmp_path,
            lambda value: value.update(
                databricks={
                    "enabled": True,
                    "catalog": "research",
                    "schema": "thresholds",
                    "staging_volume": "/Volumes/research/thresholds/data",
                    "table_prefix": "reg_sho",
                }
            ),
        )
    )
    assert config.databricks.enabled
    assert not config.root.exists()
