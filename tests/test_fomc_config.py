"""FOMC-AC11: own configuration; loading is independent of passwords and services."""

import socket
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import config_from_dict, load_config

EXAMPLE = Path("config/fomc_meeting_minutes/pipeline.example.yml")


def test_fomc_ac11_config_is_pipeline_owned_relative_and_no_service_access(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("config loading attempted service access")

    monkeypatch.setattr(socket, "create_connection", unexpected)
    monkeypatch.delenv("FOMC_POSTGRES_PASSWORD", raising=False)
    cfg = load_config(EXAMPLE)
    assert cfg.postgres.schema == "fomc_meeting_minutes" and cfg.postgres.role is None
    assert cfg.root == Path("data/fomc_meeting_minutes").resolve()
    assert cfg.manifest == Path("manifests/fomc-meeting-minutes.yml").resolve()
    assert not hasattr(cfg.postgres, "password")


@pytest.mark.parametrize(
    "field,value",
    [
        ("port", True),
        ("port", 0),
        ("host", ""),
        ("schema", "aaii"),
        ("schema", "public"),
        ("schema", "fomc_meeting_minutes;DROP SCHEMA aaii"),
        ("role", "aaii;DROP SCHEMA aaii"),
        ("password_env", "literal secret"),
        ("connect_timeout", 0),
        ("sslmode", "invalid"),
        ("unknown", 1),
    ],
)
def test_fomc_ac11_bad_postgres_config_refuses_before_connection(field, value):
    value_dict = yaml.safe_load(EXAMPLE.read_text())
    value_dict["postgres"][field] = value
    with pytest.raises(ValueError):
        config_from_dict(value_dict, base_dir=EXAMPLE.parent)


def test_fomc_ac11_unknown_top_level_and_inline_passwords_are_rejected():
    value = yaml.safe_load(EXAMPLE.read_text())
    for changed in ({**value, "unknown": 1}, {**value, "warehouse": "sqlite"}):
        with pytest.raises(ValueError):
            config_from_dict(changed, base_dir=EXAMPLE.parent)
    changed = deepcopy(value)
    changed["postgres"]["password"] = "invented-private-marker"
    with pytest.raises(ValueError) as failure:
        config_from_dict(changed, base_dir=EXAMPLE.parent)
    assert "invented-private-marker" not in str(failure.value)


def test_fomc_ac11_programmatic_store_cannot_bypass_namespace_validation():
    from dataclasses import replace

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import (
        PostgresInventoryStore,
    )

    config = load_config(EXAMPLE)
    with pytest.raises(ValueError):
        PostgresInventoryStore(replace(config.postgres, schema="aaii"))
