"""Pipeline-specific configuration validation performs no data/service access."""

import io
import json
import zipfile
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.cli import main
from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config

EXAMPLE = Path("config/sec_fails_to_deliver/pipeline.example.yml")


def write_config(tmp_path, section=None, **updates):
    value = yaml.safe_load(EXAMPLE.read_text())
    if section:
        value[section].update(updates)
    else:
        value.update(updates)
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_example_loads_without_secrets_input_files_or_connections(tmp_path, monkeypatch):
    monkeypatch.delenv("SEC_FTD_POSTGRES_PASSWORD", raising=False)
    config = load_config(write_config(tmp_path))
    assert config.warehouse == "postgres"
    assert config.root == (tmp_path / "../../data/sec_fails_to_deliver/postgres").resolve()
    assert config.postgres.schema == "sec_ftd"
    assert config.postgres.password_env == "SEC_FTD_POSTGRES_PASSWORD"
    assert config.storage.quantity_precision == 38
    assert config.storage.price_scale == 12
    assert not config.databricks.enabled
    assert (
        config.partitions["september_2026_b"].manifest
        == (tmp_path / "../../manifests/sec-fails-to-deliver-202609b.yml").resolve()
    )


@pytest.mark.parametrize(
    ("section", "updates"),
    [
        (None, {"pipeline": "aaii-investor-sentiment"}),
        (None, {"config_version": True}),
        (None, {"config_version": 2}),
        (None, {"warehouse": "databricks"}),
        (None, {"root": ""}),
        (None, {"unknown": "not allowed"}),
        ("postgres", {"port": True}),
        ("postgres", {"port": 65536}),
        ("postgres", {"schema": "public;drop table x"}),
        ("postgres", {"role": "invalid role"}),
        ("postgres", {"host": "bad\x00host"}),
        ("postgres", {"sslmode": "invalid"}),
        ("postgres", {"connect_timeout": 0}),
        ("postgres", {"password": "must never be echoed"}),
        ("postgres", {"password_env": "bad name"}),
        ("storage", {"quantity_precision": 39}),
        ("storage", {"quantity_precision": True}),
        ("storage", {"price_scale": 39}),
        ("storage", {"price_scale": -1}),
        ("storage", {"timezone": "America/New_York"}),
        ("storage", {"overflow": "round"}),
        ("storage", {"preserve_numeric_text": False}),
        ("storage", {"preserve_numeric_text": 1}),
        ("storage", {"schema_version": "unknown"}),
        ("databricks", {"enabled": "false"}),
        ("databricks", {"enabled": True}),
        ("databricks", {"table_prefix": "bad prefix"}),
        ("databricks", {"token": "must never be echoed"}),
        ("databricks", {"staging_volume": "/tmp/not-a-volume"}),
        (None, {"partitions": {}}),
        (None, {"partitions": {"bad name": {"manifest": "m.yml", "input": "f.zip"}}}),
        (None, {"partitions": {"valid": {"manifest": "m.yml", "input": ""}}}),
    ],
)
def test_invalid_settings_fail_without_echoing_values(tmp_path, section, updates):
    with pytest.raises(ValueError) as exc:
        load_config(write_config(tmp_path, section, **updates))
    assert "must never be echoed" not in str(exc.value)


def test_valid_enabled_delta_target_and_custom_numeric_profile(tmp_path):
    path = write_config(
        tmp_path,
        "databricks",
        enabled=True,
        catalog="research",
        schema="settlement",
        staging_volume="/Volumes/research/settlement/staging/sec_ftd",
    )
    value = yaml.safe_load(path.read_text())
    value["storage"].update(price_precision=24, price_scale=6)
    path.write_text(yaml.safe_dump(value))
    config = load_config(path)
    assert config.databricks.enabled
    assert config.storage.price_precision == 24
    assert config.storage.price_scale == 6


def test_storage_contract_fingerprint_changes_only_for_semantic_settings(tmp_path):
    original = load_config(write_config(tmp_path))
    changed_connection = load_config(write_config(tmp_path, "postgres", host="db.example"))
    changed_storage = load_config(write_config(tmp_path, "storage", price_scale=6))
    assert original.storage.contract_id == changed_connection.storage.contract_id
    assert original.storage.contract_id != changed_storage.storage.contract_id
    assert len(original.storage.contract_id) == 64


@pytest.mark.parametrize(
    "updates", [{"quantity_precision": 39}, {"price_scale": 39}, {"timezone": "local"}]
)
def test_storage_settings_cannot_bypass_validation_when_constructed_directly(updates):
    settings = load_config(EXAMPLE).storage
    with pytest.raises(ValueError):
        replace(settings, **updates)


def test_paths_resolve_from_config_directory_independently_of_cwd(tmp_path, monkeypatch):
    path = write_config(tmp_path)
    before = load_config(path)
    monkeypatch.chdir(tmp_path.parent)
    after = load_config(path)
    assert before == after


@pytest.mark.parametrize(
    "text", ["warehouse: sqlite\nwarehouse: postgres\n", "? [bad, key]\n: x\n"]
)
def test_duplicate_and_unhashable_yaml_keys_are_rejected(tmp_path, text):
    path = tmp_path / "bad.yml"
    path.write_text(text)
    with pytest.raises(ValueError):
        load_config(path)


def test_validate_config_reports_implemented_backend_without_reading_secrets(tmp_path, capsys):
    path = write_config(tmp_path)
    assert main(["validate-config", "--config", str(path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["config_validation"] == "PASSED"
    assert result["warehouse"] == "postgres"
    assert result["storage_implemented"] is True
    assert result["delta_implemented"] is False
    assert result["manifests_validated"] is False
    assert result["partition_ids"] == ["september_2026_a", "september_2026_b"]
    assert "password" not in json.dumps(result)
    assert "host" not in json.dumps(result)


def test_inspect_can_select_manifest_and_input_entirely_from_config(tmp_path, capsys):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "cnsfails202610a.txt",
            Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes(),
        )
    input_path = tmp_path / "fixture.zip"
    input_path.write_bytes(buffer.getvalue())
    partitions = {
        "invented": {
            "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
            "input": "fixture.zip",
        }
    }
    path = write_config(tmp_path, partitions=partitions)
    assert main(["inspect", "--config", str(path), "--partition", "invented"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["row_count"] == 2
    assert result["trailers_validated"] is True
    assert sorted(p.name for p in tmp_path.iterdir()) == ["fixture.zip", "pipeline.yml"]


def test_unknown_config_partition_fails_without_input_io(tmp_path, capsys):
    path = write_config(tmp_path)
    assert main(["inspect", "--config", str(path), "--partition", "missing"]) == 1
    captured = capsys.readouterr()
    assert not captured.out
    assert "partition" in json.loads(captured.err)["error"]


@pytest.mark.parametrize(
    "arguments",
    [
        ["inspect", "--config", "unused.yml"],
        ["inspect", "--config", "unused.yml", "--partition", "x", "--input", "x.zip"],
        ["inspect", "--manifest", "unused.yml"],
        ["inspect", "--manifest", "unused.yml", "--input", "x.zip", "--partition", "x"],
    ],
)
def test_incomplete_or_mixed_input_selection_is_rejected(arguments):
    with pytest.raises(SystemExit) as exc:
        main(arguments)
    assert exc.value.code == 2
