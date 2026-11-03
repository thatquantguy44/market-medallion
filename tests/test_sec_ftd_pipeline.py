"""T04 first local medallion path and atomic failure behavior, invented inputs."""

import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import gold, run
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import SQLiteWarehouse

EXAMPLE = Path("config/sec_fails_to_deliver/pipeline.example.yml")
MANIFEST = Path("manifests/sec-fails-to-deliver.yml").resolve()
FIXTURE = Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes()
CLOCK = datetime(2026, 11, 3, 9, tzinfo=UTC)


def bundle(payload=FIXTURE, name="cnsfails202610a.txt"):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, payload)
    return stream.getvalue()


def configured(tmp_path, payload=None):
    source = tmp_path / "input.zip"
    source.write_bytes(bundle() if payload is None else payload)
    value = yaml.safe_load(EXAMPLE.read_text())
    value.update(
        warehouse="sqlite",
        root=str(tmp_path / "root"),
        partitions={"fixture": {"input": str(source), "manifest": str(MANIFEST)}},
    )
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path, source


def invoke(path):
    return run(path, "fixture", clock=lambda: CLOCK)


def test_fixture_import_archives_exact_bytes_and_reopens_gold_with_true_clocks(tmp_path):
    config_path, source = configured(tmp_path)
    original = source.read_bytes()
    result = invoke(config_path)
    config = load_config(config_path)
    assert result["status"] == "SUCCEEDED"
    assert result["inserted_versions"] == 2
    assert result["coverage_state"] == "PARTIAL"
    capture = config.root / "bronze" / result["run_id"]
    assert (capture / "payload.bin").read_bytes() == original
    assert (capture / "manifest.yml").read_bytes() == MANIFEST.read_bytes()
    receipt = json.loads((capture / "receipt.json").read_text())
    assert receipt["provider_published_at"] is None
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        rows = warehouse.select_facts()
        assert len(rows) == 2
        assert rows[0]["cusip"] == "000000001"
        assert rows[0]["first_seen_at"] == CLOCK
        assert rows[0]["prior_close_price"] == Decimal("12.3400")
        assert rows[1]["quantity_fails"] == 2**70
        assert rows[1]["prior_close_price"] is None
        assert warehouse.select_runs()[0]["status"] == "SUCCEEDED"
        assert warehouse.select_coverage()[0]["expected_date_count"] is None
    assert len(gold(config_path, limit=20)["rows"]) == 2


def test_same_payload_adds_a_capture_but_no_fact_versions(tmp_path):
    path, _ = configured(tmp_path)
    first = invoke(path)
    second = invoke(path)
    assert first["run_id"] != second["run_id"]
    assert second["capture_sequence"] == 2 and second["inserted_versions"] == 0
    config = load_config(path)
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert [r["capture_sequence"] for r in warehouse.select_runs()] == [1, 2]
        assert {r["run_id"] for r in warehouse.select_facts()} == {first["run_id"]}
        assert warehouse.connection.execute("SELECT count(*) FROM sec_ftd_facts").fetchone()[0] == 2


@pytest.mark.parametrize(
    "kind,code,state",
    [
        ("challenge", "INVALID_ARCHIVE", "FAILED"),
        ("missing", "SOURCE_UNAVAILABLE", "UNAVAILABLE"),
        ("empty", "EMPTY_PARTITION_UNSUPPORTED", "FAILED"),
        ("oversized", "INPUT_TOO_LARGE", "FAILED"),
    ],
)
def test_missing_rejected_empty_and_oversized_inputs_have_distinct_durable_outcomes(
    tmp_path, kind, code, state
):
    path, source = configured(tmp_path)
    if kind == "challenge":
        source.write_bytes(b"<html>invented secret-looking challenge</html>")
    elif kind == "missing":
        source.unlink()
    elif kind == "empty":
        source.write_bytes(
            bundle(
                FIXTURE.splitlines(keepends=True)[0]
                + b"Trailer record count 0\nTrailer total quantity of shares 0\n"
            )
        )
    else:
        manifest = tmp_path / "small.yml"
        value = yaml.safe_load(MANIFEST.read_text())
        value["limits"]["archive_bytes"] = 8
        manifest.write_text(yaml.safe_dump(value))
        config_value = yaml.safe_load(path.read_text())
        config_value["partitions"]["fixture"]["manifest"] = str(manifest)
        path.write_text(yaml.safe_dump(config_value))
    result = invoke(path)
    assert result["status"] != "SUCCEEDED" and result["error_code"] == code
    assert "secret-looking" not in json.dumps(result)
    config = load_config(path)
    capture = config.root / "bronze" / result["run_id"]
    assert (capture / "receipt.json").exists()
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert warehouse.select_facts() == []
        assert warehouse.select_coverage()[0]["coverage_state"] == state
        assert warehouse.select_runs()[0]["error_code"] == code
    receipt = json.loads((capture / "receipt.json").read_text())
    if kind in ("missing", "oversized"):
        assert receipt["raw_sha256"] is None
    else:
        assert (capture / "payload.bin").read_bytes() == source.read_bytes()


def test_valid_correction_is_observed_without_overwriting_previous_version(tmp_path):
    path, source = configured(tmp_path)
    first = invoke(path)
    source.write_bytes(bundle(FIXTURE.replace(b"12.3400", b"12.3500")))
    corrected = invoke(path)
    assert corrected["status"] == "SUCCEEDED" and corrected["inserted_versions"] == 1
    config = load_config(path)
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert warehouse.select_facts()[0]["run_id"] == corrected["run_id"]
        assert warehouse.select_facts()[1]["run_id"] == first["run_id"]
        assert warehouse.select_facts()[0]["prior_close_price"] == Decimal("12.35")
        assert warehouse.connection.execute("SELECT count(*) FROM sec_ftd_facts").fetchone()[0] == 3


def test_mid_insert_database_failure_rolls_back_facts_and_success_audit_together(tmp_path):
    path, _ = configured(tmp_path)
    config = load_config(path)
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        warehouse.connection.execute(
            "CREATE TRIGGER injected_failure BEFORE INSERT ON sec_ftd_facts "
            "WHEN NEW.cusip='000000002' BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
    result = invoke(path)
    assert result["error_code"] == "WAREHOUSE_WRITE_FAILED"
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert warehouse.select_facts() == []
        assert warehouse.select_runs()[0]["status"] == "FAILED"
        assert warehouse.select_coverage()[0]["coverage_state"] == "FAILED"


def test_invalid_manifest_and_unimplemented_postgres_fail_before_root_writes(tmp_path):
    path, _ = configured(tmp_path)
    value = yaml.safe_load(path.read_text())
    value["warehouse"] = "postgres"
    path.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError, match="T19"):
        invoke(path)
    assert not (tmp_path / "root").exists()
    value["warehouse"] = "sqlite"
    manifest = tmp_path / "wrong.yml"
    manifest.write_text("source: not-sec\n")
    value["partitions"]["fixture"]["manifest"] = str(manifest)
    path.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError):
        invoke(path)
    assert not (tmp_path / "root").exists()


def test_capture_completion_not_attempt_start_is_the_fact_first_seen_clock(tmp_path):
    path, _ = configured(tmp_path)
    clocks = iter([CLOCK, CLOCK + timedelta(seconds=1), CLOCK + timedelta(seconds=2)])
    result = run(path, "fixture", clock=lambda: next(clocks))
    config = load_config(path)
    receipt = json.loads((config.root / "bronze" / result["run_id"] / "receipt.json").read_text())
    assert receipt["capture_started_at"] != receipt["captured_at"]
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert warehouse.select_facts()[0]["first_seen_at"] == CLOCK + timedelta(seconds=1)
        assert warehouse.select_runs()[0]["completed_at"] == CLOCK + timedelta(seconds=2)


@pytest.mark.parametrize(
    "payload", [b"PK\x03\x04broken", bundle(name="../cnsfails202610a.txt"), bundle()[:-12]]
)
def test_corrupt_and_unsafe_archives_remain_original_rejected_captures(tmp_path, payload):
    path, source = configured(tmp_path, payload)
    result = invoke(path)
    assert result["error_code"] == "INVALID_ARCHIVE"
    config = load_config(path)
    assert (config.root / "bronze" / result["run_id"] / "payload.bin").read_bytes() == payload
    assert gold(path, limit=20)["rows"] == []


def test_overlapping_valid_partition_is_retained_but_cannot_replace_gold(tmp_path):
    path, source = configured(tmp_path)
    invoke(path)
    manifest_value = yaml.safe_load(MANIFEST.read_text())
    manifest_value["partition"] = {"start": "2026-10-15", "end": "2026-10-31"}
    manifest_value["member_name"] = "cnsfails202610b.txt"
    manifest = tmp_path / "overlap.yml"
    manifest.write_text(yaml.safe_dump(manifest_value))
    value = yaml.safe_load(path.read_text())
    value["partitions"]["fixture"]["manifest"] = str(manifest)
    path.write_text(yaml.safe_dump(value))
    source.write_bytes(
        bundle(
            FIXTURE.replace(b"20261015", b"20261031").replace(b"20261001", b"20261015"),
            name="cnsfails202610b.txt",
        )
    )
    rejected = invoke(path)
    assert rejected["error_code"] == "OVERLAPPING_PARTITION"
    assert gold(path, limit=20)["row_count"] == 2


def test_clock_regression_and_contract_change_fail_without_new_captures(tmp_path):
    path, _ = configured(tmp_path)
    invoke(path)
    with pytest.raises(ValueError, match="clock"):
        run(path, "fixture", clock=lambda: CLOCK - timedelta(seconds=1))
    config = load_config(path)
    assert len(list((config.root / "bronze").iterdir())) == 1
    value = yaml.safe_load(path.read_text())
    value["storage"]["price_scale"] = 6
    path.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError, match="contract"):
        invoke(path)
    assert len(list((config.root / "bronze").iterdir())) == 1


def test_configured_numeric_overflow_retains_bronze_and_promotes_no_facts(tmp_path):
    path, _ = configured(tmp_path)
    value = yaml.safe_load(path.read_text())
    value["storage"]["quantity_precision"] = 1
    path.write_text(yaml.safe_dump(value))
    result = invoke(path)
    assert result["error_code"] == "STORAGE_VALIDATION_FAILED"
    assert result["row_count"] == 0
    config = load_config(path)
    assert (config.root / "bronze" / result["run_id"] / "payload.bin").exists()
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert warehouse.select_facts() == []
        assert warehouse.select_runs()[0]["status"] == "REJECTED"


def test_modified_receipt_metadata_cannot_be_promoted_even_if_payload_hash_matches(
    tmp_path, monkeypatch
):
    from market_data_pipeline.pipelines.sec_fails_to_deliver import pipeline

    path, _ = configured(tmp_path)
    original = pipeline.archive_capture

    def tampered(*args, **kwargs):
        receipt, payload = original(*args, **kwargs)
        config = load_config(path)
        receipt_path = config.root / "bronze" / receipt["run_id"] / "receipt.json"
        value = json.loads(receipt_path.read_text())
        value["capture_sequence"] = 99
        receipt_path.write_text(json.dumps(value))
        return receipt, payload

    monkeypatch.setattr(pipeline, "archive_capture", tampered)
    result = invoke(path)
    assert result["error_code"] == "CAPTURE_INTEGRITY_FAILED"
    assert gold(path, limit=20)["rows"] == []
