"""T04 exact capture bytes, bounded rejected inputs and immutable provenance."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.sec_fails_to_deliver.bronze import archive_capture, load_capture
from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config

MANIFEST = Path("manifests/sec-fails-to-deliver.yml").read_bytes()
SETTINGS = load_config(Path("config/sec_fails_to_deliver/pipeline.example.yml")).storage
CLOCK = datetime(2026, 11, 3, 9, tzinfo=UTC)


def capture(tmp_path, path, **kwargs):
    return archive_capture(
        tmp_path / "root",
        run_id="a" * 32,
        sequence=1,
        captured_at=CLOCK,
        manifest_payload=MANIFEST,
        settings=SETTINGS,
        input_path=path,
        **kwargs,
    )


def test_original_payload_manifest_and_settings_survive_reopen(tmp_path):
    path = tmp_path / "approved.zip"
    raw = b"<html>invented rejected challenge</html>"
    path.write_bytes(raw)
    receipt, payload = capture(tmp_path, path)
    directory = tmp_path / "root/bronze" / receipt["run_id"]
    assert payload == raw
    assert (directory / "manifest.yml").read_bytes() == MANIFEST
    assert (directory / "payload.bin").read_bytes() == raw
    assert receipt["raw_sha256"] == hashlib.sha256(raw).hexdigest()
    assert receipt["payload_complete"] is True
    assert receipt["provider_published_at"] is None
    assert receipt["origin_retrieved_at"] is None
    loaded, archived, manifest, settings = load_capture(directory)
    assert loaded == receipt and archived == raw and manifest == MANIFEST
    assert settings == SETTINGS
    with pytest.raises(FileExistsError):
        capture(tmp_path, path)


def test_missing_source_is_not_archived_as_empty_payload(tmp_path):
    receipt, payload = capture(tmp_path, tmp_path / "missing.zip")
    directory = tmp_path / "root/bronze" / receipt["run_id"]
    assert receipt["source_state"] == "UNAVAILABLE"
    assert receipt["raw_sha256"] is None
    assert payload is None
    assert not (directory / "payload.bin").exists()
    assert load_capture(directory)[1] is None


def test_excess_size_retains_only_explicitly_incomplete_prefix(tmp_path):
    value = MANIFEST.decode().replace("archive_bytes: 8388608", "archive_bytes: 8")
    path = tmp_path / "large.zip"
    path.write_bytes(b"x" * 100)
    receipt, payload = archive_capture(
        tmp_path / "root",
        run_id="b" * 32,
        sequence=1,
        captured_at=CLOCK,
        manifest_payload=value.encode(),
        settings=SETTINGS,
        input_path=path,
    )
    assert payload == b"x" * 9
    assert receipt["source_state"] == "OVERSIZED"
    assert receipt["payload_complete"] is False and receipt["raw_sha256"] is None
    assert receipt["captured_sha256"] == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize("file", ["payload.bin", "manifest.yml", "storage.json"])
def test_modified_capture_artifacts_fail_digest_validation(tmp_path, file):
    path = tmp_path / "approved.zip"
    path.write_bytes(b"invented")
    receipt, _ = capture(tmp_path, path)
    directory = tmp_path / "root/bronze" / receipt["run_id"]
    (directory / file).write_bytes(b"modified")
    with pytest.raises(ValueError):
        load_capture(directory)


def test_invalid_manifest_fails_before_input_or_capture_writes(tmp_path):
    with pytest.raises(ValueError):
        archive_capture(
            tmp_path / "root",
            run_id="a" * 32,
            sequence=1,
            captured_at=CLOCK,
            manifest_payload=b"bad: manifest\n",
            settings=SETTINGS,
            input_path=tmp_path / "missing.zip",
        )
    assert not (tmp_path / "root").exists()


def test_legacy_version1_receipt_remains_readable(tmp_path):
    path = tmp_path / "input.zip"
    path.write_bytes(b"invented")
    receipt, _ = capture(tmp_path, path)
    directory = tmp_path / "root/bronze" / receipt["run_id"]
    receipt["receipt_version"] = "sec-ftd-capture/1"
    receipt.pop("coverage_evidence_sha256")
    (directory / "receipt.json").write_text(json.dumps(receipt))
    assert load_capture(directory)[1] == b"invented"


def test_completeness_artifact_tampering_is_detected(tmp_path):
    path = tmp_path / "input.zip"
    path.write_bytes(b"invented")
    receipt, _ = capture(tmp_path, path, coverage_evidence=b'{"invented":"proof"}')
    directory = tmp_path / "root/bronze" / receipt["run_id"]
    (directory / "coverage-evidence.json").write_bytes(b"modified")
    with pytest.raises(ValueError):
        load_capture(directory)
