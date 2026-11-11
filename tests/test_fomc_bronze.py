"""Invented archive receipts and byte-identical failure/retry cases."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
    capture,
    commit_capture,
    read_capture,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest


class Store:
    def __init__(self, fail=False):
        self.rows = {}
        self.fail = fail

    def save_capture(self, receipt, payload):
        if self.fail:
            raise RuntimeError("database unavailable")
        key = receipt["capture_id"]
        if key in self.rows and self.rows[key] != (receipt, payload):
            raise ValueError("conflicting capture")
        self.rows[key] = (receipt, payload)


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def create(root, manifest, store, payload=b"invented raw bytes", **overrides):
    kwargs = dict(
        source_url="https://www.federalreserve.gov/monetarypolicy/files/invented.pdf",
        kind="document",
        captured_at=datetime(2026, 11, 11, tzinfo=UTC),
        status=200,
        headers={"Content-Type": "application/pdf", "Set-Cookie": "secret"},
        max_bytes=100,
    )
    kwargs.update(overrides)
    return capture(root, payload, manifest=manifest, store=store, **kwargs)


def test_byte_exact_storage_and_safe_receipt(tmp_path, manifest):
    store = Store()
    receipt = create(tmp_path, manifest, store, b"\x00\xfforiginal\n")
    observed, payload = read_capture(tmp_path, receipt["capture_id"])
    assert payload == b"\x00\xfforiginal\n" and observed == receipt
    assert store.rows[receipt["capture_id"]] == (receipt, payload)
    assert receipt["headers"] == {"content-type": "application/pdf"}
    assert receipt["accepted"] is True
    assert (tmp_path / "captures" / receipt["capture_id"] / "committed.json").is_file()
    commit_capture(tmp_path, receipt["capture_id"], store)


def test_rejected_response_preserves_exact_evidence(tmp_path, manifest):
    store = Store()
    receipt = create(tmp_path, manifest, store, b"unabridged failure", status=503, max_bytes=4)
    assert receipt["accepted"] is False
    assert set(receipt["reasons"]) == {"http_status", "size_limit"}
    assert read_capture(tmp_path, receipt["capture_id"])[1] == b"unabridged failure"
    assert store.rows[receipt["capture_id"]][1] == b"unabridged failure"


def test_database_failure_leaves_verifiable_retryable_files(tmp_path, manifest):
    store = Store(fail=True)
    with pytest.raises(RuntimeError):
        create(tmp_path, manifest, store)
    directory = next((tmp_path / "captures").iterdir())
    assert not (directory / "committed.json").exists()
    receipt, payload = read_capture(tmp_path, directory.name)
    store.fail = False
    commit_capture(tmp_path, directory.name, store)
    assert store.rows[directory.name] == (receipt, payload)


def test_file_failure_prevents_database_write_and_tampering_is_detected(tmp_path, manifest):
    store = Store()
    root = tmp_path / "not-a-directory"
    root.write_bytes(b"x")
    with pytest.raises(OSError):
        create(root, manifest, store)
    assert store.rows == {}
    receipt = create(tmp_path, manifest, store)
    (tmp_path / "captures" / receipt["capture_id"] / "payload.bin").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="hash"):
        read_capture(tmp_path, receipt["capture_id"])


def test_unreviewed_urls_and_invalid_clock_are_rejected(tmp_path, manifest):
    store = Store()
    with pytest.raises(ValueError):
        create(tmp_path, manifest, store, source_url="https://evil.example/invented.pdf")
    with pytest.raises(ValueError):
        create(tmp_path, manifest, store, captured_at=datetime(2026, 1, 1))
    assert store.rows == {}


def test_committed_receipt_tampering_is_detected(tmp_path, manifest):
    import json

    receipt = create(tmp_path, manifest, Store())
    path = tmp_path / "captures" / receipt["capture_id"] / "receipt.json"
    altered = json.loads(path.read_text())
    altered["source_url"] = "https://www.federalreserve.gov/monetarypolicy/files/other.pdf"
    path.write_text(json.dumps(altered))
    with pytest.raises(ValueError, match="hash"):
        read_capture(tmp_path, receipt["capture_id"])


def test_payload_write_failure_retains_attempt_receipt(tmp_path, manifest, monkeypatch):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io import bronze

    store = Store()
    original = bronze.os.link

    def fail_payload(source, destination):
        if destination.name == "payload.bin":
            raise OSError("simulated write failure")
        original(source, destination)

    monkeypatch.setattr(bronze.os, "link", fail_payload)
    with pytest.raises(OSError):
        create(tmp_path, manifest, store)
    directory = next((tmp_path / "captures").iterdir())
    assert (directory / "receipt.json").is_file()
    assert not (directory / "committed.json").exists()
    assert store.rows == {}
