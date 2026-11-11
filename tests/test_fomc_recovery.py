"""T03: durable pre-request intents and explicit offline recovery at archive boundaries."""

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.io import bronze
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition import (
    AcquisitionLimits,
    AcquisitionRun,
    acquire_one,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.recovery import recover_attempt
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

URL = "https://www.federalreserve.gov/monetarypolicy/files/invented.pdf"


class Store:
    def __init__(self):
        self.rows = {}

    def save_capture(self, receipt, payload):
        self.rows[receipt["capture_id"]] = (receipt, payload)


class Response(io.BytesIO):
    status = 200
    headers = {}


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def acquire(root, manifest, store, client=None):
    return acquire_one(
        root,
        manifest=manifest,
        store=store,
        client=client or (lambda req: Response(b"original")),
        source_url=URL,
        kind="document",
        max_bytes=100,
        now=lambda: datetime(2026, 11, 11, tzinfo=UTC),
    )


@pytest.mark.parametrize("boundary", ["receipt.json", "committed.json", "outcome"])
def test_write_boundary_failures_leave_intent_and_recover_without_new_capture(
    tmp_path, manifest, monkeypatch, boundary
):
    store = Store()
    original = bronze._write

    def fail(path, data):
        if path.name == boundary or (
            boundary == "outcome" and path.parent.name == "attempt_outcomes"
        ):
            raise OSError("simulated interrupted write")
        original(path, data)

    monkeypatch.setattr(bronze, "_write", fail)
    with pytest.raises(OSError):
        acquire(tmp_path, manifest, store)
    attempts = list((tmp_path / "attempts").glob("*.json"))
    assert len(attempts) == 1
    intent = json.loads(attempts[0].read_text())
    assert intent["phase"] == "get" and intent["source_url"] == URL
    monkeypatch.setattr(bronze, "_write", original)
    result = recover_attempt(tmp_path, intent["capture_id"], store)
    if boundary == "receipt.json":
        assert result["status"] == "interrupted_before_receipt" and store.rows == {}
    else:
        assert result["status"] == "committed" and len(store.rows) == 1
        assert (tmp_path / "captures" / intent["capture_id"] / "committed.json").is_file()
        assert (tmp_path / "attempt_outcomes" / (intent["capture_id"] + ".json")).is_file()


def test_missing_payload_link_recovers_from_verified_object(tmp_path, manifest, monkeypatch):
    store = Store()
    original = bronze.os.link

    def fail(source, destination):
        if destination.name == "payload.bin":
            raise OSError("link interrupted")
        original(source, destination)

    monkeypatch.setattr(bronze.os, "link", fail)
    with pytest.raises(OSError):
        acquire(tmp_path, manifest, store)
    identity = next((tmp_path / "attempts").glob("*.json")).stem
    monkeypatch.setattr(bronze.os, "link", original)
    assert recover_attempt(tmp_path, identity, store)["status"] == "committed"
    assert bronze.read_capture(tmp_path, identity)[1] == b"original"


def test_intent_write_failure_prevents_provider_request(tmp_path, manifest, monkeypatch):
    requests = []

    def fail(*args):
        raise OSError("intent cannot be written")

    monkeypatch.setattr(bronze, "_write", fail)
    with pytest.raises(OSError):
        acquire(tmp_path, manifest, Store(), lambda req: requests.append(req))
    assert requests == []


def test_startup_failure_archives_sanitized_rejected_attempt(tmp_path, manifest, monkeypatch):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io import acquisition

    store = Store()

    def fail(*args):
        raise ValueError("sensitive proxy://username:password@private")

    monkeypatch.setattr(acquisition, "prepare_fomc_http", fail)
    with pytest.raises(ValueError, match="startup failed"):
        AcquisitionRun.start(
            tmp_path,
            manifest=manifest,
            store=store,
            source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            settings=None,
            limits=AcquisitionLimits(
                max_requests=1, inventory_max_bytes=100, document_max_bytes=100
            ),
        )
    assert len(store.rows) == 1
    receipt, payload = next(iter(store.rows.values()))
    assert receipt["attempt_phase"] == "probe" and not receipt["accepted"] and payload == b""
    assert "startup_probe_error" in receipt["reasons"]
    assert "sensitive" not in str(receipt) and "password" not in str(receipt)


def test_incomplete_read_preserves_partial_received_bytes(tmp_path, manifest):
    from http.client import IncompleteRead

    class Broken(Response):
        def read(self, size):
            raise IncompleteRead(b"partial")

    receipt = acquire(tmp_path, manifest, Store(), lambda req: Broken())
    assert not receipt["body_complete"] and not receipt["accepted"]
    assert bronze.read_capture(tmp_path, receipt["capture_id"])[1] == b"partial"


def test_start_selects_once_and_reuses_client_with_existing_budget(tmp_path, manifest, monkeypatch):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io import acquisition

    calls = []

    class Client:
        route = "direct"
        fallback_used = False

        def __call__(self, request):
            calls.append(request.full_url)
            return Response(b"original")

    starts = []

    def prepare(*args):
        starts.append(True)
        return Client()

    monkeypatch.setattr(acquisition, "prepare_fomc_http", prepare)
    run = AcquisitionRun.start(
        tmp_path,
        manifest=manifest,
        store=Store(),
        source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        settings=None,
        limits=AcquisitionLimits(max_requests=1, inventory_max_bytes=100, document_max_bytes=100),
    )
    run.acquire(URL, "document")
    with pytest.raises(ValueError, match="budget"):
        run.acquire(URL, "document")
    assert starts == [True] and calls == [URL]
    outcomes = [json.loads(p.read_text()) for p in (tmp_path / "attempt_outcomes").glob("*.json")]
    assert {o["result"] for o in outcomes} == {"probe_selected", "captured"}


def test_object_write_failure_reports_missing_bytes_without_db_commit(
    tmp_path, manifest, monkeypatch
):
    store = Store()
    original = bronze._write

    def fail(path, data):
        if path.parent.name == "sha256":
            raise OSError("object write interrupted")
        original(path, data)

    monkeypatch.setattr(bronze, "_write", fail)
    with pytest.raises(OSError):
        acquire(tmp_path, manifest, store)
    identity = next((tmp_path / "attempts").glob("*.json")).stem
    monkeypatch.setattr(bronze, "_write", original)
    assert recover_attempt(tmp_path, identity, store)["status"] == "missing_original_bytes"
    assert store.rows == {}


def test_unreviewed_manifest_is_rejected_before_startup_intent_or_network(
    tmp_path, manifest, monkeypatch
):
    from dataclasses import replace

    from market_data_pipeline.pipelines.fomc_meeting_minutes.io import acquisition

    bad = replace(manifest, schema_id="unreviewed-profile")
    requests = []

    def prepare(*args):
        requests.append(True)
        raise AssertionError("network must not be reached")

    monkeypatch.setattr(acquisition, "prepare_fomc_http", prepare)
    with pytest.raises(ValueError):
        AcquisitionRun.start(
            tmp_path,
            manifest=bad,
            store=Store(),
            source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            settings=None,
            limits=AcquisitionLimits(
                max_requests=1, inventory_max_bytes=100, document_max_bytes=100
            ),
        )
    assert requests == [] and not (tmp_path / "attempts").exists()
