"""Bounded fake HTTP responses never perform network I/O."""

import io
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition import acquire_one
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import read_capture
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest


class Store:
    def save_capture(self, receipt, payload):
        self.receipt, self.payload = receipt, payload


class Response(io.BytesIO):
    def __init__(self, payload, status=200, headers=None):
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def test_acquisition_bounded_and_records_incomplete_bytes(tmp_path, manifest):
    store = Store()
    response = Response(b"0123456789")
    receipt = acquire_one(
        tmp_path,
        manifest=manifest,
        store=store,
        client=lambda req: response,
        source_url="https://www.federalreserve.gov/monetarypolicy/files/fake.pdf",
        kind="document",
        max_bytes=4,
        now=lambda: datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert receipt["body_complete"] is False and not receipt["accepted"]
    assert "incomplete_body" in receipt["reasons"]
    assert read_capture(tmp_path, receipt["capture_id"])[1] == b"01234"


def test_http_rejection_is_captured(tmp_path, manifest):
    store = Store()

    def client(req):
        raise HTTPError(
            req.full_url, 404, "missing", {"Content-Type": "text/plain"}, io.BytesIO(b"missing")
        )

    receipt = acquire_one(
        tmp_path,
        manifest=manifest,
        store=store,
        client=client,
        source_url="https://www.federalreserve.gov/monetarypolicy/files/fake.pdf",
        kind="document",
        max_bytes=100,
        now=lambda: datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert receipt["http_status"] == 404 and not receipt["accepted"]
    assert store.payload == b"missing"


def test_unreviewed_url_does_not_call_http(tmp_path, manifest):
    def client(req):
        raise AssertionError("network call")

    with pytest.raises(ValueError):
        acquire_one(
            tmp_path,
            manifest=manifest,
            store=Store(),
            client=client,
            source_url="https://other.example/x.pdf",
            kind="document",
            max_bytes=10,
        )


def test_run_enforces_configured_request_and_byte_limits(tmp_path, manifest):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition import (
        AcquisitionRun,
        load_acquisition_limits,
    )

    config = tmp_path / "limits.yml"
    config.write_text("max_requests: 1\ninventory_max_bytes: 100\ndocument_max_bytes: 4\n")
    limits = load_acquisition_limits(config)
    requests = []

    def client(req):
        requests.append(req.full_url)
        return Response(b"12345")

    run = AcquisitionRun(tmp_path, manifest=manifest, store=Store(), client=client, limits=limits)
    receipt = run.acquire(
        "https://www.federalreserve.gov/monetarypolicy/files/fake.pdf", "document"
    )
    assert not receipt["body_complete"] and len(requests) == 1
    with pytest.raises(ValueError, match="request budget"):
        run.acquire("https://www.federalreserve.gov/monetarypolicy/files/another.pdf", "document")
    assert len(requests) == 1
    config.write_text("max_requests: 0\ninventory_max_bytes: 100\ndocument_max_bytes: 4\n")
    with pytest.raises(ValueError):
        load_acquisition_limits(config)


def test_redirect_handler_does_not_follow_target():
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition import _NoRedirect

    assert _NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.example/") is None


def test_transport_failure_is_recorded_without_exception_details(tmp_path, manifest):
    from urllib.error import URLError

    store = Store()

    def client(req):
        raise URLError("potentially sensitive proxy details")

    receipt = acquire_one(
        tmp_path,
        manifest=manifest,
        store=store,
        client=client,
        source_url="https://www.federalreserve.gov/monetarypolicy/files/fake.pdf",
        kind="document",
        max_bytes=100,
    )
    assert receipt["http_status"] is None
    assert "transport_error" in receipt["reasons"] and not receipt["body_complete"]
    assert "sensitive" not in str(receipt)
    assert store.payload == b""


def test_png_asset_acquisition_preserves_original_bytes_and_budget(tmp_path, manifest):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition import (
        AcquisitionLimits,
        AcquisitionRun,
    )

    payload = b"\x89PNG\r\n\x1a\nsynthetic-image-fixture"
    store = Store()
    run = AcquisitionRun(
        tmp_path,
        manifest=manifest,
        store=store,
        client=lambda req: Response(payload),
        limits=AcquisitionLimits(max_requests=1, inventory_max_bytes=100, document_max_bytes=100),
    )
    receipt = run.acquire("https://www.federalreserve.gov/sep20190619figure3a.png", "asset")
    assert receipt["kind"] == "asset" and receipt["accepted"]
    assert read_capture(tmp_path, receipt["capture_id"])[1] == payload
    assert run.requests == 1


def test_operating_scope_blocks_pre1990_year_pages_before_request(tmp_path, manifest):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition import (
        AcquisitionLimits,
        AcquisitionRun,
    )

    requests = []
    run = AcquisitionRun(
        tmp_path,
        manifest=manifest,
        store=Store(),
        client=lambda req: requests.append(req),
        limits=AcquisitionLimits(max_requests=1, inventory_max_bytes=100, document_max_bytes=100),
        start_year=1990,
    )
    with pytest.raises(ValueError, match="scope"):
        run.acquire(
            "https://www.federalreserve.gov/monetarypolicy/fomchistorical1989.htm", "inventory"
        )
    assert requests == [] and run.requests == 0
