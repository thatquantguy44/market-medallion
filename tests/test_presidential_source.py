"""PR016-R12/N03: source gates and bounded acquisition are fail closed."""

import io
from datetime import UTC, datetime
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import pytest

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.source import fetch_selected
from market_data_pipeline.pipelines.presidential_research.storage import load_capture

CONFIG = load_config("config/presidential_research/sources.toml")
NOW = datetime(2026, 11, 13, tzinfo=UTC)


class Response(io.BytesIO):
    status = 200

    def __init__(self, content, headers=None, status=200):
        super().__init__(content)
        self.headers = headers or {}
        self.status = status


def test_no_probe_for_unreviewed_sources(tmp_path):
    with patch(
        "market_data_pipeline.pipelines.presidential_research.source.prepare_http",
        side_effect=AssertionError("must not probe"),
    ):
        with pytest.raises(ValueError, match="SOURCE_ACCESS_NOT_REVIEWED"):
            fetch_selected(tmp_path, CONFIG, ["FF_MKT_001"])
        with pytest.raises(ValueError, match="HTTPS"):
            fetch_selected(tmp_path, CONFIG, ["SHILLER_001"])
    assert not list(tmp_path.iterdir())


def test_transient_retry_partial_batch_and_synthetic_classification(tmp_path):
    calls = []

    def client(request):
        calls.append(request.full_url)
        if len(calls) == 1:
            raise URLError("transient secret should not escape")
        if len(calls) == 2:
            return Response(b"observation_date,FEDFUNDS\n1970-01-01,8.98\n")
        raise HTTPError(request.full_url, 403, "private", {}, None)

    result = fetch_selected(
        tmp_path, CONFIG, ["FEDFUNDS_001", "FF_MKT_001"], transport=client, clock=lambda: NOW
    )
    assert result["status"] == "PARTIAL_OR_FAILED"
    good, failed = result["sources"]
    assert good["status"] == "PARSED" and good["input_kind"] == "SYNTHETIC_FIXTURE"
    assert failed == {
        "source_id": "FF_MKT_001",
        "status": "FETCH_FAILED",
        "error_code": "SOURCE_REQUEST_FAILED",
    }
    receipt, payload, _ = load_capture(tmp_path, "FEDFUNDS_001", good["capture_id"])
    assert receipt["input_kind"] == "SYNTHETIC_FIXTURE" and b"1970" in payload
    assert len(calls) == 3


@pytest.mark.parametrize(
    "response",
    [
        Response(b"x", {"Content-Length": "999999999"}),
        Response(b"x", {"Content-Length": "2"}),
        Response(b""),
        Response(b"x", status=404),
    ],
)
def test_bad_response_not_archived(tmp_path, response):
    result = fetch_selected(
        tmp_path, CONFIG, ["FEDFUNDS_001"], transport=lambda request: response, clock=lambda: NOW
    )
    assert result["sources"][0]["status"] == "FETCH_FAILED"
    assert not list(tmp_path.iterdir())


def test_malformed_raw_is_retained_for_review(tmp_path):
    result = fetch_selected(
        tmp_path,
        CONFIG,
        ["FEDFUNDS_001"],
        transport=lambda request: Response(b"not a csv"),
        clock=lambda: NOW,
    )
    source = result["sources"][0]
    assert source["status"] == "REJECTED"
    assert load_capture(tmp_path, "FEDFUNDS_001", source["capture_id"])[1] == b"not a csv"
