"""T17 real-source diagnosis: NYSE Download supports GET, while HEAD returns 404."""

import json
from pathlib import Path
from urllib.error import HTTPError

import pytest
import yaml
from test_reg_sho_operations_export import Response, _config

from market_data_pipeline.pipelines.reg_sho_thresholds import acquisition
from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import acquire_and_run
from market_data_pipeline.shared.http import prepare_http

PAGE = "https://www.nyse.com/regulation/regulation-sho"
DOWNLOAD = (
    "https://www.nyse.com/api/regulatory/threshold-securities/download?"
    "selectedDate=2026-11-03&market=NYSE"
)


def _nyse_config(tmp_path):
    path = _config(tmp_path)
    value = yaml.safe_load(path.read_text())
    value["network"] = {"mode": "direct"}
    value["partitions"]["fixture"].update(
        provider="nyse",
        market="NYSE",
        manifest=str(Path("manifests/reg-sho-nyse.yml").resolve()),
        source_url=DOWNLOAD,
    )
    path.write_text(yaml.safe_dump(value))
    return path


def _prepare(monkeypatch, opener):
    def prepare(request, settings):
        return prepare_http(request, settings, opener_factory=lambda *_args: opener)

    monkeypatch.setattr(acquisition, "prepare_http", prepare)
    monkeypatch.setenv("REG_SHO_USER_AGENT", "MarketMedallion https://example.test/contact")


def test_nyse_uses_official_page_probe_and_keeps_download_bytes(tmp_path, monkeypatch):
    config = _nyse_config(tmp_path)
    payload = Path("tests/fixtures/reg_sho_thresholds/nyse-empty.txt").read_bytes()
    requests = []

    def opener(request, *, timeout):
        requests.append((request.get_method(), request.full_url, request.get_header("Accept")))
        if request.get_method() == "HEAD" and request.full_url == DOWNLOAD:
            raise HTTPError(DOWNLOAD, 404, "HEAD not mapped", {}, None)
        if request.get_method() == "GET" and request.get_header("Accept") != "*/*":
            raise HTTPError(DOWNLOAD, 406, "Not acceptable", {}, None)
        if request.get_method() == "HEAD":
            assert request.full_url == PAGE
        response = Response(payload)
        response.code = 200
        return response

    _prepare(monkeypatch, opener)
    result = acquire_and_run(config, "fixture")
    assert result["status"] == "SUCCEEDED"
    assert result["coverage_state"] == "COMPLETE_EMPTY"
    assert requests == [("HEAD", PAGE, "*/*"), ("GET", DOWNLOAD, "*/*")]
    bronze = tmp_path / "root" / "bronze" / result["run_id"]
    assert (bronze / "payload.bin").read_bytes() == payload
    receipt = json.loads((bronze / "receipt.json").read_text())
    assert receipt["acquisition"]["http_probe_url"] == PAGE
    assert receipt["source_url"] == DOWNLOAD


def test_nyse_rejected_startup_does_not_attempt_download(tmp_path, monkeypatch):
    config = _nyse_config(tmp_path)
    requests = []

    def opener(request, *, timeout):
        requests.append(request.get_method())
        raise HTTPError(request.full_url, 403, "Denied", {}, None)

    _prepare(monkeypatch, opener)
    with pytest.raises(ValueError, match="HTTP 403"):
        acquisition.acquire_partition(config, "fixture")
    assert requests == ["HEAD"]
    assert not (tmp_path / "root").exists()


def test_mismatched_provider_manifest_fails_before_probe(tmp_path, monkeypatch):
    config = _nyse_config(tmp_path)
    value = yaml.safe_load(config.read_text())
    value["partitions"]["fixture"]["manifest"] = str(Path("manifests/reg-sho-nasdaq.yml").resolve())
    config.write_text(yaml.safe_dump(value))

    def no_probe(*_args, **_kwargs):
        raise AssertionError("mismatched provider must not probe")

    monkeypatch.setattr(acquisition, "prepare_http", no_probe)
    with pytest.raises(ValueError, match="provider manifest"):
        acquisition.acquire_partition(config, "fixture")
