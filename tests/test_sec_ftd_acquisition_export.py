import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.acquisition import acquire_partition
from market_data_pipeline.pipelines.sec_fails_to_deliver.bronze import load_capture
from market_data_pipeline.pipelines.sec_fails_to_deliver.operations import health
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import acquire_and_run, run
from market_data_pipeline.pipelines.sec_fails_to_deliver.research_export import (
    export_research_artifact,
)


class Response:
    status = 200
    code = 200

    def __init__(self, payload, headers=None):
        self.payload = payload
        self.headers = headers or {"Content-Type": "application/octet-stream"}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, maximum):
        return self.payload[:maximum]


def _zip():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "cnsfails202610a.txt",
            Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes(),
        )
    return stream.getvalue()


def _config(tmp_path, *, enabled=True):
    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    value.update(warehouse="sqlite", root=str(tmp_path / "root"))
    value["acquisition"]["enabled"] = enabled
    value["partitions"] = {
        "fixture": {
            "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
            "input": str(tmp_path / "local.zip"),
            "source_url": (
                "https://www.sec.gov/files/data/other/fails-deliver-data/cnsfails202610a.zip"
            ),
        }
    }
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_acquire_run_archives_https_provenance_and_exports_current_research(tmp_path, monkeypatch):
    path = _config(tmp_path)
    monkeypatch.setenv("SEC_FTD_USER_AGENT", "MarketMedallion research operator@example.test")
    payload = _zip()
    now = datetime(2026, 11, 4, 15, 30, tzinfo=UTC)
    seen = []

    def transport(request):
        seen.append(request)
        return Response(payload, {"Content-Type": "application/octet-stream", "ETag": '"v1"'})

    result = acquire_and_run(path, "fixture", transport=transport, clock=lambda: now)
    assert result["status"] == "SUCCEEDED"
    assert result["transport"] == "https"
    assert len(seen) == 1
    assert seen[0].get_header("User-agent").endswith("operator@example.test")
    receipt, captured, _, _ = load_capture(tmp_path / "root" / "bronze" / result["run_id"])
    assert captured == payload
    assert receipt["receipt_version"] == "sec-ftd-capture/3"
    assert receipt["source_url"].endswith("cnsfails202610a.zip")
    assert receipt["origin_retrieved_at"] == "2026-11-04T15:30:00.000000Z"
    assert receipt["http_route"] == "injected"

    destination = tmp_path / "exchange"
    exported = export_research_artifact(path, destination, generated_at=now)
    pointer = json.loads((destination / "current.json").read_text())
    generation = destination / pointer["generation"]
    manifest = json.loads((generation / "manifest.json").read_text())
    assert exported["observation_count"] == 2
    assert exported["source_coverage"] == "NOT_CERTIFIED"
    assert pq.read_table(generation / "observations.parquet").num_rows == 2
    assert manifest["semantics"]["unit"] == "shares outstanding as of settlement date"
    assert "CURRENT_VINTAGE" == manifest["source"]["knowledge_basis"]


def test_acquisition_is_explicit_bounded_and_health_never_requests(tmp_path, monkeypatch):
    disabled = _config(tmp_path, enabled=False)
    monkeypatch.setenv("SEC_FTD_USER_AGENT", "operator@example.test")
    with pytest.raises(ValueError, match="disabled"):
        acquire_partition(disabled, "fixture", transport=lambda _request: Response(_zip()))
    result = health(disabled)
    assert result["request_performed"] is False
    assert result["acquisition_enabled"] is False
    assert result["partitions"][0]["acquisition_configured"] is True

    enabled = _config(tmp_path, enabled=True)
    monkeypatch.setenv("SEC_FTD_USER_AGENT", "no-contact")
    with pytest.raises(ValueError, match="User-Agent"):
        acquire_partition(enabled, "fixture", transport=lambda _request: Response(_zip()))


def test_research_export_is_bounded_and_requires_a_coherent_publication(tmp_path):
    path = _config(tmp_path, enabled=False)
    with pytest.raises(ValueError, match="current publication"):
        export_research_artifact(path, tmp_path / "exchange")
    (tmp_path / "local.zip").write_bytes(_zip())
    result = run(path, "fixture", clock=lambda: datetime(2026, 11, 4, tzinfo=UTC))
    assert result["status"] == "SUCCEEDED"
    with pytest.raises(ValueError, match="max_rows"):
        export_research_artifact(path, tmp_path / "exchange", max_rows=1)
