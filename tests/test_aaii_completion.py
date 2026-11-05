import io
import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.aaii.io.warehouse import Warehouse

FIXTURES = Path("tests/fixtures/aaii")


def test_live_dagster_xls_retains_provenance_versions_and_asof(tmp_path, monkeypatch):
    import market_data_pipeline.pipelines.aaii.source as source
    from market_data_pipeline.pipelines.aaii.dagster_defs import defs

    day = [26]
    payload = [(FIXTURES / "synthetic.xls").read_bytes()]

    class Response(io.BytesIO):
        code = 200
        headers = {"Content-Type": "application/vnd.ms-excel", "ETag": "synthetic-only"}

    monkeypatch.setattr(
        source, "prepare_http", lambda *a, **k: lambda *a, **k: Response(payload[0])
    )
    monkeypatch.setattr(source, "utc_now", lambda: datetime(2026, 10, day[0], 12, tzinfo=UTC))
    config_file = tmp_path / "runtime.yml"
    config_file.write_text(yaml.safe_dump({"warehouse": "sqlite", "root": "warehouse"}))
    root = tmp_path / "warehouse"
    job = defs.resolve_job_def("aaii_sentiment_job")
    config = {
        "ops": {
            "aaii_sentiment": {
                "config": {
                    "live": True,
                    "config_path": str(config_file),
                    "manifest_path": "manifests/aaii-investor-sentiment-xls.yml",
                    "start": "2026-10-01",
                    "end": "2026-10-26",
                }
            }
        }
    }
    for number, filename in enumerate(
        ["synthetic.xls", "synthetic.xls", "synthetic-correction.xls", "synthetic.xls"]
    ):
        day[0] = 26 + number
        payload[0] = (FIXTURES / filename).read_bytes()
        result = job.execute_in_process(run_config=config)
        assert result.success
        assert result.asset_materializations_for_node("aaii_sentiment")
    with Warehouse(root) as warehouse:
        assert len(warehouse.versions()) == 5
        assert len(warehouse.gold()) == 3
        old = warehouse.gold("2026-10-27T23:59:00Z")
        revised = warehouse.gold("2026-10-28T23:59:00Z")
        assert old[1]["bullish"] == 0.4
        assert revised[1]["bullish"] == 0.45
        assert warehouse.gold()[1]["bullish"] == 0.4
        assert [run["inserted_versions"] for run in warehouse.runs()] == [3, 0, 1, 1]
    metadata = sorted((root / "bronze").rglob("metadata.json"))
    assert len(metadata) == 4
    for path in metadata:
        envelope = json.loads(path.read_text())
        assert envelope["selection"] == {"start": "2026-10-01", "end": "2026-10-26"}
        assert envelope["http_headers"]["ETag"] == "synthetic-only"
        assert (path.parent / "payload.xls").read_bytes() in {
            (FIXTURES / "synthetic.xls").read_bytes(),
            (FIXTURES / "synthetic-correction.xls").read_bytes(),
        }


def test_live_dagster_failure_keeps_previous_publication(tmp_path, monkeypatch):
    import market_data_pipeline.pipelines.aaii.source as source
    from market_data_pipeline.pipelines.aaii.dagster_defs import defs
    from market_data_pipeline.pipelines.aaii.pipeline import run

    root = tmp_path / "warehouse"
    run(
        FIXTURES / "synthetic.xls",
        Path("manifests/aaii-investor-sentiment-xls.yml"),
        root,
        start="2026-10-01",
        end="2026-10-26",
        captured_at=datetime(2026, 10, 26, tzinfo=UTC),
    )
    pointer = (root / "parquet/current.json").read_bytes()

    class Response(io.BytesIO):
        code = 200
        headers = {}

    monkeypatch.setattr(
        source, "prepare_http", lambda *a, **k: lambda *a, **k: Response(b"not a workbook")
    )
    monkeypatch.setattr(source, "utc_now", lambda: datetime(2026, 10, 30, tzinfo=UTC))
    result = defs.resolve_job_def("aaii_sentiment_job").execute_in_process(
        raise_on_error=False,
        run_config={
            "ops": {
                "aaii_sentiment": {
                    "config": {
                        "live": True,
                        "root": str(root),
                        "manifest_path": "manifests/aaii-investor-sentiment-xls.yml",
                        "start": "2026-10-01",
                        "end": "2026-10-26",
                    }
                }
            }
        },
    )
    assert not result.success
    assert (root / "parquet/current.json").read_bytes() == pointer
    with Warehouse(root) as warehouse:
        assert len(warehouse.versions()) == 3
        assert warehouse.runs()[-1]["status"] == "failed"
