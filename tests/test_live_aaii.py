import io
import json
from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.aaii.data.xls import normalize_rows, selection
from market_data_pipeline.pipelines.aaii.manifest import load_manifest

MANIFEST = Path("manifests/aaii-investor-sentiment-xls.yml")


def cells():
    fixture = json.loads(Path("tests/fixtures/aaii/workbook_rows.json").read_text())
    rows = fixture["rows"]
    for row in rows:
        if isinstance(row[0], str) and row[0][:4].isdigit():
            row[0] = date.fromisoformat(row[0])
    return fixture["header"], rows


def test_bounded_xls_cells_keep_reported_dates_and_exclude_summaries():
    header, rows = cells()
    result = normalize_rows(
        header,
        rows,
        load_manifest(MANIFEST),
        date(2026, 10, 26),
        selection("2026-10-01", "2026-10-26"),
    )
    assert [row.survey_date for row in result] == ["2026-10-01", "2026-10-08", "2026-10-22"]
    assert result[-1].bullish == 0.37
    assert result[-1].provider_published_at is None


def test_xls_header_drift_missing_selected_shares_and_duplicates_fail():
    header, rows = cells()
    manifest = load_manifest(MANIFEST)
    bounds = selection("2026-10-01", "2026-10-26")
    for bad_header, bad_rows in [
        (["Unknown", *header[1:]], rows),
        (header, [[date(2026, 10, 1), None, 0.2, 0.8]]),
        (header, [rows[2], rows[2]]),
        (header, [["10/01/2026", 0.35, 0.2, 0.45]]),
    ]:
        with pytest.raises(ValueError):
            normalize_rows(bad_header, bad_rows, manifest, date(2026, 10, 26), bounds)


def test_errors_outside_selection_do_not_invalidate_the_selected_observations():
    header, rows = cells()
    rows.insert(0, [date(1990, 1, 1), "Excel error cell", None, None])
    result = normalize_rows(
        header,
        rows,
        load_manifest(MANIFEST),
        date(2026, 10, 26),
        selection("2026-10-01", "2026-10-26"),
    )
    assert len(result) == 3


@pytest.mark.parametrize(
    "start,end", [(None, None), ("2026-10-26", "2026-10-01"), ("2000-01-01", "2026-10-26")]
)
def test_live_selection_must_be_explicit_ordered_and_bounded(start, end):
    with pytest.raises(ValueError):
        selection(start, end)


def test_http_failure_is_archived_and_cannot_promote_rows(tmp_path, monkeypatch):
    import market_data_pipeline.pipelines.aaii.source as source
    from market_data_pipeline.pipelines.aaii.io.warehouse import Warehouse

    class Response(io.BytesIO):
        code = 503
        headers = {"Content-Type": "text/html"}

    with pytest.raises(ValueError, match="HTTP 503"):
        source.run_live(
            MANIFEST,
            tmp_path,
            start="2026-10-01",
            end="2026-10-26",
            transport=lambda *a, **k: Response(b"Unavailable"),
        )
    raw = next((tmp_path / "bronze").rglob("payload.xls"))
    assert raw.read_bytes() == b"Unavailable"
    envelope = json.loads((raw.parent / "metadata.json").read_text())
    assert envelope["http_status"] == 503
    assert envelope["selection"]["start"] == "2026-10-01"
    with Warehouse(tmp_path) as warehouse:
        assert not warehouse.versions()
        assert warehouse.runs()[0]["status"] == "failed"
