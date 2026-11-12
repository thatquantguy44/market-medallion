"""FOMC-AC06: explicit scope, bounded settings and source-owned retry semantics."""

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import (
    load_operations,
    retry_delay,
    select_documents,
    selected_years,
)


@pytest.fixture
def settings():
    return load_operations(Path("config/fomc_meeting_minutes/operations.example.yml"))


def test_year_scope_and_all_budgets_are_explicit(settings):
    assert selected_years(1990, 1991, 1990, settings) == (1990, 1991)
    assert settings.scheduled_live_access is False
    for bounds in [(1989, 1990), (1991, 1990), (1990, 1992), (True, 1990)]:
        with pytest.raises(ValueError):
            selected_years(*bounds, 1990, settings)
    for change in [
        {"max_requests": 2},
        {"max_attempts": 0},
        {"max_total_bytes": 0},
        {"request_interval_seconds": float("nan")},
        {"scheduled_live_access": "true"},
    ]:
        with pytest.raises(ValueError):
            replace(settings, **change)


def test_retry_after_numeric_and_http_date_never_retry_earlier(settings):
    now = datetime(2026, 11, 12, 17, tzinfo=UTC)
    assert retry_delay({"http_status": 429, "headers": {"retry-after": "5"}}, 1, now, settings) == 5
    assert (
        retry_delay(
            {"http_status": 503, "headers": {"retry-after": "Thu, 12 Nov 2026 17:00:07 GMT"}},
            1,
            now,
            settings,
        )
        == 7
    )
    assert (
        retry_delay({"http_status": 503, "headers": {"retry-after": "120"}}, 1, now, settings)
        is None
    )
    assert retry_delay({"http_status": 404, "headers": {}}, 1, now, settings) is None
    assert (
        retry_delay({"http_status": 503, "headers": {}}, settings.max_attempts, now, settings)
        is None
    )
    assert retry_delay({"http_status": None, "headers": {}}, 1, now, settings) >= 1


def test_document_selection_uses_meeting_associations_and_deduplicates_urls(settings):
    import json

    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import (
        inventory_from_dict,
    )
    from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    value = json.loads(
        Path("tests/fixtures/fomc_meeting_minutes/invented-current.json").read_text()
    )
    inventory = inventory_from_dict(value, manifest)
    rows = select_documents(inventory, (2020,), settings)
    assert len({d.source_url for d in rows}) == len(rows)
    assert all(d.document_id in {e.document_id for e in inventory.entries} for d in rows)
