"""T03: scoped native publication retains reviewed exclusions and bundled claims."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_fomc_postgres import store as store

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
    PostgresBronzeStore,
    capture,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

__all__ = ["store"]
pytestmark = pytest.mark.postgres


def test_scoped_native_evidence_and_unavailable_outcomes_commit_atomically(store, tmp_path):
    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    base = "https://www.federalreserve.gov/monetarypolicy/"
    samples = {
        "fomc_historical_year.htm": b"""<div id="article">
    <h3>Historical Materials by Year</h3>
        <div class="panel">
    <h5 class="panel-heading">1990 - 1999</h5>
        <a href="fomchistorical1990.htm">1990</a>
    <a href="fomchistorical1991.htm">1991</a>
    </div>
    </div>""",
        "fomccalendars.htm": b"""<div class="panel">
    <h4 class="panel-heading">1992 FOMC Meetings</h4>
        <div class="fomc-meeting">
    <div class="fomc-meeting__month">June</div>
    <div class="fomc-meeting__date">16-17</div>
    </div>
    </div>""",
        "fomchistorical1990.htm": b"""<div class="panel">
    <h5 class="panel-heading">March 2 Meeting - 1990</h5>
        <p>Minutes: See end of minutes of March 15 meeting</p>
    </div>
        <div class="panel">
    <h5 class="panel-heading">March 15 Meeting - 1990</h5>
        <p>
    <a href="/monetarypolicy/fake.htm">Minutes</a>
    </p>
    </div>
        <div class="panel">
    <h5 class="panel-heading">March 17-18 (cancelled) Meeting - 1990</h5>
    </div>""",
        "fomchistorical1991.htm": b"""<div class="panel">
    <h5 class="panel-heading">December 30 Conference Call - 1991</h5>
        <p>
    <a href="">Minutes</a>
    </p>
    </div>""",
    }
    ids = []
    for name, payload in samples.items():
        ids.append(
            capture(
                tmp_path,
                payload,
                manifest=manifest,
                store=bronze,
                source_url=base + name,
                kind="inventory",
                captured_at=datetime.now(UTC),
                status=200,
                headers={},
                max_bytes=2000,
            )["capture_id"]
        )
    result = bronze.publish_inventory_captures(
        ids, manifest, root=tmp_path, historical_years=(1990, 1992), start_year=1990
    )
    evidence = bronze.read_inventory_discovery(result["run_id"])
    assert evidence["operating_start_year"] == 1990
    native = evidence["native_page_evidence"][base + "fomchistorical1990.htm"]
    assert (
        native["cross_references"][0]["label_raw"]
        == "Minutes: See end of minutes of March 15 meeting"
    )
    assert any(e["reason"] == "cancelled_meeting" for e in native["exclusions"])
    assert {e["minutes_status"] for e in store.read_inventory()["entries"]} >= {
        "listed",
        "unavailable",
        "not_listed",
    }
    assert len(store.read_inventory()["documents"]) == 1
