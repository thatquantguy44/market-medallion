"""FOMC-AC01/T03: offline expected-page discovery; never guess year URLs."""

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.discovery import (
    discover_inventory_pages,
    parse_historical_index,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

BASE = "https://www.federalreserve.gov/monetarypolicy/"
INDEX = BASE + "fomc_historical_year.htm"
CALENDAR = BASE + "fomccalendars.htm"
FIXTURE = Path("tests/fixtures/fomc_meeting_minutes/invented-year-index.html").read_bytes()
CALENDAR_HTML = b"""<div class="panel"><h4 class="panel-heading">1938 FOMC Meetings</h4>
<div class="fomc-meeting"><div class="fomc-meeting__month">June</div>
<div class="fomc-meeting__date">16-17</div></div></div>"""


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def test_index_preserves_original_link_evidence_and_ignores_navigation(manifest):
    parsed = parse_historical_index(FIXTURE, INDEX, manifest)
    assert not parsed.issues
    assert [p.year for p in parsed.year_pages] == [1936, 1937]
    assert parsed.year_pages[0].source_page_url == BASE + "fomchistorical1936.htm"
    assert parsed.year_pages[0].label_raw == "1936"
    assert parsed.year_pages[0].href_raw == "/monetarypolicy/fomchistorical1936.htm"
    assert len(parsed.payload_sha256) == 64
    with pytest.raises(FrozenInstanceError):
        parsed.year_pages[0].year = 2000


@pytest.mark.parametrize(
    "old,new,code",
    [
        (b">1937</a>", b">1938</a>", "conflicting_index_year"),
        (
            b"/monetarypolicy/fomchistorical1937.htm",
            b"https://other.example/fomchistorical1937.htm",
            "unreviewed_year_url",
        ),
        (b">1937</a>", b">More years</a>", "unsupported_index_label"),
        (b"1930 - 1939", b"Historical years", "unsupported_index_heading"),
        (b"fomchistorical1937.htm", b"fomchistorical1937.htm?x=1", "unreviewed_year_url"),
    ],
)
def test_index_source_changes_are_issues_instead_of_silent_exclusions(manifest, old, new, code):
    result = parse_historical_index(FIXTURE.replace(old, new), INDEX, manifest)
    assert code in {i.code for i in result.issues}


def test_index_reports_missing_years_without_manufacturing_links(manifest):
    changed = FIXTURE.replace(b"1937", b"1938")
    parsed = parse_historical_index(changed, INDEX, manifest)
    assert [p.year for p in parsed.year_pages] == [1936, 1938]
    assert "missing_index_years" in {i.code for i in parsed.issues}
    assert all("1937" not in p.source_page_url for p in parsed.year_pages)


@pytest.mark.parametrize(
    "payload,url,kwargs",
    [
        (FIXTURE, CALENDAR, {}),
        (FIXTURE, INDEX, {"max_bytes": 10}),
        (FIXTURE, INDEX, {"max_year_pages": 1}),
        (FIXTURE, INDEX, {"max_year_pages": True}),
        (b"\xff", INDEX, {}),
        (b'<div id="article"><h3>Unexpected page</h3></div>', INDEX, {}),
        (b'<div id="article"><h3>Historical Materials by Year</h3></div>', INDEX, {}),
    ],
)
def test_index_rejects_wrong_shape_or_exhausted_bounds(manifest, payload, url, kwargs):
    with pytest.raises(ValueError):
        parse_historical_index(payload, url, manifest, **kwargs)


def test_discovery_records_explicit_bounded_expected_pages_and_hashes(manifest):
    result = discover_inventory_pages(
        FIXTURE, CALENDAR_HTML, manifest, start_year=1936, end_year=1938
    )
    assert not result.issues
    assert result.expected_pages == tuple(
        sorted(
            [
                INDEX,
                CALENDAR,
                BASE + "fomchistorical1936.htm",
                BASE + "fomchistorical1937.htm",
            ]
        )
    )
    assert result.historical_years == (1936, 1937)
    assert result.calendar_years == (1938,)
    assert result.selected_years == (1936, 1937, 1938)
    evidence = result.to_dict()
    evidence["expected_pages"].clear()
    assert result.expected_pages
    assert result.payload_hashes[0][1] != result.payload_hashes[1][1]


def test_discovery_overlap_retains_both_sources(manifest):
    calendar = CALENDAR_HTML.replace(b"1938", b"1937")
    result = discover_inventory_pages(FIXTURE, calendar, manifest, start_year=1937, end_year=1937)
    assert not result.issues
    assert result.expected_pages == tuple(
        sorted([INDEX, CALENDAR, BASE + "fomchistorical1937.htm"])
    )


def test_missing_selected_year_and_source_issues_block_coverage(manifest):
    result = discover_inventory_pages(
        FIXTURE, CALENDAR_HTML, manifest, start_year=1936, end_year=1939
    )
    assert "undiscovered_selected_years" in {i.code for i in result.issues}
    result = discover_inventory_pages(
        FIXTURE,
        CALENDAR_HTML.replace(b"16-17", b"16 (unreviewed vote)"),
        manifest,
        start_year=1936,
        end_year=1938,
    )
    assert "unsupported_meeting_label" in {i.code for i in result.issues}


@pytest.mark.parametrize(
    "start,end", [(1935, 1936), (1937, 1936), (1936, 2100), (True, 1937), (1936, 1937.0)]
)
def test_discovery_requires_a_bounded_explicit_year_selection(manifest, start, end):
    with pytest.raises(ValueError):
        discover_inventory_pages(FIXTURE, CALENDAR_HTML, manifest, start_year=start, end_year=end)


def test_index_rejects_year_links_moved_outside_reviewed_panels(manifest):
    changed = FIXTURE.replace(
        b"</div></div></body>",
        b'</div><a href="/monetarypolicy/fomchistorical1938.htm">1938</a></div></body>',
    )
    parsed = parse_historical_index(changed, INDEX, manifest)
    assert "unsupported_index_link_location" in {i.code for i in parsed.issues}


def test_index_empty_year_panel_is_an_issue(manifest):
    changed = FIXTURE.replace(
        b'<ul class="list-unstyled">',
        b'<ul class="list-unstyled"></ul></div><div class="panel">'
        b'<h5 class="panel-heading">1940 - 1949</h5><ul>',
    )
    parsed = parse_historical_index(changed, INDEX, manifest)
    assert "empty_index_panel" in {i.code for i in parsed.issues}


def test_current_calendar_empty_or_changed_year_panels_block_discovery(manifest):
    for extra in (
        b'<div class="panel"><h4 class="panel-heading">1939 FOMC Meetings</h4></div>',
        b'<div class="panel"><h4 class="panel-heading">1940 FOMC Meetings (revised)</h4></div>',
    ):
        result = discover_inventory_pages(
            FIXTURE, CALENDAR_HTML + extra, manifest, start_year=1936, end_year=1938
        )
        assert result.issues
