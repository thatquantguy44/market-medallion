"""T03 1990-onward closeout: reviewed native regimes and explicit source outcomes."""

from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.native import parse_native_page
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

BASE = "https://www.federalreserve.gov/monetarypolicy/"


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


@pytest.mark.parametrize(
    "label,year,expected",
    [
        ("June 30-July 1 Meeting - 1992", 1992, (date(1992, 6, 30), date(1992, 7, 1))),
        ("January 31-February 1 Meeting - 1995", 1995, (date(1995, 1, 31), date(1995, 2, 1))),
    ],
)
def test_historical_cross_month_dates_are_explicit(manifest, label, year, expected):
    payload = f"""<div class="panel">
    <h5 class="panel-heading">{label}</h5>
    <p>
    <a href="/monetarypolicy/fake.htm">Minutes</a>
    </p>
    </div>""".encode()
    result = parse_native_page(payload, BASE + f"fomchistorical{year}.htm", manifest)
    assert not result.issues and result.inventory.meetings[0].meeting_dates == expected


def test_pdf_size_label_inherits_minutes_kind_and_legacy_http_upgrades_only_for_refs(manifest):
    payload = b"""<div class="panel">
    <h5 class="panel-heading">June 16-17 Meeting - 2003</h5>
    <p>Minutes: <a href="/monetarypolicy/files/fake.pdf">320 KB PDF</a>
    </p>
    <p>
    <a href="http://www.federalreserve.gov/fomc/minutes/20030318.htm#phone">Minutes</a>
    </p>
    </div>"""
    result = parse_native_page(payload, BASE + "fomchistorical2003.htm", manifest)
    assert not result.issues and len(result.inventory.documents) == 2
    assert any(
        d.source_url == "https://www.federalreserve.gov/fomc/minutes/20030318.htm"
        for d in result.inventory.documents
    )
    assert any(e.href_raw.startswith("http:") for e in result.inventory.entries)
    with pytest.raises(ValueError):
        manifest.capture_url("http://www.federalreserve.gov/fomc/minutes/20030318.htm", "document")


def test_explicit_notation_and_cancelled_labels_are_preserved_exclusions(manifest):
    payload = b"""<div class="panel">
    <h5 class="panel-heading">March 17-18 (cancelled) Meeting - 2020</h5>
    </div>
    <div class="panel">
    <h5 class="panel-heading">March 19 (notation vote) - 2020</h5>
    <p>
    <a href="/newsevents/vote.htm">Press Release</a>
    </p>
    </div>
    <div class="panel">
    <h5 class="panel-heading">March 15 Meeting - 2020</h5>
    <p>
    <a href="/monetarypolicy/fake.htm">Minutes</a>
    </p>
    </div>"""
    result = parse_native_page(payload, BASE + "fomchistorical2020.htm", manifest)
    assert not result.issues and len(result.inventory.meetings) == 1
    assert {e.reason for e in result.exclusions} >= {
        "cancelled_meeting",
        "notation_vote_not_meeting",
    }


def test_notation_with_minutes_is_an_issue_instead_of_an_exclusion(manifest):
    payload = b"""<div class="panel">
    <h4 class="panel-heading">2025 FOMC Meetings</h4>
    <div class="fomc-meeting">
    <div class="fomc-meeting__month">August</div>
    <div class="fomc-meeting__date">22 (notation vote)</div>
    <div class="fomc-meeting__minutes">
    <a href="/monetarypolicy/fake.htm">HTML</a>
    </div>
    </div>
    </div>"""
    result = parse_native_page(payload, BASE + "fomccalendars.htm", manifest)
    assert any(i.code == "unexpected_nonmeeting_minutes" for i in result.issues)


def test_cross_referenced_minutes_keep_distinct_meetings_and_raw_claim(manifest):
    payload = b"""<div class="panel">
    <h5 class="panel-heading">March 2 (unscheduled) Meeting - 2020</h5>
    <p>Minutes: See end of minutes of March 15 meeting</p>
    </div>
    <div class="panel">
    <h5 class="panel-heading">March 15 (unscheduled) Meeting - 2020</h5>
    <p>Minutes (Released April 08, 2020): <a href="/monetarypolicy/fake.htm">HTML</a>
    | <a href="/monetarypolicy/files/fake.pdf">PDF</a>
    </p>
    </div>"""
    result = parse_native_page(payload, BASE + "fomchistorical2020.htm", manifest)
    assert not result.issues and len(result.inventory.meetings) == 2
    assert len(result.inventory.documents) == 2 and len(result.inventory.entries) == 4
    assert {e.minutes_status for e in result.inventory.entries} == {"listed"}
    assert result.cross_references[0].label_raw == "Minutes: See end of minutes of March 15 meeting"
    assert len(result.cross_references[0].target_urls) == 2


def test_bundled_two_day_target_is_resolved_without_changing_call_dates(manifest):
    payload = b"""<div class="panel">
    <h5 class="panel-heading">October 7 Conference Call - 2008</h5>
    <p>Minutes: See end of minutes of October 28/29 meeting</p>
    </div>
    <div class="panel">
    <h5 class="panel-heading">October 28-29 Meeting - 2008</h5>
    <p>
    <a href="/monetarypolicy/files/fake.pdf">Minutes</a>
    </p>
    </div>"""
    result = parse_native_page(payload, BASE + "fomchistorical2008.htm", manifest)
    assert not result.issues and len(result.inventory.documents) == 1
    assert len(result.inventory.entries) == 2
    assert any(
        m.event_kind == "conference_call" and m.meeting_dates == (date(2008, 10, 7),)
        for m in result.inventory.meetings
    )


def test_empty_source_minutes_link_is_unavailable_and_policy_addendum_is_outside_scope(manifest):
    payload = b"""<div class="panel">
    <h5 class="panel-heading">December 30 Conference Call - 1994</h5>
    <p>
    <a href="">Minutes</a>
    </p>
    <p>
    <a href="/newsevents/policy.htm">Addendum to the Policy Normalization Principles and Plans</a>
    </p>
    </div>"""
    result = parse_native_page(payload, BASE + "fomchistorical1994.htm", manifest)
    assert not result.issues and not result.inventory.documents
    assert result.inventory.entries[0].minutes_status == "unavailable"
    assert {e.reason for e in result.exclusions} >= {
        "empty_minutes_href",
        "outside_minutes_policy_document",
    }


def test_legacy_index_gaps_before_selected_scope_do_not_block_modern_discovery(manifest):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.discovery import (
        discover_inventory_pages,
    )

    index = b"""<div id="article">
    <h3>Historical Materials by Year</h3>
    <div class="panel">
    <h5 class="panel-heading">1990 - 1999</h5>
    <a href="fomchistorical1990.htm">1990</a>
    </div>
    </div>"""
    calendar = b"""<div class="panel">
    <h4 class="panel-heading">1991 FOMC Meetings</h4>
    <div class="fomc-meeting">
    <div class="fomc-meeting__month">March</div>
    <div class="fomc-meeting__date">15</div>
    </div>
    </div>"""
    result = discover_inventory_pages(index, calendar, manifest, start_year=1990, end_year=1991)
    assert not result.issues and len(result.expected_pages) == 3


def test_scoped_publication_rejects_older_years_before_warehouse_access(tmp_path, manifest):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore

    bronze = PostgresBronzeStore(object())
    with pytest.raises(ValueError, match="scope"):
        bronze.publish_inventory_captures(
            [], manifest, root=tmp_path, historical_years=(1936, 1937), start_year=1990
        )


def test_unscheduled_panel_without_event_noun_and_hyphen_target_are_retained(manifest):
    payload = b"""<div class="panel">
    <h5 class="panel-heading">October 4 (unscheduled) - 2019</h5>
    <p>Minutes: See end of minutes of October 29-30 meeting</p>
    </div>
    <div class="panel">
    <h5 class="panel-heading">October 29-30 Meeting - 2019</h5>
    <p>
    <a href="/monetarypolicy/fake.htm">Minutes</a>
    </p>
    </div>"""
    result = parse_native_page(payload, BASE + "fomchistorical2019.htm", manifest)
    assert not result.issues and len(result.inventory.meetings) == 2
    assert len(result.cross_references) == 1


def test_unknown_panels_and_attachment_labels_cannot_silently_disappear(manifest):
    payload = b"""<div class="panel">
    <h5 class="panel-heading">Unexpected event - 2019</h5>
    </div>
    <div class="panel">
    <h5 class="panel-heading">October 29-30 Meeting - 2019</h5>
    <p>
    <a href="/monetarypolicy/unknown.htm">Unknown attachment</a>
    </p>
    </div>"""
    result = parse_native_page(payload, BASE + "fomchistorical2019.htm", manifest)
    assert {i.code for i in result.issues} >= {
        "unsupported_meeting_label",
        "unreviewed_document_label",
    }
