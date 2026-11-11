"""Invented HTML exercises reviewed native inventory shapes, without network access."""

from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.native import parse_native_page
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

BASE = "https://www.federalreserve.gov/monetarypolicy/"


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def test_current_calendar_explicit_ranges_and_releases(manifest):
    page = b"""<div class="panel"><div class="panel-heading"><h4>2026 FOMC Meetings</h4></div>
    <div class="row fomc-meeting"><div class="fomc-meeting__month">Apr/May</div>
    <div class="fomc-meeting__date">30-1*</div><div class="fomc-meeting__minutes">
    <strong>Minutes:</strong><a href="/monetarypolicy/files/fake.pdf">PDF</a>
    <a href="/monetarypolicy/fake.htm">HTML</a> (Released May 20, 2026)</div></div>
    <div class="row fomc-meeting"><div class="fomc-meeting__month">June</div>
    <div class="fomc-meeting__date">16-17</div><div class="fomc-meeting__minutes"></div></div>
    <div class="row fomc-meeting"><div class="fomc-meeting__month">August</div>
    <div class="fomc-meeting__date">22 (notation vote)</div></div></div>"""
    parsed = parse_native_page(page, BASE + "fomccalendars.htm", manifest)
    assert len(parsed.inventory.meetings) == 2
    assert any(
        m.meeting_dates == (date(2026, 4, 30), date(2026, 5, 1)) for m in parsed.inventory.meetings
    )
    assert {e.provider_release_date for e in parsed.inventory.entries if e.document_id} == {
        date(2026, 5, 20)
    }
    assert {e.minutes_status for e in parsed.inventory.entries} == {"listed", "not_listed"}
    assert not parsed.issues
    assert any(e.reason == "notation_vote_not_meeting" for e in parsed.exclusions)


def test_historical_executive_sessions_and_year_conflict(manifest):
    page = b"""<div class="panel panel-default"><div class="panel-heading">
    <h5>May 25 Meeting - 1936</h5></div>
    <div class="panel-body"><p><a href="/monetarypolicy/files/one.pdf">
    Historical Minutes (123 KB PDF)</a></p>
    <p>Intermeeting Executive Committee Minutes:<br>
    <a href="/monetarypolicy/files/two.pdf">May 25, 1936, 11:30 a.m. (123 KB PDF)</a><br>
    <a href="/monetarypolicy/files/three.pdf">May 25, 1936, 6:25 p.m. (123 KB PDF)</a><br>
    <a href="/monetarypolicy/files/four.pdf">November 20, 1937 (123 KB PDF)</a></p></div></div>"""
    parsed = parse_native_page(page, BASE + "fomchistorical1936.htm", manifest)
    assert len(parsed.inventory.meetings) == 3 and len(parsed.inventory.documents) == 3
    assert {
        m.session_label_raw
        for m in parsed.inventory.meetings
        if m.committee_body == "executive_committee"
    } == {"11:30 a.m.", "6:25 p.m."}
    assert any(i.code == "conflicting_year" and "1937" in i.label for i in parsed.issues)


def test_malformed_source_or_unreviewed_document_is_reported(manifest):
    page = b"""<div class="panel"><div class="panel-heading"><h5>May 25 Meeting - 1936</h5></div>
    <div class="panel-body"><p><a href="https://evil.example/secret.pdf">Minutes</a></p></div></div>"""
    parsed = parse_native_page(page, BASE + "fomchistorical1936.htm", manifest)
    assert not parsed.inventory.documents
    assert any(i.code == "unreviewed_document_url" for i in parsed.issues)
    with pytest.raises(ValueError):
        parse_native_page(b"x" * 101, BASE + "fomccalendars.htm", manifest, max_bytes=100)
    with pytest.raises(ValueError):
        parse_native_page(b"nonsense", BASE + "fomccalendars.htm", manifest)


def test_explicit_pending_and_excluded_links_are_preserved(manifest):
    page = b"""<div class="panel"><div class="panel-heading"><h4>2026 FOMC Meetings</h4></div>
    <div class="row fomc-meeting"><div class="fomc-meeting__month">June</div>
    <div class="fomc-meeting__date">16-17</div><div><strong>Statement:</strong>
    <a href="/monetarypolicy/files/statement.pdf">PDF</a></div>
    <div class="fomc-meeting__minutes">Minutes: pending release</div></div></div>"""
    result = parse_native_page(page, BASE + "fomccalendars.htm", manifest)
    assert result.inventory.entries[0].minutes_status == "pending_release"
    assert [(x.label, x.href) for x in result.exclusions] == [
        ("PDF", "/monetarypolicy/files/statement.pdf")
    ]


def test_unknown_minutes_addendum_is_flagged_instead_of_reclassified(manifest):
    page = b"""<div class="panel"><div class="panel-heading"><h4>2026 FOMC Meetings</h4></div>
    <div class="row fomc-meeting"><div class="fomc-meeting__month">June</div>
    <div class="fomc-meeting__date">16-17</div><div class="fomc-meeting__minutes">
    <a href="/monetarypolicy/files/addendum.pdf">Minutes addendum</a></div></div></div>"""
    result = parse_native_page(page, BASE + "fomccalendars.htm", manifest)
    assert not result.inventory.documents
    assert any(issue.code == "unresolved_minutes_addendum" for issue in result.issues)


def test_historical_format_links_inherit_only_explicit_paragraph_kind(manifest):
    page = b"""<div class="panel"><h5 class="panel-heading">June 16-17 Meeting - 2020</h5>
    <p>Beige Book: <a href="/monetarypolicy/files/beige.pdf">PDF</a></p>
    <p>Minutes (Released July 08, 2020): <a href="/monetarypolicy/fake.htm">HTML</a>
    | <a href="/monetarypolicy/files/fake.pdf">PDF</a></p></div>"""
    result = parse_native_page(page, BASE + "fomchistorical2020.htm", manifest)
    assert len(result.inventory.documents) == 2 and not result.issues
    assert {d.document_kind for d in result.inventory.documents} == {"minutes"}
    assert {e.provider_release_date for e in result.inventory.entries} == {date(2020, 7, 8)}
    assert len(result.exclusions) == 1


def test_reviewed_legacy_uppercase_minutes_folder_preserves_original_case(manifest):
    page = b"""<div class="panel"><h5 class="panel-heading">February 2-3 Meeting - 1993</h5>
    <p><a href="/fomc/MINUTES/1993/19930203min.htm">Minutes</a>
    (Released March 26, 1993)</p></div>"""
    result = parse_native_page(page, BASE + "fomchistorical1993.htm", manifest)
    assert not result.issues
    assert result.inventory.documents[0].source_url.endswith("/fomc/MINUTES/1993/19930203min.htm")


def test_explicit_addendum_is_included_but_ambiguous_label_remains_issue(manifest):
    page = b"""<div class="panel"><h5 class="panel-heading">June 16-17 Meeting - 2020</h5>
    <p><a href="/monetarypolicy/files/fake.pdf">Minutes</a></p>
    <p><a href="/monetarypolicy/fake-addendum.htm">Addendum to these minutes</a></p>
    <p><a href="/monetarypolicy/files/unknown.pdf">Addendum</a></p></div>"""
    result = parse_native_page(page, BASE + "fomchistorical2020.htm", manifest)
    assert len(result.inventory.documents) == 2
    assert any(
        e.document_label_raw == "Addendum to these minutes" for e in result.inventory.entries
    )
    assert any(i.code == "unresolved_minutes_addendum" for i in result.issues)


def test_cross_reference_and_cancelled_labels_do_not_become_complete_empty_minutes(manifest):
    page = b"""<div class="panel">
    <h5 class="panel-heading">March 2 (unscheduled) Meeting - 2020</h5>
    <p>Minutes: See end of minutes of March 15 meeting</p></div>
    <div class="panel">
    <h5 class="panel-heading">March 17-18 (cancelled) Meeting - 2020</h5></div>"""
    result = parse_native_page(page, BASE + "fomchistorical2020.htm", manifest)
    assert {i.code for i in result.issues} == {"unresolved_minutes_cross_reference"}
    assert any(e.reason == "cancelled_meeting" for e in result.exclusions)
