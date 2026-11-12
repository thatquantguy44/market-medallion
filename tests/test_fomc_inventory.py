"""FOMC-AC01: invented normalized candidates; no real Board observations or HTML parser."""

import json
import socket
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import date
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import inventory_from_dict
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import (
    load_manifest,
    manifest_from_dict,
)

FIXTURES = Path("tests/fixtures/fomc_meeting_minutes")


def invented(name="current"):
    return json.loads((FIXTURES / f"invented-{name}.json").read_text())


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def test_fomc_ac01_current_candidates_keep_variants_dates_pending_and_absence(manifest):
    result = inventory_from_dict(invented(), manifest)
    assert len(result.meetings) == 4 and len(result.documents) == 3
    assert len(result.entries) == 5
    assert {entry.minutes_status for entry in result.entries} == {
        "listed",
        "pending_release",
        "not_listed",
    }
    joint = next(
        m
        for m in result.meetings
        if m.event_kind == "conference_call" and len(m.meeting_dates) == 2
    )
    assert joint.meeting_dates == (date(2020, 3, 2), date(2020, 3, 15))
    assert all(not hasattr(m, "first_seen_at") for m in result.meetings)
    assert all(not hasattr(entry, "retrieved_at") for entry in result.entries)
    assert any(d.representation == "html" for d in result.documents)
    assert all("#" not in d.source_url for d in result.documents)
    assert any(entry.href_raw.endswith("#page=2") for entry in result.entries if entry.href_raw)
    assert {entry.provider_release_date for entry in result.entries} == {None, date(2026, 4, 8)}
    with pytest.raises(FrozenInstanceError):
        result.meetings[0].committee_body = "changed"


def test_fomc_ac01_historical_bundle_is_one_reference_with_two_meetings(manifest):
    result = inventory_from_dict(invented("historical"), manifest)
    assert len(result.meetings) == 5 and len(result.documents) == 5 and len(result.entries) == 6
    bundled = next(d for d in result.documents if d.source_url.endswith("/invented-bundle.pdf"))
    assert (
        len(
            {
                entry.meeting_id
                for entry in result.entries
                if entry.document_id == bundled.document_id
            }
        )
        == 2
    )
    executive = [m for m in result.meetings if m.committee_body == "executive_committee"]
    assert len(executive) == 2 and executive[0].meeting_id != executive[1].meeting_id
    assert {m.session_label_raw for m in executive} == {"11:30 a.m.", "6:25 p.m."}


def test_fomc_ac01_union_is_idempotent_and_order_independent(manifest):
    value = invented()
    value["pages"] += invented("historical")["pages"]
    original = deepcopy(value)
    expected = inventory_from_dict(value, manifest)
    assert value == original
    value["pages"] += deepcopy(value["pages"])
    for page in value["pages"]:
        page["meetings"].reverse()
        for meeting in page["meetings"]:
            meeting["documents"].reverse()
    value["pages"].reverse()
    assert inventory_from_dict(value, manifest) == expected
    assert {d.document_kind for d in expected.documents} == set(manifest.document_kinds)


def test_fomc_ac01_cross_page_overlap_preserves_conflicting_source_claims(manifest):
    value = invented()
    page = deepcopy(value["pages"][0])
    page["source_page_url"] = "https://www.federalreserve.gov/monetarypolicy/fomchistorical2026.htm"
    page["meetings"] = [page["meetings"][0]]
    page["meetings"][0]["meeting_label_raw"] = "Invented alternate source label"
    page["meetings"][0]["documents"][0]["provider_release_date"] = "2026-04-09"
    value["pages"].append(page)
    result = inventory_from_dict(value, manifest)
    assert len(result.meetings) == 4 and len(result.documents) == 3
    assert len(result.entries) == 7
    assert len(result.source_pages) == 2
    assert {e.provider_release_date for e in result.entries if e.document_id} == {
        None,
        date(2026, 4, 8),
        date(2026, 4, 9),
    }


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_id",), "other/1"),
        (("unknown",), True),
        (("pages",), []),
        (("pages", 0, "unknown"), True),
        (("pages", 0, "meetings", 0, "unknown"), True),
        (("pages", 0, "meetings", 0, "committee_body"), "board_of_governors"),
        (("pages", 0, "meetings", 0, "event_kind"), "unknown"),
        (("pages", 0, "meetings", 0, "meeting_dates"), []),
        (("pages", 0, "meetings", 0, "meeting_dates"), ["20260318"]),
        (("pages", 0, "meetings", 0, "meeting_dates"), ["1935-12-31"]),
        (("pages", 0, "meetings", 0, "meeting_dates"), ["2026-02-30"]),
        (("pages", 0, "meetings", 0, "meeting_dates"), ["2026-03-18", "2026-03-17"]),
        (("pages", 0, "meetings", 0, "meeting_dates"), ["2026-03-18", "2026-03-18"]),
        (("pages", 0, "meetings", 0, "meeting_label_raw"), " "),
        (("pages", 0, "meetings", 0, "session_label_raw"), ""),
        (("pages", 0, "meetings", 0, "minutes_status"), "complete"),
        (("pages", 0, "meetings", 0, "minutes_status"), "pending_release"),
        (("pages", 0, "meetings", 0, "documents"), []),
        (("pages", 0, "meetings", 0, "documents", 0, "unknown"), True),
        (("pages", 0, "meetings", 0, "documents", 0, "document_kind"), "transcript"),
        (("pages", 0, "meetings", 0, "documents", 0, "representation"), "txt"),
        (("pages", 0, "meetings", 0, "documents", 0, "representation"), "html"),
        (
            ("pages", 0, "meetings", 0, "documents", 0, "provider_release_date"),
            "2026-04-08T14:00:00Z",
        ),
    ],
)
def test_fomc_ac01_bad_shapes_and_unsupported_scope_are_rejected(manifest, path, value):
    source = invented()
    parent = source
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    with pytest.raises(ValueError):
        inventory_from_dict(source, manifest)


@pytest.mark.parametrize(
    "href",
    [
        "https://example.invalid/invented.pdf",
        "http://www.federalreserve.gov/monetarypolicy/invented.pdf",
        "https://user:secret@www.federalreserve.gov/monetarypolicy/invented.pdf",
        "https://www.federalreserve.gov:443/monetarypolicy/invented.pdf",
        "/monetarypolicy/invented.pdf?token=private",
        "/monetarypolicy/%2e%2e/invented.pdf",
        "javascript:alert(1)",
        "/newsevents/invented.pdf",
        "/monetarypolicy/invented.txt",
        "/monetarypolicy/invented\n.pdf",
        "/monetarypolicy/invented\\.pdf",
    ],
)
def test_fomc_ac01_document_url_guards_reject_unreviewed_routes(manifest, href):
    value = invented()
    value["pages"][0]["meetings"][0]["documents"][0]["href"] = href
    with pytest.raises(ValueError):
        inventory_from_dict(value, manifest)


def test_fomc_ac01_one_url_cannot_claim_different_document_kinds(manifest):
    value = invented()
    doc = deepcopy(value["pages"][0]["meetings"][0]["documents"][0])
    doc["document_kind"] = "historical_minutes"
    value["pages"][0]["meetings"][0]["documents"].append(doc)
    with pytest.raises(ValueError):
        inventory_from_dict(value, manifest)


@pytest.mark.parametrize(
    "options", [{"max_pages": 1}, {"max_records": 1}, {"max_pages": True}, {"max_records": 0}]
)
def test_fomc_ac01_operating_budgets_are_explicit(manifest, options):
    value = invented("historical")
    with pytest.raises(ValueError):
        inventory_from_dict(value, manifest, **options)


def test_fomc_ac01_manifest_is_strict_and_legacy_minutes_route_is_reviewed(manifest):
    assert manifest_from_dict(manifest.to_dict()) == manifest
    for key, value in [
        ("first_meeting_year", True),
        ("official_host", "example.invalid"),
        ("unknown", 1),
    ]:
        changed = manifest.to_dict()
        changed[key] = value
        with pytest.raises(ValueError):
            manifest_from_dict(changed)
    value = invented()
    doc = value["pages"][0]["meetings"][0]["documents"][1]
    doc["href"] = "/fomc/minutes/invented-legacy.htm"
    result = inventory_from_dict(value, manifest)
    assert any(d.source_url.endswith("/fomc/minutes/invented-legacy.htm") for d in result.documents)


def test_fomc_ac01_pure_boundary_uses_no_io_or_clock(manifest, monkeypatch):
    value = invented()

    def unexpected_io(*args, **kwargs):
        pytest.fail("inventory contract attempted I/O")

    monkeypatch.setattr(Path, "open", unexpected_io)
    monkeypatch.setattr(socket, "create_connection", unexpected_io)
    result = inventory_from_dict(value, manifest)
    assert len(result.documents) == 3


@pytest.mark.parametrize(
    "href",
    [
        "https://www.federalreserve.gov/monetarypolicy/../newsevents/invented.pdf",
        "https://www.federalreserve.gov/monetarypolicy/./invented.pdf",
    ],
)
def test_fomc_ac01_ambiguous_absolute_path_segments_are_rejected(manifest, href):
    value = invented()
    value["pages"][0]["meetings"][0]["documents"][0]["href"] = href
    with pytest.raises(ValueError):
        inventory_from_dict(value, manifest)


@pytest.mark.parametrize(
    "url",
    [
        "/monetarypolicy/fomccalendars.htm",
        "https://example.invalid/monetarypolicy/fomccalendars.htm",
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm?year=2026",
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm#2026",
        "https://www.federalreserve.gov/monetarypolicy/fomchistorical1900.htm",
        "https://www.federalreserve.gov/monetarypolicy/unknown.htm",
    ],
)
def test_fomc_ac01_inventory_page_urls_are_pinned(manifest, url):
    value = invented()
    value["pages"][0]["source_page_url"] = url
    with pytest.raises(ValueError):
        inventory_from_dict(value, manifest)


@pytest.mark.parametrize(
    "field,changed",
    [
        ("committee_body", "executive_committee"),
        ("event_kind", "conference_call"),
        ("session_label_raw", "Invented second session"),
        ("meeting_dates", ["2026-03-18"]),
    ],
)
def test_fomc_ac01_meeting_identity_distinguishes_body_event_dates_and_session(
    manifest, field, changed
):
    value = invented()
    extra = deepcopy(value["pages"][0]["meetings"][0])
    extra[field] = changed
    value["pages"][0]["meetings"].append(extra)
    result = inventory_from_dict(value, manifest)
    assert len(result.meetings) == 5 and len(result.documents) == 3 and len(result.entries) == 7


def test_fomc_ac01_empty_page_and_conflicting_pending_claim_are_evidence_not_coverage(manifest):
    value = invented()
    extra = deepcopy(value["pages"][0]["meetings"][0])
    extra["minutes_status"] = "not_listed"
    extra["documents"] = []
    value["pages"].append(
        {
            "source_page_url": "https://www.federalreserve.gov/monetarypolicy/fomchistorical2026.htm",
            "meetings": [extra],
        }
    )
    value["pages"].append(
        {
            "source_page_url": "https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm",
            "meetings": [],
        }
    )
    result = inventory_from_dict(value, manifest)
    assert len(result.meetings) == 4 and len(result.source_pages) == 3 and len(result.entries) == 6
    assert not hasattr(result, "archive_complete")


def test_fomc_ac01_unknown_candidate_types_and_missing_fields_are_rejected(manifest):
    for value in (
        None,
        [],
        True,
        {"schema_id": "fomc-inventory/1"},
        {"schema_id": "fomc-inventory/1", "pages": [None]},
    ):
        with pytest.raises(ValueError):
            inventory_from_dict(value, manifest)
    for field in invented()["pages"][0]["meetings"][0]:
        value = invented()
        del value["pages"][0]["meetings"][0][field]
        with pytest.raises(ValueError):
            inventory_from_dict(value, manifest)
    with pytest.raises(ValueError):
        inventory_from_dict(invented(), None)


def test_fomc_ac01_duplicate_manifest_keys_are_rejected(tmp_path):
    path = tmp_path / "invented.yml"
    path.write_text("schema_id: fomc-inventory/1\nschema_id: other/1\n")
    with pytest.raises(ValueError):
        load_manifest(path)


def test_fomc_ac01_diagnostics_never_echo_invalid_sensitive_url(manifest):
    value = invented()
    marker = "invented-private-marker"
    value["pages"][0]["meetings"][0]["documents"][0]["href"] = (
        "https://user:" + marker + "@www.federalreserve.gov/monetarypolicy/invented.pdf"
    )
    with pytest.raises(ValueError) as failure:
        inventory_from_dict(value, manifest)
    assert marker not in str(failure.value)
