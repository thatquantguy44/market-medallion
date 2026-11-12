"""FOMC-AC07: stable chunk identities, complete spans and explicit review eligibility."""

from copy import deepcopy
from pathlib import Path

import pytest
from test_fomc_text import pdf_bytes

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunks import chunk_text, validate
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest, extract_text
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

URL = "https://www.federalreserve.gov/monetarypolicy/fomcminutes20200315.htm"
VERSION = digest("invented document version")


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def extraction(manifest, text):
    return extract_text(('<div id="article"><p>' + text + "</p></div>").encode(), URL, manifest)


def union_length(intervals):
    count, end = 0, 0
    for start, stop in sorted(intervals):
        count += max(0, stop - max(start, end))
        end = max(end, stop)
    return count


def test_long_unicode_text_is_exact_covered_overlapped_and_deterministic(manifest):
    source = extraction(manifest, ("Ω—中 e\u0301 👩\u200d💻 1.25 footnote ¹; " * 1000))
    value = chunk_text(source, document_version_id=VERSION, max_tokens=35, overlap_tokens=5)
    validate(value, source)
    assert chunk_text(source, document_version_id=VERSION, max_tokens=35, overlap_tokens=5) == value
    rows = value["chunks"]
    assert len(rows) > 100
    assert all(source["full_text"][r["start"] : r["end"]] == r["text"] for r in rows)
    assert union_length([(r["start"], r["end"]) for r in rows]) == len(source["full_text"])
    assert all(0 < r["token_count"] <= 35 and r["eligible"] for r in rows)
    assert all(a["token_end"] - b["token_start"] == 5 for a, b in zip(rows, rows[1:], strict=False))
    assert len({r["chunk_id"] for r in rows}) == len(rows)


def test_paragraph_boundaries_and_section_citations_are_preserved(manifest):
    body = (
        b'<div id="article"><h2>First</h2><p>one two three four</p>'
        b"<p>five six seven eight nine</p><h2>Appendix</h2><p>last footnote</p></div>"
    )
    source = extract_text(body, URL, manifest)
    value = chunk_text(
        source, document_version_id=VERSION, max_tokens=8, min_tokens=4, overlap_tokens=2
    )
    first = value["chunks"][0]
    assert first["text"].endswith("\n") and first["heading"] == "First"
    assert first["unit_ordinal"] == 0 and first["page"] is None
    assert value["chunks"][-1]["heading"] == "Appendix"
    assert sum(u["covered_characters"] for u in value["coverage"]) + value[
        "separator_characters"
    ] == len(source["full_text"])


def test_pdf_page_boundaries_blank_pages_and_review_flags(manifest):
    source = extract_text(
        pdf_bytes(["first page", None, "last page"]), URL.replace(".htm", ".pdf"), manifest
    )
    value = chunk_text(source, document_version_id=VERSION, max_tokens=8, overlap_tokens=2)
    assert [r["page"] for r in value["chunks"]] == [1, 3]
    assert all(r["eligible"] for r in value["chunks"])
    assert value["coverage"][1]["issues"] == ["missing_native_text"]
    assert value["coverage"][1]["characters"] == 0
    assert all(r["source_url"] == source["source_url"] for r in value["chunks"])


def test_review_text_is_retained_with_ineligible_exact_spans(manifest):
    source = extract_text(
        b'<div id="article"><h2>Chart</h2>'
        b'<img src="/monetarypolicy/files/f.png" alt="rate"><p>caption</p></div>',
        URL,
        manifest,
    )
    value = chunk_text(source, document_version_id=VERSION)
    assert value["chunks"] and all(not r["eligible"] for r in value["chunks"])
    assert value["coverage"][0]["review_characters"] == len(source["full_text"])
    assert value["chunks"][0]["review_reasons"] == ["image_content"]


def test_changed_extraction_profile_or_observed_version_makes_new_set(manifest):
    source = extraction(manifest, "one two three four five six seven eight nine")
    first = chunk_text(source, document_version_id=VERSION, max_tokens=5, overlap_tokens=1)
    changed_profile = chunk_text(
        source, document_version_id=VERSION, max_tokens=6, overlap_tokens=1
    )
    reverted_version = chunk_text(
        source, document_version_id=digest("later reversion"), max_tokens=5, overlap_tokens=1
    )
    changed = extraction(manifest, "one two three four five SIX seven eight nine")
    changed_extraction = chunk_text(
        changed, document_version_id=VERSION, max_tokens=5, overlap_tokens=1
    )
    values = [first, changed_profile, reverted_version, changed_extraction]
    assert len({v["set_id"] for v in values}) == 4
    assert len({v["chunks"][0]["chunk_id"] for v in values}) == 4


def test_unbroken_word_run_splits_without_losing_characters(manifest):
    source = extraction(manifest, "中" * 10000)
    value = chunk_text(source, document_version_id=VERSION, max_tokens=8, overlap_tokens=1)
    assert len(value["chunks"]) > 40
    assert union_length([(r["start"], r["end"]) for r in value["chunks"]]) == 10000


def test_tampered_spans_and_missing_chunks_are_refused(manifest):
    source = extraction(manifest, "one two three four five six seven eight nine")
    value = chunk_text(source, document_version_id=VERSION, max_tokens=4, overlap_tokens=1)
    for change in ["span", "missing"]:
        bad = deepcopy(value)
        if change == "span":
            bad["chunks"][0]["end"] -= 1
        else:
            bad["chunks"].pop(1)
        del bad["set_id"]
        bad["set_id"] = digest(bad)
        with pytest.raises(ValueError, match="chunk"):
            validate(bad, source)


def test_bounds_and_failed_extraction_never_publish_truncated_chunks(manifest):
    source = extraction(manifest, "word " * 100)
    with pytest.raises(ValueError, match="limit"):
        chunk_text(
            source, document_version_id=VERSION, max_tokens=4, overlap_tokens=1, max_chunks=1
        )
    with pytest.raises(ValueError):
        chunk_text(source, document_version_id=VERSION, max_tokens=4, overlap_tokens=4)
    failed = extract_text(b"bad PDF", URL.replace(".htm", ".pdf"), manifest)
    with pytest.raises(ValueError, match="failed"):
        chunk_text(failed, document_version_id=VERSION)


def test_unattributed_review_issue_excludes_every_unit_without_losing_text(manifest):
    source = extraction(manifest, "all original text")
    source["issues"] = ["unknown_review_case"]
    source["status"] = "review_required"
    del source["extraction_id"]
    source["extraction_id"] = digest(source)
    value = chunk_text(source, document_version_id=VERSION)
    assert all(
        not r["eligible"] and r["review_reasons"] == ["unknown_review_case"]
        for r in value["chunks"]
    )
    assert value["coverage"][0]["review_characters"] == len(source["full_text"])


def test_whitespace_only_pdf_text_is_accounted_for_and_ineligible(manifest):
    source = extract_text(pdf_bytes(["   "]), URL.replace(".htm", ".pdf"), manifest)
    value = chunk_text(source, document_version_id=VERSION)
    assert value["coverage"][0]["covered_characters"] == len(source["full_text"])
    assert all(not r["eligible"] for r in value["chunks"])


def test_table_rows_and_footnotes_keep_parent_section_and_exact_context(manifest):
    body = (
        b'<div id="article"><h2>Decisions</h2><table><tr><th>Rate</th><th>Votes</th></tr>'
        b"<tr><td>1.25</td><td>10</td></tr><tr><td>1.50</td><td>2</td></tr></table>"
        b"<p>Footnote 1: voting explanation</p><h2>Appendix</h2><p>Last note</p></div>"
    )
    source = extract_text(body, URL, manifest)
    value = chunk_text(
        source, document_version_id=VERSION, max_tokens=6, min_tokens=3, overlap_tokens=1
    )
    decisions = [r for r in value["chunks"] if r["heading"] == "Decisions"]
    assert any("Footnote" in r["text"] for r in decisions)
    assert any("1.25" in r["text"] for r in decisions)
    assert {r["unit_ordinal"] for r in decisions} == {0}
    assert all(r["text"] == source["full_text"][r["start"] : r["end"]] for r in decisions)
    assert value["coverage"][0]["covered_characters"] == len(source["units"][0]["text"])
