"""FOMC-AC03: full text survives sections, long passages, Unicode and PDF pages."""

from io import BytesIO
from pathlib import Path

import pytest
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import extract_text, validate
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

URL = "https://www.federalreserve.gov/monetarypolicy/fomcminutes20200315.htm"


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def pdf_bytes(texts):
    """Invented, native PDF pages; None is an intentionally blank/scanned-text page."""
    writer = PdfWriter()
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    for text in texts:
        page = writer.add_blank_page(width=612, height=792)
        page[NameObject("/Resources")] = DictionaryObject(
            {NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})}
        )
        if text is not None:
            stream = DecodedStreamObject()
            stream.set_data(b"BT /F1 12 Tf 72 720 Td (" + text.encode() + b") Tj ET")
            page[NameObject("/Contents")] = writer._add_object(stream)
    out = BytesIO()
    writer.write(out)
    return out.getvalue()


def test_html_keeps_all_sections_tables_notes_unicode_and_long_passages(manifest):
    long = "long passage Ω—中 " * 2000
    body = (
        '<div id="article"><h1>First</h1><p>' + long + "</p>"
        "<h2>Middle</h2><table><tr><th>Rate</th><th>Votes</th></tr>"
        "<tr><td>1.25</td><td>10</td></tr></table><p>Footnote ¹</p>"
        "<h2>Appendix</h2><p>Last</p></div>"
    ).encode()
    result = extract_text(body, URL, manifest)
    validate(result)
    assert result["status"] == "native_extracted"
    assert long.strip() in result["full_text"]
    assert "Rate\tVotes" in result["full_text"]
    assert "1.25\t10" in result["full_text"]
    assert "Footnote ¹" in result["full_text"] and result["full_text"].endswith("Last")
    assert [u["heading"] for u in result["units"]] == ["First", "Middle", "Appendix"]
    for unit in result["units"]:
        assert result["full_text"][unit["start"] : unit["end"]] == unit["text"]


def test_pdf_every_page_has_text_or_explicit_review_outcome(manifest):
    result = extract_text(
        pdf_bytes(["First footnote 1", None, "Last appendix"]),
        URL.replace(".htm", ".pdf"),
        manifest,
    )
    validate(result)
    assert result["page_count"] == 3 and len(result["units"]) == 3
    assert result["status"] == "review_required"
    assert result["units"][1]["issues"] == ["missing_native_text"]
    assert result["units"][2]["page"] == 3
    assert "First footnote 1" in result["full_text"] and "Last appendix" in result["full_text"]


@pytest.mark.parametrize("payload", [b"not pdf", b"%PDF-1.7\ntruncated"])
def test_malformed_pdf_is_retained_as_failure(manifest, payload):
    result = extract_text(payload, URL.replace(".htm", ".pdf"), manifest)
    assert result["status"] == "failed" and result["issues"] == ["invalid_pdf"]
    validate(result)


def test_limits_fail_without_a_truncated_success(manifest):
    result = extract_text(
        pdf_bytes(["first", "last"]), URL.replace(".htm", ".pdf"), manifest, max_pages=1
    )
    assert result["status"] == "failed" and result["issues"] == ["page_limit"]
    assert result["full_text"] == ""


def test_html_image_alt_is_preserved_but_not_claimed_as_complete_text(manifest):
    body = (
        b'<div id="article"><p>Figure</p>'
        b'<img src="/monetarypolicy/files/f.png" alt="Rate chart"></div>'
    )
    result = extract_text(body, URL, manifest)
    assert result["status"] == "review_required"
    assert "Rate chart" in result["full_text"]
    assert result["units"][0]["assets"] == [
        "https://www.federalreserve.gov/monetarypolicy/files/f.png"
    ]


def test_public_empty_password_pdf_opens_but_real_password_is_explicit_failure(manifest):
    for password, expected in [("", "native_extracted"), ("secret", "failed")]:
        reader = __import__("pypdf").PdfReader(BytesIO(pdf_bytes(["all pages"])))
        writer = PdfWriter(clone_from=reader)
        writer.encrypt(password)
        out = BytesIO()
        writer.write(out)
        result = extract_text(out.getvalue(), URL.replace(".htm", ".pdf"), manifest)
        assert result["status"] == expected
        if password:
            assert result["issues"] == ["encrypted_pdf"]
        else:
            assert "all pages" in result["full_text"]


def test_content_stream_limit_keeps_every_page_as_an_explicit_failure(manifest):
    result = extract_text(
        pdf_bytes(["first", "last"]), URL.replace(".htm", ".pdf"), manifest, max_stream_bytes=1
    )
    assert result["status"] == "failed"
    assert [u["page"] for u in result["units"]] == [1, 2]
    assert result["issues"] == ["content_stream_limit"]


def test_extraction_corruption_is_refused(manifest):
    result = extract_text(b'<div id="article"><p>intact</p></div>', URL, manifest)
    result["full_text"] += "tampered"
    with pytest.raises(ValueError, match="integrity"):
        validate(result)
