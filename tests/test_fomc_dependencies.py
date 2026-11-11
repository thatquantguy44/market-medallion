"""FOMC-T03/AC02: source-declared addenda and substantive HTML image dependencies."""

from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.dependencies import (
    discover_html_dependencies,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

BASE = "https://www.federalreserve.gov/monetarypolicy/"
PARENT = BASE + "invented-minutes.htm"
HTML = b"""<div id="t4_nav" class="list-group">
<a href="invented-minutes.htm">FOMC Minutes</a>
<a href="invented-addendum.htm">Summary of Economic Projections</a></div>
<div id="article"><h3>Minutes of the Federal Open Market Committee</h3>
<p>These assessments are described in the Summary of Economic Projections (SEP),
which is an addendum to these minutes.</p>
<img src="/monetarypolicy/files/invented-figure.png" alt="Invented chart">
<a href="/monetarypolicy/files/reference.pdf">Reference</a></div>
<footer><img src="https://other.example/logo.png"></footer>"""


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def test_explicit_sep_addendum_and_article_images_are_required(manifest):
    result = discover_html_dependencies(HTML, PARENT, manifest)
    assert not result.issues
    assert {(d.relation, d.source_url, d.kind) for d in result.dependencies} == {
        ("minutes_addendum", BASE + "invented-addendum.htm", "document"),
        ("content_asset", BASE + "files/invented-figure.png", "asset"),
    }
    assert {d.parent_url for d in result.dependencies} == {PARENT}
    assert any(
        d.label_raw == "Invented chart" and d.href_raw.startswith("/") for d in result.dependencies
    )
    assert [e.label for e in result.exclusions] == ["Reference"]
    assert result.payload_sha256


def test_sep_link_without_explicit_minutes_association_is_an_issue(manifest):
    payload = HTML.replace(
        b"which is an addendum to these minutes.", b"published with the press conference."
    )
    result = discover_html_dependencies(payload, PARENT, manifest)
    assert not any(d.relation == "minutes_addendum" for d in result.dependencies)
    assert "unresolved_sep_association" in {i.code for i in result.issues}


def test_explicit_addendum_anchor_is_required_and_same_document_sections_stay_embedded(manifest):
    payload = b"""<div id="article"><p>Minutes</p>
    <a href="invented-supplement.pdf">Addendum to these minutes (PDF)</a>
    <a href="#appendix">Addendum to these minutes</a>
    <a href="unknown.htm">Addendum</a></div>"""
    result = discover_html_dependencies(payload, PARENT, manifest)
    assert len(result.dependencies) == 1
    assert result.dependencies[0].source_url == BASE + "invented-supplement.pdf"
    assert result.embedded_references == ("#appendix",)
    assert "unresolved_minutes_addendum" in {i.code for i in result.issues}


@pytest.mark.parametrize(
    "old,new,code",
    [
        (
            b"/monetarypolicy/files/invented-figure.png",
            b"https://other.example/chart.png",
            "unreviewed_asset_url",
        ),
        (b"/monetarypolicy/files/invented-figure.png", b"/images/site.png", "unreviewed_asset_url"),
        (b"<img src=", b'<img srcset="one.png 1x, two.png 2x" src=', "unsupported_asset_attribute"),
        (
            b"<p>These",
            b'<iframe src="unknown.htm"></iframe><p>These',
            "unsupported_embedded_content",
        ),
        (
            b"<p>These",
            b'<object data="unknown.pdf"></object><p>These',
            "unsupported_embedded_content",
        ),
        (
            b"<p>These",
            b'<p style="background-image: url(figure.png)">These',
            "unsupported_inline_asset",
        ),
        (
            b'<div id="article">',
            b'<base href="https://other.example/"><div id="article">',
            "unsupported_base_url",
        ),
        (
            b"invented-addendum.htm",
            b"https://other.example/addendum.htm",
            "unreviewed_addendum_url",
        ),
    ],
)
def test_unsupported_dependencies_are_blocking_issues(manifest, old, new, code):
    result = discover_html_dependencies(HTML.replace(old, new), PARENT, manifest)
    assert code in {i.code for i in result.issues}


@pytest.mark.parametrize(
    "payload,kwargs",
    [
        (b"<p>login</p>", {}),
        (HTML, {"max_bytes": 10}),
        (HTML, {"max_dependencies": 1}),
        (HTML, {"max_dependencies": True}),
    ],
)
def test_missing_article_or_exhausted_limits_reject(manifest, payload, kwargs):
    with pytest.raises(ValueError):
        discover_html_dependencies(payload, PARENT, manifest, **kwargs)


def test_reviewed_root_and_folder_png_urls_preserve_exact_paths(manifest):
    for href in ("/sep20190619figure3a.png", "/monetarypolicy/files/invented-chart.png"):
        assert (
            manifest.canonical_asset_url(href, base_url=PARENT)
            == "https://www.federalreserve.gov" + href
        )
    for href in (
        "/sep20190619figure3a.svg",
        "/other.png",
        "https://other.example/sep20190619figure3a.png",
    ):
        with pytest.raises(ValueError):
            manifest.canonical_asset_url(href, base_url=PARENT)
