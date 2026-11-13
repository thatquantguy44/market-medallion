"""FOMC-AC02/03: reviewed 2007-era print container and declared legacy encoding.

Invented text/amounts, reduced from the official June 2008 page's HTML structure.
Original response remains in the ignored archive; this fixture is offline.
"""

from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.dependencies import (
    discover_html_dependencies,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import extract_text, validate
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

URL = "https://www.federalreserve.gov/monetarypolicy/fomc20080625.htm"
HTML = """<html><head><meta http-equiv="Content-Type"
content="text/html; charset=iso-8859-1"></head><body>
<nav>Outside navigation</nav><div id="content"><div class="generalContentText" id="printThis">
<div id="printIcon"><a href="/feeds/feed.xml">
<img src="/monetarypolicy/gifjpg/rss_small.jpg" alt="RSS Feed"></a>
<a href="javascript:printable()"><img src="/gifjpg/printIcon.gif"
alt="Print">Print</a></div><img src="/monetarypolicy/gifjpg/masthead2.jpg"
alt="Board logo" id="defaultPrintImage">
<h1>Minutes of the Federal Open Market Committee</h1><p>Invented June meeting</p>
<table id="subNavLinks"><tr><td><a href="fomcminutes20080625ep.htm">Summary of Economic
Projections</a>
</td></tr></table><div id="TwoColumns"><div id="leftText">
<p>First: Zakrajšek’s invented “quote”.</p><p>The projections are described in the
<em>Summary of Economic Projections</em>, which is attached as an addendum to these minutes.</p>
<table><tr><th>Rate</th><td>1.25</td></tr></table><p>Last: vote and footnote 1.</p>
<p><a href="#pagetop"><img src="/gifjpg/UPARROW.GIF" alt="Return to top"></a></p>
</div><div id="rightText"></div></div></div><footer>Outside footer</footer></div>
</body></html>""".encode("cp1252")


@pytest.fixture
def manifest():
    return load_manifest(Path("manifests/fomc-meeting-minutes.yml"))


def test_print_container_decodes_declared_charset_and_keeps_whole_text(manifest):
    r = extract_text(HTML, URL, manifest)
    validate(r)
    assert r["status"] == "native_extracted", r["issues"]
    for fragment in ("Zakrajšek’s", "“quote”", "1.25", "Last: vote and footnote 1."):
        assert fragment in r["full_text"]
    for fragment in ("Outside navigation", "Outside footer", "[Image:", "Print"):
        assert fragment not in r["full_text"]
    assert r["profile"]["html_encoding"] == "windows-1252"
    assert r["profile"]["html_mode"] == "reviewed_print_container_blocks/1"
    assert extract_text(HTML, URL, manifest) == r


def test_print_dependencies_include_attached_sep_and_exclude_only_reviewed_site_images(manifest):
    r = discover_html_dependencies(HTML, URL, manifest)
    assert not r.issues
    assert r.profile_id == "fomc-html-print/1"
    assert [(d.relation, d.source_url) for d in r.dependencies] == [
        (
            "minutes_addendum",
            "https://www.federalreserve.gov/monetarypolicy/fomcminutes20080625ep.htm",
        )
    ]
    assert len([e for e in r.exclusions if e.reason == "legacy_site_image"]) == 4
    substantive = HTML.replace(b"/gifjpg/UPARROW.GIF", b"/monetarypolicy/files/unknown.svg")
    assert "unreviewed_asset_url" in {
        i.code for i in discover_html_dependencies(substantive, URL, manifest).issues
    }


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p.replace(b"charset=iso-8859-1", b"charset=unknown"),
        lambda p: p.replace(b"charset=iso-8859-1", b""),
        lambda p: p.replace(b' id="printThis"', b' id="unknown"'),
        lambda p: p.replace(b'<div id="rightText">', b'<div id="printThis">'),
        lambda p: p.replace(b' id="content"', b' id="unknownContent"'),
    ],
)
def test_unreviewed_encoding_or_ambiguous_container_stays_failed(manifest, mutation):
    assert extract_text(mutation(HTML), URL, manifest)["status"] == "failed"


def test_print_layout_is_path_bound_and_does_not_change_existing_utf8_profiles(manifest):
    assert (
        extract_text(HTML, URL.replace("fomc20080625", "unreviewed"), manifest)["status"]
        == "failed"
    )
    modern = b'<div id="article"><p>Existing UTF-8 body</p></div>'
    r = extract_text(modern, URL, manifest)
    assert r["status"] == "native_extracted"
    assert r["profile"]["html_mode"] == "reviewed_container_blocks/1"
    assert "html_encoding" not in r["profile"]


def test_conflicting_declarations_and_undecodable_bytes_fail_closed(manifest):
    conflicting = HTML.replace(b"</head>", b'<meta charset="utf-8"></head>')
    undefined_byte = HTML.replace(b"1.25", b"1.\x8125")
    for payload in (conflicting, undefined_byte):
        assert extract_text(payload, URL, manifest)["status"] == "failed"


def test_reviewed_legacy_encoding_labels_decode_to_same_unicode(manifest):
    windows = HTML.replace(b"charset=iso-8859-1", b"charset=windows-1252")
    assert (
        extract_text(windows, URL, manifest)["full_text"]
        == extract_text(HTML, URL, manifest)["full_text"]
    )


@pytest.mark.parametrize(
    "filename",
    [
        "fomcminutes20080716Chart2a.gif",
        "chart1.gif",
        "chart2a.gif",
        "chart2b.gif",
    ],
)
def test_declared_historical_gif_charts_remain_required_review_content(manifest, filename):
    payload = HTML.replace(
        b"<p>Last: vote and footnote 1.</p>",
        b"<p>Last: vote and footnote 1.</p><img "
        + f'src="/monetarypolicy/gifjpg/{filename}" alt="Invented chart">'.encode(),
    )
    r = extract_text(payload, URL, manifest)
    assert r["status"] == "review_required", r["issues"]
    assert r["issues"] == ["image_content"]
    deps = discover_html_dependencies(payload, URL, manifest)
    assert not deps.issues
    assert any(d.kind == "asset" and d.source_url.endswith(filename) for d in deps.dependencies)


@pytest.mark.parametrize("version", [b"GIF87a", b"GIF89a"])
def test_gif_asset_framing_matches_url_and_rejects_truncation_or_wrong_formats(version):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.assets import asset_framing_valid

    url = "https://www.federalreserve.gov/monetarypolicy/gifjpg/fomcminutes20080716Chart1.gif"
    payload = version + bytes.fromhex(
        "01000100800000000000ffffff21f90401000000002c00000000010001000002024401003b"
    )
    assert asset_framing_valid(payload, url)
    for bad in (payload[:-1], payload[:9], b"<html>login</html>", b"\x89PNG\r\n\x1a\n"):
        assert not asset_framing_valid(bad, url)
    assert not asset_framing_valid(payload[:6] + b"\x00\x00" + payload[8:], url)
    assert not asset_framing_valid(payload, url.replace(".gif", ".png"))


@pytest.mark.parametrize(
    "filename",
    [
        "20080131Chart2a.gif",
        "20080131Chart2b.gif",
        "fomcminutes20080131Chart1.jpg",
    ],
)
def test_january_2008_chart_names_are_source_declared_dependencies(manifest, filename):
    payload = HTML.replace(
        b"Last: vote and footnote 1.</p>",
        b"Last: vote and footnote 1.</p><img "
        + f'src="/monetarypolicy/gifjpg/{filename}">'.encode(),
    )
    r = extract_text(payload, URL, manifest)
    assert r["status"] == "review_required", r["issues"]
    assert r["issues"] == ["image_content"]
    assert not discover_html_dependencies(payload, URL, manifest).issues


def test_jpeg_chart_framing_rejects_wrong_format_and_missing_end_marker():
    from market_data_pipeline.pipelines.fomc_meeting_minutes.data.assets import asset_framing_valid

    url = "https://www.federalreserve.gov/monetarypolicy/gifjpg/fomcminutes20080131Chart1.jpg"
    framed = b"\xff\xd8invented framed bytes\xff\xd9"
    assert asset_framing_valid(framed, url)
    assert not asset_framing_valid(framed[:-1], url)
    assert not asset_framing_valid(b"<html>login</html>", url)
    assert not asset_framing_valid(b"GIF89a\x01\x00\x01\x00;", url)
    assert not asset_framing_valid(framed, url.replace(".jpg", ".gif"))


@pytest.mark.parametrize(
    "filename",
    [
        "20080430_Chart1.jpg",
        "20080430_Chart2a.jpg",
        "20080430_Chart2b.jpg",
    ],
)
def test_april_2008_chart_dependencies_preserve_source_owned_filename_variation(manifest, filename):
    payload = HTML.replace(
        b"Last: vote and footnote 1.</p>",
        b"Last: vote and footnote 1.</p><img "
        + f'src="/monetarypolicy/gifjpg/{filename}">'.encode(),
    )
    assert extract_text(payload, URL, manifest)["status"] == "review_required"
    deps = discover_html_dependencies(payload, URL, manifest)
    assert not deps.issues
    assert any(d.href_raw.endswith(filename) for d in deps.dependencies)


@pytest.mark.parametrize(
    "url",
    [
        "https://other.example/monetarypolicy/gifjpg/chart1.jpg",
        "https://www.federalreserve.gov/monetarypolicy/unreviewed/chart1.gif",
        "https://www.federalreserve.gov/monetarypolicy/gifjpg/chart1.svg",
        "https://www.federalreserve.gov/monetarypolicy/gifjpg/chart1.jpg?token=x",
    ],
)
def test_image_folder_admission_keeps_host_format_and_query_boundaries(manifest, url):
    with pytest.raises(ValueError):
        manifest.canonical_asset_url(url)


@pytest.mark.parametrize("connector", [" that", ""])
def test_january_2009_explicit_attached_sep_association(manifest, connector):
    payload = HTML.replace(
        b"</em>, which is attached as an addendum",
        f"</em>{connector} is attached as an addendum".encode(),
    )
    deps = discover_html_dependencies(payload, URL, manifest)
    assert not deps.issues
    assert any(d.relation == "minutes_addendum" for d in deps.dependencies)
    unrelated = payload.replace(b"is attached as an addendum to these minutes", b"is unrelated")
    assert "unresolved_sep_association" in {
        i.code for i in discover_html_dependencies(unrelated, URL, manifest).issues
    }


@pytest.mark.parametrize(
    "filename",
    [
        "SEP20120125_figure1.gif",
        "SEP20120125_figure2.gif",
        "SEP20120125_figure3a.gif",
        "SEP20120125_figure4.gif",
    ],
)
def test_2012_declared_gif_charts_in_official_files_folder_are_required(manifest, filename):
    payload = HTML.replace(
        b"Last: vote and footnote 1.</p>",
        b"Last: vote and footnote 1.</p><img "
        + f'src="/monetarypolicy/files/{filename}">'.encode(),
    )
    result = extract_text(payload, URL, manifest)
    assert result["status"] == "review_required", result["issues"]
    assert result["issues"] == ["image_content"]
    deps = discover_html_dependencies(payload, URL, manifest)
    assert not deps.issues
    assert any(d.href_raw.endswith(filename) and d.kind == "asset" for d in deps.dependencies)


@pytest.mark.parametrize("definition", [" (SEP)", "(SEP)", None])
def test_2014_sep_acronym_requires_local_definition_and_explicit_addendum(manifest, definition):
    payload = HTML.replace(
        b"<em>Summary of Economic Projections</em>, which is attached as an addendum",
        b"SEP, which is attached as an addendum",
    )
    defined = definition is not None
    if defined:
        payload = payload.replace(
            b"<p>First:",
            f"<p>Summary of Economic Projections{definition}.</p><p>First:".encode(),
        )
    result = discover_html_dependencies(payload, URL, manifest)
    assert any(d.relation == "minutes_addendum" for d in result.dependencies) == defined
    assert ("unresolved_sep_association" in {i.code for i in result.issues}) == (not defined)
