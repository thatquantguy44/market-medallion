"""FOMC-QAC06/12: archived HTML content previews cannot fetch active resources."""

from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_preview import render_html
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest

BASE = "https://www.federalreserve.gov/monetarypolicy/"


def test_preview_preserves_source_text_tables_images_and_blocks_active_content():
    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    body = b"""<div id="article"><h2>Review</h2><p onclick="evil()">Rate 5 &frac14; percent</p>
    <script>fetch('https://outside.test')</script><iframe src="https://outside.test"></iframe>
    <table><tr><td colspan="2">Footnote 1.</td></tr></table>
    <a href="https://outside.test">Source text</a>
    <img src="files/invented.png" alt="Chart"></div>"""
    image = b"\x89PNG\r\n\x1a\ninvented"
    html = render_html(
        body, BASE + "invented.htm", manifest, {BASE + "files/invented.png": image}
    ).decode()
    assert "default-src 'none'" in html and "img-src data:" in html
    assert "onclick" not in html and "<script" not in html and "<iframe" not in html
    assert "outside.test" not in html and "<a " not in html
    assert "<h2>Review</h2>" in html and "Rate 5 ¼ percent" in html
    assert '<td colspan="2">Footnote 1.</td>' in html
    assert 'src="data:image/png;base64,' in html and 'alt="Chart"' in html


def test_missing_archived_image_is_a_failure_not_a_live_fallback():
    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    with pytest.raises(ValueError, match="image"):
        render_html(
            b'<div id="article"><img src="files/invented.png"></div>',
            BASE + "invented.htm",
            manifest,
            {},
        )
