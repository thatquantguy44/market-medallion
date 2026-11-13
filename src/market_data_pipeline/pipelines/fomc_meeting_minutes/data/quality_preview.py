"""FOMC-QAC06: inert content previews from exact archived HTML and image bytes."""

import base64
import re
from html import escape
from pathlib import PurePosixPath
from urllib.parse import urlsplit

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.dependencies import (
    discover_html_dependencies,
    document_container,
    document_tree,
)

PROFILE = "fomc-offline-article/1"


def render_html(payload, url, manifest, assets):
    tree, _ = document_tree(payload, url, 20_000_000)
    article, _ = document_container(tree, url)
    dependencies = discover_html_dependencies(payload, url, manifest)
    decorative = {e.href for e in dependencies.exclusions if e.reason == "legacy_site_image"}
    allowed = set(
        (
            "div p span h1 h2 h3 h4 h5 h6 table tr td th thead tbody tfoot caption "
            "ul ol li dl dt dd pre blockquote em strong b i u small sub sup section article br hr"
        ).split()
    )

    def visit(node):
        if isinstance(node, str):
            return escape(node)
        if (
            node.tag in {"script", "style", "iframe", "object", "embed", "input", "form"}
            or node.attrs.get("id") == "printIcon"
        ):
            return ""
        if node.tag == "img":
            href = node.attrs.get("src", "")
            if href in decorative:
                return ""
            target = manifest.capture_url(manifest.canonical_url(href, base_url=url), "asset")
            if target not in assets:
                raise ValueError("FOMC review preview requires archived image bytes")
            suffix = PurePosixPath(urlsplit(target).path).suffix.lower()
            mime = {
                ".png": "image/png",
                ".gif": "image/gif",
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
            }[suffix]
            src = "data:" + mime + ";base64," + base64.b64encode(assets[target]).decode("ascii")
            return (
                '<img src="'
                + src
                + '" alt="'
                + escape(node.attrs.get("alt", ""), quote=True)
                + '">'
            )
        children = "".join(visit(child) for child in node.children)
        if node.tag not in allowed:
            return children
        attrs = ""
        for key in ("colspan", "rowspan", "start", "value"):
            value = node.attrs.get(key, "")
            if re.fullmatch(r"[0-9]{1,3}", value):
                attrs += " " + key + '="' + value + '"'
        opening = "<" + node.tag + attrs + ">"
        return opening if node.tag in {"br", "hr"} else opening + children + "</" + node.tag + ">"

    body = visit(article)
    return (
        """<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="Content-Security-Policy"
content="default-src 'none'; img-src data:; style-src 'unsafe-inline'">
<title>Archived FOMC content review</title><style>
body{font:17px/1.5 Georgia,serif;max-width:1100px;margin:36px auto;padding:0 24px}
img{max-width:100%;height:auto;display:block;margin:24px 0}table{border-collapse:collapse}
td,th{border:1px solid #bbb;padding:6px}pre{white-space:pre-wrap}
</style></head><body>"""
        + body
        + "</body></html>"
    ).encode("utf-8")
