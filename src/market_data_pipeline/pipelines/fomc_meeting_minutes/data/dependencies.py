"""FOMC-T03: offline, source-declared HTML dependencies, without inferred filenames.

Reviewed shapes and URLs: specs/spec017/attachments_contract.md.
Unsupported content remains an issue; this is not a browser renderer.
"""

import hashlib
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.native import (
    Exclusion,
    Issue,
    _addendum_kind,
    _parse_tree,
)


@dataclass(frozen=True, order=True)
class Dependency:
    parent_url: str
    source_url: str
    relation: str
    kind: str
    href_raw: str
    label_raw: str


@dataclass(frozen=True)
class Dependencies:
    parent_url: str
    payload_sha256: str
    dependencies: tuple[Dependency, ...]
    embedded_references: tuple[str, ...]
    exclusions: tuple[Exclusion, ...]
    issues: tuple[Issue, ...]
    profile_id: str = "fomc-html-article/1"


def discover_html_dependencies(
    payload, parent_url, manifest, *, max_bytes=2_000_000, max_dependencies=256
):
    """Discover declared minutes addenda and PNGs inside the substantive article.

    SEP navigation is eligible only with an explicit source declaration that it
    is an addendum to these minutes. Styling/scripts/site icons are not fetched.
    Unsupported article resources block dependency coverage rather than vanish.
    """
    parent = manifest.canonical_url(parent_url)
    if parent != parent_url or not parent.endswith((".htm", ".html")):
        raise ValueError("FOMC dependency discovery requires a canonical HTML document URL")
    if type(max_dependencies) is not int or not 1 <= max_dependencies <= 1000:
        raise ValueError("FOMC dependency limit must be an integer in [1,1000]")
    tree = _parse_tree(payload, max_bytes)
    articles = [n for n in tree.root.descendants() if n.attrs.get("id") == "article"]
    legacy = False
    bodies = list(tree.root.descendants(tag="body"))
    if (
        not articles
        and len(bodies) == 1
        and re.fullmatch(
            r"/fomc/(?:MINUTES/[0-9]{4}/[0-9]{8}min|minutes/[0-9]{8})\.htm", urlsplit(parent).path
        )
        and re.search(r"Minutes of the Federal Open Market Committee", bodies[0].text(), re.I)
    ):
        article = bodies[0]
        legacy = True
    elif len(articles) == 1:
        article = articles[0]
    else:
        raise ValueError("FOMC HTML document requires a reviewed substantive container")
    issues, exclusions, embedded, dependencies = [], [], set(), set()
    if any("href" in node.attrs for node in tree.root.descendants(tag="base")):
        issues.append(Issue("unsupported_base_url", "HTML base URL"))

    def add(href, label, relation):
        try:
            if relation == "content_asset":
                url = manifest.canonical_asset_url(href, base_url=parent)
            else:
                url = manifest.canonical_url(href, base_url=parent)
                if not url.endswith((".pdf", ".htm", ".html")):
                    raise ValueError
        except ValueError:
            issues.append(
                Issue(
                    "unreviewed_asset_url"
                    if relation == "content_asset"
                    else "unreviewed_addendum_url",
                    label[:4096],
                )
            )
            return
        if url == parent:
            embedded.add(href)
            return
        dependencies.add(
            Dependency(
                parent,
                url,
                relation,
                "asset" if relation == "content_asset" else "document",
                href,
                label[:4096],
            )
        )
        if len(dependencies) > max_dependencies:
            raise ValueError("FOMC dependency count exceeds the configured bound")

    sep_declared = bool(
        re.search(
            r"Summary of Economic Projections(?: \(SEP\))?, which is an addendum to these minutes",
            article.text(),
            re.I,
        )
    )
    sep_links = []
    navs = [n for n in tree.root.descendants() if n.attrs.get("id") == "t4_nav"]
    if len(navs) > 1:
        issues.append(Issue("ambiguous_document_navigation", "t4_nav"))
    article_anchors = list(article.descendants(tag="a"))
    article_anchor_ids = {id(a) for a in article_anchors}
    anchors = article_anchors + [a for n in navs for a in n.descendants(tag="a")]
    for anchor in anchors:
        label, href = anchor.text(), anchor.attrs.get("href", "")
        if _addendum_kind(label):
            add(href, label, "minutes_addendum")
        elif label == "Summary of Economic Projections":
            try:
                if manifest.canonical_url(href, base_url=parent) == parent:
                    continue
            except ValueError:
                pass
            sep_links.append((href, label))
            if sep_declared:
                add(href, label, "minutes_addendum")
            else:
                issues.append(Issue("unresolved_sep_association", label))
        elif re.search(r"addend", label, re.I):
            issues.append(Issue("unresolved_minutes_addendum", label[:4096]))
        elif id(anchor) in article_anchor_ids:
            exclusions.append(Exclusion(label[:4096], href[:4096], "outside_declared_dependencies"))
    if sep_declared and not sep_links:
        issues.append(Issue("missing_declared_addendum", "Summary of Economic Projections"))
    if len({href for href, _ in sep_links}) > 1:
        issues.append(Issue("ambiguous_declared_addendum", "Summary of Economic Projections"))
    legacy_scripts = list(article.descendants(tag="script")) if legacy else []
    legacy_last_script = legacy_scripts[-1] if legacy_scripts else None
    for node in article.descendants():
        if (
            legacy
            and node.tag == "img"
            and urljoin(parent, node.attrs.get("src", ""))
            in {
                "https://www.federalreserve.gov/GIFJPG/sm_head2.gif",
                "https://www.federalreserve.gov/gifjpg/uparrow.gif",
            }
        ):
            exclusions.append(
                Exclusion(node.attrs.get("alt", ""), node.attrs.get("src", ""), "legacy_site_image")
            )
            continue
        if (
            legacy
            and node.tag == "script"
            and not node.attrs
            and (
                node is legacy_last_script
                and node.text().startswith("(function(){function c(){var b=a.contentDocument")
                and "window.__CF$cv$params" in node.text()
                and "/cdn-cgi/challenge-platform/scripts/jsd/main.js" in node.text()
            )
        ):
            exclusions.append(Exclusion("provider edge challenge script", "", "legacy_site_script"))
            continue
        if "srcset" in node.attrs or "data-src" in node.attrs or "data-srcset" in node.attrs:
            issues.append(Issue("unsupported_asset_attribute", node.tag))
        if re.search(r"url\s*\(", node.attrs.get("style", ""), re.I):
            issues.append(Issue("unsupported_inline_asset", node.tag))
        if node.tag == "img":
            add(node.attrs.get("src", ""), node.attrs.get("alt", ""), "content_asset")
        elif node.tag in {
            "object",
            "canvas",
            "style",
            "form",
            "embed",
            "iframe",
            "source",
            "video",
            "audio",
            "script",
            "link",
        }:
            issues.append(Issue("unsupported_embedded_content", node.tag))
        elif node.tag in {"image", "use"}:
            href = node.attrs.get("href", node.attrs.get("xlink:href", ""))
            if href and not href.startswith("#"):
                issues.append(Issue("unsupported_embedded_content", node.tag))
    return Dependencies(
        parent,
        hashlib.sha256(payload).hexdigest(),
        tuple(sorted(dependencies)),
        tuple(sorted(embedded)),
        tuple(exclusions),
        tuple(issues),
        "fomc-legacy-body/1" if legacy else "fomc-html-article/1",
    )
