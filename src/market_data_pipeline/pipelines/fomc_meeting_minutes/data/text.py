"""FOMC-AC03: deterministic, offline native text; explicit gaps, never truncation."""

import hashlib
import json
import re
from io import BytesIO

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.dependencies import (
    discover_html_dependencies,
    document_container,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.native import _parse_tree

PDF_VERSION = "7.0.1"


def encoded(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def profile(*, max_bytes=20_000_000, max_pages=1000, max_stream_bytes=16_000_000):
    for value, bound in [
        (max_bytes, 100_000_000),
        (max_pages, 10000),
        (max_stream_bytes, 100_000_000),
    ]:
        if type(value) is not int or not 1 <= value <= bound:
            raise ValueError("FOMC text limits require bounded positive integers")
    return {
        "algorithm": "fomc-native-text/1",
        "pdf_engine": "pypdf",
        "pdf_version": PDF_VERSION,
        "pdf_mode": "plain_all_orientations",
        "pdf_empty_password": True,
        "html_mode": "reviewed_container_blocks/1",
        "max_bytes": max_bytes,
        "max_pages": max_pages,
        "max_stream_bytes": max_stream_bytes,
    }


def _unit(text, *, page=None, heading="", kind="section", issues=(), assets=()):
    if "\x00" in text:
        text, issues = "", (*issues, "invalid_text_encoding")
    return {
        "page": page,
        "heading": heading,
        "kind": kind,
        "text": text,
        "issues": sorted(set(issues)),
        "assets": sorted(set(assets)),
    }


def _finish(payload, url, settings, units, issues=(), page_count=None):
    offset = 0
    for i, unit in enumerate(units):
        unit.update(
            ordinal=i,
            start=offset,
            end=offset + len(unit["text"]),
            text_sha256=hashlib.sha256(unit["text"].encode()).hexdigest(),
        )
        offset = unit["end"] + 2
    text = "\n\n".join(unit["text"] for unit in units)
    codes = sorted(set(issues) | {c for u in units for c in u["issues"]})
    fatal = set(codes) - {"missing_native_text", "image_content", "replacement_character"}
    result = {
        "schema_id": "fomc-native-text/1",
        "source_url": url,
        "source_sha256": hashlib.sha256(payload).hexdigest(),
        "profile": settings,
        "profile_sha256": digest(settings),
        "page_count": page_count,
        "status": "failed" if fatal else "review_required" if codes else "native_extracted",
        "issues": codes,
        "units": units,
        "full_text": text,
        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
    }
    result["extraction_id"] = digest(result)
    return result


def extract_text(payload, source_url, manifest, **limits):
    """Return immutable-ready derivatives. No clocks, storage, HTTP or OCR calls.

    pypdf API/limits: https://pypdf.readthedocs.io/en/stable/user/extract-text.html
    Plain extraction includes all orientations and never filters headers/footers.
    """
    settings = profile(**limits)
    if not isinstance(payload, bytes):
        raise ValueError("FOMC extraction requires original bytes")
    url = manifest.capture_url(source_url, "document")
    if len(payload) > settings["max_bytes"]:
        return _finish(payload, url, settings, [], ["byte_limit"])
    if url.lower().endswith(".pdf"):
        return _pdf(payload, url, settings)
    return _html(payload, url, manifest, settings)


def _pdf(payload, url, settings):
    import pypdf

    if pypdf.__version__ != PDF_VERSION:
        raise RuntimeError("install the pinned fomc_text optional dependency")
    try:
        if not payload.startswith(b"%PDF-") or not payload.rstrip().endswith(b"%%EOF"):
            raise ValueError
        reader = pypdf.PdfReader(BytesIO(payload), strict=True)
        if reader.is_encrypted and not reader.decrypt(""):
            return _finish(payload, url, settings, [], ["encrypted_pdf"])
        count = len(reader.pages)
        if count < 1:
            return _finish(payload, url, settings, [], ["empty_pdf"])
        if count > settings["max_pages"]:
            return _finish(payload, url, settings, [], ["page_limit"], page_count=count)
    except Exception:
        return _finish(payload, url, settings, [], ["invalid_pdf"])
    units = []
    for i, page in enumerate(reader.pages, 1):
        try:
            content = page.get_contents()
            if content is not None and len(content.get_data()) > settings["max_stream_bytes"]:
                units.append(_unit("", page=i, kind="page", issues=["content_stream_limit"]))
                continue
            text = page.extract_text(orientations=(0, 90, 180, 270))
            issues = []
            if not text.strip():
                issues.append("missing_native_text")
            # keys enumerate image references, including nested forms, without image decoding.
            if len(page.images.keys()):
                issues.append("image_content")
            if "\ufffd" in text:
                issues.append("replacement_character")
            units.append(_unit(text, page=i, kind="page", issues=issues))
        except Exception:
            units.append(_unit("", page=i, kind="page", issues=["page_extraction_failed"]))
    return _finish(payload, url, settings, units, page_count=count)


def _html(payload, url, manifest, settings):
    try:
        result = discover_html_dependencies(payload, url, manifest, max_bytes=settings["max_bytes"])
        tree = _parse_tree(payload, settings["max_bytes"])
        article, _ = document_container(tree, url)
    except ValueError:
        return _finish(payload, url, settings, [], ["unsupported_html"])
    units, pieces, assets = [], [], []
    heading = ""
    block_tags = {
        "p",
        "div",
        "tr",
        "li",
        "ul",
        "ol",
        "table",
        "blockquote",
        "pre",
        "hr",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "section",
        "dl",
        "dt",
        "dd",
    }
    decorative = {e.href for e in result.exclusions if e.reason == "legacy_site_image"}

    def flush():
        text = "".join(pieces).strip()
        if text or assets:
            issues = ["image_content"] if assets else []
            if "\ufffd" in text:
                issues.append("replacement_character")
            units.append(_unit(text, heading=heading, issues=issues, assets=assets))
        pieces.clear()
        assets.clear()

    def visit(node, pre=False):
        nonlocal heading
        if isinstance(node, str):
            pieces.append(node if pre else re.sub(r"\s+", " ", node))
            return
        if node.tag in {"script", "style"}:
            return
        if node.tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            flush()
            heading = node.text()
        if node.tag == "img":
            href = node.attrs.get("src", "")
            if href not in decorative:
                try:
                    assets.append(
                        manifest.capture_url(manifest.canonical_url(href, base_url=url), "asset")
                    )
                except ValueError:
                    assets.append(href)
                pieces.append("[Image: " + node.attrs.get("alt", "") + "]")
            return
        if node.tag == "br":
            pieces.append("\n")
        for child in node.children:
            visit(child, pre or node.tag == "pre")
        if node.tag in {"td", "th"}:
            pieces.append("\t")
        if node.tag in block_tags:
            pieces.append("\n")

    visit(article)
    flush()
    issues = [i.code for i in result.issues]
    if not units:
        issues.append("missing_native_text")
    return _finish(payload, url, settings, units, issues)


def validate(value):
    """Verify full-text/offset/profile integrity before mirror writes or readback."""
    try:
        data = dict(value)
        identity = data.pop("extraction_id")
        text = value["full_text"]
        checks = [
            identity == digest(data),
            value["schema_id"] == "fomc-native-text/1",
            value["profile_sha256"] == digest(value["profile"]),
            value["status"] in {"native_extracted", "review_required", "failed"},
            hashlib.sha256(text.encode()).hexdigest() == value["text_sha256"],
            text == "\n\n".join(u["text"] for u in value["units"]),
        ]
        offset = 0
        for i, unit in enumerate(value["units"]):
            checks.extend(
                [
                    unit["ordinal"] == i,
                    unit["start"] == offset,
                    unit["end"] == offset + len(unit["text"]),
                    text[unit["start"] : unit["end"]] == unit["text"],
                    hashlib.sha256(unit["text"].encode()).hexdigest() == unit["text_sha256"],
                ]
            )
            offset = unit["end"] + 2
        if value["page_count"] is not None and "page_limit" not in value["issues"]:
            checks.append(
                [u["page"] for u in value["units"]] == list(range(1, value["page_count"] + 1))
            )
        if not all(checks):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError("FOMC extraction integrity mismatch") from None
    return value
