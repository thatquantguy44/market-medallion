"""FOMC-AC01/T03: pure, bounded discovery from archived official source indexes.

Source shape: https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm
Expected pages describe inventory coverage only, never document/text completeness.
"""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from urllib.parse import urlsplit

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.native import (
    Exclusion,
    Issue,
    _parse_tree,
    parse_native_page,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import Manifest

BASE = "https://www.federalreserve.gov/monetarypolicy/"
INDEX_URL = BASE + "fomc_historical_year.htm"
CALENDAR_URL = BASE + "fomccalendars.htm"


@dataclass(frozen=True)
class YearPage:
    year: int
    source_page_url: str
    label_raw: str
    href_raw: str


@dataclass(frozen=True)
class YearIndex:
    source_page_url: str
    payload_sha256: str
    year_pages: tuple[YearPage, ...]
    issues: tuple[Issue, ...]
    exclusions: tuple[Exclusion, ...]


@dataclass(frozen=True)
class Discovery:
    selected_years: tuple[int, ...]
    historical_years: tuple[int, ...]
    calendar_years: tuple[int, ...]
    expected_pages: tuple[str, ...]
    payload_hashes: tuple[tuple[str, str], ...]
    issues: tuple[Issue, ...]

    def to_dict(self) -> dict:
        """Detached JSON-compatible evidence, retained even when discovery has issues."""
        return json.loads(json.dumps({"schema_id": "fomc-inventory-discovery/1", **asdict(self)}))


def parse_historical_index(
    payload: bytes,
    source_page_url: str,
    manifest: Manifest,
    *,
    max_bytes: int = 2_000_000,
    max_year_pages: int = 128,
) -> YearIndex:
    """Follow explicit labeled year links in #article, without constructing URLs."""
    page = manifest.canonical_url(source_page_url, source_page=True)
    if page != INDEX_URL:
        raise ValueError("FOMC year discovery requires the official historical-year index")
    if type(max_year_pages) is not int or not 1 <= max_year_pages <= 128:
        raise ValueError("FOMC year-page limit must be an integer in [1,128]")
    tree = _parse_tree(payload, max_bytes)
    articles = [n for n in tree.root.descendants() if n.attrs.get("id") == "article"]
    if len(articles) != 1 or not any(
        h.text() == "Historical Materials by Year" for h in articles[0].descendants(tag="h3")
    ):
        raise ValueError("FOMC index has no recognized historical-year article")
    issues, exclusions, pages = [], [], set()
    panels = list(articles[0].descendants(cls="panel"))
    if not panels:
        raise ValueError("FOMC index has no recognized year panels")
    recognized_anchors = set()
    for panel in panels:
        heading = next(panel.descendants(cls="panel-heading"), None)
        label = heading.text() if heading else ""
        bucket = re.fullmatch(r"([0-9]{4})(?:\+|\s*-\s*([0-9]{4}))", label)
        if not bucket or (bucket[2] and int(bucket[1]) > int(bucket[2])):
            issues.append(Issue("unsupported_index_heading", label[:4096]))
        anchors = list(panel.descendants(tag="a"))
        if not anchors:
            issues.append(Issue("empty_index_panel", label[:4096]))
        for anchor in anchors:
            recognized_anchors.add(id(anchor))
            text, href = anchor.text(), anchor.attrs.get("href", "")
            if not re.fullmatch(r"[0-9]{4}", text):
                issues.append(Issue("unsupported_index_label", text[:4096]))
                exclusions.append(Exclusion(text[:4096], href[:4096], "unresolved_index_link"))
                continue
            year = int(text)
            try:
                url = manifest.canonical_url(href, base_url=page, source_page=True)
                match = re.fullmatch(
                    r"/monetarypolicy/fomchistorical([0-9]{4})\.htm", urlsplit(url).path
                )
                if not match:
                    raise ValueError
            except ValueError:
                issues.append(Issue("unreviewed_year_url", text))
                continue
            if int(match[1]) != year or (
                bucket and not (int(bucket[1]) <= year <= (int(bucket[2]) if bucket[2] else 9999))
            ):
                issues.append(Issue("conflicting_index_year", text))
                continue
            pages.add(YearPage(year, url, text, href))
            if len(pages) > max_year_pages:
                raise ValueError("FOMC year-page discovery limit exceeded")
    for anchor in articles[0].descendants(tag="a"):
        if id(anchor) not in recognized_anchors and (
            re.fullmatch(r"[0-9]{4}", anchor.text())
            or "fomchistorical" in anchor.attrs.get("href", "").lower()
        ):
            issues.append(Issue("unsupported_index_link_location", anchor.text()[:4096]))
    years = {p.year for p in pages}
    if not years:
        issues.append(Issue("no_discovered_years", "Historical Materials by Year"))
    else:
        missing = sorted(set(range(manifest.first_meeting_year, max(years) + 1)) - years)
        if missing:
            issues.append(Issue("missing_index_years", ",".join(map(str, missing))[:4096]))
    return YearIndex(
        page,
        hashlib.sha256(payload).hexdigest(),
        tuple(sorted(pages, key=lambda p: (p.year, p.source_page_url, p.href_raw))),
        tuple(issues),
        tuple(exclusions),
    )


def discover_inventory_pages(
    index_payload: bytes,
    calendar_payload: bytes,
    manifest: Manifest,
    *,
    start_year: int,
    end_year: int,
    max_bytes: int = 2_000_000,
) -> Discovery:
    """Define a bounded historical-page selection plus both source inventories.

    The entire current calendar remains evidence even for a historical batch.
    A calendar/year overlap requires both pages. Missing labels remain issues;
    neither the wall clock nor an assumed number of meetings supplies coverage.
    """
    if (
        type(start_year) is not int
        or type(end_year) is not int
        or not manifest.first_meeting_year <= start_year <= end_year <= 9999
        or end_year - start_year >= 128
    ):
        raise ValueError("FOMC discovery requires 1–128 explicit supported years")
    index = parse_historical_index(index_payload, INDEX_URL, manifest, max_bytes=max_bytes)
    calendar = parse_native_page(calendar_payload, CALENDAR_URL, manifest, max_bytes=max_bytes)
    selected = tuple(range(start_year, end_year + 1))
    historical_years = tuple(sorted({p.year for p in index.year_pages if p.year in selected}))
    issues = [*index.issues, *calendar.issues]
    missing = set(selected) - set(historical_years) - set(calendar.inventory_years)
    if missing:
        issues.append(Issue("undiscovered_selected_years", ",".join(map(str, sorted(missing)))))
    urls = {INDEX_URL, CALENDAR_URL} | {
        p.source_page_url for p in index.year_pages if p.year in selected
    }
    if len(urls) > 128:
        raise ValueError("FOMC discovery exceeds the 128-page publication bound")
    return Discovery(
        selected,
        historical_years,
        calendar.inventory_years,
        tuple(sorted(urls)),
        (
            (INDEX_URL, index.payload_sha256),
            (CALENDAR_URL, hashlib.sha256(calendar_payload).hexdigest()),
        ),
        tuple(issues),
    )
