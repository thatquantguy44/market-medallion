"""Conservative, offline parser for reviewed Federal Reserve inventory page shapes.

Issues mark incomplete discovery: callers must retain the original page and must
not treat a successful parse as a completeness guarantee.
"""

import calendar
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from html.parser import HTMLParser
from urllib.parse import urlsplit

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import inventory_from_dict


@dataclass(frozen=True)
class Issue:
    code: str
    label: str


@dataclass(frozen=True)
class Exclusion:
    label: str
    href: str
    reason: str


@dataclass(frozen=True)
class NativeResult:
    inventory: object
    issues: tuple[Issue, ...]
    exclusions: tuple[Exclusion, ...]


@dataclass
class _Node:
    tag: str
    attrs: dict = field(default_factory=dict)
    children: list = field(default_factory=list)

    def text(self):
        return " ".join(
            " ".join(c.text() if isinstance(c, _Node) else c for c in self.children).split()
        )

    def descendants(self, tag=None, cls=None):
        for child in self.children:
            if isinstance(child, _Node):
                if (tag is None or child.tag == tag) and (
                    cls is None or cls in child.attrs.get("class", "").split()
                ):
                    yield child
                yield from child.descendants(tag, cls)


class _Tree(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = _Node("root")
        self.stack = [self.root]
        self.count = 0

    def handle_starttag(self, tag, attrs):
        self.count += 1
        if self.count > 100000 or len(self.stack) > 128:
            raise ValueError("FOMC HTML structure exceeds limits")
        node = _Node(tag, dict(attrs))
        self.stack[-1].children.append(node)
        if tag not in {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if self.stack[-1].tag == tag:
            self.stack.pop()

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        self.stack[-1].children.append(data)


_MONTHS = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
_MONTHS.update({name.lower(): i for i, name in enumerate(calendar.month_abbr) if name})


def _dates(months, days, year):
    months = months.replace(".", "").split("/")
    day_match = re.fullmatch(r"(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\*?", days.strip())
    if not day_match or not 1 <= len(months) <= 2:
        raise ValueError
    numbers = [int(x) for x in day_match.groups() if x]
    if len(months) == 2 and len(numbers) != 2:
        raise ValueError
    dates = [date(year, _MONTHS[months[0].strip().lower()], numbers[0])]
    if len(numbers) == 2:
        second_month = _MONTHS[months[-1].strip().lower()]
        second_year = year + (second_month < dates[0].month)
        dates.append(date(second_year, second_month, numbers[1]))
        if dates[1] != dates[0] + timedelta(days=1):
            raise ValueError
    return [d.isoformat() for d in dates]


def _release(text):
    match = re.search(r"\(Released\s+([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})\)", text, re.I)
    if not match:
        return None
    try:
        return date(int(match[3]), _MONTHS[match[1].lower()], int(match[2])).isoformat()
    except (ValueError, KeyError):
        return None


def _document(anchor, kind, page, manifest, issues, release=None):
    label, href = anchor.text(), anchor.attrs.get("href", "")
    try:
        url = manifest.canonical_url(href, base_url=page)
        path = urlsplit(url).path.lower()
        representation = (
            "pdf" if path.endswith(".pdf") else "html" if path.endswith((".htm", ".html")) else None
        )
        if not representation or not label:
            raise ValueError
    except ValueError:
        issues.append(Issue("unreviewed_document_url", label[:4096]))
        return None
    return {
        "document_kind": kind,
        "document_label_raw": label,
        "href": href,
        "representation": representation,
        "provider_release_date": release,
    }


def _row(label, days, docs, body="fomc", kind="meeting", session=None, pending=False):
    return {
        "committee_body": body,
        "event_kind": kind,
        "meeting_dates": days,
        "meeting_label_raw": label,
        "session_label_raw": session,
        "minutes_status": "listed" if docs else "pending_release" if pending else "not_listed",
        "documents": docs,
    }


def parse_native_page(payload, source_page_url, manifest, *, max_bytes=2_000_000):
    """Parse a reviewed calendar/year page. This function performs no I/O or clock reads."""
    page = manifest.canonical_url(source_page_url, source_page=True)
    if (
        type(max_bytes) is not int
        or max_bytes <= 0
        or not isinstance(payload, bytes)
        or len(payload) > max_bytes
    ):
        raise ValueError("FOMC HTML payload exceeds the configured bound")
    try:
        content = payload.decode("utf-8-sig")
    except UnicodeError:
        raise ValueError("FOMC HTML must be UTF-8") from None
    tree = _Tree()
    tree.feed(content)
    issues, rows, panels, exclusions = [], [], [], []
    current = page.endswith("/fomccalendars.htm")
    match = re.search(r"fomchistorical(\d{4})\.htm$", page)
    if not current and not match:
        raise ValueError("FOMC native parser requires a calendar or historical year page")
    year_page = int(match[1]) if match else None
    for panel in tree.root.descendants(cls="panel"):
        heading = next(panel.descendants(cls="panel-heading"), None)
        if heading is None:
            continue
        label = heading.text()
        if current:
            year_match = re.fullmatch(r"(\d{4}) FOMC Meetings", label)
            if not year_match:
                continue
            panels.append(panel)
            year = int(year_match[1])
            for meeting in panel.descendants(cls="fomc-meeting"):
                month = next(meeting.descendants(cls="fomc-meeting__month"), None)
                day = next(meeting.descendants(cls="fomc-meeting__date"), None)
                raw = f"{month.text() if month else ''} {day.text() if day else ''} {year}".strip()
                try:
                    days = _dates(month.text(), day.text(), year)
                except (ValueError, KeyError, AttributeError):
                    issues.append(Issue("unsupported_meeting_label", raw))
                    continue
                minutes = next(meeting.descendants(cls="fomc-meeting__minutes"), None)
                docs = []
                minutes_anchors = list(minutes.descendants(tag="a")) if minutes else []
                for anchor in meeting.descendants(tag="a"):
                    if not any(anchor is item for item in minutes_anchors):
                        exclusions.append(
                            Exclusion(
                                anchor.text(),
                                anchor.attrs.get("href", ""),
                                "outside_minutes_container",
                            )
                        )
                pending = False
                if minutes:
                    release = _release(minutes.text())
                    pending = bool(
                        re.search(r"pending release|to be released", minutes.text(), re.I)
                    )
                    for anchor in minutes_anchors:
                        if re.search(r"addend", anchor.text(), re.I):
                            issues.append(
                                Issue("unresolved_minutes_addendum", anchor.text()[:4096])
                            )
                            continue
                        doc = _document(anchor, "minutes", page, manifest, issues, release)
                        if doc:
                            docs.append(doc)
                rows.append(_row(raw, days, docs, pending=pending))
        else:
            heading_match = re.fullmatch(
                r"([A-Za-z./]+)\s+(\d{1,2}(?:\s*[-–]\s*\d{1,2})?\*?)"
                r"\s+(Meeting|Conference Call)\s*-\s*(\d{4})",
                label,
                re.I,
            )
            if not heading_match:
                # Panel titles unrelated to meetings do not establish inventory coverage.
                if re.search(r"Meeting|Conference Call", label, re.I):
                    issues.append(Issue("unsupported_meeting_label", label[:4096]))
                continue
            panels.append(panel)
            try:
                if int(heading_match[4]) != year_page:
                    raise ValueError
                days = _dates(heading_match[1], heading_match[2], year_page)
            except (ValueError, KeyError):
                issues.append(Issue("unsupported_meeting_label", label[:4096]))
                continue
            docs = []
            for p in panel.descendants(tag="p"):
                ptext = p.text()
                if re.search(r"Intermeeting Executive Committee Minutes", ptext, re.I):
                    for anchor in p.descendants(tag="a"):
                        text = anchor.text()
                        found = re.match(
                            r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})(?:,\s*([^()]+?))?\s*(?:\(|$)", text
                        )
                        if not found:
                            issues.append(Issue("unsupported_executive_label", text[:4096]))
                            continue
                        if int(found[3]) != year_page:
                            issues.append(Issue("conflicting_year", text[:4096]))
                            continue
                        try:
                            executive_days = _dates(found[1], found[2], year_page)
                        except (ValueError, KeyError):
                            issues.append(Issue("unsupported_executive_label", text[:4096]))
                            continue
                        doc = _document(anchor, "historical_minutes", page, manifest, issues)
                        if doc:
                            rows.append(
                                _row(
                                    text,
                                    executive_days,
                                    [doc],
                                    "executive_committee",
                                    session=found[4].strip() if found[4] else None,
                                )
                            )
                    continue
                for anchor in p.descendants(tag="a"):
                    text = anchor.text().lower()
                    kind = next(
                        (
                            v
                            for k, v in (
                                ("historical minutes", "historical_minutes"),
                                ("minutes of actions", "minutes_of_actions"),
                                ("record of policy actions", "record_of_policy_actions"),
                                ("memoranda of discussion", "memorandum_of_discussion"),
                                ("memorandum of discussion", "memorandum_of_discussion"),
                                ("minutes", "minutes"),
                            )
                            if text.startswith(k)
                        ),
                        None,
                    )
                    if "addend" in text and ("minute" in text or "discussion" in text):
                        issues.append(Issue("unresolved_minutes_addendum", anchor.text()[:4096]))
                    elif kind:
                        doc = _document(anchor, kind, page, manifest, issues, _release(ptext))
                        if doc:
                            docs.append(doc)
                    else:
                        exclusions.append(
                            Exclusion(
                                anchor.text(),
                                anchor.attrs.get("href", ""),
                                "unrecognized_or_out_of_scope_label",
                            )
                        )
                        if re.search(r"minute|policy action|memorand.*discussion", text):
                            issues.append(Issue("unreviewed_document_label", anchor.text()[:4096]))
            rows.append(
                _row(
                    label,
                    days,
                    docs,
                    kind="conference_call"
                    if heading_match[3].lower() == "conference call"
                    else "meeting",
                )
            )
    if not panels:
        raise ValueError("FOMC source page has no recognized inventory panels")
    inventory = inventory_from_dict(
        {"schema_id": manifest.schema_id, "pages": [{"source_page_url": page, "meetings": rows}]},
        manifest,
    )
    return NativeResult(inventory, tuple(issues), tuple(exclusions))
