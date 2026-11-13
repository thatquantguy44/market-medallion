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
class CrossReference:
    meeting_label_raw: str
    target_meeting_label_raw: str
    label_raw: str
    target_urls: tuple[str, ...]


@dataclass(frozen=True)
class NativeResult:
    inventory: object
    issues: tuple[Issue, ...]
    exclusions: tuple[Exclusion, ...]
    inventory_years: tuple[int, ...] = ()
    cross_references: tuple[CrossReference, ...] = ()


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


def _document_kind(label):
    for title, kind in (
        ("historical minutes", "historical_minutes"),
        ("minutes of actions", "minutes_of_actions"),
        ("record of policy actions", "record_of_policy_actions"),
        ("memoranda of discussion", "memorandum_of_discussion"),
        ("memorandum of discussion", "memorandum_of_discussion"),
        ("minutes", "minutes"),
    ):
        if re.match(re.escape(title) + r"\b", label, re.I):
            return kind
    return None


def _addendum_kind(label):
    # Explicit association only; a bare "Addendum" or "Minutes addendum" is unresolved.
    match = re.match(r"addendum to (?:these |the )?(.+)", label, re.I)
    return _document_kind(match[1]) if match else None


def _outside_minutes_paragraph(text, anchor):
    if re.match(
        r"(?:Agenda|Beige Book|Greenbook|Bluebook|Tealbook|Transcript|"
        r"Presentation materials|Staff statements|Statement|Press Conference|Press Release|"
        r"SEP|[0-9]{4} Memos|Related Current FAQs|Policy Normalization Principles and Plans|"
        r"Balance Sheet Normalization Principles and Plans)\b",
        text,
        re.I,
    ):
        return True
    href = anchor.attrs.get("href", "")
    if text.startswith("Accessible version"):
        return (
            bool(re.fullmatch(r"/monetarypolicy/files/FOMC[0-9]{8}material\.htm", href))
            or href == "/monetarypolicy/files/FOMC20191004accessible.zip"
        )
    return False


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


def _parse_tree(payload, max_bytes, *, encoding="utf-8-sig"):
    if (
        type(max_bytes) is not int
        or max_bytes <= 0
        or not isinstance(payload, bytes)
        or len(payload) > max_bytes
    ):
        raise ValueError("FOMC HTML payload exceeds the configured bound")
    try:
        content = payload.decode(encoding)
    except UnicodeError:
        raise ValueError(
            "FOMC HTML must be UTF-8"
            if encoding == "utf-8-sig"
            else "FOMC HTML does not match its declared encoding"
        ) from None
    tree = _Tree()
    tree.feed(content)
    return tree


def _historical_dates(label, year):
    label = re.sub(r"\s+\(unscheduled\)$", "", label, flags=re.I)
    cross = re.fullmatch(r"([A-Za-z.]+)\s+(\d{1,2})\s*[-–]\s*([A-Za-z.]+)\s+(\d{1,2})", label)
    if cross:
        return _dates(cross[1] + "/" + cross[3], cross[2] + "-" + cross[4], year)
    normal = re.fullmatch(r"([A-Za-z./]+)\s+(\d{1,2}(?:\s*[-–]\s*\d{1,2})?\*?)", label)
    if not normal:
        raise ValueError
    return _dates(normal[1], normal[2], year)


def _exclude_nonmeeting(node, label, reason, has_minutes, issues, exclusions):
    if has_minutes:
        issues.append(Issue("unexpected_nonmeeting_minutes", label[:4096]))
        return
    exclusions.append(Exclusion(label[:4096], "", reason))
    for anchor in node.descendants(tag="a"):
        exclusions.append(Exclusion(anchor.text(), anchor.attrs.get("href", ""), reason))


def parse_native_page(payload, source_page_url, manifest, *, max_bytes=2_000_000):
    """Parse a reviewed calendar/year page. This function performs no I/O or clock reads."""
    page = manifest.canonical_url(source_page_url, source_page=True)
    tree = _parse_tree(payload, max_bytes)
    issues, rows, panels, exclusions = [], [], [], []
    inventory_years = set()
    pending_references, cross_references = [], []
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
                if re.search(r"FOMC Meetings", label, re.I):
                    issues.append(Issue("unsupported_inventory_heading", label[:4096]))
                continue
            panels.append(panel)
            year = int(year_match[1])
            inventory_years.add(year)
            native_meetings = list(panel.descendants(cls="fomc-meeting"))
            if not native_meetings:
                issues.append(Issue("empty_inventory_panel", label[:4096]))
            for meeting in native_meetings:
                month = next(meeting.descendants(cls="fomc-meeting__month"), None)
                day = next(meeting.descendants(cls="fomc-meeting__date"), None)
                raw = f"{month.text() if month else ''} {day.text() if day else ''} {year}".strip()
                notation = re.fullmatch(
                    r"(\d{1,2})\s+\(notation vote\)", day.text() if day else "", re.I
                )
                if notation:
                    try:
                        _dates(month.text(), notation[1], year)
                    except (ValueError, KeyError, AttributeError):
                        issues.append(Issue("unsupported_meeting_label", raw))
                        continue
                    minutes = next(meeting.descendants(cls="fomc-meeting__minutes"), None)
                    _exclude_nonmeeting(
                        meeting,
                        raw,
                        "notation_vote_not_meeting",
                        bool(minutes and minutes.text()),
                        issues,
                        exclusions,
                    )
                    continue
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
                        addendum_kind = _addendum_kind(anchor.text())
                        if re.search(r"addend", anchor.text(), re.I) and not addendum_kind:
                            issues.append(
                                Issue("unresolved_minutes_addendum", anchor.text()[:4096])
                            )
                            continue
                        doc = _document(
                            anchor, addendum_kind or "minutes", page, manifest, issues, release
                        )
                        if doc:
                            docs.append(doc)
                rows.append(_row(raw, days, docs, pending=pending))
        else:
            nonmeeting = re.fullmatch(
                r"(.+?)\s+\((cancelled|notation vote)\)(?:\s+Meeting)?\s*-\s*(\d{4})", label, re.I
            )
            if nonmeeting:
                panels.append(panel)
                try:
                    if int(nonmeeting[3]) != year_page:
                        raise ValueError
                    _historical_dates(nonmeeting[1], year_page)
                except (ValueError, KeyError):
                    issues.append(Issue("unsupported_meeting_label", label))
                    continue
                reason = (
                    "cancelled_meeting"
                    if nonmeeting[2].lower() == "cancelled"
                    else "notation_vote_not_meeting"
                )
                has_minutes = any(_document_kind(p.text()) for p in panel.descendants(tag="p"))
                if reason == "cancelled_meeting":
                    has_minutes = has_minutes or any(p.text() for p in panel.descendants(tag="p"))
                _exclude_nonmeeting(panel, label, reason, has_minutes, issues, exclusions)
                continue
            heading_match = re.fullmatch(
                r"(.+?)\s+(Meeting|Conference Call)\s*-\s*(\d{4})", label, re.I
            )
            if not heading_match:
                unscheduled = re.fullmatch(r"(.+?\s+\(unscheduled\))\s*-\s*([0-9]{4})", label, re.I)
                if unscheduled:
                    heading_match = re.fullmatch(
                        r"(.+?)\s+(Meeting|Conference Call)\s*-\s*([0-9]{4})",
                        unscheduled[1] + " Meeting - " + unscheduled[2],
                        re.I,
                    )
                else:
                    issues.append(Issue("unsupported_meeting_label", label[:4096]))
                    continue
            panels.append(panel)
            try:
                if int(heading_match[3]) != year_page:
                    raise ValueError
                days = _historical_dates(heading_match[1], year_page)
            except (ValueError, KeyError):
                issues.append(Issue("unsupported_meeting_label", label[:4096]))
                continue
            docs, cross_claims = [], []
            unavailable = False
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
                anchors = list(p.descendants(tag="a"))
                context_kind = _document_kind(ptext)
                if context_kind and not anchors and re.search(r"see\b", ptext, re.I):
                    reference = re.fullmatch(
                        r"Minutes:\s*See end of minutes of "
                        r"([A-Za-z]+) (\d{1,2}(?:[-/]\d{1,2})?) meeting",
                        ptext,
                        re.I,
                    )
                    if reference:
                        cross_claims.append((ptext, reference[1], reference[2]))
                    else:
                        issues.append(Issue("unresolved_minutes_cross_reference", ptext[:4096]))
                for anchor in anchors:
                    text = anchor.text().lower()
                    addendum_kind = _addendum_kind(anchor.text())
                    kind = addendum_kind or _document_kind(anchor.text())
                    if kind is None and re.fullmatch(
                        r"(?:HTML|PDF|[0-9]+(?:\.[0-9]+)?\s+(?:KB|MB)\s+PDF)(?:\s*\([^)]*\))?",
                        anchor.text(),
                        re.I,
                    ):
                        kind = context_kind
                    if text == "addendum to the policy normalization principles and plans":
                        exclusions.append(
                            Exclusion(
                                anchor.text(),
                                anchor.attrs.get("href", ""),
                                "outside_minutes_policy_document",
                            )
                        )
                        continue
                    if kind and not anchor.attrs.get("href", ""):
                        unavailable = True
                        exclusions.append(Exclusion(anchor.text(), "", "empty_minutes_href"))
                        continue
                    if "addend" in text and not addendum_kind:
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
                        if context_kind or not _outside_minutes_paragraph(ptext, anchor):
                            issues.append(Issue("unreviewed_document_label", anchor.text()[:4096]))
            rows.append(
                _row(
                    label,
                    days,
                    docs,
                    kind="conference_call"
                    if heading_match[2].lower() == "conference call"
                    else "meeting",
                )
            )
            if unavailable and not docs:
                rows[-1]["minutes_status"] = "unavailable"
            for claim in cross_claims:
                pending_references.append((rows[-1], claim))
    direct_by_dates = {}
    for candidate in rows:
        if (
            candidate["documents"]
            and candidate["committee_body"] == "fomc"
            and candidate["event_kind"] == "meeting"
        ):
            direct_by_dates.setdefault(tuple(candidate["meeting_dates"]), []).append(candidate)
    for row, (claim, month, days_raw) in pending_references:
        try:
            target_dates = _dates(month, days_raw.replace("/", "-"), year_page)
        except (ValueError, KeyError):
            target_dates = []
        targets = [
            target for target in direct_by_dates.get(tuple(target_dates), []) if target is not row
        ]
        if len(targets) != 1:
            issues.append(Issue("unresolved_minutes_cross_reference", claim[:4096]))
            continue
        target = targets[0]
        row["documents"] = [dict(document) for document in target["documents"]]
        row["minutes_status"] = "listed"
        cross_references.append(
            CrossReference(
                row["meeting_label_raw"],
                target["meeting_label_raw"],
                claim,
                tuple(
                    sorted(
                        {
                            manifest.canonical_url(d["href"], base_url=page)
                            for d in target["documents"]
                        }
                    )
                ),
            )
        )
    if not panels:
        raise ValueError("FOMC source page has no recognized inventory panels")
    inventory = inventory_from_dict(
        {"schema_id": manifest.schema_id, "pages": [{"source_page_url": page, "meetings": rows}]},
        manifest,
    )
    if year_page is not None:
        inventory_years.add(year_page)
    return NativeResult(
        inventory,
        tuple(issues),
        tuple(exclusions),
        tuple(sorted(inventory_years)),
        tuple(cross_references),
    )
