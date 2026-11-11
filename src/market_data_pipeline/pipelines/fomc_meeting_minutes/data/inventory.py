"""FOMC-AC01: pure normalized candidate intake, not a native HTML/provider API parser.

Source labels and associations are evidence; this does not certify complete
source discovery, archive coverage, document contents or publication vintages.
"""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date
from urllib.parse import urlsplit

from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import Manifest


@dataclass(frozen=True)
class MeetingRef:
    meeting_id: str
    committee_body: str
    event_kind: str
    meeting_dates: tuple[date, ...]
    session_label_raw: str | None


@dataclass(frozen=True)
class DocumentRef:
    document_id: str
    source_url: str
    document_kind: str
    representation: str


@dataclass(frozen=True)
class InventoryEntry:
    meeting_id: str
    document_id: str | None
    source_page_url: str
    meeting_label_raw: str
    minutes_status: str
    document_label_raw: str | None
    href_raw: str | None
    provider_release_date: date | None


@dataclass(frozen=True)
class Inventory:
    schema_id: str
    meetings: tuple[MeetingRef, ...]
    documents: tuple[DocumentRef, ...]
    entries: tuple[InventoryEntry, ...]
    source_pages: tuple[str, ...]

    def to_dict(self) -> dict:
        """Return a detached JSON-compatible inventory snapshot; dates remain date-only."""
        return json.loads(json.dumps(asdict(self), default=lambda value: value.isoformat()))


def _record(value: object, keys: str) -> dict:
    if not isinstance(value, dict) or set(value) != set(keys.split()):
        raise ValueError("missing or unknown FOMC inventory fields")
    return value


def _text(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 4096
        or any((ord(c) < 32 and c not in "\t\r\n") or 0xD800 <= ord(c) <= 0xDFFF for c in value)
    ):
        raise ValueError("FOMC labels require nonempty bounded text without invalid controls")
    return value


def _day(value: object) -> date:
    try:
        if not isinstance(value, str):
            raise ValueError
        result = date.fromisoformat(value)
        if result.isoformat() != value:
            raise ValueError
    except ValueError:
        raise ValueError("FOMC dates require explicit ISO YYYY-MM-DD") from None
    return result


def _identifier(prefix: str, schema: str, *parts) -> str:
    encoded = json.dumps([schema, *parts], ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    return prefix + hashlib.sha256(encoded).hexdigest()


def _meeting(value: object, manifest: Manifest) -> tuple[MeetingRef, str, str, list]:
    row = _record(
        value,
        "committee_body event_kind meeting_dates meeting_label_raw "
        "session_label_raw minutes_status documents",
    )
    body, kind, dates = row["committee_body"], row["event_kind"], row["meeting_dates"]
    if body not in manifest.committee_bodies or kind not in ("meeting", "conference_call"):
        raise ValueError("unknown FOMC committee body or event kind")
    if not isinstance(dates, list) or not 1 <= len(dates) <= 366:
        raise ValueError("FOMC meeting requires explicit bounded date labels")
    days = tuple(_day(v) for v in dates)
    if days != tuple(sorted(set(days))) or any(d.year < manifest.first_meeting_year for d in days):
        raise ValueError("FOMC meeting dates must be unique, ordered and in the supported regime")
    session = row["session_label_raw"]
    if session is not None:
        session = _text(session)
    label = _text(row["meeting_label_raw"])
    status, docs = row["minutes_status"], row["documents"]
    if (
        status not in ("listed", "pending_release", "not_listed")
        or not isinstance(docs, list)
        or (status == "listed") != bool(docs)
    ):
        raise ValueError("FOMC minutes status contradicts the listed documents")
    identity = _identifier(
        "fm_", manifest.schema_id, body, kind, [d.isoformat() for d in days], session
    )
    return MeetingRef(identity, body, kind, days, session), label, status, docs


def _document(
    value: object, manifest: Manifest, page: str
) -> tuple[DocumentRef, str, str, date | None]:
    row = _record(
        value, "document_kind document_label_raw href representation provider_release_date"
    )
    kind, representation = row["document_kind"], row["representation"]
    if kind not in manifest.document_kinds or representation not in ("pdf", "html"):
        raise ValueError("unknown or out-of-scope FOMC document kind/representation")
    label = _text(row["document_label_raw"])
    url = manifest.canonical_url(row["href"], base_url=page)
    suffix = urlsplit(url).path.lower()
    if not (
        suffix.endswith(".pdf") if representation == "pdf" else suffix.endswith((".htm", ".html"))
    ):
        raise ValueError("FOMC URL suffix differs from its declared representation")
    release = row["provider_release_date"]
    release = _day(release) if release is not None else None
    identity = _identifier("fd_", manifest.schema_id, url, kind, representation)
    return DocumentRef(identity, url, kind, representation), label, row["href"], release


def inventory_from_dict(
    value: object, manifest: Manifest, *, max_pages: int = 128, max_records: int = 20000
) -> Inventory:
    """Validate already-normalized candidates with no I/O, clocks or inferred dates.

    Exact duplicates coalesce; every distinct source-page/label/release claim
    remains in entries. Different URLs cannot be content-deduplicated here.
    """
    if not isinstance(manifest, Manifest):
        raise ValueError("FOMC inventory requires a validated manifest")
    if any(type(v) is not int or v <= 0 for v in (max_pages, max_records)):
        raise ValueError("FOMC inventory budgets must be positive integers")
    data = _record(value, "schema_id pages")
    pages = data["pages"]
    if data["schema_id"] != manifest.schema_id:
        raise ValueError("unsupported FOMC normalized inventory schema")
    if not isinstance(pages, list) or not 1 <= len(pages) <= max_pages:
        raise ValueError("FOMC inventory requires bounded nonempty pages")
    meetings, documents, entries, source_pages = {}, {}, set(), set()
    records = 0
    for candidate in pages:
        page = _record(candidate, "source_page_url meetings")
        url = manifest.canonical_url(page["source_page_url"], source_page=True)
        if not isinstance(page["meetings"], list):
            raise ValueError("FOMC inventory page meetings must be a list")
        source_pages.add(url)
        for item in page["meetings"]:
            meeting, label, status, docs = _meeting(item, manifest)
            records += 1 + len(docs)
            if records > max_records:
                raise ValueError("FOMC inventory record budget exceeded")
            meetings[meeting.meeting_id] = meeting
            if not docs:
                entries.add(
                    InventoryEntry(meeting.meeting_id, None, url, label, status, None, None, None)
                )
            for candidate_doc in docs:
                doc, doc_label, href, release = _document(candidate_doc, manifest, url)
                if doc.source_url in documents and documents[doc.source_url] != doc:
                    raise ValueError("conflicting FOMC document kind/representation for one URL")
                documents[doc.source_url] = doc
                entries.add(
                    InventoryEntry(
                        meeting.meeting_id,
                        doc.document_id,
                        url,
                        label,
                        status,
                        doc_label,
                        href,
                        release,
                    )
                )

    def entry_key(entry):
        return (
            entry.meeting_id,
            entry.document_id or "",
            entry.source_page_url,
            entry.meeting_label_raw,
            entry.minutes_status,
            entry.document_label_raw or "",
            entry.href_raw or "",
            entry.provider_release_date.isoformat() if entry.provider_release_date else "",
        )

    return Inventory(
        manifest.schema_id,
        tuple(sorted(meetings.values(), key=lambda m: m.meeting_id)),
        tuple(sorted(documents.values(), key=lambda d: d.source_url)),
        tuple(sorted(entries, key=entry_key)),
        tuple(sorted(source_pages)),
    )
