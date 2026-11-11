# FOMC-T04 — full text, revisions and coherent local catalogs

Scope: the existing 1990-onward archive, not wider acquisition. Traces
FOMC-AC03–05/AC12. Continue on `feat/spec017-fomc-inventory`.

Use optional pinned `pypdf==7.0.1` for strict native PDF parsing and the existing
reviewed HTML tree/container profiles. No HTTP, OCR service, scheduler or model
is called. The pure transform accepts bytes and returns a deterministic complete
extraction record: source hash, exact engine/algorithm/limits profile, full Unicode
text/hash, ordered page/section units, half-open character offsets and explicit
issues. Every readable PDF page has an entry, including empty/failed pages.
Public PDFs that open with the empty user password are supported without supplying
or guessing a protected password. All text returned by the engine is retained, including headers, tables, footnotes
and appendices. HTML preserves block/line/table boundaries; raw bytes remain the
canonical source. Rotated PDF text is retained using plain extraction (all four
orientations), without header/footer filtering. This is native extraction, not
semantic table reconstruction or a visual-completeness certificate.

PDF images, blank text and replacement characters require review. HTML substantive
images retain their alt text/reference and require review; archived linked addenda
are extracted independently. Invalid/password-required/oversized containers and unsupported
HTML profiles produce explicit failures. Byte/page/content-stream limits fail
rather than silently shortening output. No inferred or synthetic OCR text.
`native_extracted`, `review_required` and `failed` are distinct states; neither
native text nor a complete byte archive certifies reviewed OCR quality.

Immutable filesystem JSON mirrors full text/pages/profile hashes in PostgreSQL.
Captures are verified against committed filesystem and database originals before
extraction. Derivatives key on source bytes + representation URL + exact profile;
retries reuse them. Local artifact writes precede the database transaction; an
interruption leaves a verifiable reusable artifact and never a new current catalog.
A catalog generation includes its exact inventory run, selected capture set,
extraction IDs/profiles, explicit source/unacquired/review gaps and all meeting
associations. Publication verifies every mirror before atomically changing the
PostgreSQL pointer; filesystem readers resolve that authoritative generation,
never glob directories or select a newer orphan. Catalogs are explicitly bounded;
no count of acquired representations certifies all historical originals/text.

Provider content versions are consecutive changes per source representation URL,
ordered by actual capture timestamp/ID. Same-byte repeats retain audit captures
without new versions; A→B→A makes three observed versions. Extractor changes make
new derivatives without provider revisions. Capture-time first-seen is retained;
release claims remain date-only and unknown time stays unknown. As-of catalogs
filter actual captures, retaining unavailable/pending/failed/unacquired outcomes.
Empty-target replay copies only verified original receipts/bytes and derivative
artifacts and rebuilds the same catalog without network or re-extraction.

Implementation order: characterize invented multi-page/Unicode/long HTML/PDF and
failure fixtures → extract archived samples and inspect rendered source pages →
immutable mirror migration/recovery → correction/reversion/catalog/replay checks →
update evidence/register/handoff and build. New migrations are explicit and owned;
rollback selects an older catalog/code, retaining immutable tables/artifacts.
Back up/verify local state before any operational schema migration. NAAIM remains
paused, FINRA remains SSV-T05 and schedules remain absent.

Sources reviewed 2026-11-11:
- https://pypdf.readthedocs.io/en/stable/user/extract-text.html
- https://pypdf.readthedocs.io/en/stable/modules/PdfReader.html
- https://pypi.org/project/pypdf/7.0.1/

The pypdf guidance documents that extraction cannot recover image text and may
require substantial memory; finite input/page/stream bounds are not a hard OS
memory sandbox. OCR/native text accuracy requires separate source review.
