# FOMC-T01 — normalized inventory and identity contract

**Status:** implemented; 61 offline tests pass. Traces FOMC-AC01. This is the pipeline's own
normalized intake contract, **not a Federal Reserve JSON API**. A later native
HTML adapter must produce these candidates from the archived official pages.
This slice performs no HTTP, file reads (apart from explicit manifest loading),
archival, text extraction, chunking or embeddings and assigns no capture clocks.

`inventory_from_dict(value, manifest)` accepts `schema_id: fomc-inventory/1` and
a nonempty `pages` list. Each page has an absolute `source_page_url` and a
`meetings` list. Each meeting has `committee_body` (`fomc` or `executive_committee`),
`event_kind` (`meeting` or `conference_call`), explicit ordered unique ISO
`meeting_dates`, verbatim `meeting_label_raw`, nullable `session_label_raw`,
`minutes_status` (`listed`, `pending_release`, `not_listed`) and `documents`.
Each document has `document_kind`, `document_label_raw`, original `href`,
`representation` (`pdf` or `html`) and nullable date-only `provider_release_date`.
Unknown fields and malformed or contradictory shapes reject the whole candidate
set. A listed meeting requires documents; other statuses require an empty list.

The manifest pins the source/dataset, this schema, five document kinds, the two
committee bodies, official host and earliest supported meeting year (1936).
Dates can describe a multi-day meeting or non-contiguous joint proceedings;
calendar gaps are never filled. A session label distinguishes same-day sessions
and is part of identity; source adapters must preserve it rather than infer that
all same-day Executive Committee records are duplicates.

Meeting IDs hash the schema, body, event kind, explicit dates and session label.
Document-reference IDs hash the schema, resolved URL, kind and representation.
PDF/HTML are separate representations, not extra meetings. Repeated URLs share
one document reference, while distinct URLs retain distinct references even if
their bytes might later match. Content-hash deduplication belongs to Bronze.
The same URL with conflicting kind/representation fails. Entries preserve every
unique meeting/document/page association, raw label/href and release-date claim;
conflicting source claims are retained as evidence, not silently reconciled.
Repeated identical candidates coalesce and input ordering does not affect IDs
or canonical output order. No source date/label parsing is claimed here.

URLs require HTTPS on `www.federalreserve.gov`, without credentials, ports,
queries, whitespace, backslashes or percent-encoded paths. Inventory pages are
restricted to the current calendar, historical-year index and historical year
pages. Document paths are restricted to reviewed monetary-policy and legacy
`/fomc/minutes/` locations, with a matching PDF/HTML suffix. The T03 source
review additionally accepts the case-preserved `/fomc/MINUTES/` legacy folder.
PNG assets use a separate reviewed dependency profile and do not become
normalized meeting/document records. Fragments are removed
from document identity while the original href remains evidence. Unknown hosts,
formats and document kinds are rejected; no fallback to mirrors or transcripts.
These rules are intentionally a bounded initial profile, not all Board assets.

Operating guards: at most 128 input pages and 20,000 meeting/document records by
default (positive integer overrides), 366 date labels per meeting and 4,096
characters per label/URL. They are resource limits, not provider field widths.
All accepted records are frozen. Results represent observed inventory candidates,
not verified archive/text coverage. Pending and not-listed observations remain
explicit even when another page has a listed observation for the same meeting.

Official references and current/historical source checks are linked in
[Spec017](README.md#official-sources-and-access). The 1936 page has same-day
Executive Committee sessions and a displayed 1937 label under a 1936 heading;
the future HTML adapter must surface such date inconsistencies for review.
Invented fixtures do not certify native page parsing or historic completeness.
