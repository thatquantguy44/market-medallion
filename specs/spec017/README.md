# Spec 017 — Complete FOMC meeting-minutes archive

**Status:** FOMC-T01/T02 complete; FOMC-T03 is in progress with reviewed native
page parsing, bounded acquisition, original-byte storage in filesystem and
PostgreSQL, and explicit capture-to-inventory links. A five-item bounded sample
reconciles; issue-gated multi-page publication and a bounded historical-year
discovery/expected-page gate are implemented, while addendum/assets,
original-byte deduplication, recovery, source-wide coverage, text/chunks and jobs
remain incomplete. Next:
[FOMC-T03 handoff](todo.md#fomc-t03--native-inventory-adapter-and-immutable-bronze-in-progress).
**Reviewed:** 2026-11-11. **Pipeline ID:** `fomc-meeting-minutes`.
**Package:** `src/market_data_pipeline/pipelines/fomc_meeting_minutes/`.
Configuration owns [`config/fomc_meeting_minutes/`](../../config/fomc_meeting_minutes/README.md).
FINRA remains at SSV-T05; NAAIM remains paused.

## Objective and scope

Save current and historical officially published FOMC minutes in their entirety,
with original documents, searchable full text, versioned chunks and traceable
observed revisions. Embeddings are an optional enhancement after the core archive.
Default historical target: all discoverable in-scope records from **1936 through
the latest released minutes**, acquired in bounded year batches. Completeness is
measured against archived official inventories, not an assumed meetings-per-year count.

“In entirety” includes every page, section, attendance list, vote, table, footnote,
appendix and explicitly designated minutes addendum. Retain original PDF and HTML
representations where published; retain substantive linked content assets when
needed to preserve HTML-only content. Whole source documents remain canonical;
extracted/OCR text is a separately versioned derivative. No character/token limit
may silently shorten a document. A published source redaction remains a redaction.

Modern minutes and historical predecessors have separate document kinds:
`minutes`, `historical_minutes`, `minutes_of_actions`, `record_of_policy_actions`
and `memorandum_of_discussion`. Include officially linked Executive Committee
minutes under a distinct committee/body identity. Discover kinds from official
labels, rather than imposing year cutoffs that discard transition-year records.
Verbatim transcripts, statements, press conferences, speeches, Beige/Green/Blue/
Tealbooks and unrelated references are outside this archive. A separate SEP is
included only when explicitly part of the minutes/addendum, not merely because
it is linked from the same meeting page.

## Official sources and access

| Source | Contract evidence / use |
| --- | --- |
| [Meeting calendars and information](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm) | Current/recent meeting inventory, original HTML/PDF links and release labels. Follow discovered links; do not guess filenames or release dates. |
| [Historical materials by year](https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm) | Official year inventory reaches 1936. Union with the current calendar; deduplicate overlap. |
| [Historical-materials guide](https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm) | Describes modern minutes from 1993, historical minutes in 1936–1967, action records/minutes in 1967–1992 and discussion memoranda in 1967–1976. These are distinct source products. |
| [Board disclaimer](https://www.federalreserve.gov/disclaimer.htm) | Board information is generally public domain unless indicated otherwise; cite the Board and retain document-specific/non-Board notices. |

Source-shape checkpoints: [1936](https://www.federalreserve.gov/monetarypolicy/fomchistorical1936.htm),
[1967](https://www.federalreserve.gov/monetarypolicy/fomchistorical1967.htm),
[1993](https://www.federalreserve.gov/monetarypolicy/fomchistorical1993.htm) and
[a modern minutes page](https://www.federalreserve.gov/monetarypolicy/fomcminutes20260916.htm).
Historical policy records can [bundle several meetings](https://www.federalreserve.gov/monetarypolicy/files/fomcropa19670110.pdf)
in one PDF. Archive the full artifact once per content hash while retaining each
source URL and all meeting-to-document associations.
These documentary checks do not certify the complete archive or extraction quality.

## Storage and minimum contract

Use the existing Python 3.12, manifest-driven medallion conventions. The
implemented source-owned files include `manifest.py`, `data/inventory.py`,
`config.py`, `data/native.py`, `io/postgres.py`, `io/acquisition.py` and
`io/bronze.py`. Future work includes `data/text.py`, `data/chunks.py`, a linked
catalog, a CLI and, later, Dagster definitions.
Keep the parsing core pure; select and pin PDF/OCR dependencies during the
first extraction slice.

- **Bronze:** immutable original bytes, inventories and receipts under ignored
  `data/fomc_meeting_minutes/`; SHA-256, byte count, exact/final URL, safe HTTP
  evidence, capture/run IDs and manifest version. Archive before extraction;
  preserve rejected/truncated responses as failed evidence.
- **Silver:** document/representation versions, complete text plus ordered page/
  section records, deterministic chunks, extractor/version, page count and
  extraction status. Preserve source language, punctuation, numerals and notices; no summarization or cleaning
  that changes substance. OCR retains originals and page links and is labelled
  `OCR_REVIEW_REQUIRED` until reviewed. Extraction failure never deletes the archive.
- **Gold:** a local catalog linking current and historical document versions to
  originals/full text, with an atomic committed pointer. The user selected the
  existing PostgreSQL service as the operational catalog target, with a separate
  `fomc_meeting_minutes` schema. Filesystem originals remain part of the archive.
  T02 currently publishes only inventory metadata snapshots; full-document
  catalog publication and any external consumer integration remain later work.

A logical document has `document_id`, `document_kind`, `committee_body`, verbatim
meeting label, explicit meeting-date associations (including joint/non-contiguous
meetings), source URL and representation. A version adds raw/text hashes, observed
version order and source/capture clocks. PDF/HTML variants and bundled volumes are
not extra meetings; preserve many-to-many associations instead of duplicating records.

Keep `meeting_dates`, nullable source release date/timestamp with its precision,
`retrieved_at` and capture-time `first_seen_at` distinct. Historical imports become
known at actual capture time. Do not infer a publication time from a meeting date,
a modern release lag, HTTP Date or Last-Modified. Unchanged captures add audit
receipts without duplicate content versions; corrections and later reversions
remain queryable. Extractor changes are not provider revisions.

## PostgreSQL and configuration — user-selected backend

The pipeline reuses this project's existing PostgreSQL server/database with its
own schema/tables and configuration folder. Checked-in `pipeline.example.yml`
defines nonsecret settings; ignored `pipeline.local.yml` names the actual target
and the existing password environment variable. Loading configuration makes no
service request and resolves no password. FOMC does not import another pipeline's
config, SQL tables or adapter internals; it uses its own PostgreSQL adapter.

T02 implements six tables: `inventory_runs`, `inventory_meetings`,
`inventory_documents`, `inventory_entries`, `inventory_pages` and `catalog_state`.
They retain immutable metadata imports, date-only labels, manifest/content hashes
and a transactional current pointer. Imports are marked `invented_fixture` or
`normalized_inventory`; neither is a certified source archive. Original-document
versions and provider first-seen clocks are not assigned by this metadata store.

T03 adds an independently marked Bronze migration with `bronze_schema_migrations`
and `bronze_captures`, retaining original bytes in PostgreSQL `bytea` and the
filesystem. A separate lineage migration adds `bronze_inventory_links` and
`bronze_document_links`, linking exact URLs to existing inventory claims without
moving the inventory pointer. Five bounded captures have passed exact
readback; the inventory catalog remains empty. Issue-gated publication atomically
links each source capture and refuses to drop earlier source pages. A separate
`fomc-bronze-discovery-postgres/1` migration stores hashed
selected-page evidence atomically with the metadata inventory and links; it is
verified only in isolated schemas. [Bounded year discovery](discovery_contract.md)
requires archived index/calendar inputs and an exact expected-page set; it does
not certify source-wide completeness. Hash-based original deduplication, full
text, document versions and chunks remain future work. The current archive keeps
one byte copy per capture. Database failures after
local staging can be retried; startup and pre-receipt crash evidence need further
work before T03 is accepted.
See [configuration/commands](../../config/fomc_meeting_minutes/README.md) and the
[current implementation handoff](todo.md).

## Core text chunks and optional embedding enhancement

**Core chunks:** retain full extracted text and derive section/paragraph-aware
chunks; start evaluation at 500–800 tokens with 50–100 tokens of overlap. These
are tuning defaults, not completeness limits. Split oversized passages without
truncating content, and keep tables/footnotes linked to their context.

Each chunk stores `chunk_id`, document/extraction-version IDs, ordinal, exact
text/hash, section and page references, and half-open Unicode character offsets
into that extraction version. A pinned `chunker_profile_id` records algorithm,
tokenizer/version, size and overlap. IDs derive from the input version, profile
and spans; repeats are idempotent and changed extraction/chunking creates a new
set. Verify that chunk spans cover all eligible extracted text. Report excluded
or unextractable content explicitly; chunk coverage cannot certify OCR quality.

**Optional embeddings:** a separate, independently retryable source-owned job
consumes committed eligible chunks. Embedding failure never blocks archival,
full-text publication or keyword search. OCR eligibility is explicit, and
unreviewed/excluded chunks remain visible in coverage. The pipeline owns data
artifacts; downstream applications own search interfaces and question answering.

- Cache vectors by the exact **embedding-input hash + embedding profile** and
  link them to chunks. Any heading/date prefix or formatting is part of the
  hashed input. Retain chunk text unchanged and reuse cached vectors for
  unchanged input/profile rather than calling the model again.
- The immutable profile records provider, model, reported revision when available,
  dimensions, input formatting/tokenizer, normalization, distance metric and
  stored-vector encoding/precision. Store vectors, profile/input IDs, generation
  time, a hash of the declared stored encoding and processing status. Preserve
  old profiles/results; do not mix model spaces, even if their dimensions match. An unversioned provider alias is not a guarantee of identical
  regeneration; retain actual outputs and record that limitation.
- Keep required PostgreSQL archive/text storage independent of embeddings.
  The enhancement extends the same source-owned schema with chunk/profile/vector
  records and chunk-to-vector links using [pgvector](https://github.com/pgvector/pgvector).
  Support keyword plus vector retrieval; begin with exact vector search and
  evaluate approximate indexes only against measured latency/recall needs.
  Pin model dimensions compatible with the selected storage/index configuration.
- External generation is opt-in with pinned configuration and finite token,
  request, retry and cost budgets. Use the run-scoped shared HTTP client and
  pipeline-owned network settings with separately reviewed embedding-provider
  hosts. Credentials stay in environment-backed configuration. Local inspection
  and replay reuse stored outputs and never call an embedding service.

Track chunk and embedding coverage separately from archive/text coverage.
Publish an index generation with its selected document/extraction versions,
profile and coverage manifest; failures cannot advance a completed-index pointer.
Retrieval filters document kind, version, source/capture cutoff and OCR eligibility,
and returns original-page citations. Generation time never replaces source release
or first-seen time. Query embeddings must use the selected index's model profile.
The enhancement adds no requirement for an external service or vector extension
to the core archive. Proposed later files: `embeddings.py` and `io/vector.py`.

## Acquisition and completeness rules

Outbound requests use the run-scoped [shared HTTP client](../../docs/development/outbound_http.md)
and pipeline-owned `network` settings. Restrict discovery/redirects to reviewed
Federal Reserve hosts and in-scope links. Configure finite year/document/request/
byte/time budgets, conservative request pacing and source-owned retries honoring
Retry-After. A size limit rejects and records an outcome; it never truncates content.
Local inspection, health and replay perform no network probe.

Refresh current-year and configurable overlapping prior-year inventories for
new releases and corrections; old completed years are not permanently skipped.
Every discovered item has an explicit outcome: archived, pending release,
unavailable, rejected or failed. A conference call may have no published minutes;
retain the inventory evidence without substituting a transcript. Report separately:

- **Archive coverage:** requested/discovered/acquired documents and required
  representations/addenda, missing items, valid containers and PDF page counts.
- **Text coverage:** native extraction, OCR review, unextractable pages and errors.
  A complete byte archive does not imply verified complete searchable text.

A failed refresh leaves the last committed catalog intact and reports the failure.
No existing source revision or original file is overwritten. Retention is retain-all
for referenced documents/versions initially; no automatic deletion is planned.

## Acceptance and first implementation handoff

| ID | Deterministic acceptance evidence |
| --- | --- |
| FOMC-AC01 | Invented current/historical inventories cover overlapping years, action/discussion kinds, Executive Committee records, joint meetings and a shared bundled PDF without double counting. |
| FOMC-AC02 | Complete HTML/PDF bytes and declared minutes addenda survive byte/hash readback; malformed, oversized, HTML-login and truncated responses retain failed evidence and cannot publish success. |
| FOMC-AC03 | Multi-page/section fixtures retain first/middle/last content, tables, footnotes and appendices. Every PDF page is accounted for; scans and missing text are visibly flagged. Real source-shaped samples receive separate text/page review before completeness claims. |
| FOMC-AC04 | Repeated capture, correction and reversion preserve history and actual first-seen clocks; date-only source release labels remain date-only. |
| FOMC-AC05 | Pending/unavailable/failed items and extraction gaps remain distinct; a failed write cannot advance the committed catalog; empty-target replay reproduces hashes and versions without network. |
| FOMC-AC06 | Bounded current refresh/backfill obey limits, host/redaction policy and retries; no job runs without explicit activation and every schedule defaults stopped. |
| FOMC-AC07 | Core chunk fixtures prove deterministic IDs, exact source offsets/page citations, Unicode and long-passage handling, complete eligible-text coverage, overlap, and new sets after extraction/chunker changes. Full text remains intact. |
| FOMC-AC08 (enhancement) | Repeated embedding input/profile hits the cache; changed input/profile creates distinct records. Readback preserves declared vector dimensions/encoding/hashes/links; nonfinite or wrong-sized vectors reject and incompatible model spaces cannot be queried together. |
| FOMC-AC09 (enhancement) | Injected provider failures, oversized inputs and budget exhaustion leave the core archive/catalog usable, record retryable outcomes and accurate embedding coverage, and cannot publish a falsely complete index. Offline replay makes no model call. |
| FOMC-AC10 (enhancement) | Retrieval fixtures enforce selected versions, publication/capture cutoffs and OCR eligibility and resolve every result to exact source pages. Query/index profiles match; any approximate index is evaluated against exact-search results. |
| FOMC-AC11 | Own config folder loads without secrets/service access; isolated PostgreSQL inventory imports round-trip exactly, retain history, reject namespace/run conflicts, and survive rollback/concurrent retries without a false current pointer. T02 evidence is recorded separately. |
| FOMC-AC12 | Future archive/text/chunk slices mirror original bytes and complete text/lineage into the FOMC PostgreSQL schema; byte hashes, Unicode text, pages/chunks, revisions and DB/file publication recovery reconcile with filesystem artifacts. Full archive/text/chunk reconciliation remains incomplete. |

**Current pickup:** [finish FOMC-T03](todo.md#fomc-t03--native-inventory-adapter-and-immutable-bronze-in-progress).
The archive is a bounded foundation, not a historical backfill or text index.
Embeddings remain an independently selected enhancement.

Verification commands (PostgreSQL integration is opt-in; see the config README):

```bash
.venv/bin/python -m pytest -q tests/test_fomc_*.py
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
```

Preserve originals, source identity and capture clocks. Review source/date
inconsistencies and third-party notices before dependent acquisition. Do not
fabricate missing text, backdate knowledge, replace minutes with another product,
copy sibling runtime config/secrets, or enable live scheduling through this spec.
