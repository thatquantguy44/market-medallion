# FOMC-T04 acceptance — bounded native text and catalog

Accepted locally on 2026-11-11, on `feat/spec017-fomc-inventory`.
The selected scope remains **1990 onward**. [Text contract](text_contract.md)
and [sanitized evidence](evidence/t04_extraction.json) define the implementation
and its verified limits. T03's 33-page inventory and 72 original captures remain
unchanged. No new provider requests were made.

## Implemented and verified

- **FOMC-AC03:** pinned optional `pypdf==7.0.1`, shared reviewed HTML containers,
  complete returned Unicode text, ordered PDF pages / HTML sections, hashes and
  exact half-open character offsets. Fixtures cover first/middle/last content,
  long passages, tables, footnotes, appendices, blank pages, malformed/password-
  required PDFs, bounds, Unicode and corrupt derivatives. Limits produce explicit
  failures; no successful result silently truncates text.
- **FOMC-AC04:** consecutive raw-content changes create observed versions per
  representation URL. Repeats add capture evidence; A→B→A preserves three versions.
  Extractor/profile changes create derivatives independently. First-seen uses the
  real capture clock; release claims retain their date-only precision.
- **FOMC-AC05:** verified original/text mirrors, explicit gap outcomes and immutable
  catalog generations. Extraction/write failures preserve the current pointer;
  retained local artifacts support retries. Readers resolve the authoritative
  PostgreSQL pointer and verify the selected generation. Empty-target replay
  reproduces original receipts, hashes, text, versions and catalog without
  HTTP or extraction. A capture cutoff uses the supplied metadata snapshot;
  it does not reconstruct earlier inventory/publication knowledge.
- **FOMC-AC12 (text slice):** five explicit owned PostgreSQL tables mirror full text,
  profiles, units, capture links and committed catalogs. Readback checks both
  representations, complete text and offsets. Rollback/corruption, idempotence,
  long Unicode and replay checks pass in isolated FOMC schemas. Chunk storage
  remains T05.

## Actual local sample

All **12 archived document representations** are extracted: **74 PDF pages** and
**108 ordered text units**, totaling **616,712 characters**. Seven results are
`native_extracted`; five remain `review_required`; none has a fatal extraction
failure. The 1990 PDFs open with an empty password and permit copying. Their
existing text layers are retained with image-review flags; no new OCR is run.

Poppler independently confirms every PDF page count. Per-page normalized word
comparison yields minimum overlap 0.992095; this detects gross omissions and is
not an accuracy certificate. First/middle/last original pages of each PDF were
rendered and visually inspected (12 pages), including footnotes, the appended
March 2, 2020 proceedings and the 1990 Secretary's note. Two classic HTML body
profiles, modern HTML and separate SEP addenda retain their full returned text.

The committed catalog retains **460 unacquired declared references**, **49 source
not-listed outcomes**, the known **one unavailable** 1994 href and **five image
review gaps**. SEP charts, scanned/image-backed pages and the 2026 cover image
remain visible review cases. Native extraction does not certify OCR accuracy,
semantic table reconstruction or every visual mark. Full history acquisition and
accuracy review remain explicitly incomplete.

Before migration, a fresh backup restored all 17 existing tables and 72 captures.
After migration, another backup restored all **22 tables**, full text/units,
current catalog, metadata and original captures exactly. All 17 existing tables
and every original receipt/byte/capture clock remain unchanged. An actual empty-
target replay reproduced the catalog and all 72 receipt bytes/captures. Disposable
restore/replay databases and the replay root were removed. Other namespaces are
unchanged; jobs/schedules remain absent.

**931 regression tests passed with 46 optional skips; all 219 FOMC checks passed
with PostgreSQL enabled**, including 36 isolated backend checks. Ruff lint/format,
diff and package/artifact checks passed. Results are recorded in the evidence.
At this checkpoint T05 was next. Its later [acceptance](t05_acceptance.md) records
deterministic chunk coverage, exact citations, coherent generations and explicit
review eligibility. Broader acquisition remains T06.
