# FOMC implementation tasks and pickup

Updated: 2026-11-11. The user selected implementation, reuse of this project's
PostgreSQL location and a pipeline-owned configuration folder. No recurring
schedule or embedding service was selected. Work on `feat/spec017-fomc-inventory`.

## FOMC-T01 — normalized inventory contract (complete)

Traces FOMC-AC01. Strict manifest, reviewed URL boundary, frozen meeting/document
references and source-page association evidence are implemented. **61 offline
tests pass** with invented normalized current/historical candidates. This is
not a Federal Reserve JSON API and does not parse native HTML. Same-day sessions,
non-contiguous joint dates, bundled-document references, format variants,
overlapping inventories and conflicting source claims retain distinct evidence.
See [inventory contract](inventory_contract.md).

## FOMC-T02 — configurable PostgreSQL inventory catalog (complete)

Traces FOMC-AC11. Own config loader, example/ignored local config and PostgreSQL
adapter are implemented. **14 offline configuration tests and five real
PostgreSQL integration tests pass**. Verification covers exact metadata readback,
old/current snapshots, idempotent/concurrent retries, failed-transaction rollback,
corruption refusal, empty inventories and reinitialization. Tests use isolated
FOMC schemas; the production schema is initialized with zero inventory imports.
Configuration and operating commands: [config folder](../../config/fomc_meeting_minutes/README.md).
The [sanitized backend evidence](evidence/postgres_inventory.json) records actual
location/schema and verification scope. At T02, complete documents/text were not stored.

Full offline regression: **823 passed, 15 optional skips**. The five FOMC
PostgreSQL tests additionally passed against the existing service in isolated
schemas, for **80 focused checks** total. Ruff lint/format and build/artifact
checks pass. Local config remains ignored and owner-only.

## FOMC-T03 — native inventory adapter and immutable Bronze (in progress)

Split into reviewable pieces: discovery (`data/native.py`, `test_fomc_native.py`),
archive (`io/bronze.py`, `test_fomc_bronze.py`, PostgreSQL tests), and bounded
acquisition (`io/acquisition.py`, `acquisition.example.yml`, acquisition tests).

Implemented: an offline HTML parser for reviewed current/year-page shapes returns
normalized candidates, exclusions and issues; explicit pending labels are not
inferred from a clock. It detects the 1936-page/1937 Executive Committee label
and the 2025 notation vote without guessing. Archived source-review pages for
1936 and 1967 provide bounded parser characterization, not corpus-wide coverage.
The run-scoped HTTP adapter refuses redirects, uses configured request/byte budgets,
and retains a marked incomplete prefix if a response exceeds its cap. Original
response bytes and a safe receipt are staged on disk before PostgreSQL. Failed
HTTP responses are retained; a failed database write leaves retryable local
bytes without a commit marker. Separate `fomc-bronze-postgres/1` and
`fomc-bronze-lineage-postgres/1` migrations add capture and link tables without changing T02's catalog marker or
current pointer. Exact URL/kind checks enforce caller-asserted links to existing
inventory runs; bundled documents resolve to all retained meeting associations.
GET transport failures retain sanitized rejected receipts; failed payload writes
leave a local attempt receipt when that receipt was successfully written. A
publication method checks matching committed filesystem/database bytes, rejects
parser issues or empty pages, and atomically saves and links a union of up to 128
pages. Rollback on link failure and refusal to drop previously included source
pages are verified. The union is not a source-wide completeness claim.

[Bounded source evidence](evidence/native_bronze_sample.json): one official
calendar, the PDF and HTML for a single meeting, and locally imported, previously
captured 1936 and 1967 historical pages were read back byte-for-byte from both
filesystem and PostgreSQL. Five original captures now exist in the production
FOMC Bronze tables; **zero normalized inventory runs** have been published. Current-page parsing returns an unresolved notation-vote
issue. No source-wide completeness or text extraction is claimed.

**Next pickup to finish T03:** Start by characterizing the official historical-year
index with offline fixtures and implementing bounded, reviewed year-page discovery
in the FOMC package. Record the expected page set and unresolved source labels in
the inventory evidence before changing publication behavior. Then:

- Enumerate the official historical-year index and define an explicit expected
  page set before certifying a union as complete. Publication prevents dropping
  existing source pages, but cannot detect pages that were never discovered.
  Standalone linking methods associate existing runs and do not prove derivation.
- Resolve/document addendum and HTML-asset discovery, inventory-year enumeration,
  unknown native labels and pending/missing cases with source-specific fixtures.
  Unknown/addendum labels are issues, not proof of exclusion or completeness.
- Add content-addressed original storage with an explicit reviewed migration;
  the current implementation retains one byte copy per capture, so repeated or
  bundled artifacts are not yet deduplicated by hash. Preserve every URL and
  association while deduplicating the original bytes.
- Extend interrupted-write and crash-recovery evidence across every file/database
  boundary. Retry currently applies after both local payload and receipt exist;
  startup-probe failures and failures before the receipt is written do not yet
  produce a full acquisition receipt.
- Extend bounded historical conformance and verify supported label regimes before
  certifying T03; do not backfill, start a schedule or advance a catalog merely
  because the current five-item sample passed.

Verification: **841 offline tests passed, 19 optional skips**; separately all
**9 FOMC PostgreSQL integration tests passed** in isolated schemas. Ruff and
source/wheel builds pass. These results describe this T03 checkpoint.

The next slice after T03 is T04 complete text/extraction. T03's T01 candidate
parse and Bronze success do not publish complete documents/text to downstream
consumers. Real source data is ignored; only sanitized aggregate evidence is saved.

## Later slices (not started)

- FOMC-T04: complete native/OCR text, PostgreSQL text/document-version storage,
  source/capture clocks, correction/reversion and coherent catalog/replay
  (FOMC-AC03–05/FOMC-AC12). Keep archive and text completeness distinct.
- FOMC-T05: deterministic chunks and PostgreSQL chunk metadata/text (FOMC-AC07/12).
- FOMC-T06: bounded historical/current acquisition, health and stopped orchestration
  (FOMC-AC06), after local archival/replay evidence passes.
- FOMC-T07: independently selected optional embeddings (FOMC-AC08–10). No model,
  paid service, vector extension or deployment activation is selected.

Before continuing read root AGENTS.md, README.md, docs/pipelines.md,
specs/spec001/README.md, docs/handoffs/project_bootstrap.md and this spec/contract.
Verify the local workflow library. QuantSmith's external checkout still differs
from its pin; adopted guidance was read from hash-verified pinned Git objects.
Keep NAAIM paused. FINRA remains at SSV-T05 for its next continuation.
