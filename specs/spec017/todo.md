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

**Year-index discovery increment (2026-11-11):** Complete for the bounded
selected-page contract in [discovery contract](discovery_contract.md).
`data/discovery.py` retains native year-link evidence and derives an explicit
expected page set from archived index/calendar bytes. Unknown/conflicting/moved
labels, gaps, empty panels and uncovered selected years block coverage. Opt-in
publication with `historical_years=(start,end)` requires an exact expected-page
match and commits hashed discovery evidence, capture links and the metadata
pointer atomically. The separately marked discovery migration upgrades the
owned Bronze schema; it is tested only in isolated FOMC schemas, not applied to
production. Legacy metadata unions retain no discovery coverage claim.

[Source characterization](evidence/year_index_discovery.json) records one
bounded official index request, 85 historical year links (1936–2020), retained
calendar headings 2021–2027 and an explicit 87-page expected set. No year-page
backfill was performed. The index is local review evidence only; production
still holds five Bronze captures and zero inventory runs. The 2025 notation
vote and 1936-page/1937 Executive Committee conflict remain blocking issues.

Discovery increment verification: **866 offline tests passed, 25 optional skips**;
**133 FOMC checks passed** with the FOMC integration configuration, including all
**15 real PostgreSQL tests** in isolated schemas. Ruff lint/format and source/wheel
builds pass. Artifact inspection confirms the discovery module, invented HTML
fixture, contract and sanitized evidence are packaged; ignored runtime inputs
are absent. No production schema migration or inventory publication was performed.

**Attachment/storage/recovery increment (2026-11-11):** Implemented the
[declared-addendum and PNG dependency contract](attachments_contract.md), reviewed
legacy/modern native-label fixes, [content-addressed original migration and
durable attempt recovery](storage_recovery_contract.md). Dependency checks retain
parent/child raw labels and inherited bundled-meeting associations, verify both
archives and commit atomically. A bounded source sample reconciles two minutes,
two SEP addenda and all 22 linked PNGs, plus the 1993/2020 year pages.

Filesystem originals now share hash-addressed objects; explicit local and
PostgreSQL migrations preserve every receipt/URL/clock. Durable intents precede
GET/startup selection, and offline recovery retains the same capture identity.
All source and backend evidence is isolated/local review only; production remains
at five Bronze captures and zero normalized inventory runs. No new production
migration, backfill or schedule was performed. Evidence:
[attachments](evidence/attachment_source_sample.json),
[storage/recovery](evidence/storage_recovery_sample.json).

**Next pickup to finish T03:** Review the remaining source exceptions and extend
bounded historical conformance before accepting source-wide discovery:

- The current calendar's 2025 notation vote has no supported meeting label.
- The 1936 page displays a 1937 Executive Committee label; preserve the conflict
  and verify the official document before choosing a disposition.
- The 2020 page cross-references March 2 minutes to the March 15 document and
  labels March 17–18 cancelled. These remain issues rather than guessed
  associations or certified empty meeting rows.
- Unsupported asset formats/structures and unknown/addendum labels remain
  blocking issues. Extend only with reviewed source-specific fixtures.
- Broader historical label regimes and recursive archive/container completeness
  remain uncertified. Full PDF/page/text quality remains T04; do not backfill,
  publish a completed document catalog or enable schedules from this sample.

Earlier T03 checkpoint verification was 841 offline tests and nine real FOMC
PostgreSQL tests; the discovery increment above records its later checks.
Final increment verification: **900 offline tests passed, 37 optional skips**;
all **179 focused FOMC checks passed**, including **27 real PostgreSQL tests** in
isolated schemas. Ruff lint/format, source/wheel builds and artifact inclusion/
runtime-input exclusion checks pass. No production schema migration or inventory
publication was performed.

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
