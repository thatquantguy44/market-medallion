# SEC N-PORT ordered tasks

All tasks are **planned/unchecked** as of 2026-11-03. This document is the
task tracker for Spec 014; do not duplicate status in a second task file.
Each session should close one S/M task with evidence. Split before exceeding
five changed files. `P` below means
`src/market_data_pipeline/pipelines/sec_nport/`; this package does not exist yet.
Dependencies are delivery gates; no imports from FINRA are required.

## SEC014-T01 — Admit source and disclosure era

Dependencies: Spec 013 delivery; requirements: R01; size: S.
Likely files: this spec, `data_contract.md`, source evidence record (3).

- [ ] Record official archive/schema/access sources, review date and evidence.
- [ ] Resolve actual public reporting regime and classify unresolved schema,
  amendment, timing and reuse questions explicitly.
- [ ] Verify: reviewer can distinguish observed documentation from proposed
  fields and execute no download from the document-only step (AC01).

## SEC014-T02 — Design adversarial offline archive fixtures

Dependencies: T01; requirements: R02, R03; size: M.
Likely files: fixture generator/spec, fixture README, two focused tests (4).

- [ ] Invent a minimal multi-table archive with two filings, one amendment,
  several identifiers/borrowers and an independent expected reconciliation.
- [ ] Include missing-member, join-fan-out, decimal and disclosure-time cases.
- [ ] Verify: source-contract tests fail meaningfully before parser work;
  fixture review confirms no provider data (AC02–AC04).

## Checkpoint A — source contract

- [ ] Schema era, units and nulls are evidenced or have a named unresolved gate.
- [ ] Fixture expectations independently define correct grain and totals.
- [ ] NAAIM remains paused and no source runtime/schedule has been enabled.

## SEC014-T03 — Bound and archive local ZIP extraction

Dependencies: T02; requirements: R02, N02; size: M.
Likely files: `P/archive.py`, `P/io/bronze.py`, archive test, manifest (4).

- [ ] Preserve exact archive/member receipts and reject path traversal,
  symlinks, duplicate members, truncation and expansion-limit breaches.
- [ ] Stream the declared allowlist under explicit byte/member/row budgets.
- [ ] Verify: focused archive tests preserve rejected raw evidence while
  producing no committed publication (AC02).

## SEC014-T04 — Parse exact typed source tables

Dependencies: T03; requirements: R03, R11, N01, N03; size: M.
Likely files: `P/data/tables.py`, `P/manifest.py`, contract test, fixture (4).

- [ ] Parse declared headers/encodings, string IDs, dates, Decimal values and
  source-supported null/flag states with raw member/row pointers.
- [ ] Reject schema drift, unknown codes and non-finite or invalid decimals.
- [ ] Verify: `.venv/bin/python -m pytest -q tests/test_sec_nport_contract.py`
  passes exact type/null/precision cases (AC03, AC07; future test path).

## SEC014-T05 — Validate joins and filing selection

Dependencies: T04; requirements: R04, R05, R11; size: M.
Likely files: `P/data/joins.py`, `P/data/selection.py`, selection test (3).

- [ ] Resolve each child within its archive/filing context; expose orphans,
  duplicate parents and ambiguous surrogate reuse without multiplying amounts.
- [ ] Select eligible fund/report portfolios with explicit amendment method
  and exclusion reasons; preserve all candidate accessions.
- [ ] Verify: independent expected totals and ambiguous-amendment tests cover
  one-to-many identifiers, borrower aggregates and missing children (AC04–AC07).

## SEC014-T06 — Persist versions transactionally

Dependencies: T05; requirements: R05, R08, N03, N04; size: M.
Likely files: `P/io/warehouse.py`, `P/pipeline.py`, warehouse test, SQL (4).

- [ ] SQLite stores raw-linked immutable versions and actual first-seen clocks;
  repeat/correction/reversion and proven tombstones are explicit. Common C06's
  original monotonic capture sequence resolves equal clocks and survives replay.
- [ ] Fact/audit writes commit atomically; incomplete editions cannot advance
  a complete-coverage watermark.
- [ ] Verify: two identical loads add no fact versions; injected transaction
  failure leaves prior state queryable; equal-clock and regressing-clock cases
  follow the documented capture-order policy (AC05, AC08).

## Checkpoint B — local relational slice

- [ ] One invented archive traverses Bronze → typed rows → SQLite facts.
- [ ] Joining identifiers or borrowers cannot inflate holding money.
- [ ] Repeat, amendment and rollback evidence is retained.

## SEC014-T07 — Replay and bounded historical manifests

Dependencies: T06; requirements: R05, R08, N03; size: M.
Likely files: `P/replay.py`, `P/backfill.py`, replay test, CLI test (4).

- [ ] Empty-target replay preserves original retrieval/knowledge clocks and
  rejected outcomes without contacting the source.
- [ ] Backfill bounds enumerate archive editions; overlap refresh can observe
  corrections, and missing refreshed rows do not imply unproven withdrawals.
- [ ] Verify: network-disabled replay equals original logical state and as-of
  cutoffs before/after amendment capture differ as expected; original capture
  sequence is preserved when timestamps tie (AC05, AC09).

## SEC014-T08 — Publish lending research facts and coverage

Dependencies: T06; requirements: R06, R07, R11, N06; size: M.
Likely files: `P/data/gold.py`, gold test, dictionary, methodology (4).

- [ ] Separate fund, holding, borrower and collateral outputs preserve source
  grain, report-date coverage, currencies and unresolved identities.
- [ ] Optional ratios require a matching population/currency and nonzero
  denominator; disclose exclusions and avoid a market-utilization label.
- [ ] Verify: independent arithmetic, share-class deduplication, null/zero and
  non-USD tests reconcile every admitted aggregate (AC06, AC07).

## SEC014-T09 — Publish committed typed Parquet snapshots

Dependencies: T07, T08; requirements: R08, N03, N04; size: M.
Likely files: `P/io/parquet.py`, `P/io/publication.py`, `P/io/warehouse.py`,
publication test, `P/pipeline.py` (5).

- [ ] Decimal/string/null schemas round-trip; manifest names one immutable
  snapshot with counts, checksums, schema/method and completeness.
- [ ] Implement common C07's stage/fsync → DB pending snapshot → finalize/verify
  files → atomic pointer → published-journal sequence, with generation-pinned
  readers and root-locked restart reconciliation.
- [ ] Verify: type round-trip and failure injection at every journal boundary,
  including pointer-before-final-mark, select a coherent generation and never
  falsely advance success for incomplete archives (AC03, AC08).

## SEC014-T10 — Emit the local research exchange artifact

Dependencies: T09; requirements: R09, N05, N06; size: M.
Likely files: `P/research_export.py`, artifact schema, export test, CLI (4).

- [ ] Bound selection, dates, coverage, current/observed-as-of basis and input
  tokens are validated before writing the source-owned artifact atomically.
- [ ] Consumer fields expose research observations without raw ZIPs, operational
  secrets or implied real-time lendable inventory.
- [ ] Verify: consumer contract fixtures reject malformed/SIM/mismatched input
  and reproduce exported totals from source versions (AC10).

## Checkpoint C — reproducible research delivery

- [ ] Silver/Gold/Parquet/artifact values and counts reconcile independently.
- [ ] Every reported amount has the selected filing and input-version lineage.
- [ ] Replay and export do not require QuantMeridian to exist.

## SEC014-T11 — Add local health and audit diagnostics

Dependencies: T10; requirements: R10; size: M.
Likely files: `P/operations.py`, operations test, `P/cli.py` (3).

- [ ] No-request health reports edition age, report-date distribution,
  successful/failed attempts, schema drift and committed-output availability.
- [ ] Acquisition health and the age of disclosed holdings remain distinct.
- [ ] Verify: stale/missing/partial/last-attempt-failed fixtures produce
  actionable sanitized states without fetching a provider (AC11).

## SEC014-T11A — Implement bounded archive transport and CLI

Dependencies: T11; requirements: R02, R08, N02, N04; size: M.
Likely files: `P/source.py`, `P/cli.py`, transport test, manifest (4).

- [ ] Add the admitted official archive/index route with explicit period,
  response-byte/time/retry bounds and configuration; reuse T03's Bronze writer.
- [ ] Preserve exact responses and selection evidence; classify terminal HTTP
  errors separately, respect applicable Retry-After and refuse 200 HTML
  challenges, truncation and exhausted transient retries before promotion.
- [ ] Verify: injected transport plus CLI tests cover success, oversized/
  partial/invalid bodies, 400/403, 429, 5xx and timeout with no provider access;
  failed attempts cannot advance complete publication (AC13).

## SEC014-T12 — Characterize one bounded public archive

Dependencies: T11A; requirements: R01, R12; size: M.
Likely files: source evidence, runbook, contract, verification record (4).

- [ ] Record one permitted bounded fetch or an approved local source archive;
  document origin, chosen period, byte bounds and source authenticity. For a
  request, also record user agent/access policy and destination before fetching;
  keep raw ZIP/members and real values ignored.
- [ ] Reconcile actual member/header/row/key/clock behavior to the admitted
  contract; a mismatch updates evidence and blocks expansion.
- [ ] Verify: sanitized receipt/checksum/coverage/replay evidence demonstrates
  actual source conformance; a local-input path records explicit owner
  acceptance when the live route remains unresolved. Fixtures alone do not
  establish real-source verification or recurring activation (AC01, AC12).

## SEC014-T13 — Add stopped local orchestration

Dependencies: T12; requirements: R10; size: M.
Likely files: `P/dagster_defs.py`, root definitions, Dagster test, job register (4).

- [ ] A pipeline-owned job calls verified operations with bounded configuration
  and acquisition acknowledgement; all created schedules default stopped.
- [ ] No discovery/build/test starts a service or changes NAAIM schedule state.
- [ ] Verify: injected-transport job tests and schedule inspection confirm
  bounded run metadata and stopped defaults (AC11).

## SEC014-T14 — Close the release evidence and next gate

Dependencies: T13; requirements: R12; size: M.
Likely files: runbook, spec evidence, master handoff, pipeline register, roadmap (5).

- [ ] Map AC01–AC13 to evidence; document commands, limits, schema/open issues,
  retention choice, dry-run cleanup and empty-target recovery.
- [ ] Record the Spec 015 pickup point and remaining deployment decisions.
- [ ] Verify: focused/full offline suite, Ruff, formatting, package build and
  link/command review pass; docs describe actual scope only (AC12).

## Final checkpoint

- [ ] Every requirement and acceptance criterion has recorded implementation evidence.
- [ ] The artifact is useful to research with correct coverage and source timing.
- [ ] No unverified public monthly disclosure or lending-availability claim remains.
- [ ] Any schedule remains stopped and NAAIM remains paused.
