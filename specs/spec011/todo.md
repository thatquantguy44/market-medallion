# Spec011 implementation tasks

**Status:** T01 documentary review recorded; T02 complete for two approved local
September2026 archives; T03 bounded parser and rejected-input retention complete.
T18 portable types/serialization are complete for both source packages.
T05 revisions and T06 deterministic offline replay are complete. Next: T07 Parquet publication.
PostgreSQL/Databricks adapters and live parity in T19–T22 remain unimplemented.
**Updated:** 2026-11-03. Evidence: [source_review.md](source_review.md).
IDs below use S11- prefixes; R/A IDs refer to [README](README.md). Common
requirements C01–C11 apply to every task. Estimated scopes are S=1–2 files and
M=3–5 files. Split a task further if the actual change exceeds that scope.

## S11-T01 — Admit source use and select the first partitions

**Dependencies:** none. **Scope:** S. **Requirements:** R01,R11,N03,N04.

- [x] Record route/cost/automation/retention/transform/audience evidence for SEC,
  Nasdaq and NYSE independently; identify actual unresolved restrictions.
- [x] Select one modern SEC semi-monthly partition and bounded list dates/markets;
  record expected release/creation clocks, calendar basis and sample budgets.
- [x] Review the Nasdaq December2026 notice as a future effective-date change.

**Evidence:** the source checkpoint records the selected October2026 partitions
and independent unresolved source gates. This completes the documentary matrix,
not live admission for exchange downloads. The SEC request returned403; the user
then supplied two September2026 archives for local conformance instead.

**Verify:** official links and dated source/access matrix reconcile; actual
provider-dependent work remains gated only where evidence is unresolved.
**Likely files:** `README.md`, `data_contract.md`. No source data download here.

## S11-T02 — Pin the modern SEC schema and manifest

**Dependencies:** T01. **Scope:** M. **Requirements:** R01,R02,R03.

- [x] Pin observed header/member/encoding, natural-key uniqueness and period rules.
- [x] Validate a source-owned manifest before I/O; unknown schema fields fail.
- [x] Add invented leading-zero CUSIP, null-price and valid-row fixtures.

**Evidence:** profile `sec-ftd-six-column/2` reconciles both approved September
archives: 50,332 and 66,693 rows, ASCII/LF, exact header/member and both trailer
totals, zero duplicate keys and independent all-field equality. Explicit bounds
are Sep1–14 and Sep15–30; no universal filename-to-date split is inferred.
See the checkpoint for hashes, commands and retained aggregate evidence.

**Verify:** focused manifest tests reject unknown regime/period/type settings;
fixture headers match admitted evidence. **Likely files:** source manifest,
`sec_fails_to_deliver/manifest.py`, two fixture/test files. **Evidence:** A01.

## S11-T03 — Parse bounded SEC archives safely

**Dependencies:** T02. **Scope:** M. **Requirements:** R02,R03,N01,N02.

- [x] Reject traversal, duplicate member, corrupt ZIP, excessive expansion and
  HTML challenge files while retaining original rejected payload provenance.
- [x] Parse typed date/CUSIP/count/decimal tokens without guessing or coercion.
- [x] Validate duplicate keys and selected-period containment before promotion.

**Evidence:** parser hazards fail before fact promotion and T04 archives original
bounded rejected payloads. Oversized raw inputs retain a bounded explicitly
incomplete prefix, never a fabricated original hash. See the runbook for limits.

**Verify:** A01/A02 adversarial pytest fixtures; invalid input yields no fact
promotion. **Likely files:** parser, archive adapter, focused tests, two fixtures.

### Checkpoint 1 — Source contract and parser

- [x] Contract evidence and first selected bounds are recorded.
- [x] Documentary-profile A01/A02 parser components and manifest tests pass offline.
- [x] No acquisition adapter or schedule was enabled by fixture work.

## S11-T18 — Pin the portable storage contract before persistence

**Dependencies:** T02. **Scope:** M. **Requirements:** R03,R07,R12,N01.

- [x] Add a dedicated SEC FTD config folder/example, strict source-owned loader
  and no-request CLI validation; configure manifest/input selection and numeric
  profile intent without reading secrets or claiming implemented backends.
- [x] Pin versioned normalized records/warehouse operations and explicit SQL,
  Arrow and Spark field types for each source, retaining independent source grains.
- [x] Preserve exact numeric text; select checked decimal precision/scale and
  overflow policy without float or int64 coercion of FTD balances.
- [x] Add invented serialization round trips for leading-zero IDs, counts above
  int64, decimal38/39-digit limits, price scale, UTC clocks and unknown nulls.

**Verify:** A14/A15 schema tests compare normalized values across serialization
boundaries; lossy numeric projections fail before publication. No Spark dependency
is needed to import the pure-Python core. **Likely files:** data contract,
source-owned storage models/schema modules, focused schema tests. Implement
source-specific schema modules separately if the slice exceeds five files.

**Complete:** both source-owned `storage.py` modules define normalized records,
SQL/native bindings, explicit Arrow and Spark StructType JSON, semantic validation
and warehouse Protocols. Invented records round-trip through actual SQLite and
Parquet; PostgreSQL native bindings round-trip without a server. Decimal38 limits,
39-digit overflow, scale loss, configurable precision, UTC/date/nulls, withdrawn
records and complete-empty/failed lists are covered. Parser raw numeric tokens
are retained. All 117,025 approved September observations pass a numeric/date
preflight; ignored evidence is `data/sec_ftd/storage-types-202609.json`.
Full suite: **283 passed, 2 existing opt-in PostgreSQL skips**; Ruff, changed-file
format and sdist/wheel build pass. These tests close T18, not the full live
A14/A15 gates. Root binding/persistence is T04; actual PostgreSQL and Delta
integration evidence remains T19–T22. See
[the config guide](../../config/sec_fails_to_deliver/README.md).

## S11-T04 — Deliver one local FTD medallion partition

**Dependencies:** T03,T18. **Scope:** M. **Requirements:** R02,R03,R07,R12,N03.

- [x] Archive original input/receipt, typed rows and run/partition completeness.
- [x] Persist SQLite facts with source/regime identity and publish initial Gold.
- [x] Use the T18 normalized warehouse boundary and exact types; no SQLite-only
  queries or implicit float conversion may leak into transforms or selection.
- [x] Distinguish source absent, rejected and successful/empty partition states.

**Verify:** invented fixture round trip/reopen matches values and original bytes;
wrong namespace/regime cannot share a table contract. **Likely files:** pipeline,
Bronze adapter, SQLite adapter, integration tests, package init.

**Complete:** immutable capture/receipt, contract-bound SQLite adapter, transactional
facts/coverage/audit and database-backed Gold, exposed by configured `run` and
bounded `gold`. Real September import reopened with 117,025 unique facts; an
unchanged third capture inserted zero facts. Failed/missing/unsupported-empty
and oversized inputs are distinct. At the T04 checkpoint, changed/overlapping
replacements were retained but rejected; T05 now admits validated corrections.
Overlapping partition scopes remain rejected. Root mismatch and clock regression fail before capture.
Mid-insert and receipt-tampering regressions prove no partial fact promotion.
Full suite **316 passed, 2 existing PostgreSQL skips**; lint/changed format/build
pass. Ignored evidence: `data/sec_ftd/t04-verification.json`.
[Runbook](../../docs/deployment/sec_ftd_runbook.md). PostgreSQL stays the configured
operational default but is explicitly unavailable until T19; no fallback or new
service/schedule was enabled. Parquet/publication recovery remains T07.

## S11-T05 — Preserve FTD revisions and withdrawals

**Dependencies:** T04. **Scope:** M. **Requirements:** R06,R07,N01.

- [x] Persist sequence-ordered corrections, reversions and first-seen timestamps.
- [x] Mark row withdrawal only after a validated complete partition replacement.
- [x] Keep unchanged fact version counts stable and coverage versions separate.

**Verify:** A05 at before/equal/after capture cutoffs; partial replacement cannot
withdraw a row. **Likely files:** version model, warehouse, selection, tests.

**Evidence:** 346 tests passed, 2 skipped; lint and package builds passed.
Invented fixtures cover corrections, reversions, equal-clock capture cutoffs,
partial omissions, scoped withdrawals, reappearance and transaction rollback.
The existing September SQLite root remains compatible: 117,025 fact versions,
four audit runs, and zero new versions on an unchanged repeat capture. Real
September coverage remains PARTIAL; no completeness evidence was supplied.

## S11-T06 — Replay FTD without network

**Dependencies:** T05. **Scope:** M. **Requirements:** R06,R07,N01.

- [x] Replay original receipts/members in persisted capture sequence order.
- [x] Reproduce equal-clock corrections and original version first-seen clocks.
- [x] Require empty destination and explicit supported parser/schema versions.
- [x] Replay archived completeness evidence from receipt versions 1/2, preserving
  the original audit/admission outcome. Never re-admit a historically rejected
  capture under newer correction rules or consult a live evidence file.

**Verify:** A09 same normalized versions/history/Gold after clean replay;
transport stub fails if invoked. **Likely files:** replay module, receipt model,
warehouse helper, replay tests.

**Complete:** immutable terminal outcome receipts bind original audit/coverage
and per-run fact fingerprints. Legacy captures reconcile against the read-only
source ledger; finalized inventories plus sealed captures replay without its database. Validation and
private staging precede atomic publication into a separate empty root. A
historical rejection is retained rather than reevaluated as newly acceptable.
Full suite: **419 passed, 2 existing opt-in PostgreSQL skips**. Real September
replay reproduces 117,025 fact versions, four coverage records and four audits;
cutoff counts are 0 / 50,332 / 117,025 and all source-file digests are unchanged.
Ignored evidence: `data/sec_ftd/t06-verification.json`. No network request,
PostgreSQL/Delta activation or source-store rewrite occurred.

### Checkpoint 2 — Independent FTD research state

- [x] A05/A09 pass and modern absence remains coverage-qualified (A06).
- [x] Same payload, correction, reversion and withdrawal remain inspectable.
- [x] No interpretation claims fail age or new daily failed shares.

## S11-T07 — Publish coherent FTD Parquet snapshots

**Dependencies:** T06. **Scope:** M. **Requirements:** R07,N02.

- [ ] Stage typed files, committed DB snapshot metadata and atomic current pointer.
- [ ] Recover pending/staged publication deterministically under the root lock.
- [ ] Never expose a partial new generation or falsely successful watermark.

**Verify:** A10 failure injections at staging/commit/finalize/pointer transitions;
reopening uses either coherent old or coherent completed new snapshot.
**Likely files:** publisher, pipeline, recovery helper, publication tests.

## S11-T19 — Deliver the SEC FTD PostgreSQL adapter

**Dependencies:** T07,T18. **Scope:** M. **Requirements:** R03,R06,R07,R12,N03.

- [ ] Implement source-owned PostgreSQL schema/transactions, writer serialization,
  warehouse identity and sanitized environment-based connection configuration.
- [ ] Run the same capture/revision/reversion/withdrawal/replay/cutoff cases as
  SQLite, including exact quantities/prices, failed promotion and equal clocks.
- [ ] Test rollback, concurrent/retried captures and wrong schema/root identity
  against a dedicated disposable PostgreSQL database; preserve existing stores.

**Verify:** A14 actual SQLite/PostgreSQL normalized parity, with engine version
and test evidence recorded. **Likely files:** PostgreSQL adapter, source config,
connection example, backend-contract tests, PostgreSQL integration tests.

## S11-T20 — Deliver the threshold PostgreSQL adapter

**Dependencies:** T11,T18. **Scope:** M. **Requirements:** R04,R06,R07,R12,N03.

- [ ] Implement the threshold-owned PostgreSQL adapter/configuration independently
  of FTD tables, preserving provider/market/list-date/rule and coverage identity.
- [ ] Prove SQLite/PostgreSQL membership, coverage, episode, cutoff and replay
  parity for complete-empty, failed lists, corrections and withdrawals.
- [ ] Verify isolated schema/root identity, rollback and writer serialization
  against a dedicated PostgreSQL test database.

**Verify:** A14 actual backend parity; missing lists never become empty/exit
events. **Likely files:** threshold PostgreSQL adapter, source config/example,
shared contract cases within tests, PostgreSQL integration tests.

## S11-T21 — Publish SEC FTD snapshots to Databricks Delta

**Dependencies:** T07,T18,T19. **Scope:** M. **Requirements:** R03,R06,R07,R12,N02.

- [ ] Add source-owned explicit-schema snapshot validation and Unity Catalog
  Delta publication; preserve exact values, version IDs, capture sequence and audit.
- [ ] Reject decimal overflow/scale loss before writes; publish coherent pinned
  facts/coverage/audit generations with idempotent retries and failure recovery.
- [ ] Supply a configured staging/target example and actual Spark/Delta or
  Databricks round-trip tests; record whether a workspace smoke ran.

**Verify:** A15 exact field equality, repeated publish adds no revisions, and
failure between table commits keeps the prior consumer generation selected.
**Likely files:** FTD Delta publisher, publication tests, engine integration tests,
source-named notebook/example, runbook. Cloud provisioning is separate.

## S11-T22 — Publish threshold snapshots to Databricks Delta

**Dependencies:** T11,T18,T20. **Scope:** M. **Requirements:** R04,R06,R07,R12,N02.

- [ ] Add a threshold-owned Delta publisher for membership, coverage, episodes
  and audit with explicit types and independent provider/market identity.
- [ ] Preserve revision/withdrawal/cutoff behavior and coherent pinned generations
  across retry, failed publication and complete-empty/missing-list inputs.
- [ ] Supply target/staging configuration and actual Spark/Delta or Databricks
  round-trip tests, including repeated publication and observed ordering.

**Verify:** A15 normalized membership/coverage/episode equality with the source
snapshot; partial publication never advances current state. **Likely files:**
threshold Delta publisher, publication tests, engine integration tests,
source-named notebook/example, runbook.

## S11-T08 — Pin Nasdaq list and coverage contract

**Dependencies:** T01,T02. **Scope:** M. **Requirements:** R01,R04,N03.

- [ ] Define threshold manifest independent of SEC, selected market/date and trailer.
- [ ] Add invented complete-list/empty/date-mismatch/flag/trailer fixtures.
- [ ] Parse list envelope and membership separately; preserve raw flags/symbols.

**Verify:** A03/A04 schema, row-count and trailer tests. **Likely files:** threshold
manifest, Nasdaq parser, tests, two fixtures. Exact field rules follow source sample.

## S11-T09 — Deliver local Nasdaq list state

**Dependencies:** T08. **Scope:** M. **Requirements:** R01,R04,R06,R07.

- [ ] Persist per-list coverage and membership with source capture lineage.
- [ ] Apply complete replacement corrections/removals without erasing history.
- [ ] Preserve missing/failed/valid-empty partitions as distinct outcomes.

**Verify:** A03–A05 fixture run/reopen and replacement tests. **Likely files:**
threshold pipeline, SQLite adapter, coverage model, integration/revision tests.

### Checkpoint 3 — Nasdaq membership evidence

- [ ] Complete and failed/empty list cases are distinguishable.
- [ ] A missing file cannot generate an exit or recreate absent dates.
- [ ] NYSE remains unimplemented/unverified until its next task passes.

## S11-T10 — Admit and implement the NYSE adapter

**Dependencies:** T01,T09. **Scope:** M. **Requirements:** R01,R04,R11.

- [ ] Resolve public date/market parameters, schema and applicable access rights.
- [ ] Add a separate provider parser/envelope using invented source-shaped input.
- [ ] Prove selected NYSE market/date coverage without assuming Nasdaq conventions.

**Verify:** A03/A04 date/market/schema and complete-empty/failure regressions;
source-shaped sample reconciliation if permitted. **Likely files:** NYSE source
contract/adapter, two fixtures, adapter tests. Record blockers rather than bypass.

## S11-T11 — Build observed threshold episodes

**Dependencies:** T09,T10. **Scope:** M. **Requirements:** R06,R07,R08.

- [ ] Derive observed entries/exits only across complete adjacent calendar dates.
- [ ] Expose missing-date gap and left/right-censoring states explicitly.
- [ ] Replay membership/episodes at cutoffs using versioned coverage/calendar.

**Verify:** A08/A09 holiday, initial presence, final presence, missing date,
correction and withdrawal fixtures. **Likely files:** episode transform,
calendar contract, replay module, episode/replay tests.

## S11-T12 — Specify dated identity and scoped features

**Dependencies:** T05,T11. **Scope:** M. **Requirements:** R03,R05,R08.

- [ ] Add evidence-backed mapping inputs with effective/knowledge dates and
  unresolved/ambiguous states; preserve native facts when mapping fails.
- [ ] Define prior-balance changes and coverage-qualified observation counts.
- [ ] Validate any reference-value proxy and price-null semantics separately.

**Verify:** A06/A07 reused symbol/overlap/missing mapping/date/zero/null fixtures;
join counts never inflate FTD shares. **Likely files:** identity contract,
feature transform, mapping fixture, identity/feature tests.

### Checkpoint 4 — Research semantics

- [ ] Source-native timelines remain useful independently of mappings.
- [ ] Censoring/coverage and current-vintage/observed-as-of modes are explicit.
- [ ] No false short-interest, lendable-supply or fail-age output exists.

## S11-T13 — Publish native research artifacts

**Dependencies:** T07,T11. **Scope:** M. **Requirements:** R07,R09.

- [ ] Publish validated bounded `sec-ftd-research/1` and
  `reg-sho-thresholds-research/1` artifacts with units/dates/clocks/coverage.
- [ ] Retain source attribution and opaque revision tokens; exclude private paths.
- [ ] Reject oversized/invalid generation and preserve the previous artifact clock.

**Verify:** A11 schema and partial/missing/withdrawn export fixtures independently.
**Likely files:** two export modules, artifact schema, two export tests.

## S11-T14 — Add the optional settlement research projection

**Dependencies:** T12,T13. **Scope:** M. **Requirements:** R05,R09.

- [ ] FTD-owned `research.py` reads public artifacts plus explicit mapping input.
- [ ] Record input snapshots/mapping versions/cutoff and unmatched counts.
- [ ] Keep observation-date comparison and latest-known-as-of modes separate.

**Verify:** A07/A11 cardinality/clock/failure fixtures; no threshold-internals
imports or private-store access. **Likely files:** projection, public contract,
projection fixture/tests. Skip only with explicit scope decision, not silence.

## S11-T15 — Deliver bounded local/live operator commands

**Dependencies:** T13; live calls additionally require admitted routes. T14 is
optional and does not block native commands. **Scope:** M. **Requirements:** R10,N02,N03.

- [ ] Add validated manual input/acquisition/date bounds and dry-run backfill plan.
- [ ] Add no-request health reporting publication state and expected source lag.
- [ ] Enforce timeout/bytes/retry limits with sanitized network failure evidence.

**Verify:** A12 injected-transport and CLI invocation tests; no accidental network
on validate/check/replay. **Likely files:** source module, two CLI modules,
operations, focused CLI tests. Split live transport if this exceeds five files.

### Checkpoint 5 — Local research operation

- [ ] CLI examples are updated to actually tested commands.
- [ ] Both source export contracts validate and recovery/replay pass.
- [ ] No schedule or external service is required for research use.

## S11-T16 — Add stopped source-owned Dagster definitions

**Dependencies:** T15. **Scope:** M. **Requirements:** R10,N04.

- [ ] Define jobs/assets and source-calendar schedules with stopped defaults.
- [ ] Compose implemented definitions and add the job-register entries.
- [ ] Verify NAAIM remains stopped and all new live schedules require explicit enablement.

**Verify:** A12 definition/skip/default tests with no provider call.
**Likely files:** two `dagster_defs.py`, repository composition, job register,
Dagster tests. Runtime activation is a separate owner decision.

## S11-T17 — Reconcile and close the research delivery

**Dependencies:** T01–T13,T15–T16,T18–T22; T14 only for the optional joined projection.
G3 schedule activation/G4 consumer UI are not required.
**Scope:** M. **Requirements:** R01,R11,R12,N01–N04.

- [ ] Compare selected real permitted source partitions or approved local files
  against parsed counts/values/coverage; record actual exclusions and evidence.
- [ ] Run focused/full offline tests, lint, format and package checks; ensure
  all A01–A15 criteria have concrete evidence or explicit unresolved status.
- [ ] Record actual PostgreSQL parity and Spark/Delta or Databricks integration
  evidence for both sources. Skipped integrations do not close R12/A14/A15.
- [ ] Update source task evidence, pipeline register, program readiness and
  master handoff; accept G2 before beginning Spec012.

**Verify:** reproducible evidence record with no credentials/raw values in Git;
any missing selected provider coverage keeps G2 open unless owner explicitly
changes scope. **Likely files:** this todo, README, pipeline register, program
roadmap, master handoff. Break documentation across a second small task if needed.
