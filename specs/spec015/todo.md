# NY Fed primary-dealer ordered tasks

All tasks are **planned/unchecked**, 2026-11-03. This is Spec 015's task tracker.
`P` means `src/market_data_pipeline/pipelines/nyfed_primary_dealers/`;
the package does not exist. Each task is S/M and should touch at most five
files; split any larger slice before implementation. Spec 014 is a delivery-
order gate, not a runtime/import dependency.

## NYF015-T01 — Admit official routes and source terms

Dependencies: Spec 014 delivery; requirements: R01; size: S.
Likely files: this spec, `data_contract.md`, dated source evidence (3).

- [ ] Record API/schema/terms versions and actual current discovery/read routes.
- [ ] Assign unresolved unit/era/suppression/publication questions to evidence
  tasks; identify the initial financing/fails research scope.
- [ ] Verify: reviewer can distinguish documented paths from proposed flags,
  field names or deployment choices (AC01).

## NYF015-T02 — Define adversarial metadata and value fixtures

Dependencies: T01; requirements: R02, R04, R05; size: M.
Likely files: fixture README, metadata fixture, value fixture, contract test (4).

- [ ] Invent two eras, reused opaque key, suppressed/zero/negative values,
  missing date, correction/reversion and metadata-only change.
- [ ] Calculate an independent expected level/change and coverage table.
- [ ] Verify: test cases fail before parser work and clearly catch unit/basis
  mixing and unsupported suppression assumptions (AC02, AC04, AC05).

## NYF015-T03 — Parse metadata and admit a series allowlist

Dependencies: T02; requirements: R02, R05; size: M.
Likely files: `P/data/metadata.py`, `P/manifest.py`, manifest, metadata test (4).

- [ ] Preserve exact series-break/date/catalog identifiers with immutable
  metadata versions, source definitions, units and aggregation basis.
- [ ] Reject unknown/ambiguous era membership and unadmitted series before
  publication; record excluded catalog entries explicitly.
- [ ] Verify: boundary, duplicate metadata, unknown key and invalid allowlist
  fixtures pass (AC02).

## Checkpoint A — source meaning is explicit

- [ ] Every selected series has a documented unit/population/measurement basis.
- [ ] The actual suppression encoding remains unresolved until source evidence exists.
- [ ] No runtime job or source request has been enabled by discovery work.

## NYF015-T04 — Parse observations exactly

Dependencies: T03; requirements: R04; size: M.
Likely files: `P/data/observations.py`, observation test, fixture (3).

- [ ] Validate response envelope, strict source dates, opaque string keys,
  finite decimals and evidence-supported value-state markers.
- [ ] Retain raw locations and refuse coercion from unavailable to zero.
- [ ] Verify: `.venv/bin/python -m pytest -q tests/test_nyfed_primary_dealers_contract.py`
  covers exact types, keys, values and malformed cases (AC04; future test path).

## NYF015-T04A — Archive local input bytes and capture receipts

Dependencies: T04; requirements: R03, R06, R07, N03; size: M.
Likely files: `P/io/bronze.py`, local-input test, `P/pipeline.py`, `P/cli.py` (4).

- [ ] Before normalization/persistence, archive exact local response and
  metadata bytes with hashes, source identity, bounds, raw pointers and clocks.
- [ ] Under the root lock, assign/persist common C06's monotonic original
  capture sequence; document rejection of inconsistent clock regression or
  ambiguous imported ordering. Network transport later reuses this writer.
- [ ] Verify: immutable raw capture/receipt, same-clock ordering and invalid-
  ordering fixtures pass before T05/T06 can persist or replay facts (AC03, AC06).

## NYF015-T05 — Persist fact and metadata versions

Dependencies: T04A; requirements: R06, R07, N03, N04; size: M.
Likely files: `P/io/warehouse.py`, `P/pipeline.py`, SQL, warehouse test (4).

- [ ] SQLite stores source facts, metadata versions, first-seen clocks,
  original capture sequence and Bronze-linked audit under transactional keys.
- [ ] Repeat content adds no fact version; correction/reversion/metadata change
  and established withdrawal evidence create explicit history.
- [ ] Verify: repeat/change/reversion and injected commit failure preserve
  earlier values and leave no partial complete watermark (AC06, AC08).

## NYF015-T06 — Replay captured knowledge into an empty target

Dependencies: T05; requirements: R06, R07, N03; size: M.
Likely files: `P/replay.py`, replay test, `P/cli.py` (3).

- [ ] Replay consumes archived response/metadata inputs with original knowledge
  clocks; no live catalog lookup or new first-seen time is introduced.
- [ ] As-of selection respects both metadata and observation versions.
- [ ] Verify: network-disabled empty-root replay and pre/post-correction
  cutoffs reproduce the expected logical output and original capture sequence
  when timestamps tie (AC06, AC07).

## Checkpoint B — local reproducibility

- [ ] One invented date traverses metadata → facts → SQLite audit.
- [ ] Same-content replay is deterministic and changed values stay versioned.
- [ ] Historical source dates cannot backdate capture-time knowledge.

## NYF015-T07 — Produce compatible research comparisons

Dependencies: T05; requirements: R05, R08; size: M.
Likely files: `P/data/gold.py`, feature test, dictionary/methodology (3).

- [ ] Preserve native levels and calculate changes only with matching era,
  unit, population, basis and two observed values.
- [ ] Expose prior source date, gap and exclusion reason; cumulative fail
  totals and point-in-time settlement measures stay distinct.
- [ ] Verify: independent arithmetic plus era/unit/gap/suppression fixtures
  demonstrate correct unavailability instead of fabricated values (AC05).

## NYF015-T08 — Publish committed typed Parquet

Dependencies: T06, T07; requirements: R07, N03, N04; size: M.
Likely files: `P/io/parquet.py`, `P/io/publication.py`, `P/io/warehouse.py`,
publication test, `P/pipeline.py` (5).

- [ ] Fact/metadata/audit schemas round-trip exact decimal text, null states
  and source dimensions into an immutable snapshot.
- [ ] Implement C07's stage/fsync → DB pending snapshot → finalize/verify files
  → atomic pointer → published journal, with generation-pinned readers and
  root-locked pending-publication reconciliation.
- [ ] Verify: type/count/hash reconciliation and failure injection at every
  journal boundary, including pointer-before-final-mark, select one coherent
  generation without false success evidence (AC07, AC08).

## NYF015-T09 — Emit the attributed local research artifact

Dependencies: T08; requirements: R10, N05; size: M.
Likely files: `P/research_export.py`, artifact schema, export test, CLI (4).

- [ ] Bound the selected series/history and carry units/era/basis, source and
  knowledge clocks, coverage, notices and input-version tokens.
- [ ] Label source values and pipeline derivations separately with method IDs.
- [ ] Verify: artifact consumer tests reject malformed/missing-attribution/
  SIM-labelled data and reconcile to source facts (AC09).

## Checkpoint C — source-owned research delivery

- [ ] SQLite/Parquet/artifact figures agree with independent expectations.
- [ ] Export does not require any sibling runtime or source-provider credential.
- [ ] Derived observations carry compatible units/basis and correct provenance.

## NYF015-T10 — Implement bounded official transport

Dependencies: T09; requirements: R03, N02, N04; size: M.
Likely files: `P/source.py`, transport test, manifest, `P/cli.py` (4).

- [ ] Reuse T04A's raw/receipt writer for exact metadata/data responses; cap requests, dates, series,
  response bytes, retry count and timeouts under recorded configuration.
- [ ] Handle 400 distinctly; honor applicable retry guidance for 429/transient
  failures without promoting a partial request set.
- [ ] Verify: injected transport covers invalid/empty/partial/oversize/timeout/
  throttled responses without real provider access (AC03).

## NYF015-T11 — Refresh overlaps and report local health

Dependencies: T10; requirements: R09, R11, N06; size: M.
Likely files: `P/operations.py`, operations test, `P/cli.py`, config example (4).

- [ ] An explicit overlap re-reads completed dates; periodic historical audit
  bounds and missing-response policy are recorded rather than assumed complete.
- [ ] No-request health distinguishes expected-release lag, capture health,
  source-date age, suppression and a failed latest attempt.
- [ ] Verify: revision-outside-watermark, holiday/gap, stale/partial and
  suppression fixtures pass (AC06, AC10).

## NYF015-T12 — Characterize bounded actual source behavior

Dependencies: T11; requirements: R01, R12; size: M.
Likely files: source evidence, contract, runbook, verification record (4).

- [ ] Record one permitted bounded fetch or approved local source input, its
  provenance and the date/series selection. Before a fetch, also record route,
  response/byte/retry bounds and source-root.
- [ ] Validate actual units, suppression, era/date membership, completeness,
  source notices and response schema; real payloads remain ignored.
- [ ] Verify: sanitized receipt/checksum/count/coverage evidence demonstrates
  actual source conformance and unresolved discrepancies; a local-input path
  records explicit owner acceptance when the live route is unresolved.
  Fixtures alone do not establish conformance or activation (AC01, AC12).

## NYF015-T13 — Add stopped pipeline-owned Dagster definitions

Dependencies: T12; requirements: R11, N06; size: M.
Likely files: `P/dagster_defs.py`, root definitions, job test, job register (4).

- [ ] Call verified bounded operations through source-owned assets/jobs;
  schedules default stopped and require deliberate later activation.
- [ ] Materialization metadata exposes source-date coverage and current capture
  outcome without starting a service or changing NAAIM state.
- [ ] Verify: offline injected-job tests and schedule inspection confirm no
  provider request at definition-load time (AC11).

## NYF015-T14 — Close program research delivery and handoff

Dependencies: T13; requirements: R12, N01; size: M.
Likely files: runbook, spec evidence, program roadmap, pipeline register, handoff (5).

- [ ] Map AC01–AC12 to actual evidence; document recovery, rollback, retention
  decision, commands, manual controls and remaining data limitations.
- [ ] Record all five program workstreams' actual status and separately scope
  any later combined feature, terminal integration or deployment.
- [ ] Verify: focused/full offline suite, Ruff, formatting, package build and
  documentation/command review pass with no inflated coverage claim (AC12).

## Final checkpoint

- [ ] Every source value has an admitted era, unit, population and basis.
- [ ] Revisions and unavailable values remain honest in history and artifacts.
- [ ] Recovery/replay can reconstruct the research output without the provider.
- [ ] Schedules stay stopped and NAAIM stays paused.
