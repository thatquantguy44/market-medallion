# Program implementation plan

**State:** Spec011 offline SEC parser/manifest/inspection implemented;
two approved September2026 SEC archives reconciled. T18 portable types and
serialization are complete for both sources. T03–T05 local SEC persistence,
observed revisions, cutoff Gold and gated withdrawals are complete. Next: T06 replay.
Spec011 requires PostgreSQL/Databricks delivery and parity tests in T18–T22.
Exchange conformance and later deliveries
remain planned. See [checkpoint](../../specs/spec011/source_review.md).
**Canonical source tasks:** `specs/spec011/todo.md` through
`specs/spec015/todo.md`. [Program coordination tasks](todo.md) own only the
milestone tracking and combined reporting work.

## Design decisions and rationale

1. Keep the first workstream as two independently replayable source pipelines.
   The SEC reports balances with CUSIP identifiers; exchanges report membership
   lists with venue/date/symbol keys. A single scalar schema would erase the
   differences and turn absent rows into misleading zeroes.
2. Treat each later source as independent. Delivery priority does not require
   importing a predecessor's storage adapter or adopting AAII's schema.
3. Preserve the existing medallion stack and pure-Python transforms. Design
   portable exact types before the SQLite/Parquet development slice. Spec011
   must also deliver PostgreSQL and Databricks adapters with actual integration
   evidence before G2; cloud provisioning remains separate.
4. Preserve capture knowledge explicitly. Historical observations are useful
   for description; leakage-free historical knowledge is claimed only where
   actual availability evidence supports it.
5. Publish versioned local artifacts at each source's G2 gate. This gives a
   stable consumer boundary without coupling the two repositories' secrets,
   schemas or deployment state.
6. Keep manual runtime operation and stopped schedule definitions. Optional
   scheduling and public presentation require their own delivered checks;
   NAAIM's paused state is preserved throughout this program.

## Execution sequence

Contract evidence → invented fixtures → one complete local partition → typed
current/revision state → observed-as-of/replay → bounded research artifact →
bounded source reconciliation → research-delivery gate → next numbered spec.

Within each source, introduce the first end-to-end file path before adding all
schemas, adapters or derived metrics. Checkpoints every two or three tasks
catch unit, completeness, identifier and clock defects before wide backfills.
The detailed order and likely files are in the selected spec task list.

## Shared verification themes

| Concern | Evidence required before a research delivery |
| --- | --- |
| Availability | Observation vs retrieval vs first-seen/publication clocks are separate and testable |
| Identity | Raw source identifiers retained; ambiguous, temporal and missing joins stay explicit |
| Completeness | Expected selected partitions reconcile to received/valid/empty/missing outcomes |
| Revision | Same payload stable; corrections, reversion and authoritative removal preserve prior knowledge |
| Publication | Atomic current pointer; failed stages do not expose mixed snapshots |
| Replay | Archived bytes rebuild the same selected versions and values without network |
| Consumer | Artifact schema, unit, coverage, revision basis and attribution validate independently |
| Operations | No-request health, bounded retries, dry-run retention, empty-target recovery and stopped defaults |

## Risks and mitigation

| Risk | Consequence | Planned response |
| --- | --- | --- |
| Free website differs from permitted bulk/reuse route | A working downloader may not meet the intended use | Finish access/terms matrix first; keep invented fixtures and local-input path separate from live admission |
| Symbol-only cross-source joins | Corporate actions or reuse attach data to the wrong issue | Evidence-backed dated mapping; unresolved state; retain native source outputs |
| Publication lag mistaken for data freshness | Research leaks future information or calls a delayed feed stale | Source release ledger, nullable unknown clocks, explicit current-vintage/observed-as-of modes |
| Absent rows or suppression treated as zero | False changes and understated risk | Typed coverage/missing states, authoritative replacement rules and explicit retraction fixtures |
| N-PORT joins multiply holdings | Inflated fund/security exposures | Assert join cardinality and totals against the same accession before aggregating |
| Historical schemas spliced together | False trend breaks or levels | Pin schemas and definition eras; disallow equivalence without documented metadata |
| Large file expansion | Memory/disk failure and partial publication | Streaming/staged loading, configurable limits, sample profiling and atomic commit |
| Broad shared platform refactor | Delays all five research deliveries | Extract helpers only when at least two implemented contracts support identical behavior |

## Release and handoff policy

Use a dedicated source branch per bounded implementation slice after reconciling
current main/PR state; do not add new source code to an NAAIM branch. Preserve
user changes and local state. A source PR references requirements and checked
tasks, lists its actual selected coverage, and records evidence without raw
provider data or credentials. Merge/release actions follow user direction.

At G2, update `docs/pipelines.md`, the master roadmap and master handoff with
actual package paths, covered dates/universes, commands, tests, known gaps and
the next unchecked task. Add jobs to `docs/deployment/dagster_jobs.md` only when
implemented. Rollback pins the prior committed artifact; do not overwrite or
delete Bronze or historical versions to hide a failed run.
