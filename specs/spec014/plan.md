# SEC N-PORT implementation plan

Status: planned, 2026-11-03. No implementation claims.
Scope/requirements: [README](README.md); fields: [data contract](data_contract.md);
execution checklist: [todo](todo.md); sequence: [program](../../docs/securities_finance/README.md).

## Entry and dependency direction

Spec 013's research delivery gate opens this workstream. This is an ordered
delivery dependency; the N-PORT runtime must not import FINRA source internals.
The source can parse a local fixture independently. Existing shared atomic
file/YAML primitives may be reused where contracts match.

Implementation order:

```text
documentary access/schema era
  → invented archive/member cases
  → safe local extraction + exact typed rows
  → relational integrity + filing selection
  → transactional local versions + replay
  → lending research facts + complete publication
  → local artifact + health
  → bounded source capture + evidence
  → optional stopped Dagster definitions + handoff
```

## Decisions and rationale

| Decision | Rationale | Alternative deferred |
| --- | --- | --- |
| Public bulk ZIP first | Reproducible edition/member set and bounded local replay. | Parsing arbitrary fund webpages or undocumented third-party APIs. |
| Separate tables/grains | Prevents identifier/borrower fan-out from multiplying money. | A single universal observations table. |
| SQLite + typed Parquet | Local research can run without another service. | New PostgreSQL/cloud deployment. |
| Decimals and string identifiers | Source precision/large surrogate IDs survive round-trip. | Binary floats and inferred integer IDs. |
| Explicit disclosure era | New rules must not be projected onto past source availability. | Fixed monthly lag derived from a future rule. |
| Full filing lineage retained | Amendments and archive corrections are observable. | Overwriting latest portfolio rows. |
| Coverage before aggregate | Unknown or excluded funds cannot masquerade as a complete lending market. | Filling absent loans/holdings with zeros. |

## Phase A — reduce schema and access uncertainty

SEC014-T01 records official sources, fair access, schema and disclosure era.
SEC014-T02 specifies source-shaped invented archive members plus a small
independent reconciliation example. Resolve the cardinality of child holding
IDs and the completeness of amendments before selecting a current filing.

Checkpoint A requires documentary evidence, explicit open questions and
fixture expectations. A missing runtime dependency must be justified and pinned
in a separate scoped task before it is used. No bulk fetch is needed here.

## Phase B — one archive through exact local facts

SEC014-T03 makes archive extraction bounded and auditable. SEC014-T04 parses
one declared table set. SEC014-T05 validates relationships and builds filing
eligibility without duplicating holdings. SEC014-T06 persists immutable
versions and handles repeat/correction/reversion cases in one transaction.

Target bounds are configuration, not claims about current data: initially one
archive per run; explicit compressed bytes, expanded bytes, member count and
per-member/row limits; fail before unbounded expansion. Characterization sets
the values. Use disk streaming and batch writes; the plan must not require a
ZIP, all TSV tables and their joins to coexist in RAM.

Checkpoint B runs the local fixture twice and applies an invented changed
edition. Silver/Gold counts and lineage must reconcile exactly, including
no new fact version for an identical observation. Common C06's persisted capture
sequence resolves equal timestamps; replay preserves that original sequence and
tests reject clock regression or ambiguous imported ordering.

## Phase C — replay and useful research publication

SEC014-T07 implements empty-root replay and bounded historical manifests.
SEC014-T08 adds separate loan/collateral/borrower research facts with coverage.
SEC014-T09 adds committed Parquet snapshots and rollback-safe pointers.
SEC014-T10 emits the narrow local artifact. Do not couple publication to
the future QuantMeridian UI or require sibling credentials.

T09 implements common C07's cross-store journal, not an assumed SQLite/filesystem
transaction: stage/fsync files, commit facts plus pending snapshot, finalize and
verify files, replace the pointer, then mark publication complete. Readers pin
the same generation in DB and files. Restart reconciles pending work under the
root lock. Failure injection at every boundary must prove either the prior
coherent generation or the completed new one without false success evidence.

Gold selectors carry explicit report-date and observed-as-of modes. A reader
must choose the committed snapshot manifest; glob-reading all Parquet files is
unsupported. A prior complete snapshot can remain stored after failure, but
the current health response must reveal that the latest attempted refresh
failed. Do not relabel old data as a successful fresh acquisition.

Checkpoint C independently reconciles reported amounts at each grain, proves
Decimal/ID round-trip and compares network-disabled replay with the captured
logical output. Every artifact aggregate must trace to selected filing versions.

## Phase D — observed source behavior and local operation

SEC014-T11 adds no-request health and source diagnostics. SEC014-T11A implements
the bounded official archive transport and CLI with injected HTTP/content/retry
failures; it reuses the already delivered Bronze writer. SEC014-T12 then performs
the source characterization described by the run manifest using either a
permitted bounded fetch or an approved local archive with documented provenance
and explicit owner acceptance when the live route is unresolved. Fixtures alone
do not establish actual source conformance. Raw data remains ignored and
reconciliation evidence is sanitized. Resolve any
schema mismatch in the contract before expanding coverage. SEC014-T13 may
introduce stopped orchestration only after callable source operations work.
SEC014-T14 records release evidence, runbook and next gate.

The default operating model is manual. Schedules stay stopped; startup,
backfill and dependency installation are not implicit consequences of creating
these specs. A future operator chooses retention that preserves active
publication/replay dependencies and reviews a dry-run deletion plan first.

## Requirements to tasks

| Requirement | Tasks | Acceptance |
| --- | --- | --- |
| R01 | T01, T12 | AC01 |
| R02 | T02, T03, T11A | AC02, AC13 |
| R03, N01 | T02–T04 | AC03 |
| N02 | T03, T11A | AC02, AC13 |
| R04 | T05 | AC04 |
| R05 | T05, T06, T07 | AC05 |
| R06, R07 | T08 | AC06 |
| R08, N03, N04 | T06, T07, T09, T11A | AC08, AC09, AC13 |
| R09, N05, N06 | T10 | AC10 |
| R10 | T11, T13 | AC11 |
| R11 | T04, T05, T08 | AC07 |
| R12 | T14 | AC12 |

All identifiers use the `SEC014-` prefix in the specification and checklist.
Acceptance tests should name the covered IDs or record them in an evidence map.

## Risks, recovery and unresolved decisions

- **Join fan-out:** count parent/child cardinality before every aggregation;
  an unresolved relationship blocks its aggregate, with the raw edition retained.
- **Regulatory/schema change:** keep a versioned era and metadata checksum;
  do not silently accept a new field dictionary.
- **Late/amended filings:** overlap archive discovery and append versions;
  never assume a closed quarter cannot change.
- **Partial archive:** preserve its audit; leave the committed pointer intact.
- **Storage growth:** report projected compressed/expanded and retained bytes;
  select retention explicitly after the first bounded capture.
- **Misleading coverage:** publish fund/report-date distribution and exclusions;
  loan holdings do not establish borrow availability.

Open decisions are exact current headers/units/nulls, amendment replacement
semantics, metadata support for acceptance/dissemination clocks, complete
edition replacement rules, initial quarter bounds, memory/disk budgets and
retention duration. Each has an owning discovery/test task above; none is
silently answered by a proposed field name.
