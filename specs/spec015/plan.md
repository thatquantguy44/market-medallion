# NY Fed primary-dealer implementation plan

Status: planned, 2026-11-03. Scope: [spec](README.md).
Fields/interfaces: [data contract](data_contract.md).
Execution tracker: [tasks](todo.md).
Program: [five-stage roadmap](../../docs/securities_finance/README.md).

## Dependency order and first delivery

Spec 014 opens the delivery gate. Runtime dependencies stay source-independent;
there is no need for a fund holding, FINRA record or SEC fail to parse a primary-
dealer observation. Cross-source research joins remain later consumer work.

The first vertical slice is an invented metadata capture plus one as-of date
through exact typed rows, SQLite versions and a small attributed local artifact.
Extend that working slice to revisions, era changes, replay and bounded history.
Only then characterize an approved date/series set from the official source.

```text
current API/terms evidence
  → metadata + source-shaped fixtures
  → exact rows + era/unit admission
  → immutable local fact/metadata versions
  → replay + comparable research features
  → committed Parquet + attributed artifact
  → bounded source acquisition + overlapping refresh
  → health + stopped orchestration + completion handoff
```

## Architecture choices

| Decision | Reason | Deferred alternative |
| --- | --- | --- |
| Discover source catalog/eras | IDs, units and definitions can change independently of code. | Hardcoded IDs selected by a third-party wrapper. |
| Persist metadata with values | Replay needs the definitions known at capture. | Read the provider's latest catalog during replay. |
| Single-as-of read first | Explicit and reviewable acquisition unit. | Default all-surveys download. |
| Source-native units/basis | Prevents mixing balances, averages and cumulative measures. | Normalize everything to one generic scalar. |
| SQLite then Parquet | Useful local research without service provisioning. | New warehouse/deployment dependency. |
| Derivation guards | Meaning and comparability are testable requirements. | A stress score assembled from whatever data arrived. |

## Phase A — source and metadata contract

NYF015-T01 records routes/schema/terms and the metadata questions still requiring
actual payload evidence. NYF015-T02 makes an invented catalog with two eras and
adversarial observation cases. NYF015-T03 admits metadata/allowlist semantics.
Choose the initial financing/fails series by their documented definitions,
units and measurement bases; preserve the full source description for review.

Checkpoint A closes before values are fetched. Unresolved series meanings
remain excluded rather than being mapped from abbreviations. An unverified
sentinel is not treated as a suppression code.

## Phase B — local reproducible facts

NYF015-T04 parses exact date/key/value/state fields against captured metadata.
NYF015-T04A first archives exact local raw input and metadata bytes with receipts
and C06's original monotonic capture sequence. NYF015-T05 then stores immutable
fact and metadata versions transactionally.
NYF015-T06 adds network-free replay and observed-as-of queries, including
correction/reversion and metadata-change behavior.

No destructive migration of another pipeline's tables is needed. Create
source-owned tables and commit facts/audits under one source-root transaction
boundary. Use canonical hashes independent of JSON object ordering while
retaining original response bytes for forensic replay.

Checkpoint B proves idempotency and that a cutoff before a correction returns
the earlier captured value and definition. A historical date loaded today
cannot enter an observed-as-of query yesterday. Equal-clock, clock-regression
and ambiguous imported-order tests cover C06 before replay is accepted.

## Phase C — comparable research output

NYF015-T07 creates native level/change views and explicit incompatibility reasons.
NYF015-T08 publishes typed Parquet snapshots with atomic commit pointers.
NYF015-T09 emits a bounded artifact carrying terms/attribution and derived labels.
Independent arithmetic, null/suppression, era and basis fixtures validate it.

T08 implements common C07's explicit journal across SQLite and files: stage/
fsync files, commit facts plus pending snapshot, finalize/verify files, replace
the pointer, then mark the journal published. Generation-pinned readers and
root-locked restart reconciliation prevent mixed reads. Failure injection at
every boundary proves a coherent prior or completed new generation, including
pointer-before-final-journal-mark recovery and no false success watermark.

Gold features always include the original units and prior observation date.
A response with a gap cannot masquerade as an ordinary one-week change.
Cross-era bridges and broad aggregate ratios require their own methodology,
not an implementation convenience inside the parser.

Checkpoint C reconciles SQLite, Parquet, artifact and replay values and input
tokens. Consumer tests can run without a provider, a scheduler or QuantMeridian.

## Phase D — acquisition and operation

NYF015-T10 adds bounded injected transport. NYF015-T11 implements an overlapping
date refresh and source-aware no-request health. NYF015-T12 characterizes a
small recorded date/series selection against actual source behavior, using a
permitted bounded fetch or an approved local source input with documented
provenance and explicit owner acceptance when the live route is unresolved.
Fixture-only evidence does not establish actual source conformance. NYF015-T13
adds stopped Dagster definitions; NYF015-T14 records completion evidence.

Retry budgets, bytes, dates and series counts are explicit manifest/config
values. Provider 400 errors are classified separately from retryable transient
failures. Do not invent an upstream date-filter parameter: use only documented
read shapes and local selection when appropriate, revealing the true response
scope and enforcing a response cap.

No schedule is enabled by this plan. Manual operation remains the default;
the user decides if/when automated polling is resumed or deployed. NAAIM stays
paused throughout. Retention requires an inventory of active snapshot/replay
dependencies and a dry-run plan before deleting archived input.

## Traceability

| Requirement | Tasks | Acceptance |
| --- | --- | --- |
| R01 | T01, T12 | AC01 |
| R02, R05 | T02, T03, T07 | AC02, AC05 |
| R03 | T04A, T10 | AC03 |
| N02 | T10 | AC03 |
| R04 | T04 | AC04 |
| R06, R09 | T04A, T05, T06, T11 | AC06 |
| R07, N03, N04 | T04A, T05, T06, T08 | AC07, AC08 |
| R08 | T07 | AC05 |
| R10, N05 | T09 | AC09 |
| R11, N06 | T11, T13 | AC10, AC11 |
| R12, N01 | T14 | AC12 |

IDs use the `NYF015-` prefix in the spec/tasks/evidence map. Each future test
names the covered acceptance or records it in a reviewable traceability index.

## Risks and unresolved questions

- **Changing series definitions:** version eras and metadata; do not stitch
  because descriptions appear similar.
- **Suppressed output:** record unknown values explicitly; never interpolate
  from neighboring values or totals.
- **Revisions outside overlap:** choose a documented rolling overlap plus
  periodic historical audit budget; distinguish observed from unobserved history.
- **Misread units/basis:** require source-form evidence and compatible guards
  before deriving changes or ratios.
- **Partial request set:** record expected/received partitions and keep the old
  complete publication pointer while the current attempt remains visibly failed.
- **Source-size drift:** enforce caps and surface planned/actual request cost.

Open questions are exact current suppression markers, unit/scale sources for
each initial key, era endpoint boundary inclusivity, metadata change/retraction
signals, historic release-time availability, initial date/series scope,
refresh overlap, periodic audit depth and retention duration. T01/T03/T12 own
evidence; T04–T11 must not silently fill those gaps with assumptions.
