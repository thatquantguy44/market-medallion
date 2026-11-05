# Spec011 implementation plan

**State:** initial SEC parser/manifest/inspection implemented and reconciled
against two approved September2026 archives. T18 portable types/serialization
are complete for both sources. T03/T04 complete immutable Bronze, root-bound
SQLite facts/coverage/audit and database Gold. T05 observed revisions, cutoff
queries and evidence-gated withdrawals are complete. T06 offline replay is
verified against invented fixtures and the real September root. Next: T07 publication.
PostgreSQL and Databricks are required delivery targets, not implemented yet.
Canonical tasks: [todo.md](todo.md).
See [source review and commands](source_review.md) for actual verification.
This is the first research delivery in the [program roadmap](../../docs/securities_finance/roadmap.md).

## Vertical slices and decisions

1. **SEC modern local input:** pin one source-shaped schema and selected
   partition, write safe ZIP/plain parsers and complete a local immutable
   Bronze → typed Silver → SQLite Gold path. First pin backend-independent
   records, warehouse operations and explicit portable numeric schemas in T18.
   Resolve failures on a two-row invented file before enlarging scope.
2. **SEC knowledge and publication:** add append-only observed revisions,
   complete replacement withdrawals, cutoff selection, Parquet/current pointer,
   offline replay and failure-injection recovery. Source balances and missing-row
   coverage are already independently useful research output. T19 adds the
   PostgreSQL adapter/parity run; T21 publishes committed snapshots to Delta.
3. **Nasdaq local list:** pin the observed current file shape, trailer/date
   semantics and complete-list envelope. Model membership and coverage, then
   corrections/episodes with explicit calendar/censoring tests.
4. **NYSE local list:** finish its separate date/market/schema contract and
   adapter. Keep it independent of Nasdaq parsing. Prove complete-empty and
   failed-download behavior before claiming combined list coverage. T20/T22
   deliver PostgreSQL/Delta parity for threshold membership and coverage.
5. **Research exchange:** emit two source contracts. The optional joined view
   in `sec_fails_to_deliver/research.py` reads these contracts plus admitted
   mapping data; it never imports threshold internals or queries its store.
6. **Live/operations:** add only the admitted bounded source fetches, explicit
   overlap refresh, no-request health and stopped schedules. Reconcile one real
   selected partition/list sample if permitted. Record exact covered dates and
   gaps. G2 acceptance permits an owner-accepted approved local-file route.

## Proposed files and command boundary

Each package grows as its slice works: `manifest.py`, `source.py`, `pipeline.py`,
`data/`, `io/`, `replay.py`, `operations.py`, `cli.py`, `__main__.py`,
`dagster_defs.py`. FTD additionally owns `research.py` for its public projection.
Source manifests/fixtures remain separate even when a common ZIP helper is
eventually extracted after demonstrated reuse.

SEC FTD's implemented runtime-config entrypoint is
`config/sec_fails_to_deliver/pipeline.example.yml` with a source-owned
`config.py` loader. The user selected PostgreSQL by default and disabled
Databricks publication until configured. See
[the decision/setup guide](../../config/sec_fails_to_deliver/README.md).
Configurable precision/scale identifies a storage contract. Schema generation
and representability checks are implemented in T18; T04 enforces SQLite root binding.
Both source `storage.py` modules declare normalized schemas and warehouse
Protocols. Shared field codecs have two concrete source users. SQLite/Parquet
round trips and native PostgreSQL bindings are tested; PostgreSQL server and
Spark/Delta integrations remain T19–T22.

Each source owns its warehouse boundary and SQLite/PostgreSQL/Delta adapters.
SEC T04 uses `bronze.py`, `warehouse.py` and `pipeline.py`; threshold persistence
and the future Delta publishers remain separate source-owned modules. Do not import AAII's
provider-specific storage schema or require Spark for ordinary local imports.
The [portable data contract](data_contract.md#9-storage-and-platform-compatibility)
defines required exact-text values, checked numeric projections and UTC/date
types. SQLite must not dictate the logical model or force float/int64 coercion.

Current tested SEC commands are in the
[runbook](../../docs/deployment/sec_ftd_runbook.md); `run --config ... --partition ...`
and `gold --config ... --limit ...` use an explicitly selected SQLite config.
The older interface sketches below are **not implemented**:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver run \
  --input tests/fixtures/sec_fails_to_deliver/modern.zip --root data/ftd-demo
.venv/bin/python -m market_data_pipeline.pipelines.reg_sho_thresholds run \
  --provider nasdaq --input tests/fixtures/reg_sho_thresholds/list.txt \
  --list-date 2026-01-07 --root data/reg-sho-demo
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver check \
  --root data/ftd-demo
```

The exact CLI surface is pinned by its implementation task. Live paths require
configured admitted routes and date/partition limits; there is no unbounded
`download-all` default. Current existing checks remain in the common contract.

## Persistence and crash recovery

Use a single-root writer lock. Persist a monotonic capture sequence before
normalization so observations with identical UTC timestamps replay in their
actual accepted order. One successful replay uses the original sequence.

Cross-store publication is a state machine, not a distributed DB transaction:
stage and fsync a snapshot under a new immutable ID; commit selected-warehouse fact/audit
state and a publication record naming that snapshot; finalize/fsync its files;
atomically replace the current pointer only once both the committed record and
file manifest verify. Readers select the published snapshot ID and reject any
mismatched DB/file generation. On crash, an incomplete/staged publication is
resumed or abandoned under the same lock; the prior pointer stays usable.

Inject failures at staging, DB commit, file finalization and pointer update.
Tests must show no mixed generations, no falsely successful watermark, safe
retry and preserved original knowledge times. Exact durability mechanics may
reuse tested repository helpers; they do not justify coupling source schemas.

PostgreSQL uses the same transaction/query contract with pipeline-scoped writer
serialization, parameterized values and a schema/warehouse identity. Delta is
a downstream snapshot publisher. Its facts/coverage/audit tables have independent
commits, so publish a verified generation manifest naming their versions before
advancing consumer state. Retry incomplete generations without changing revision
IDs or capture ordering; never present a mixture of table generations as current.

Verify PostgreSQL parity with an actual isolated database. Verify Delta schema,
values, idempotent publication and interrupted-publication recovery with an actual
Spark/Delta or Databricks engine. Offline mocked calls are additional tests.
Databricks cloud provisioning, workspace activation and schedules remain separate;
record workspace validation as run/not run rather than inferring it from a build.

## Traceability and checkpoints

| Requirements | Tasks | Evidence |
| --- | --- | --- |
| R01 | T01,T02,T09,T10 | Source/access/sample matrix and selected coverage |
| R02,R03 | T02,T03,T04 | A01,A02,A06 parser/partition fixtures |
| R04 | T08,T09,T10 | A03,A04 per-provider list tests |
| R05 | T12 | A07 dated mapping/cardinality tests |
| R06 | T05,T06,T11 | A05,A09 revision/replay tests |
| R07 | T04,T06,T07,T18–T22 | A09,A10,A14,A15 storage/crash/parity tests |
| R08 | T11,T12 | A03,A06,A08 features/episodes |
| R09 | T13,T14 | A07,A11 export and projection |
| R10 | T15,T16 | A12 CLI/health/stopped schedule tests |
| R11 | T17 | A13 bounded reconciliation and handoff |
| R12 | T18,T19,T20,T21,T22,T17 | A14 PostgreSQL parity; A15 actual Delta round trip and publication recovery |
| N01–N04 | T03–T22 | Determinism, budgets, isolation and regression evidence |

At checkpoints after T03, T06, T09, T12 and T15, run affected offline tests and
review unresolved contracts. At T17 run source/full tests, lint, format/package
checks, A14/A15 backend evidence and confirm no schedule enabled. T18–T22 are
required before G2; their larger numeric/schema tests start before T04.
Update register/handoff with evidence,
then accept G2 and begin Spec012. The optional joined projection T14 does not
block native-source delivery; the gate records whether that extension shipped.

## Risks and decisions to resolve

- NYSE: validate a properly parameterized public download and admitted market
  values before naming its physical schema. A broken default link is no-data
  evidence only for that request, not proof the historical feed is unavailable.
- Nasdaq: confirm rights and historical listing/trailer behavior; account for
  the announced December 2026 effective-date change in schedule/date tests.
- SEC: absence needs date/universe coverage evidence; older threshold-era
  history must use a separate contract before zero or prevalence calculations.
- Identity: a full public historical security master may not be available under
  suitable rights. Independent native output is required; coverage-qualified
  joins must remain useful without an unsupported universal mapping project.
- September sample sizes fit the selected limits; wider history and retention
  remain unmeasured. Do not reuse NAAIM's policy by default.
