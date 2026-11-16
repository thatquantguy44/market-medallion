# Implementation plan — FINRA OTC transparency

**Status:** OTC-T01–T06 implemented offline, including the six-profile local panel; T07+ planned. [todo.md](todo.md) owns task execution status.
This source follows Spec012 by delivery priority and precedes Spec014. It has
no technical dependency on daily short-sale data or a shared provider warehouse.

## Architecture decisions and rationale

Begin with one admitted weekly detail profile and local invented fixtures. Add
the second product and other tiers only after the first path preserves exact
population identity. Keep totals and monthly/block reports out of the detail
schema so aggregation cannot silently double-count the same trading activity.

Use source-owned Bronze/SQLite/Parquet plus pure typed transforms. Keep file and
API adapters separate; either must prove completeness into the same partition
contract. Prefer a verified permitted free download route. No paid API or new
credential is a silent default when a public route fails.

```text
rights + route evidence -> product/tier/schema inventory -> invented detail parser
  -> attribution identity -> raw/coverage receipts -> local weekly publication
  -> revision/removal + availability semantics -> safe venue features
  -> typed snapshot + replay -> bounded refresh + health
  -> approved file interchange -> stopped job + delivery evidence
```

## Vertical slices and checkpoints

| Slice | Tasks | User-visible or inspectable outcome |
| --- | --- | --- |
| A: admitted shape | OTC-T01–OTC-T03 | Implemented: [route/use review](source_review.md), six-profile catalog and one invented ATS/T1 parser; [Checkpoint A](checkpoint_a.md). Live conformance pending. |
| B: local panel | OTC-T04–OTC-T06 | Implemented: identity, [immutable Bronze](t05_bronze_acceptance.md), and [six-profile transactional local panel/CLI](t06_local_panel_acceptance.md). |
| C: historical truth | OTC-T07–OTC-T09 | Current/as-of membership, delayed availability and scoped features. |
| D: reproduction | OTC-T10–OTC-T11 | Committed typed snapshot and equivalent archived replay. |
| E: bounded refresh | OTC-T12–OTC-T13 | Finite correction refresh and release-aware coverage health. |
| F: handoff | OTC-T14–OTC-T16 | Admitted research artifact, stopped job, runbook and AC evidence. |

## Traceability

| Requirement | Component | Task | Acceptance |
| --- | --- | --- | --- |
| OTC-REQ01 | rights/route profile | OTC-T01 | OTC-AC01 |
| OTC-REQ02 | product/tier catalog | OTC-T02 | OTC-AC02 |
| OTC-REQ03 | weekly parser | OTC-T03 | OTC-AC03 |
| OTC-REQ04 | participant identity | OTC-T04 | OTC-AC04 |
| OTC-REQ05 | Bronze/coverage receipts | OTC-T05 | OTC-AC05 |
| OTC-REQ06 | six selected product/tier profiles through local SQLite | OTC-T06, OTC-T12 | OTC-AC06 |
| OTC-REQ07 | immutable replacement history | OTC-T07 | OTC-AC07 |
| OTC-REQ08 | separate clock/date fields | OTC-T08 | OTC-AC08 |
| OTC-REQ09 | scope-safe Gold | OTC-T09 | OTC-AC09 |
| OTC-REQ10 | Parquet/pointer | OTC-T10 | OTC-AC10 |
| OTC-REQ11 | replay | OTC-T11 | OTC-AC11 |
| OTC-NFR01 | bounded discovery/transfer | OTC-T12 | OTC-AC12 |
| OTC-NFR02 | release/coverage health | OTC-T13 | OTC-AC13 |
| OTC-REQ12 | file exchange | OTC-T14 | OTC-AC14 |
| OTC-NFR03 | stopped operation/handoff | OTC-T15, OTC-T16 | OTC-AC15 |

## Decisions to settle at the source-contract checkpoint

Record the supported live route and allowed retained/derived/exported uses,
current literal schema profiles, oldest admitted reporting week per product,
meaning of aggregated/un-attributed rows, source-ID availability, publication
calendar and dates, correction overlap, finite backfill budget, and resource
limits. Do not infer an API response schema from the older download guide.

If a stable identifier is absent, preserve route-scoped labels and disclose
limited longitudinal identity. A source providing ambiguous duplicate labels
needs an explicit disambiguation contract or an unavailable result, not a
synthetic firm merge. Keep a dated schema/terms record with each admitted route.

## T05 implementation boundary

Keep the raw-byte/receipt contract, reader and linked validation outcome coherent
in this slice. Pure metadata lives in `io/receipts.py`; atomic filesystem storage
and validation orchestration live in `io/bronze.py`. The larger test surface covers
C03/C05/C06 failure, corruption and concurrency boundaries without introducing a
shared storage framework or extending the source/API surface. No function runs
network discovery. The local limits and ownership checks are independent of any
provider SLA or retention permission. T06 keeps these receipts inspectable while adding all six fixture profiles and
transactional local publication.

## T06 bounded implementation increments

The user authorized T06 on `feat/finra-otc-contract-parser` on 2026-11-16.
All four increments are implemented and independently verified:

1. **T06a — six invented profiles:** catalog/manifest, pure weekly parser and
   profile tests. Reuse the historical ATS fixture grammar across explicit tiers;
   use a separately named, explicitly invented non-ATS grammar with declared
   attribution and optional supplied IDs. Never claim current provider schemas.
2. **T06b — archive integration:** validate new fixture schemas, retain historical
   v1 descriptors/outcomes, and verify all six through immutable Bronze.
3. **T06c — transactional local panel:** source-owned SQLite, pipeline and integration
   tests. A separately archived synthetic discovery assertion binds partition,
   schema, exact payload SHA and expected row count. Only this fixture evidence
   may produce COMPLETE_FIXTURE/COMPLETE_EMPTY_FIXTURE. Rows, coverage, terminal
   attempt and publication selection commit together. Failed/partial/unverified
   attempts retain old selections and cannot advance a success marker.
4. **T06d — operator interface:** CLI/module entrypoint and CLI tests, then acceptance,
   register and handoff. Import one explicit population at a time; inspect requires
   a bounded explicit population list. JSON uses decimal text for quantities.

Each increment targets at most five implementation/test files; fixture/catalog
and owning documentation updates accompany the relevant increment. No generic
warehouse framework or new dependency is introduced. Until T07 implements revision
and replacement history, a second complete capture for an already published
partition is recorded as REPLACEMENT_DEFERRED without replacing membership.
T06 reads select initial complete fixture populations, with explicit attribution
counts and absent requested populations; venue ratios remain T09, Parquet T10,
replay T11 and real extraction/conformance T12.

## Risks and controls

| Risk | Effect | Mitigation |
| --- | --- | --- |
| Old machine-download example mistaken for current endpoint | Broken or unpermitted acquisition. | Current route conformance and use evidence before transport implementation. |
| Detail plus summary mixed | Inflated totals and distorted shares. | Separate datasets/grains; reconciliation-only totals. |
| Tier delays ignored | Newest reporting week is mislabeled missing/stale. | Explicit required partition/release state and provider delay. |
| Name-only identity | False attribution across firm changes. | Evidence-basis field and raw labels, unresolved joins preserved. |
| Grouped small firms | Misleading concentration. | Show grouped share; defer concentration methods. |
| Pagination truncates active dataset | Incomplete population becomes denominator. | Stable sort/bounds/count reconciliation and atomic partition admission. |
| Old report overwritten | Corrections erase prior research context. | Content versions plus complete-replacement removals and captured cutoffs. |

## Verification and rollout

All ordinary checks are offline. Every acceptance ID has an invented fixture or
deterministic transaction/contract assertion. Separately admitted live checking
begins with one bounded week/tier/product and compares actual schema, context,
file/page completeness and returned count fields. Keep first conformance
evidence distinct from historical coverage claims.

After the full offline suite, Ruff and package build pass, document actual
implemented profiles and exclusions in the registers and runbook. Recovery
must reconstruct the same current/as-of result from retained bytes. A failed
new capture preserves the prior pointer but creates a visible failed refresh;
it must not claim successful current source availability. Kill switch is stop
schedule plus deny live flag; neither deletes retained research history.

Spec014 begins after this source's acceptance/handoff checkpoint. Public product
deployment, broker identity enrichment, monthly/blocks extensions and optional
concentration methods are separate follow-on decisions, not completion blockers
for the admitted initial weekly research product.
