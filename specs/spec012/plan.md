# Implementation plan — FINRA daily short-sale volume

**Status:** SSV-T01–T04 complete for the selected three-TRF API profile, 2026-11-10.
Use [todo.md](todo.md) as the single task list.
The [next-agent handoff](README.md#next-agent-handoff) is the canonical pickup
point for SSV-T05 on `feat/spec012-finra-access-review`.
The user requested the complete documentation set; this plan does not activate
acquisition or schedule execution. [Spec012](README.md) defines acceptance.

## Current bounded checkpoint

The [source review](source_review.md) records the user's personal noncommercial
purpose and confirmed Public API terms acceptance. The selected
[API profile](api_contract.md) explicitly amends SSV-T02/T03 for JSON, a trade-date
partition and page completeness. A bounded token/production-metadata check
succeeded. SSV-T02 then verified source version 1, three literal TRF/market pairs,
252 available partition dates and a bounded six-row sample. The registry rejects
unknown/overlapping scopes, schema/version changes and invalid acquisition dates.
Website TXT permission stays separate; ADF, consolidated and ORF are excluded.
The pure T03 parser now passes 111 invented-fixture tests and reconciles the six
retained rows offline. Their fractional quantities corrected the original
integer-only assumption: all API share quantities are exact Decimal values.
See the [amendment and evidence](api_contract.md#ssv-t03-fractional-quantity-amendment).
T04 adds shared quantity validation/text readback and pure per-observation ratios
with fixed 34-digit half-even arithmetic. Its 71 tests and retained six-row
reconciliation pass. Next is SSV-T05 immutable raw and rejected-capture evidence
with redacted receipts. Reusable acquisition, warehouse, export and job remain absent.

## Decisions and dependency flow

Use the explicitly selected API profile and a pure JSON parser before data
acquisition. Exact whole/fractional quantities, scope, pagination receipts and
empty-response behavior are now proved offline for the parser boundary. The original TXT path remains a separate unadmitted profile with its
own header/trailer rules. Mock responses never become observations.

```text
approved use + API registry -> invented response parser -> local single-response slice
  -> immutable revisions + complete replacement membership -> scoped Gold
  -> typed publication + replay -> bounded correction refresh + health
  -> approved research export -> stopped job + acceptance handoff
```

Spec011 completion is the delivery gate, not a runtime import or DB dependency.
No new source may read another pipeline's operational tables directly. Shared
helpers may be reused only where they contain no source-specific assumptions.

## Vertical delivery slices

| Slice | Tasks | Demonstrable outcome |
| --- | --- | --- |
| A: use and shape | SSV-T01–SSV-T03 | Rights/route record plus deterministic parser and zero-file acceptance. |
| B: local trustworthy file | SSV-T04–SSV-T06 | One invented file produces scoped counts, immutable bytes and audited SQLite. |
| C: changing source | SSV-T07–SSV-T09 | Correction/removal/reversion and denominator-safe current/as-of features. |
| D: reproducibility | SSV-T10–SSV-T11 | Atomic typed snapshots and equivalent empty-target replay. |
| E: bounded operation | SSV-T12–SSV-T13 | Finite correction refresh with honest publication/coverage health. |
| F: delivery | SSV-T14–SSV-T16 | Rights-aware artifact, stopped Dagster definitions and reproducible handoff. |

## Traceability

| Requirement | Design component | Task | Acceptance |
| --- | --- | --- | --- |
| SSV-REQ01 | approved-use manifest | SSV-T01 | SSV-AC01 |
| SSV-REQ02 | API schema/facility registry | SSV-T02 | SSV-AC02 |
| SSV-REQ03 | pure parser | SSV-T03 | SSV-AC03 |
| SSV-REQ04 | typed counts | SSV-T04 | SSV-AC04 |
| SSV-REQ05 | Bronze receipts | SSV-T05 | SSV-AC05 |
| SSV-REQ06 | local orchestration/SQLite | SSV-T06 | SSV-AC06 |
| SSV-REQ07 | immutable version selection | SSV-T07 | SSV-AC07 |
| SSV-REQ08 | replacement membership | SSV-T08 | SSV-AC08 |
| SSV-REQ09 | scope-safe Gold | SSV-T09 | SSV-AC09 |
| SSV-REQ10 | typed snapshots/pointer | SSV-T10 | SSV-AC10 |
| SSV-REQ11 | archived replay | SSV-T11 | SSV-AC11 |
| SSV-NFR01 | bounded source client | SSV-T12 | SSV-AC12 |
| SSV-NFR02 | release/coverage health | SSV-T13 | SSV-AC13 |
| SSV-REQ12 | file exchange | SSV-T14 | SSV-AC14 |
| SSV-NFR03 | operation/handoff | SSV-T15, SSV-T16 | SSV-AC15 |

## Risks and mitigations

| Risk | Consequence | Design control |
| --- | --- | --- |
| General vs dataset-specific FINRA terms unresolved | Free access mistaken for a durable reuse license. | SSV-T01 records permitted route/use before real-data operation. |
| Consolidated scope differs between docs | Double counting or ORF pollution. | Source-specific scope registry and reconciliation; retain disagreement evidence. |
| Short exempt added twice | Inflated ratio. | Subset invariant and explicit arithmetic example. |
| Header/trailer variants | Truncated file appears complete. | Versioned exact profiles and trailer reconciliation. |
| Symbol changes/reuse | False longitudinal join. | Raw symbols and separate dated matching evidence. |
| Updates after completion | Missed corrections/removals. | Configured overlapping re-fetch, not permanent partition skipping. |
| Fractional/large quantity precision | Rounding/truncation or ratio corruption in SQLite/export. | Exact Decimal decoding and lossless quantity round-trip tests. |

## Operational decisions to record during implementation

Record approved provider-data retention and backup scope, the daily publication
calendar, correction lookback size, required facility set, byte/row/time limits,
and whether a free API alternative is actually admitted. Defaults must be safe
and finite; unsupported historical schema ranges must fail before network I/O.
An unavailable source cannot silently switch to a third-party mirror.

The bounded first live conformance run, if admitted, compares headers, footer,
date, facility and file hash with the documented profile and records aggregate
validation outcomes. It is separate from ordinary tests and does not certify
all historical files. Keep all schedules stopped through that checkpoint.

## Rollback and handoff

Each publication has a prior committed pointer and immutable lineage. A rejected
new capture leaves the existing snapshot physically intact but health reports
the failed refresh. Consumers may inspect dated history but cannot label it a
successful current read. Restoring a prior pointer must be audited explicitly.

Delivery needs deterministic acceptance evidence, local commands, schema/rights
record, tested recovery and updated master registers. Source data and local
runtime files remain ignored. Start Spec013 on a fresh branch after this delivery
is accepted; do not treat optional cloud deployment as its prerequisite.
