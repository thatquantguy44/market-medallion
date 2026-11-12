# FOMC-T06 acceptance — bounded acquisition and operations

Accepted locally on 2026-11-12 on `feat/spec017-fomc-operations`, from merged
PR #11. [Operations contract](operations_contract.md), [runbook](../../docs/deployment/fomc_runbook.md)
and [sanitized evidence](evidence/t06_operations.json) record FOMC-AC06 and the
preserved AC02–05/07/12 guarantees. Active scope remains 1990 onward.

## Bounded acquisition and coherent publication

Strict source-owned operation settings bound years, inventory pages, primary/
addendum documents, assets, request attempts, received bytes, elapsed time and
retry delays. Explicit live/scheduled gates and the pinned PDF dependency are
checked before service access or HTTP selection. One run-scoped client selects
its route once. Every data GET and retry counts; two requests are reserved for
startup probes. Pacing and numeric/date Retry-After are honored, or the run stops.
Elapsed limits are checked between work and before commit; individual blocking
calls retain the configured HTTP timeout.

Inventories are refreshed for the selected years; verified nonselected pages are
carried with their original clocks, and newly discovered pages remain bounded.
Backfill reuses verified valid originals; refresh re-fetches selected originals
and their explicit recursive addenda/assets. Cycles, unsupported grammar,
invalid containers, rejected responses and missing dependencies block promotion.
Each raw response and retry retains exact accepted/rejected capture evidence.

A session lock serializes operations before intent/probe. One publication
transaction moves inventory, text, chunks and the successful operation result
together. Compare-before-commit guards protect all three pointers. Source failures,
budget exhaustion and write failures leave the previous generations selected;
raw captures and immutable derivative artifacts remain available for recovery.
Prior source revisions and capture-time first-seen clocks remain intact.

## Audit, local commands and stopped orchestration

Three explicitly owned tables mirror immutable operation intent, ordered events
and the final result. Recovery verifies exact mirrors, IDs and order. It never
invents completion or promotes catalog pointers. A filesystem completion proposal
for rolled-back catalogs cannot be recovered as a committed success.

The FOMC CLI supports validate-config, migrate, plan, run, health, recover-capture
and recover-run. Health verifies the current generation chain and distinguishes
empty, not-run, interrupted, failed and stale state from a healthy bounded run;
it exposes source gaps and review eligibility without certifying full coverage.
Validation, migration, plan, health, recovery and replay perform no HTTP probe.
Actual local health/plan/run-recovery passed with HTTP disabled.

The manual Dagster job uses the same runner. The weekday 17:10 America/New_York
schedule defaults **STOPPED**, with a separate false-by-default scheduled-access
setting checked in preview and execution. Its future refresh includes current
and overlapping prior years. Loadability, schedule preview and actual closed-gate
job execution are tested. **No FOMC schedule was activated. NAAIM remains paused.**

## Actual 1990 batch and retained coverage

The first bounded live batch completed with **16 selected primary references**:
**14 acquired PDFs and two reused verified originals**. Three inventories were
refreshed; 30 nonselected inventory pages were carried. The batch made **17 data
GETs plus two startup probes**, receiving **7,800,834 data-response bytes** under
the recorded finite settings. Four 1990 not-listed source outcomes remain explicit.
All three pointers advanced together; the exact operation receipt is mirrored.

The current archive contains **89 captures**: 41 inventories, 26 document
representations and 22 PNG assets. All 72 pre-existing original receipts, bytes
and clocks remain unchanged. Every pre-existing append-only database row is
preserved; the three current pointers intentionally changed.

The refreshed calendar removed its eight 2027 planning placeholders. Exact raw
calendar comparison confirms the removed panel. The current 33-page snapshot has
**363 meeting records, 470 document references and 553 associations**; the earlier
371-record/561-association snapshot remains queryable. This observed source change
is not a cancellation inference and removes no document reference.

The selected text/chunk generation contains **26 representations**, **239 PDF
pages**, **273 units** and **897,899 text characters**. All PDF page counts match
independent `pdfinfo` readback. Representative first/middle/last pages of a newly
acquired 1990 policy record were rendered and inspected. Native-text review remains
explicit: **seven native-extracted and 19 review-required representations**.
No OCR or model call was made and text/OCR accuracy is not certified.

There are **406 chunks: 166 eligible and 240 review-flagged**. Exact spans cover
all **897,405 source-unit characters**, with **494 extraction separators**
accounted for separately. The generation retains 446 unacquired references,
41 not-listed claims, one unavailable 1994 href and 19 review cases. Full history
remains incomplete; byte/page/chunk coverage does not establish OCR accuracy.

## Restore, replay and verification

A fresh pre-migration backup restored all **26 tables**; the post-operation backup
restored all **29 tables**, with exact row fingerprints, originals, catalogs and
operation audit. Actual empty-target replay reproduced raw receipts, selected
inventory/text/chunk generations, chunk text/eligibility and the operation's
intent/events/result with HTTP, extraction, tokenization and chunk generation
disabled. All 89 captures read back exactly. Full backup preserves historical
snapshots; selected-generation replay does not claim to restore every old catalog.
Disposable restore/replay databases and replay root were removed. Other namespaces
remain unchanged. Raw payloads, local configuration and backups remain ignored.

**952 regression tests passed with 71 optional skips; all 265 FOMC checks passed
with PostgreSQL enabled**, including **61 isolated backend checks**. Tests cover
retry receipts/dates, request/byte/time exhaustion, dependency failures, concurrent
operation refusal, coherent rollback, mirror corruption, capture/run recovery and
false-completion refusal. Ruff lint/format, diff, build and artifact checks passed.
CI now supplies an isolated FOMC config to its disposable PostgreSQL service, so
these backend checks run on every push/PR instead of being skipped.

The next core operation is another explicit bounded historical batch, starting
with **1991**, followed by no-request health/readback. Full history and review
remain tracked separately. T07 embeddings are an independently selected optional
enhancement; no model/service/vector extension has been selected.
