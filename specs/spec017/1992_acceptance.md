# FOMC 1992 bounded backfill acceptance

Accepted locally on 2026-11-12 on `feat/fomc-1992-backfill`, from merged PR #13
(`41e9d0c06a4669b1ef6e9aa308c93f829034176f`). The existing
[`scripts/run_fomc.sh`](../../scripts/run_fomc.sh), [operations contract](operations_contract.md)
and [runbook](../../docs/deployment/fomc_runbook.md) govern this 1992-only slice.
[Sanitized evidence](evidence/1992_script_backfill.json) records actual execution
and recovery. [1991 acceptance](1991_acceptance.md) remains the earlier checkpoint.

## Plan and execution

No-request health was healthy and the archived official inventory listed
16 primary 1992 references. A fresh backup first restored all 29 owned tables,
matching row fingerprints, all 108 existing originals, selected catalogs and
operation audit. The script then ran explicit `backfill --start-year 1992
--end-year 1992 --live` with unchanged finite operation/network settings and the
1990 operating floor. No runtime, configuration, schema or dependency change was
needed. No other year was selected for document acquisition.

All **16 primary representations were acquired**, with three refreshed inventory
pages and 30 verified nonselected pages carried with their original clocks.
The run used **19 data GETs plus two startup probes**, receiving **9,401,386
data-response bytes**. Four 1992 not-listed source outcomes remain explicit.
Inventory, text, chunks and the successful operation result committed together.
All 108 prior original receipts/bytes/clocks and pre-existing append-only rows
remain unchanged; the three selected catalog pointers intentionally advanced.

## Current archive and coverage

The archive now has **127 captures**: 47 inventories, 58 document representations
and 22 PNGs. The current normalized inventory is exactly equal to the prior
snapshot: **33 pages, 371 meeting claims, 470 references and 561 associations**.
Earlier snapshots and capture clocks remain queryable. Publication clocks are
preserved as supplied; historical first-seen was not backdated.

Selected text contains **58 representations, 624 PDF pages, 658 units and
1,579,673 characters**. Independent `pdfinfo` page counts match every selected
PDF, and nonempty PDF units retain exact chunk citations. Native outcomes remain
**seven native-extracted and 51 review-required representations**. These are
native derivatives with explicit review flags; no OCR/model service was called
and text/OCR accuracy is not certified.

There are **791 chunks: 166 eligible and 625 review-flagged**. Exact spans cover
all **1,578,473 source-unit characters**; **1,200 extraction separators** are
accounted for separately. Outstanding gaps remain visible: **414 unacquired
references, 49 not-listed claims, one unavailable 1994 href and 51 review cases**.
Full historical acquisition and text/OCR accuracy review remain incomplete.

## Readback, restore and replay

Post-batch backup restored all **29 tables**, matching exact table fingerprints,
all 127 original readbacks, selected catalogs and operation audit. Actual empty-
target replay reproduced all 127 original receipt bytes, the selected inventory/
text/chunk generations, chunk text/eligibility and the latest 1992 operation's
intent/events/result. HTTP, extraction, tokenization and chunk generation were
disabled during replay. Full backup preserves historical operation/catalog
snapshots; selected-generation replay does not claim to restore every old snapshot.

Health, plan and latest-run recovery also passed with HTTP disabled. Disposable
restore/replay databases and replay root were removed; other namespaces remain
unchanged. Local credentials/config, source bytes, derivatives and backups remain
ignored. No schedule was activated, no schema migration was applied and no
embedding service was selected. NAAIM remains paused; FINRA stays at SSV-T05.

**954 regression tests passed with 71 optional skips; all 267 FOMC checks passed
with PostgreSQL enabled**, including 61 isolated backend checks. Bash syntax,
Ruff lint/format, diff, build, local-link and source-artifact checks passed.

Next core operation: a bounded **1993** batch through the same script, followed
by health/readback and recovery gates. Keep document-kind distinctions across
the transition into modern minutes, the 1990 floor and all review gaps.
