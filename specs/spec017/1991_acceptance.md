# FOMC script and 1991 backfill acceptance

Accepted locally on 2026-11-12 on `feat/fomc-runner-script`, from merged T06
PR #12. The [script contract](scripts_contract.md), [user runbook](../../docs/deployment/fomc_runbook.md)
and [sanitized evidence](evidence/1991_script_backfill.json) define this continuation.
T06 acquisition/publication semantics and the 1990 operating floor remain intact.

## Runnable checkout entrypoint

The executable `scripts/run_fomc.sh` forwards unchanged arguments to the existing
FOMC CLI. It locates the checkout's `.venv/bin/python` or uses explicit
`FOMC_PYTHON`, preserves the caller's working directory and quoted argument
boundaries, and returns the CLI's JSON/exit status. It contains no provider,
parsing, retry, storage or publication implementation. Missing runtime returns
setup guidance and exit 2. No `.env`, dependency installation, migration, live
acquisition or schedule activation occurs implicitly.

The runbook documents fresh-checkout Python 3.12/Bash/PostgreSQL setup,
user-owned config and environment-backed credentials, and explicit validation,
migration, plan, bounded run and health commands. The source distribution includes
the executable script; wheel installations retain `market-medallion fomc`.
Tests run the real CLI from another directory with spaces in checkout/config
paths, check default and explicit interpreters, prove `.env` is not executed,
and preserve no-service validation, closed live gates and exit codes.

## Actual 1991 execution

The same script performed the bounded live 1991 batch: **16 selected primary
references acquired**, plus three refreshed inventories, using **19 data GETs
and two startup probes**. Received data-response bytes: **9,439,345**. Eleven
1991 not-listed source outcomes remain explicit. The runner reused 30 verified
nonselected inventory pages and moved inventory/text/chunk pointers together.
No schema migration was needed and all 89 prior originals/receipts/clocks and
pre-existing append-only rows are preserved.

The archive now contains **108 captures**: 44 inventories, 42 document
representations and 22 PNGs. Selected native text retains **434 PDF pages**,
**468 units** and **1,239,281 characters**. All PDF page counts match independent
`pdfinfo` readback and nonempty PDF units have exact chunk page citations.
Native extraction statuses are **seven native-extracted and 35 review-required**;
accuracy is not certified and no OCR/model service was called.

The **601 chunks** include **166 eligible and 435 review-flagged**, covering all
**1,238,429 source-unit characters**. Another **852 extraction separators** are
accounted for separately. Current gaps remain explicit: **430 unacquired
references, 49 not-listed claims, one unavailable 1994 href and 35 review cases**.
Full historical acquisition and text/OCR accuracy review remain incomplete.

The latest captured calendar again contains the eight 2027 planning rows absent
from the T06 capture. Current inventory is **33 pages, 371 meeting claims,
470 references and 561 associations**. Both earlier snapshots and actual capture
clocks remain queryable; this observed source variation does not imply cancelled
or newly created meetings, and no document reference was removed.

## Readback, restore, replay and verification

Pre-/post-batch backups both restored all **29 owned tables**, matching exact row
fingerprints, all original readbacks, selected catalogs and operation audit.
Actual empty-target replay preserved all 108 capture receipts, selected inventory/
text/chunk generations, chunk text/eligibility and the latest 1991 operation's
intent/events/result with HTTP, extraction, tokenization and chunk generation
disabled. Full backup retains historical operation/catalog snapshots; selected-
generation replay does not claim to recreate all old snapshots. Disposable
restore/replay databases and replay root were removed; other namespaces remain
unchanged. Health, plan and latest-run recovery passed with HTTP disabled.

**954 regression tests passed with 71 optional skips; all 267 FOMC checks passed
with PostgreSQL enabled**, including 61 isolated backend checks. Bash syntax,
Ruff, formatting, diff, build and source-artifact checks passed. CI includes the
new script tests and existing isolated FOMC backend checks.

Next core operation: an explicit bounded **1992** batch through the same script,
followed by health/readback. All schedules remain stopped; embeddings are optional
and unselected, NAAIM remains paused and FINRA remains at SSV-T05.
