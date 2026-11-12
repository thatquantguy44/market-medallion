# FOMC 1993–1994 bounded backfill acceptance

Accepted locally on 2026-11-12, extending **PR #14** on its existing
`feat/fomc-1992-backfill` branch. The added scope is 1993–1994 through the unchanged
[`scripts/run_fomc.sh`](../../scripts/run_fomc.sh) and [operations contract](operations_contract.md).
[Evidence](evidence/1993_1994_script_backfill.json) records actual acquisition,
coverage and recovery. [1992 acceptance](1992_acceptance.md) remains the earlier
checkpoint in the same PR; this continuation does not start another branch/PR.

## Added scope and execution

The no-request plan listed 16 primary HTML references across two selected years,
within the existing `max_years: 2` setting. A fresh pre-run backup restored all
29 owned tables, with exact row fingerprints, all 127 prior originals, selected
catalogs and operation audit. The script then ran explicit `backfill --start-year
1993 --end-year 1994 --live` with unchanged finite settings and the 1990 floor.

The run **acquired 15 HTML documents and reused one verified original**, covering
all 16 listed primary references. Four inventory pages were refreshed and 29
verified nonselected pages were carried with their original capture clocks.
There were **19 data GETs plus two startup probes**, receiving **1,145,768
data-response bytes**. No content assets or additional document dependencies
were acquired. All three pointers and the successful operation result committed
together. All 127 previous original bytes/receipts/clocks and pre-existing
append-only rows remain unchanged. No runtime/config/schema/dependency change
was needed.

| Year | Listed primary references | Archived/verified | Source document kind | Other source outcomes |
| --- | --- | --- | --- | --- |
| 1993 | 8 | 8 | `minutes` (HTML) | 8 not-listed claims |
| 1994 | 8 | 8 | `minutes` (HTML) | 3 not-listed claims; 1 unavailable href |

This verifies the transition into modern minutes while preserving the distinct
action/policy predecessors already acquired for 1990–1992. The December 30, 1994
unavailable conference-call anchor remains explicit. No replacement URL or text
was guessed, and no publication/first-seen clock was inferred or backdated.

## Current catalog and coverage

The archive now has **146 captures**: 51 inventories, 73 document representations
and 22 PNGs. The current normalized inventory is unchanged: **33 pages,
371 meeting claims, 470 references and 561 associations**. Prior snapshots and
actual capture clocks remain queryable.

Native text contains **73 representations, 624 PDF pages, 673 units and
2,172,146 characters**, with **22 native-extracted and 51 review-required
representations**. PDF page counts/citations remain verified; the new HTML
minutes have source/unit/character citations without invented PDF pages.

There are **993 chunks: 368 eligible and 625 review-flagged**, an increase of
**202 eligible chunks** over the 1992 checkpoint. Exact spans cover **2,170,946
source-unit characters**, with **1,200 extraction separators** accounted for
separately. Remaining gaps are explicit: **399 unacquired references, 49
not-listed claims, one unavailable 1994 href and 51 review cases**. Eligibility
follows the pinned core profile; full history and text/OCR accuracy remain
incomplete. No OCR or model service was called.

## Restore, replay and checks

The post-run backup restored all **29 tables**, matching table fingerprints,
all 146 originals, selected catalogs and operation audit. Actual empty-target
replay reproduced all 146 original receipt bytes, the selected inventory/text/
chunk generations, exact chunk text/eligibility and the latest 1993–1994 operation
intent/events/result. HTTP, extraction, tokenization and chunk generation were
disabled during replay. Full backup retains historical catalog/operation snapshots;
selected-generation replay does not claim to recreate every earlier snapshot.

Health, plan and run recovery also passed with HTTP disabled. Disposable restore/
replay databases and replay root were removed, and other namespaces remain
unchanged. Local configuration, credentials, source bytes, derivatives and backups
remain ignored. All schedules stay stopped; embeddings remain unselected,
NAAIM stays paused and FINRA stays at SSV-T05.

**954 regression tests passed with 71 optional skips; all 267 FOMC checks passed
with PostgreSQL enabled**, including 61 isolated backend checks. Bash syntax,
Ruff lint/format, diff, build, local-link and source-artifact checks passed.

Next core operation: another explicit bounded **1995–1996** batch through the
same script, with the same recovery gates and preserved review/source gaps.
