# FOMC 1995–1996 bounded backfill acceptance

Accepted locally 2026-11-13 on `feat/fomc-1995-1996-backfill`, from merged
PR #17. The [contract](1995_1996_contract.md) and
[sanitized evidence](evidence/1995_1996_dagster_backfill.json) record acquisition,
publication, Slack delivery and recovery through the existing Dagster job.
The runtime, schema, dependencies, finite budgets and source semantics are unchanged.

## Acceptance gate checklist

All requested gates are **passed and recorded on PR #18** before any 1997–1998
batch. A fresh read-only review on 2026-11-13 is recorded under
`acceptance_gate_review` in the [evidence](evidence/1995_1996_dagster_backfill.json).

| Gate | Status | Verified result / evidence |
| --- | --- | --- |
| Originals | Passed | All 172 capture readbacks match database/filesystem; all 152 prior receipt bytes and clocks remain unchanged. |
| Native text | Passed | All 89 representations read back; 2,771,872 full-text characters reconcile to source units plus separators; prior PDF page counts/citations remain verified. |
| Chunks | Passed | All 1,193 chunks retain exact source spans; coverage and eligibility match the accepted catalog (568 eligible, 625 review-flagged). |
| Archive health | Passed | Fresh health is `healthy`, issues are empty and all 29 current tables match the verified post-batch restore fingerprints; selected pointers and latest operation are unchanged. |
| Slack success | Passed | Dagster run `12d67c3f-4aab-41a1-bdb1-83a5c0532d9c` is SUCCESS; tick 147 is SUCCESS for that run with the acknowledged-delivery log present. |
| Backup and restore | Passed | Actual before/after restores verified 29 tables and 1,337/1,546 canonical files; all four retained archive checksums and restore receipts were reconfirmed. |
| Offline replay | Passed | Actual empty-target batch verification reproduced all 172 originals/receipts, selected catalogs/chunk rows and latest operation audit with HTTP and transforms disabled. |
| Acceptance boundary | Recorded | 1995–1996 is accepted for its listed references. No 1997–1998 batch was launched by this review. |

The restore/replay executions were completed during batch verification and their
retained evidence was reconfirmed here; they were not repeated during this read-only
review. It made zero provider HTTP requests and no source mutations. Existing
review flags and full-history/text-accuracy limitations remain explicit below.

## Execution and source coverage

The no-request plan selected 16 primary references. A fresh pre-run schema backup
restored all 29 tables with exact row fingerprints, selected catalogs, operation
audit and all 152 prior captures. The canonical-files backup restored **1,337
files** with exact hashes. Existing 1990 reruns had added six inventory captures
since the earlier 146-capture historical checkpoint; this batch starts at 152.

Dagster run `12d67c3f-4aab-41a1-bdb1-83a5c0532d9c` completed the manual live
1995–1996 backfill. Source operation `db6830d0-2415-46ff-8279-6ed9351e6c17`
**acquired all 16 primary HTML minutes**, eight per year, and refreshed four
inventories while carrying 29 verified nonselected pages with their old clocks.
It made **20 data GETs plus two startup probes**, receiving **1,155,323 bytes**.
No additional asset/dependency acquisition was required. The complete result and
all three catalog pointers committed together; archive health/readback passed.

| Year | Listed primary references | Archived/verified | Document kind | Other source outcomes |
| --- | --- | --- | --- | --- |
| 1995 | 8 | 8 | `minutes` (HTML) | 3 not-listed claims |
| 1996 | 8 | 8 | `minutes` (HTML) | None |

All 16 new representations are native-extracted. No source URL, publication clock
or replacement for an unavailable reference was guessed. Actual capture-time
first-seen semantics are preserved; no OCR/model call or backdating was performed.
Slack success-sensor **tick 147** was SUCCESS for this exact Dagster run and logged
acknowledgement-backed delivery. Both FOMC status sensors remain enabled; all six
acquisition/import schedules and the other eight status sensors remain stopped.

## Current archive and preserved history

There are **172 captures: 61 inventories, 89 documents and 22 PNG assets**.
Current normalized inventory is unchanged at **33 pages, 371 meeting claims,
470 references and 561 associations**. Earlier source snapshots remain queryable.

Native text contains **89 representations, 624 PDF pages, 689 units and
2,771,872 characters**. Outcomes are **38 native-extracted and 51 review-required**.
The new HTML minutes add **599,726 characters and 200 eligible chunks**.
All previously archived PDF page counts/citations still match independent readback.

There are **1,193 chunks: 568 eligible and 625 review-flagged**. Exact spans cover
**2,770,672 source-unit characters**, with **1,200 extraction separators** accounted
for separately. Remaining gaps are **383 unacquired references, 49 not-listed
claims, one unavailable 1994 href and 51 review cases**. Full historical coverage
and text/OCR accuracy certification remain incomplete.

All **152 prior originals, receipt bytes, capture clocks and append-only rows**
remain unchanged. The active source products, 1990 floor and review eligibility
rules are preserved. No operational table migration or runtime change was needed.

## Restore, replay and regression

The post-run schema backup restored all **29 tables**, with exact row fingerprints,
all **172 capture readbacks**, selected catalogs and operation audit. The canonical
filesystem backup restored **1,546 files** with exact hashes. Ignored schema and
filesystem archives and their verification receipts are retained locally.

Actual empty-target replay reproduced all 172 original receipt bytes, selected
inventory/text/chunk generations, exact chunk text/eligibility and the latest
1995–1996 operation intent/events/result. HTTP, extraction, tokenization and chunk
generation were disabled. Full backup retains historical catalog/operation
snapshots; selected-generation replay does not recreate every earlier generation.
Health, plan and run recovery also passed with HTTP disabled. Disposable databases
and restore/replay roots were removed; other operational namespaces are unchanged.

Verification: **1,050 regression tests passed, 80 optional skips; all 313 FOMC
checks passed with isolated PostgreSQL enabled**, including 70 backend checks.
Bash syntax, Ruff lint/format, diff, package builds/artifact contents and local
links pass. The workflow snapshot and all 61 adopted QuantSmith Git objects were
verified; external working-tree drift was left unchanged. Credentials, local
configuration, raw payloads, derivatives and backups remain ignored.

Next core operation: a separately bounded **1997–1998** batch through the same job
or script, followed by the same health, backup/restore and offline replay gates.
Embeddings remain independently selected; NAAIM stays paused and FINRA at SSV-T05.
