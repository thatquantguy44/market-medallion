# FOMC implementation tasks and pickup

Updated: 2026-11-13. T01–T06, portable scripts, the Dagster job and configurable
Slack status alerts are implemented. [PR #18](https://github.com/thatquantguy44/market-medallion/pull/18)
merged the accepted 1995–1996 and 1997–2026 backfills. The next three slices are
packaged on `feat/fomc-text-quality`; [Q01 reporting is accepted](quality_report_acceptance.md),
with Q02/Q03 in progress.

## Current continuation

All 21 bounded 1997–2026 Dagster batches and final annual reconciliation passed.
Read [acceptance](1997_2026_acceptance.md), [contract](1997_2026_contract.md) and
[evidence](evidence/1997_2026_dagster_backfill.json) for the current coverage,
original/text/chunk checks, acknowledged Slack success and recovery gates.
Listed-reference archival coverage now extends from 1990 through released 2026
minutes as of November 13. No unacquired inventory references remain; source
availability and text/OCR quality limitations remain explicit.

Next: **FOMC-Q01 → Q02 → Q03**, defined in the [quality contract](quality_contract.md),
[plan](quality_plan.md) and [task checklist](quality_tasks.md). These cover offline
quality reporting, evidence-bound original-page review and confirmed extraction
fixes. Q01 is accepted; Q02/Q03 tasks remain open. FOMC-T07 keeps its separate optional
embeddings scope. Deliberately run a bounded recent refresh after the next official
release. Keep capture-time first-seen clocks,
observed corrections and finite limits. Schedules remain stopped; embeddings
remain independently selected, NAAIM paused and FINRA at SSV-T05.

## FOMC-T01 — normalized inventory contract (complete)

Traces FOMC-AC01. Strict manifest, reviewed URL boundary, frozen meeting/document
references and source-page association evidence are implemented. **61 offline
tests pass** with invented normalized current/historical candidates. This is
not a Federal Reserve JSON API and does not parse native HTML. Same-day sessions,
non-contiguous joint dates, bundled-document references, format variants,
overlapping inventories and conflicting source claims retain distinct evidence.
See [inventory contract](inventory_contract.md).

## FOMC-T02 — configurable PostgreSQL inventory catalog (complete)

Traces FOMC-AC11. Own config loader, example/ignored local config and PostgreSQL
adapter are implemented. **14 offline configuration tests and five real
PostgreSQL integration tests pass**. Verification covers exact metadata readback,
old/current snapshots, idempotent/concurrent retries, failed-transaction rollback,
corruption refusal, empty inventories and reinitialization. Tests use isolated
FOMC schemas; the production schema is initialized with zero inventory imports.
Configuration and operating commands: [config folder](../../config/fomc_meeting_minutes/README.md).
The [sanitized backend evidence](evidence/postgres_inventory.json) records actual
location/schema and verification scope. At T02, complete documents/text were not stored.

Full offline regression: **823 passed, 15 optional skips**. The five FOMC
PostgreSQL tests additionally passed against the existing service in isolated
schemas, for **80 focused checks** total. Ruff lint/format and build/artifact
checks pass. Local config remains ignored and owner-only.

## FOMC-T03 — native inventory adapter and immutable Bronze (complete for 1990 onward)

Accepted 2026-11-11 under the user-selected 1990-onward scope.
[Acceptance](t03_acceptance.md) is the gate/evidence map;
[closeout evidence](evidence/t03_closeout.json) records actual local state.

Implemented: native year/current discovery, strict source/URL/identity contracts,
run-scoped bounded HTTP, immutable accepted/rejected bytes, durable attempt and
recovery evidence, explicit content-addressed storage migrations, declared
addendum/PNG lineage, and atomic issue-gated native metadata publication.

The complete in-scope inventory census has **33 pages**, **371 meeting records**,
**470 document references** and **561 source associations**. Every source panel/
calendar row is accounted for. Fourteen explicit bundled-minutes cross-references
retain distinct meeting dates and their original source claims. Notation votes,
cancelled dates, transcripts/presentation bundles and other reviewed non-minutes
products remain explicit exclusions; unknown labels still block publication.
One December 30, 1994 conference-call minutes anchor has no href and remains
`unavailable`, without a guessed replacement or publication clock.

The configured operating floor is 1990. The old normalized manifest's provider
floor remains 1936 to preserve historical receipt hashes/IDs and offline legacy
reads. Old pre-1990 captures are retained, outside current acceptance. All local
migrations are applied. **72 captures** (38 inventories, 12 document
representations, 22 PNGs) reconcile to filesystem/PostgreSQL. The original five
receipts/bytes/clocks remain unchanged. Eight dependency checks retain all PNG
meeting associations. One native inventory run is committed; no text catalog,
full original-document backfill, job or schedule is claimed.

Both the pre-migration and post-closeout backups passed full restores in disposable
databases. The post-closeout restore verifies all 17 tables, 72 captures, eight
dependency checks, current pointer and native evidence. Other namespaces remain
unchanged. Raw provider data/backups/config stay ignored.

Final verification: **918 tests passed, 38 optional skips**; all **198 FOMC
checks passed with PostgreSQL enabled**, including 28 isolated backend checks.
Ruff lint/format, diff checks and package/artifact checks passed.

The historical [native sample](evidence/native_bronze_sample.json),
[year-index discovery](evidence/year_index_discovery.json),
[attachment sample](evidence/attachment_source_sample.json) and
[storage/recovery sample](evidence/storage_recovery_sample.json) remain earlier
checkpoints; the closeout evidence supersedes their operating status/counts.

## FOMC-T04 — complete text and extraction (complete for the archived sample)

Accepted 2026-11-11 on the same feature branch. [Acceptance](t04_acceptance.md),
[text contract](text_contract.md) and [evidence](evidence/t04_extraction.json)
record the native transform, immutable mirrors, coherent catalog and replay.
Optional `pypdf==7.0.1` and the existing reviewed HTML tree preserve complete
returned text, ordered page/section records, exact offsets and extraction profiles.
Source corrections/reversions and capture-time first-seen remain independent of
extractor changes. Missing text/images and failed containers have explicit outcomes.

All 12 existing document representations are extracted: 74 PDF pages, 108 units,
616,712 characters; seven native-extracted and five review-required. All 72 raw
captures and 17 pre-existing tables remain unchanged. The local migration adds
five text tables; all 22 tables passed restore verification. A real empty-target
replay matches the catalog and all 72 original receipt bytes without re-extraction
or network. Full backfill and OCR/text accuracy certification remain incomplete.

## FOMC-T05 — deterministic chunks (complete for committed text sample)

Accepted 2026-11-11 on the same feature branch. [Acceptance](t05_acceptance.md),
[chunks contract](chunks_contract.md) and [evidence](evidence/t05_chunks.json)
record exact offsets/citations, pinned tokenizer/profile, paragraph boundaries,
overlap, source-version identity and explicit per-unit eligibility. Four additive
owned PostgreSQL tables and immutable mirrors publish a coherent chunk generation
pinned to one text catalog. Missing/corrupt inputs, failed writes and a changed
text pointer cannot advance a successful generation. Replay uses stored artifacts
with HTTP/extraction/tokenization/chunking disabled.

The actual 12-set sample has 241 chunks: 166 eligible, 75 review-flagged. All
616,520 source-unit characters are covered, with 192 extraction separators
accounted for separately; full text remains 616,712 characters. All 74 PDF pages
have citations. All 72 originals, 22 existing tables and inventory/text pointers
remain unchanged. Pre-/post-migration backups restored 22/26 tables; actual replay
preserves the generation, row text/eligibility and original receipt bytes.
Full history and text/OCR accuracy review remain incomplete.

## FOMC-T06 — bounded acquisition and operations (complete for the 1990 batch)

Accepted 2026-11-12. [Acceptance](t06_acceptance.md),
[operations contract](operations_contract.md), [runbook](../../docs/deployment/fomc_runbook.md)
and [evidence](evidence/t06_operations.json) trace AC06 and retained source/text/
chunk guarantees. Source-owned finite bounds, retries, overlapping inventory
refresh, recursive dependencies, immutable operation audit/recovery, coherent
three-pointer promotion, CLI, health and stopped Dagster definitions are implemented.

The actual 1990 batch acquired 14 PDFs and reused two originals, with 17 data GETs
plus two probes. The archive has 89 captures; 26 representations retain 239 PDF
pages and 897,899 characters, with 406 chunks (166 eligible, 240 review-flagged).
Nineteen representation review cases and 446 unacquired references remain explicit.
A refreshed calendar removed eight 2027 placeholders; the current inventory has
363 meetings, 470 references and 553 associations, with earlier claims preserved.
Pre-/post-operation restores verify 26/29 tables. Actual offline replay preserves
all 89 receipts, selected catalogs, chunk eligibility and the operation audit.

Verification: **952 regression tests passed, 71 optional skips; 265 FOMC checks
passed with PostgreSQL enabled**, including 61 isolated backend checks. Lint,
format, build and artifact checks pass. No schedule was activated.

## Portable script and 1991 continuation (complete)

Accepted 2026-11-12. [Script contract](scripts_contract.md),
[acceptance](1991_acceptance.md) and [evidence](evidence/1991_script_backfill.json)
record the user-requested `scripts/run_fomc.sh` entrypoint, own-config setup,
interpreter/path handling and retained explicit live gates. The script delegates
to the source CLI and ships in source distributions.

The actual 1991 batch acquired 16 primary representations in 19 data GETs plus
two probes. The archive now has 108 captures, 42 representations, 434 PDF pages,
1,239,281 text characters and 601 chunks (166 eligible, 435 review-flagged).
Current inventory is 371 meetings / 470 references / 561 associations; the latest
calendar again includes eight 2027 planning rows absent from T06's capture.
All earlier claims, originals and clocks remain preserved. Both backups restore
29 tables, and offline replay/readback passes. Verification: 954 regression tests
with 71 optional skips; 267 FOMC checks with PostgreSQL, including 61 backend checks.

The bounded 1992 continuation is accepted below. The portable script and its
explicit access gates remain unchanged.

## Bounded 1992 continuation (complete)

Accepted 2026-11-12 on `feat/fomc-1992-backfill`, from merged PR #13.
[Acceptance](1992_acceptance.md) and [evidence](evidence/1992_script_backfill.json)
record the unchanged script runner and finite settings, 16 acquired primary
representations, 19 data GETs plus two probes, and 9,401,386 received bytes.
Inventory/text/chunk pointers advanced coherently. All 108 prior originals,
receipts/clocks and append-only rows remain intact; no schema/runtime change
was needed. Pre-/post-batch backups both restored 29 tables. Offline replay
preserves all 127 receipts, selected catalogs/chunk eligibility and the latest
operation audit, with HTTP/extraction/tokenization/chunking disabled.

The archive has 127 captures and 58 representations: 624 PDF pages, 658 units,
1,579,673 characters and 791 chunks (166 eligible, 625 review-flagged).
Current inventory is unchanged at 371 meeting claims, 470 references and 561
associations. Gaps remain explicit: 414 unacquired references, 49 not-listed
claims, one unavailable href and 51 review cases. Full history and text/OCR
accuracy remain incomplete. Verification: 954 regression tests with 71 optional
skips; 267 FOMC checks with PostgreSQL, including 61 backend checks.

The 1993–1994 extension is accepted below in the same PR; source kinds, clocks
and review flags remain preserved.

## Bounded 1993–1994 extension to PR #14 (complete)

Accepted 2026-11-12 on the existing `feat/fomc-1992-backfill` branch.
[Acceptance](1993_1994_acceptance.md) and [evidence](evidence/1993_1994_script_backfill.json)
record the added two-year run through the unchanged script and finite settings.
All 16 listed HTML minutes references are archived/verified: eight for each year,
with 15 acquired and one reused. Four inventories were refreshed; the run made
19 data GETs plus two probes and received 1,145,768 bytes. Eleven not-listed
claims and one unavailable 1994 href remain explicit.

The archive now has 146 captures and 73 representations, with 624 PDF pages,
673 units and 2,172,146 characters. There are 993 chunks (368 eligible and 625
review-flagged), adding 202 eligible chunks. Current normalized inventory is
unchanged at 371 meetings / 470 references / 561 associations. Gaps retain 399
unacquired references, 49 not-listed claims, one unavailable href and 51 review
cases. Both backups restored 29 tables, and offline replay/readback preserves
all 146 originals, selected catalogs/chunk eligibility and the latest operation
audit. All old 127 originals/clocks and append-only rows remain intact.

Verification: 954 regression tests with 71 optional skips; 267 FOMC checks with
PostgreSQL, including 61 backend checks. Full history and text/OCR accuracy remain
incomplete. **Next core operation: bounded 1995–1996** through the same script
with health/readback and recovery gates. Keep schedules stopped and embeddings
separately selected.

## Dagster job delivery (complete)

Accepted locally 2026-11-12 on `feat/fomc-dagster-job`, from merged PR #14.
The user narrowed deployment to a Dagster job. Reuse the registered FOMC job;
no new service or schedule activation is included. The
[contract](dagster_job_contract.md), [acceptance](dagster_job_acceptance.md) and
[runbook](../../docs/deployment/fomc_runbook.md#dagster) record execution-time
recent refresh, explicit historical bounds, post-run archive health, commit-aware
failure metadata and redacted exceptions. Launch examples default to closed live
gates. Source-owned HTTP retries and the advisory lock remain authoritative;
Dagster automatic run/op retries default to zero.

Verification: **991 regression tests passed, 80 optional skips; 313 FOMC checks
passed with PostgreSQL enabled**, including 70 backend checks. Root registration,
all six stopped schedules, lint/format and package checks passed. The unchanged
operational archive remains healthy at 146 captures and 993 chunks; no provider
request, migration or additional historical batch was performed. Alert and
off-machine backup destinations remain configurable future work. Next core
operation remains bounded 1995–1996.

## Later slices (not started)

- FOMC-T07: independently selected optional embeddings (FOMC-AC08–10). No model,
  paid service, vector extension or deployment activation is selected.

Before continuing read root AGENTS.md, README.md, docs/pipelines.md,
specs/spec001/README.md, docs/handoffs/project_bootstrap.md and this spec/contract.
Verify the local workflow library. QuantSmith's external checkout still differs
from its pin; adopted guidance was read from hash-verified pinned Git objects.
Keep NAAIM paused. FINRA remains at SSV-T05 for its next continuation.
