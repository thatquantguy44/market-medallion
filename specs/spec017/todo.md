# FOMC implementation tasks and pickup

Updated: 2026-11-11. The user selected implementation, reuse of this project's
PostgreSQL location and a pipeline-owned configuration folder. No recurring
schedule or embedding service was selected. Work on `feat/spec017-fomc-inventory`.

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

## FOMC-T05 — deterministic chunks (next)

Derive section/paragraph-aware chunks from committed text. Pin chunker/tokenizer
profiles, preserve exact Unicode offsets and page/section links, and prove full
eligible-text coverage, overlap and idempotence. Keep full text intact and expose
review/OCR eligibility per source unit. Mirror chunk metadata/text in FOMC-owned
PostgreSQL tables (FOMC-AC07/12). Use archived inputs without broader acquisition.

## Later slices (not started)

- FOMC-T06: bounded historical/current acquisition, health and stopped orchestration
  (FOMC-AC06), after local archival/replay evidence passes.
- FOMC-T07: independently selected optional embeddings (FOMC-AC08–10). No model,
  paid service, vector extension or deployment activation is selected.

Before continuing read root AGENTS.md, README.md, docs/pipelines.md,
specs/spec001/README.md, docs/handoffs/project_bootstrap.md and this spec/contract.
Verify the local workflow library. QuantSmith's external checkout still differs
from its pin; adopted guidance was read from hash-verified pinned Git objects.
Keep NAAIM paused. FINRA remains at SSV-T05 for its next continuation.
