# FOMC meeting-minutes configuration

T01–T05 are accepted locally for the **1990-onward bounded archive, native-text
and chunk foundation**. This folder owns the FOMC operating settings.
[T05 acceptance](../../specs/spec017/t05_acceptance.md),
[chunks contract](../../specs/spec017/chunks_contract.md) and
[evidence](../../specs/spec017/evidence/t05_chunks.json) record actual state.
T06 bounded acquisition/operations is next. No FOMC job/schedule is enabled.

## Configuration and scope

Copy `pipeline.example.yml` to ignored `pipeline.local.yml`. Paths resolve against
the YAML directory. `start_year` defaults to 1990, accepts later years, and rejects
pre-1990 operating scope before a startup probe/year-page request. The old
normalized manifest's 1936 provider boundary remains unchanged for historical
receipt/ID compatibility and local legacy reads; it is not the operating scope.

The local file uses the existing `additional_market_data` database on
`127.0.0.1:55438`, login `market_data`, `role: null` and the dedicated FOMC schema.
It references the existing password environment-variable name. Password values
never belong in YAML, logs or Git. The example keeps generic port 55432 and
`FOMC_POSTGRES_PASSWORD`. No other pipeline's configuration/tables are imported.
Schema names are restricted to FOMC and isolated FOMC test namespaces.

`load_config` reads settings without resolving passwords, probing HTTP or opening
a database. Connections resolve the named password only when the store opens.
Network settings use the shared run-scoped direct/proxy contract.

## Current local state and inspection

There is one native metadata snapshot: 33 official source pages, 371 meeting
records, 470 declared document references and 561 source associations. One empty
official minutes href is explicitly unavailable. The archive holds 72 captures
(38 inventories, 12 document representations and 22 PNGs), with eight dependency
checks. T04 extracts all 12 archived document representations into 74 PDF pages /
108 text units (616,712 characters): seven native-extracted, five review-required.
The text catalog records the bounded selection and outstanding archive/review gaps.
T05 adds 12 chunk sets: 241 chunks (166 eligible, 75 review-flagged), with exact
source-unit coverage and all 74 PDF page citations. Full acquisition of all
declared documents and accuracy review remain incomplete.

With this project's trusted `.env` loaded, inspect without provider requests:

```bash
set -a
source .env
set +a
.venv/bin/python - <<'PY'
from pathlib import Path
from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore

cfg = load_config(Path("config/fomc_meeting_minutes/pipeline.local.yml"))
with PostgresInventoryStore(cfg.postgres) as store:
    inventory = store.read_inventory()
    print({"start_year": cfg.start_year, "inventory_runs": store.run_count(),
           "meetings": len(inventory["meetings"]) if inventory else 0})
PY
```

## Acquisition and publication APIs

`acquisition.example.yml` supplies finite request/byte budgets. Use
`AcquisitionRun.start(root, manifest=..., store=..., source_url=...,
settings=cfg.network, limits=..., start_year=cfg.start_year)` to persist startup
intent before selecting/probing one route. Reuse that run for selected URLs.
GETs refuse redirects; every attempted GET consumes the budget. Startup failures
retain sanitized rejected evidence. Injected clients can construct a run directly
for offline checks. Local reads/recovery/exports do not probe.

`discover_inventory_pages` defines an explicit expected page set from archived
index/calendar bytes and selected years. `publish_inventory_captures(...,
historical_years=(cfg.start_year, source_end_year), start_year=cfg.start_year)`
requires matching committed filesystem/database originals, rejects issues or
missing/extra pages, retains raw cross-reference/exclusion evidence and commits
the metadata pointer atomically. `read_inventory_discovery(run_id)` verifies
that evidence. Legacy metadata unions retain no source-wide discovery claim.

## Applied migrations, original objects and recovery

`bronze.migrate()`, `PostgresObjectStore(bronze).migrate()` and
`PostgresDependencyStore(bronze).migrate()` are applied locally under separate
owned markers. These 17 Bronze/inventory tables retain immutable originals and
lineage. T04 adds five text/catalog tables; T05 adds four chunk tables, for
26 total. All 22 pre-T05 tables remain unchanged. Legacy per-capture
payload columns are cleared only after verified object migration. Source URLs,
UUIDs, receipt bytes and observed clocks remain unchanged.

New local payload paths hard-link hash-addressed originals.
`migrate_local_objects(root)` explicitly relinks a verified legacy batch. After
the writer stops, `recover_attempt(root, capture_id, bronze)` retries durable
bytes under the same identity, repairs a missing payload link, or reports missing
bytes. It never guesses a successful response or refetches. Rejected/partial
captures cannot publish success. See [storage/recovery](../../specs/spec017/storage_recovery_contract.md),
[attachments](../../specs/spec017/attachments_contract.md) and
[discovery](../../specs/spec017/discovery_contract.md) for exact contracts/rollback.

Both schema backups in the closeout evidence have been restored and checked in
disposable databases. The final restore verifies all captures, tables, dependency
checks, current pointer and native evidence. Backups and provider bytes stay
ignored under the configured root. Do not remove populated migration tables/
markers or run older writers against the migrated schema.

## Text extraction, catalog inspection and replay

Install the optional pinned engine with `.[fomc_text,postgres]` (the development
lockfile already includes it). `PostgresTextStore(bronze).migrate()` explicitly
adds `text_extractions`, `text_units`, `text_capture_links`, `text_catalogs` and
`text_state` under the `fomc-text-postgres/1` marker. The reviewed local migration
is applied. All 17 pre-existing tables and original capture bytes/clocks remain
unchanged. Fresh backups before/after migration passed 17-/22-table restores.

`text.extract_capture(capture_id, manifest, root=cfg.root)` verifies original and
dependency mirrors, derives complete native text and saves immutable artifacts
under `text/extractions/<extraction_id>.json` plus PostgreSQL full-text/unit rows.
`text.read_extraction(id, root=...)` checks profile/text hashes, unit offsets and
both mirrors. Native text, image/OCR review and failed extraction are distinct.
Originals stay canonical; full text is never silently truncated.

`PostgresTextCatalog(text).publish(bindings, manifest, root=..., inventory_run_id=...,
start_year=cfg.start_year)` takes explicit capture/extraction bindings. It verifies
all mirrors/associations and atomically publishes a bounded generation. Failed
extractions/writes leave the old pointer intact; existing local artifacts support
retry. Retrying an older generation does not roll the pointer backward. Local
artifacts are immutable; PostgreSQL's `text_state.current_catalog_id` selects the
committed generation. Readers never glob artifact directories.

With this project's trusted `.env` loaded, inspect the selected catalog without
provider requests or schema changes:

```python
from pathlib import Path
from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.catalog import PostgresTextCatalog

cfg = load_config(Path("config/fomc_meeting_minutes/pipeline.local.yml"))
manifest = load_manifest(cfg.manifest)
with PostgresInventoryStore(cfg.postgres) as store:
    catalog = PostgresTextCatalog(PostgresTextStore(PostgresBronzeStore(store)))
    value = catalog.read(manifest, root=cfg.root)
    print({"catalog_id": value["catalog_id"],
           "representations": len(value["representations"]), "gaps": len(value["gaps"])})
```

`replay_text_catalog(source_catalog, target_catalog, manifest, source_root=...,
target_root=...)` requires an independent initialized empty target schema/database
and root. It copies verified originals and stored derivatives, preserving capture
IDs/receipt bytes, source clocks, versions and catalog hash, without re-extraction
or network. Use the isolated integration test as the setup example. The target
is selected explicitly; replay never resets a populated destination. An interrupted
replay leaves partial target evidence for review and does not publish a successful
catalog. Resume support for such a partial target is not implied by empty-target
replay; use a fresh disposable target after preserving failure evidence.

Rollback uses the earlier code/catalog by explicit historical catalog ID, keeping
immutable artifacts/tables. Do not drop populated text/lineage tables or run old
writers against incompatible state. Cutoffs are document-capture cutoffs against
the selected metadata snapshot; they do not reconstruct earlier inventory knowledge.

## Chunk generations, eligibility and replay

`PostgresChunkStore(text).migrate()` explicitly adds `chunk_sets`, `chunks`,
`chunk_catalogs` and `chunk_state` under `fomc-chunks-postgres/1`. The reviewed
local migration is applied. Fresh T05 backups restore all 22 pre-migration and
26 post-migration tables exactly, with original/text state preserved.

`PostgresChunkCatalog(chunks).publish(manifest, root=cfg.root)` reads a verified
committed text catalog and materializes one selected extraction per observed
version. Defaults use a pinned core Unicode tokenizer, a 650-token maximum and
75-token overlap, preferring paragraph/line boundaries after 500 tokens. Python/
Unicode versions, exact text spans/hashes, citations and eligibility reasons are
stored. Full text remains unchanged; coverage merges overlapping spans and accounts
for extraction separators separately. Future embedding profiles define their own
model-specific token limits.

Immutable mirrors live at `text/chunks/<set_id>.json` and
`text/chunk_catalogs/<catalog_id>.json`. PostgreSQL's
`chunk_state.current_catalog_id` selects one coherent generation. Failed writes
retain reusable artifacts and preserve that pointer. Default publication rejects
a text-pointer change during building; explicit `text_catalog_id` can pin a
historical input. Retrying an old generation cannot move the pointer backward.
Multiple extraction profiles require explicit `selections={document_version_id:
extraction_id}` for every observed version, preventing implicit profile mixing.

With `cfg` and `manifest` loaded as above, inspect without migration, transform
or provider requests:

```python
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunk_catalog import PostgresChunkCatalog

with PostgresInventoryStore(cfg.postgres) as store:
    text = PostgresTextStore(PostgresBronzeStore(store))
    catalog = PostgresChunkCatalog(PostgresChunkStore(text))
    value = catalog.read(manifest, root=cfg.root)
    eligible = catalog.read_chunks(manifest, root=cfg.root)
    print({"catalog_id": value["catalog_id"], "coverage": value["coverage"],
           "eligible_current_chunks": len(eligible)})
```

`read_chunks` defaults to `eligible_only=True, current_only=True`. Explicit false
values expose review text or prior observed versions with source/capture/citation
context. Reads verify original/text/chunk mirrors and the pinned generation.
Five upstream representation review cases remain; 75 chunks are review-flagged.
Eligibility is a native-unit policy and does not certify text/OCR accuracy.

`replay_chunk_catalog(source_catalog, target_catalog, manifest, source_root=...,
target_root=...)` requires an independent empty target with owned Bronze, text and
chunk migrations initialized. It reuses stored artifacts, retaining all original
receipt bytes, source clocks, version/set/chunk/generation IDs and eligibility,
with HTTP/extraction/tokenization/chunking disabled. It rejects populated targets;
retain partial failure evidence and use a fresh disposable target after review.
Read historical generations by explicit ID for rollback; keep immutable populated
tables/artifacts. No automatic deletion, job or schedule is added.

## Verification and next work

```bash
.venv/bin/python -m pytest -q tests/test_fomc_*.py
# Trusted project password environment loaded; unique test schemas only:
FOMC_TEST_POSTGRES_CONFIG=config/fomc_meeting_minutes/pipeline.local.yml \
  .venv/bin/python -m pytest -q tests/test_fomc_*.py
```

Keep `MARKET_DATA_TEST_POSTGRES_DSN` unset: other pipelines' legacy integration
tests require a separate disposable database. FOMC tests create/drop only their
unique test schemas. Next is T06 bounded acquisition, health and stopped orchestration. Full original-history acquisition and OCR/text accuracy review remain
outstanding; embeddings stay separately selected. No FOMC CLI/job/schedule is enabled.
