# FOMC meeting-minutes configuration

T01–T03 are accepted locally for the **1990-onward inventory / immutable-Bronze
foundation**. This folder owns the `fomc_meeting_minutes` schema and its operating
settings. [Acceptance](../../specs/spec017/t03_acceptance.md) and
[closeout evidence](../../specs/spec017/evidence/t03_closeout.json) record actual
migration, inventory, original-byte and restore state. T04 text/extraction is next.

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
checks. This is not a full acquisition of all declared documents or full text.

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
owned markers. Seventeen tables retain inventory metadata, immutable capture
receipts, content objects, links and dependency checks. Legacy per-capture
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

## Verification and next work

```bash
.venv/bin/python -m pytest -q tests/test_fomc_*.py
# Trusted project password environment loaded; unique test schemas only:
FOMC_TEST_POSTGRES_CONFIG=config/fomc_meeting_minutes/pipeline.local.yml \
  .venv/bin/python -m pytest -q tests/test_fomc_*.py
```

Keep `MARKET_DATA_TEST_POSTGRES_DSN` unset: other pipelines' legacy integration
tests require a separate disposable database. FOMC tests create/drop only their
unique test schemas and do not import invented fixtures into the operational
schema. Full document-history acquisition, text/page/OCR quality, catalog/replay,
chunks and embeddings remain later slices. No FOMC CLI/job/schedule is enabled.
