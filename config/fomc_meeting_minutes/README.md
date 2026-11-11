# FOMC meeting-minutes configuration

This pipeline owns this folder and the `fomc_meeting_minutes` PostgreSQL schema.
The pipeline validates inventory candidates, parses reviewed native HTML shapes,
and archives bounded source responses in this schema and on disk. Bounded historical-year discovery and expected-page publication checks are
implemented; source-wide completeness, full text, chunks, jobs and embeddings
remain open.

Copy `pipeline.example.yml` to ignored `pipeline.local.yml`. Paths resolve against
the configuration file's directory. The local configuration prepared for this
checkout points to the existing `additional_market_data` database on
`127.0.0.1:55438`, login `market_data`, with `role: null` and schema
`fomc_meeting_minutes`. It references the project's existing password environment
variable; the password value is never written into YAML. The example uses a
generic port and `FOMC_POSTGRES_PASSWORD` so other deployments can configure their
own location. No AAII configuration or tables are imported by the FOMC package.

`load_config` reads YAML without resolving passwords, connecting to PostgreSQL
or probing HTTP. Network settings use the shared parser and the run-scoped acquisition adapter. PostgreSQL connections resolve `password_env` only when
opening the store. Set a dedicated role if desired; do not inherit `aaii_writer`.
Schema names are restricted to `fomc_meeting_minutes` and its isolated test
namespaces, including for programmatic store construction.

With the trusted project `.env` loaded into the process environment, the current
programmatic entrypoint can initialize/inspect the inventory catalog:

```bash
set -a
source .env
set +a
.venv/bin/python - <<'PY'
from pathlib import Path
from market_data_pipeline.pipelines.fomc_meeting_minutes.config import load_config
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import PostgresInventoryStore

config = load_config(Path("config/fomc_meeting_minutes/pipeline.local.yml"))
with PostgresInventoryStore(config.postgres) as store:
    store.initialize()
    print({"inventory_runs": store.run_count(), "has_current_inventory": store.read_inventory() is not None})
PY
```

Current tables are `catalog_state`, `inventory_runs`, `inventory_pages`,
`inventory_meetings`, `inventory_documents` and `inventory_entries`. Imports retain
raw labels/hrefs, date-only release claims, meeting dates as `date[]`, UTC metadata
recording time, manifest/content hashes and an atomic current-inventory pointer.
A retried run ID with identical input is idempotent; changed input under the same
run ID refuses. Readback verifies the stored snapshot hash. These are metadata
import receipts, not provider document-content versions or source coverage proof.

T03 introduces a separate explicit `fomc-bronze-postgres/1` migration with
`bronze_schema_migrations` and `bronze_captures`. Captures retain original bytes
(`bytea`), source URL, UTC capture time, SHA-256 and safe response evidence;
unmigrated PostgreSQL still retains bytes per capture. The explicit object
migration below deduplicates originals while retaining every receipt and URL.
New filesystem payload paths hard-link verified hash-addressed originals.
`body_complete: false` marks a bounded prefix, whose hash is only the captured
prefix hash. Failed HTTP bodies are retained and labeled rejected. Local retries can call `commit_capture` when receipt and payload exist;
`recover_attempt` can repair a missing payload link from a verified object. The inventory
pointer is not moved by archiving. A separate `fomc-bronze-lineage-postgres/1` migration adds exact-URL links to
existing inventory/document identities; these caller-asserted links do not
certify how the inventory was derived. Complete text, document versions, chunks
are not implemented. Publication verifies matching filesystem/database captures,
rejects parser issues, atomically links each page, and refuses to drop source
pages from the prior snapshot; it does not certify source-wide coverage. pgvector is optional.

`acquisition.example.yml` supplies explicit per-run request and byte limits.
Load it with `load_acquisition_limits`, use `AcquisitionRun.start` with the configured root, manifest, store, startup
inventory URL, network settings and limits, then reuse that run for selected URLs.
Startup intent/rejection evidence is persisted before/after route selection;
`prepare_fomc_http` remains the lower-level selector without archival context. The startup
HEAD selects a route once; GETs refuse redirects and each attempted request
consumes the budget. Loading configuration and local archive reads make no HTTP
request. See the [T03 handoff](../../specs/spec017/todo.md#fomc-t03--native-inventory-adapter-and-immutable-bronze-in-progress)
before using the programmatic APIs; no CLI or Dagster job exists yet.

Real PostgreSQL verification uses unique FOMC test schemas and removes only those
schemas on teardown:

```bash
FOMC_TEST_POSTGRES_CONFIG=config/fomc_meeting_minutes/pipeline.local.yml \
  .venv/bin/python -m pytest -q tests/test_fomc_postgres.py tests/test_fomc_archive_postgres.py
```

Keep `MARKET_DATA_TEST_POSTGRES_DSN` unset for this command: other pipelines'
legacy tests require a dedicated disposable database. The FOMC-specific test
configuration is designed for isolated schemas on this existing service.
Ordinary offline tests leave these integration tests skipped. Do not import
invented fixtures into the production FOMC schema; current production inventory
tables remain empty. Five bounded source captures are stored in Bronze; they do
not constitute a published inventory or source-wide archive.

## Bounded historical-year discovery

Use `data.discovery.parse_historical_index` and `discover_inventory_pages` on
archived source bytes to define the expected set before acquisition/publication.
The latter requires explicit `start_year`/`end_year`, includes both source
inventories and follows only discovered official year-page links. Acquisition
still uses the existing per-run request/byte budgets and one selected client.

`publish_inventory_captures(..., historical_years=(start,end))` re-derives that
set from matching committed index/calendar captures, rejects source issues and
missing/extra pages, and persists hashed selected-page evidence with all links
and the current pointer in one transaction. `read_inventory_discovery(run_id)`
verifies that evidence on readback. The separate explicit
`fomc-bronze-discovery-postgres/1` migration adds `bronze_inventory_discovery`;
verification has used only isolated test schemas, leaving production unchanged.
Legacy publication without bounds retains no discovery coverage claim.

See the [discovery contract](../../specs/spec017/discovery_contract.md) for limits,
rollback, API examples and the known source issues. The new bounded index request
is local review evidence only. Production remains at five Bronze captures and
zero inventory runs; this increment performs no year-page backfill.

## Explicit original/dependency migrations and recovery

After `bronze.migrate()`, `PostgresObjectStore(bronze).migrate()` and
`PostgresDependencyStore(bronze).migrate()` apply separate, owned schema contracts.
They have been verified only in isolated FOMC schemas and have not been applied
to production. Do not run older writers during object migration. The object
migration verifies every legacy byte/hash/clock before clearing redundant copies;
dependency checks keep all original parent/child labels and meeting associations.

`migrate_local_objects(root)` explicitly relinks a verified legacy local batch to
shared originals without changing receipts or capture clocks. New captures use
content addressing automatically. After a writer stops, `recover_attempt(root,
capture_id, bronze)` retries durable evidence under the same identity; missing
original bytes remain incomplete and no request is made. See the
[storage/recovery contract](../../specs/spec017/storage_recovery_contract.md)
and [attachment contract](../../specs/spec017/attachments_contract.md) for APIs,
limits, rollback and the remaining source exceptions.
