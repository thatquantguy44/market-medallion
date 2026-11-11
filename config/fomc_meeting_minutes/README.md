# FOMC meeting-minutes configuration

This pipeline owns this folder and the `fomc_meeting_minutes` PostgreSQL schema.
The pipeline validates inventory candidates, parses reviewed native HTML shapes,
and archives bounded source responses in this schema and on disk. Year-index completeness, full text, chunks, jobs and embeddings remain open.

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
bytes are currently retained per capture rather than deduplicated by hash;
the same received bytes and receipt are written beneath the configured root.
`body_complete: false` marks a bounded prefix, whose hash is only the captured
prefix hash. Failed HTTP bodies are retained and labeled rejected. Local retries
can call `commit_capture` when both receipt and payload exist. The inventory
pointer is not moved by archiving. A separate `fomc-bronze-lineage-postgres/1` migration adds exact-URL links to
existing inventory/document identities; these caller-asserted links do not
certify how the inventory was derived. Complete text, document versions, chunks
are not implemented. Publication verifies matching filesystem/database captures,
rejects parser issues, atomically links each page, and refuses to drop source
pages from the prior snapshot; it does not certify source-wide coverage. pgvector is optional.

`acquisition.example.yml` supplies explicit per-run request and byte limits.
Load it with `load_acquisition_limits`, construct `AcquisitionRun` with a client
from `prepare_fomc_http`, and reuse that run for the selected URLs. The startup
HEAD selects a route once; GETs refuse redirects and each attempted request
consumes the budget. Loading configuration and local archive reads make no HTTP
request. See the [T03 handoff](../../specs/spec017/todo.md#fomc-t03--native-inventory-adapter-and-immutable-bronze-in-progress)
before using the programmatic APIs; no CLI or Dagster job exists yet.

Real PostgreSQL verification uses unique FOMC test schemas and removes only those
schemas on teardown:

```bash
FOMC_TEST_POSTGRES_CONFIG=config/fomc_meeting_minutes/pipeline.local.yml \
  .venv/bin/python -m pytest -q tests/test_fomc_postgres.py
```

Keep `MARKET_DATA_TEST_POSTGRES_DSN` unset for this command: other pipelines'
legacy tests require a dedicated disposable database. The FOMC-specific test
configuration is designed for isolated schemas on this existing service.
Ordinary offline tests leave these integration tests skipped. Do not import
invented fixtures into the production FOMC schema; current production inventory
tables remain empty. Five bounded source captures are stored in Bronze; they do
not constitute a published inventory or source-wide archive.
