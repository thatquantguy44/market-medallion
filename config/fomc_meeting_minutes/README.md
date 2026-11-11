# FOMC meeting-minutes configuration

This pipeline owns this folder and the `fomc_meeting_minutes` PostgreSQL schema.
The current executable slice validates normalized inventory candidates and stores
inventory metadata snapshots. Native HTML discovery, original-document archival,
full text, chunks and embeddings are not implemented yet.

Copy `pipeline.example.yml` to ignored `pipeline.local.yml`. Paths resolve against
the configuration file's directory. The local configuration prepared for this
checkout points to the existing `additional_market_data` database on
`127.0.0.1:55438`, login `market_data`, with `role: null` and schema
`fomc_meeting_minutes`. It references the project's existing password environment
variable; the password value is never written into YAML. The example uses a
generic port and `FOMC_POSTGRES_PASSWORD` so other deployments can configure their
own location. No AAII configuration or tables are imported by the FOMC package.

`load_config` reads YAML without resolving passwords, connecting to PostgreSQL
or probing HTTP. Network settings use the shared parser; they will be consumed
by a later HTTP adapter. PostgreSQL connections resolve `password_env` only when
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

Only the future archival/text slices will add original bytes (`bytea`), complete
text (`text`), document versions and chunks to this schema, preserving filesystem
originals as well. pgvector remains an optional later enhancement.

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
tables were initialized empty. No source document has been acquired or promoted.
