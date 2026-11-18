# Presidential research primary warehouse

The accepted root `data/presidential_research` is explicitly bound to PostgreSQL
schema `presidential_research`, effective role `presidential_writer`, in the
shared `additional_market_data` database. Ignored `postgres.local.yml` holds only
connection settings/environment references; `postgres.example.yml` documents them.
No inline secret is accepted. All normal workspace commands/Dagster operations
follow the root binding and fail closed if PG is unavailable. Raw archives and
frozen publications remain local evidence; the original SQLite is recovery only.

```bash
.venv/bin/python -m market_data_pipeline.pipelines.presidential_research migrate-postgres --workspace data/presidential_research --postgres-config config/presidential_research/postgres.local.yml
```

Migration is explicit, locked, transactional and retryable. Backup/restore builds
a verified separate SQLite recovery artifact from admitted captures; it never
copies stale recovery data as current primary state. The command activates no
provider access, job or schedule. See the [native contract](../../specs/spec016/postgres_primary_contract.md).
