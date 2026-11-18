# OTC PostgreSQL primary

`postgres.example.yml` documents the storage target and environment-only password.
The ignored `postgres.local.yml` selects the operative writer/schema. The bound
root is `data/postgres-primary/finra_otc_transparency`; it contains zero source
observations. This does not admit real OTC access or invent fixture production data.

```bash
.venv/bin/python -m market_data_pipeline.pipelines.finra_otc_transparency init-postgres --root data/postgres-primary/finra_otc_transparency --postgres-config config/finra_otc_transparency/postgres.local.yml
```

Fixture stores can explicitly use `migrate-postgres --invented-fixture` with the
same two arguments after the existing v1-to-v2 migration. Bound roots always use
PostgreSQL; the original SQLite file is retained only for recovery. Missing PG
fails closed. This command does not activate jobs or schedules.
