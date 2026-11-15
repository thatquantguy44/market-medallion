# Development

The AAII slice is installable and executable. Spec 002 is the active contract;
the provider-specific CFTC commands in Spec 001 remain planned. Read the root
README for installation, the manifest, and the synthetic demo workflow.

Read the pinned workflow library before substantive engineering. The current
plan/tasks are under `tasks/`. Keep provider-specific pure transforms separate
from transport, persistence, and scheduler integrations. Dependencies are pinned
in `pyproject.toml`; `requirements-dev.lock` captures the tested Python 3.12
development environment, including optional PostgreSQL and Dagster dependencies.
For a minimal local install, use `pip install -e .`; add the `postgres` or
`dagster` extras only when needed. The lock targets POSIX development/CI.

```bash
.venv/bin/python scripts/verify_dagster_docs.py
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
```

## PostgreSQL integration

Tests skip PostgreSQL unless `MARKET_DATA_TEST_POSTGRES_DSN` is set. The test
expects a dedicated empty database, never deletes existing tables, and leaves
its test records for inspection. To repeat, create a new empty test database or
recreate only the disposable container you own. This does not use the persistent
Compose database from the deployment runbook.

```bash
docker run --detach --rm --name additional-market-data-aaii-test \
  --publish 127.0.0.1:55439:5432 \
  --env POSTGRES_DB=aaii_test --env POSTGRES_USER=aaii_test \
  --env POSTGRES_HOST_AUTH_METHOD=trust postgres:17.6-alpine
docker exec additional-market-data-aaii-test pg_isready -U aaii_test -d aaii_test
MARKET_DATA_TEST_POSTGRES_DSN=postgresql://aaii_test@127.0.0.1:55439/aaii_test \
  .venv/bin/python -m pytest -q tests/test_postgres.py
docker stop additional-market-data-aaii-test
```

The credentialless service above is a disposable localhost test fixture.
Persistent PostgreSQL configuration requires a password. The CI definition runs
lint, formatting, offline/Dagster tests, a PostgreSQL service test, and package
build on Python 3.12. Inspect GitHub Actions after pushing for the hosted result;
local verification does not establish that the hosted run passed.

## Platform verification limits

Dagster is tested with in-process fixture materialization and stopped schedule
definitions. That does not verify a deployed daemon/run launcher. The Delta
publisher has offline boundary checks; real MERGE behavior, Unity Catalog grants,
and cluster compatibility require the live acceptance steps in the runbook.
See [the verification record](verification.md) for the completed checks.

Verification for implementation must include offline unit tests, a local
Bronze/Silver/Gold/audit integration test, idempotent reruns, revision timing,
failure isolation, and Bronze replay. Live provider checks are separate,
explicitly requested, bounded smoke runs; never make unit tests require network.

The reserved directories mirror `fred-bronze-to-gold-pipeline`. No code, runtime
configuration, warehouse files, or credentials have been copied from it.

## Dagster inventory maintenance

[DAGSTER.md](../../DAGSTER.md) is the single job/schedule/sensor master guide.
Update it in the same change as definitions, launch/config paths or activation
controls. `scripts/verify_dagster_docs.py` compares its marked inventory tables
with the actual root composition without instance access or source requests.
CI runs the checker after installing the pinned optional Dagster dependencies.
README/AGENTS retain discoverable links; the old deployment register is a pointer.
