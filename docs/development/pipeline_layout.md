# Pipeline package layout and import migration

[DAGSTER.md](../../DAGSTER.md) is the canonical inventory of composed jobs,
schedules/sensors and their maintenance checks.

The user requested one folder per pipeline under `src/market_data_pipeline/pipelines/`.
AAII provides the initial example below; each implemented source owns its own
package, and future sources get one when implementation is authorized. The [master register](../pipelines.md)
tracks actual packages and all planned/draft/candidate pipelines.

```text
src/market_data_pipeline/
  __init__.py
  __main__.py                      # existing repository module command
  cli.py                           # forwards existing commands to AAII
  orchestration/
    dagster_defs.py                 # existing repository Dagster entrypoint
  shared/
    files.py                       # atomic JSON, UTC timestamps, writer lock
    yaml.py                        # strict duplicate-key YAML loader
  pipelines/
    __init__.py
    aaii/
      __init__.py
      __main__.py                     # python -m market_data_pipeline.pipelines.aaii
      cli.py
      config.py                       # AAII runtime/operations config and connection settings
      manifest.py                     # AAII CSV/XLS schema and access contract
      source.py                       # explicit bounded AAII HTTP acquisition
      pipeline.py
      backfill.py
      replay.py
      operations.py
      recovery.py
      dagster_defs.py                  # AAII asset, job and stopped schedules
      data/
        csv.py
        xls.py
        gold.py
      io/
        bronze.py
        warehouse.py                  # AAII tables and date-basis contract
        parquet.py                    # AAII Silver/Gold/audit publication
        databricks.py                  # AAII Delta publishing
```

The current warehouse and publisher code contains AAII's columns, dataset IDs
and semantics. It is pipeline-owned even where it uses generic SQL/Parquet APIs.
Do not use it as an FX or CFTC schema, or import one pipeline's internals from
another pipeline. Extract another common helper only when the real contracts
match; new providers do not require a generic framework first.

The pure transforms remain independent of network, storage and scheduler work.
`shared/` has no imports from a pipeline and no AAII schema/access defaults.
Root CLI forwarding preserves current commands. When another pipeline actually
exists, extend the root dispatcher and aggregate its Dagster definitions without
changing the legacy AAII command meaning. There are no fake command routes for
planned pipelines.

## Existing commands and persisted data

Both forms invoke the same AAII CLI:

```bash
python -m market_data_pipeline run --input tests/fixtures/aaii/sentiment.csv --root data/aaii-demo
python -m market_data_pipeline.pipelines.aaii run --input tests/fixtures/aaii/sentiment.csv --root data/aaii-demo
```

Installed `market-data` and `market-medallion` commands remain valid. The existing
Dagster module `market_data_pipeline.orchestration.dagster_defs` composes implemented
pipeline-owned definitions. AAII owns its definitions in
`market_data_pipeline.pipelines.aaii.dagster_defs`; other pipelines follow the same
pattern. [DAGSTER.md](../../DAGSTER.md) owns the current inventory and default states.
This layout retains existing names, run configuration and activation controls.

Manifests, runtime YAML, fixtures, notebooks and resources retain their existing
paths. Database/schema/table names, warehouse identity, raw metadata, parser
version strings, snapshot schemas and data roots do not change. The refactor
performs no data migration and makes no live request. Roll back by reverting
code commits; no warehouse rollback is required.

## Python import changes

All repository-owned callers, tests, notebook and runbook examples are migrated.
External scripts using the old internal modules should update their imports as
shown below. This is a source layout change; the `market_data_pipeline` package
name and supported CLI/Dagster entrypoints remain stable. Old internal-module
aliases are not retained as duplicate implementations.

For the latest move, replace the `market_data_pipeline.aaii` prefix with
`market_data_pipeline.pipelines.aaii` in external imports, module commands and
Dagster module settings. The former direct package is no longer provided.

The module paths below are relative to `market_data_pipeline`:

| Previous module | Canonical module |
| --- | --- |
| `aaii.*` (previous pipeline package) | `pipelines.aaii.*` |
| `catalogs.manifest` | `pipelines.aaii.manifest` |
| `data.aaii` | `pipelines.aaii.data.csv` |
| `data.aaii_xls` | `pipelines.aaii.data.xls` |
| `data.gold` | `pipelines.aaii.data.gold` |
| `config` | `pipelines.aaii.config` |
| `sources.aaii` | `pipelines.aaii.source` |
| `pipeline` | `pipelines.aaii.pipeline` |
| `backfill` | `pipelines.aaii.backfill` |
| `replay` | `pipelines.aaii.replay` |
| `operations` | `pipelines.aaii.operations` |
| `recovery` | `pipelines.aaii.recovery` |
| `io.bronze` archive/load functions | `pipelines.aaii.io.bronze` |
| `io.warehouse` | `pipelines.aaii.io.warehouse` |
| `io.parquet` | `pipelines.aaii.io.parquet` |
| `io.databricks` | `pipelines.aaii.io.databricks` |
| `io.bronze` file/time/lock helpers | `shared.files` |
| `catalogs.manifest.UniqueLoader` | `shared.yaml.UniqueLoader` |
| `cli` implementation | `pipelines.aaii.cli` (root `cli.main` still forwards) |
| `orchestration.dagster_defs` implementation | `pipelines.aaii.dagster_defs` (root entrypoint still forwards) |

For example, use `from market_data_pipeline.pipelines.aaii.pipeline import run` or
`from market_data_pipeline.pipelines.aaii.io.parquet import current_snapshot` in new scripts.

## Adding another pipeline

Review/update the master register and its numbered spec first. Verify source
access/schema/timing, then characterize behavior using offline fixtures. Create
one snake_case package at `src/market_data_pipeline/pipelines/<pipeline_name>/`
with source-owned contracts, transforms, warehouse tables and tests;
use shared helpers where they fit. Add its CLI route and Dagster definitions only
when the slice works. Preserve identifiers, raw bytes and observed revisions.
Update the register, runbook, verification and handoff at the same checkpoint.
Do not create provider packages directly under `src/market_data_pipeline/`.
No pipeline may infer production readiness from another pipeline's adapter tests.
