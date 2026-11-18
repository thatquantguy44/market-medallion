# MarketMedallion

Repository: [market-medallion](https://github.com/thatquantguy44/market-medallion).

**Dagster jobs, schedules and UI:** [DAGSTER.md — canonical master guide](DAGSTER.md).

**Agent starting point:** read [AGENTS.md](AGENTS.md) and the
[master handoff](docs/handoffs/project_bootstrap.md#agent-start-here) for highest
priorities, current work, last verified commits/PRs and exact next tasks. Its current
panel takes precedence over older dated pickup notes.
[Automated freshness checks](docs/development/handoff_freshness.md) bind the
handoff to staged/committed source and run in CI plus a daily job.

Independent, manifest-driven **Bronze → Silver → Gold** pipelines for market
data. The first executable slice imports an authorized local **AAII Investor
Sentiment Survey CSV or official XLS**, stores observed revisions in SQLite or PostgreSQL, and
publishes typed Parquet snapshots. Dagster provides the initial orchestration.

**Status:** the AAII local pipeline is complete under [Spec 003](specs/spec003/README.md):
CSV/XLS ingestion, observed revisions, replay, Parquet, SQLite/PostgreSQL,
Dagster integration, weekly bounds/retries/freshness, historical backfill and
verified backup/restore. The local PostgreSQL warehouse contains 2,048 reported
dates through November 5, 2026; three invalid historical rows are quarantined.
Recurring AAII acquisition permission remains unverified, so both schedules stay
stopped. Databricks publishing is implemented but has not been live-tested.
CFTC has a verified provider contract but remains unimplemented. NAAIM's delayed
public research feed has an initial fixture-backed local slice; an explicit
permission reference is required for any live request. Other connectors remain
planned.

**Presidential research:** [Spec016](specs/spec016/README.md) is implemented
and its frozen local reconstruction matches a fresh donor rebuild. The receiving
merged releases include the local pipeline, PRES reader and research handoff;
see [acceptance](specs/spec016/acceptance.md). Live source refresh remains a
separate decision.

**Current FOMC build:** [Spec017](specs/spec017/README.md) T01–T06 and listed
references for **1990 through released 2026 minutes** are accepted locally as of
November 13. [Latest acceptance](specs/spec017/1997_2026_acceptance.md) records
1,081 captures, 522 text representations,
3,200 PDF pages and 9,790 chunks (8,270
eligible, 1,520 review-flagged). All 21 requested 1997–2026
Dagster batches passed health, immutable-history, backup/restore, offline replay
and acknowledged Slack success checks. No unacquired inventory references remain;
unavailable/not-listed claims and text/OCR review stay explicit. The latest
listed 2026 meeting is 2026-09-16.
The existing [Dagster job](docs/deployment/fomc_runbook.md#dagster) and
[`scripts/run_fomc.sh`](scripts/run_fomc.sh) share the bounded runner; other users
supply their own config. The three [quality slices](specs/spec017/quality_contract.md)
are implemented on `feat/fomc-text-quality` in [PR #19](https://github.com/thatquantguy44/market-medallion/pull/19):
offline reporting, an eight-case source-page review and an evidence-bound
remediation gate. [Closeout](specs/spec017/quality_remediation_acceptance.md)
records the bounded no-native-fix result: source-damaged HTML and OCR/chart work
stay explicit, with no flags cleared or derivative publication. Recent-year
refreshes cover new releases/corrections. Acquisition schedules stay stopped;
NAAIM remains paused; FINRA T01–T16 are implemented with bounded three-TRF delivery acceptance; Spec013 OTC-T01–T07 are accepted offline.

**Next source-build program:** [Securities-finance research data](docs/securities_finance/README.md),
with full specs, plans, tasks and roadmap for **SEC FTD + Reg SHO history →
FINRA daily short volume → FINRA ATS/non-ATS → SEC N-PORT → NY Fed primary
dealers** (Specs011–015). Spec011 implements independent SEC FTD and Nasdaq/NYSE
threshold pipelines with immutable Bronze, SQLite/PostgreSQL revisions, cutoffs,
replay, native research exports, bounded acquisition, health and stopped Dagster.
The approved SEC September archives reconcile 117,025 facts. SEC T21 and threshold
T22 Delta publishers have actual local Spark4.0.1/Delta4.0.1 parity and interrupted
publication recovery evidence. T22 also stages threshold membership/coverage/audit
history and derived episodes as a checksummed source snapshot. T17/G2 is complete
for the bounded native-source internal-research delivery: both SEC archives and
ten authentic Nasdaq/NYSE lists reconcile. See the [acceptance record](specs/spec011/acceptance.md)
for the user-attested permission, evidence and exclusions. The closeout is merged;
Spec012 now implements the selected three-TRF API profile end to end: admitted
immutable Bronze, audited SQLite versions/membership, scoped research, exact typed
Parquet with recoverable publication, offline replay, bounded two-pass acquisition,
release-aware health, preserve-all retention, versioned research export and stopped
Dagster orchestration. [Delivery acceptance](specs/spec012/acceptance.md) records
all 15 acceptance IDs and the user-approved real **2026-11-09** coverage:
Carteret 12,349, Chicago 5,849 and NYSE 7,032 rows. All 25,230 source observations
reconcile through publication/export/replay; research output has 12,355 symbols
with explicit per-facility missingness. API timing remains unknown.
The [portable script](scripts/run_finra_short_sale_volume.sh),
[configuration](config/finra_short_sale_volume/README.md) and
[operating runbook](docs/deployment/ssv_runbook.md) cover local use, recovery and
the default-stopped job/schedule. Optional Slack sensors also default stopped
and disabled. G1/G2 are accepted locally for this bounded one-date scope; broader
history, recurring activation and consumer integration remain separate.
[Spec013 OTC-T01–T07](specs/spec013/README.md) implement route/use admission, six
invented weekly profiles, source-evidence identity, immutable Bronze and a
versioned local SQLite/CLI panel. [T07 acceptance](specs/spec013/t07_revisions_acceptance.md)
records immutable corrections/reversions/removals, complete replacement membership,
explicit legacy migration and source/wheel equivalence. OTC-T08 reporting and
availability clocks is next. Real acquisition, derived Gold/Parquet and
orchestration remain future work.
Public API terms acceptance is user-attested; no new permission is inferred.
Databricks workspace activation remains disabled. See the
[SEC FTD runbook](docs/deployment/sec_ftd_runbook.md) and
[threshold runbook](docs/deployment/reg_sho_thresholds_runbook.md).
NAAIM is **paused** and its
daily schedule was stopped on 2026-11-03; see the master handoff before any resume.

NAAIM remains **3-month-delayed public data for internal research**. Daily
capture checks for updates to that delayed feed. Read the observation's
`as_of_date` alongside its value; recent capture time describes pipeline health,
not current-week positioning. CLI output, Gold latest and Dagster metadata
include an explicit delay label and `provider_delay_months: 3`.

**PostgreSQL consolidation:** the user selected primary PostgreSQL storage for
all existing pipelines under [Spec019](specs/spec019/README.md). AAII/FOMC, FINRA short volume and
newly migrated SEC FTD/Reg SHO histories share `additional_market_data`; three
new provider adapters remain pending. See the [schema/role/config register](docs/deployment/postgres_warehouses.md).

## Pipeline organization

[The master pipeline register](docs/pipelines.md) tracks all implemented, planned,
draft and candidate pipelines, their package locations, specs and next work.
[The canonical Dagster master guide](DAGSTER.md) tracks
the operational jobs, schedule state and activation controls.
Each implemented pipeline owns a subfolder of `src/market_data_pipeline/pipelines/`.
AAII lives in `pipelines/aaii/`, including its parsers, warehouse/publishers,
configuration, operations, CLI and Dagster definitions. Provider-independent file/YAML helpers
live in `shared/`; root CLI/Dagster entrypoints remain supported.
See [the package layout and import migration guide](docs/development/pipeline_layout.md).

SEC FTD has its own [configuration folder and decision guide](config/sec_fails_to_deliver/README.md).
Its `validate-config` and config-based local `inspect` commands are implemented;
PostgreSQL is the selected default and Databricks publication stays disabled
until configured. SEC SQLite/PostgreSQL ingestion, Gold, replay and coherent
Parquet and Delta are implemented and locally verified under Spec011.

## Run the first slice

Python 3.12 is the tested baseline. From the repository root:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m market_data_pipeline validate
.venv/bin/python -m market_data_pipeline run \
  --input tests/fixtures/aaii/sentiment.csv --root data/aaii-demo
.venv/bin/python -m market_data_pipeline gold --root data/aaii-demo
```

The fixture contains **invented test values**, not actual AAII history. For real
data, supply an authorized CSV with the manifest's mapped headers:

```csv
Date,Bullish,Neutral,Bearish
```

Use `MM/DD/YYYY` and percentage values such as `36.0` or `36.0%`. Edit a copy of
[the manifest](manifests/aaii-investor-sentiment.yml) for different headers, ISO
dates, or fractional units; pass it with `--manifest`. No automatic unit guessing
is performed. The official XLS path below preserves the original workbook. Keep the original when converting
an authorized download to CSV. AAII historical access, automation, and reuse
permissions still require verification; see the [source review](docs/catalog/README.md).

Each run archives exact input bytes and its manifest before parsing. Unchanged
observations create no new Silver versions; corrections and reversions do.
Gold exposes bullish-minus-bearish spread, prior-survey spread change, and date
gaps. Publication time stays null when unknown. As-of queries use the versions
actually captured by this pipeline, not reconstructed historical vintages.

```bash
.venv/bin/python -m market_data_pipeline gold --root data/aaii-demo \
  --as-of 2026-10-26T23:59:59-04:00
.venv/bin/python -m market_data_pipeline replay \
  --source-root data/aaii-demo --root data/aaii-replayed
```

Replay requires an empty destination. Rejected captures remain rejected and are
reported with a nonzero CLI exit status. See [the runbook](docs/deployment/aaii_runbook.md)
for failure recovery and PostgreSQL/Databricks instructions.

## Parquet and warehouses

| Target | Behavior |
| --- | --- |
| SQLite | Default local transactional warehouse; no service needed |
| Parquet | Every successful run writes Silver, Gold, and audit snapshots |
| PostgreSQL | `--warehouse postgres`, DSN from `MARKET_DATA_POSTGRES_DSN` |
| Databricks/Delta | Publish a committed snapshot staged in a Unity Catalog volume |

Always read `parquet/current.json` to select the committed snapshot. Glob-reading
all snapshots duplicates history. [Architecture](docs/deployment/architecture.md)
explains transaction boundaries and storage layout.

## Database configuration and bounded live runs

Outbound HTTP has [run-scoped proxy/direct configuration](docs/development/outbound_http.md).
The `network` section in each pipeline's YAML selects automatic fallback,
direct-only or proxy-only access. A live run probes once at startup and reuses
the selected route for its data calls/retries. Proxy URLs come from environment
variables or a trusted shell-loaded `.env`; `NO_PROXY`, custom CA bundles and
timeouts are configurable. Local imports and health checks make no probe.
AAII CLI/Dagster and NAAIM's paused live entrypoints use this client. SEC FTD and threshold acquisitions use the same run-scoped settings. NYSE
probes its published source page before its exact configured Download GET;
all local/replay/health paths remain free of network probes.

Copy [the runtime example](config/postgres.example.yml) to ignored
`config/config.yaml` and set host, port, database, login `user`, optional effective
`role`, and `schema`. The schema/role must already exist with suitable permissions.
Set the password in the environment variable named by `password_env`; an actual
password is never stored in YAML. Relative output paths resolve against the config
file's directory. Explicit `--root` and `--warehouse` flags override the file.
Configured PostgreSQL settings take precedence over the legacy DSN environment variable.

```bash
# Load this project's trusted local .env into your shell, or inject the password externally.
set -a
source .env
set +a
.venv/bin/python -m market_data_pipeline run --live \
  --config config/config.yaml \
  --manifest manifests/aaii-investor-sentiment-xls.yml \
  --start 2026-10-01 --end 2026-10-26
.venv/bin/python -m market_data_pipeline gold --config config/config.yaml
```

Live selection is limited to 90 days. The entire original XLS response is archived
with HTTP provenance and the selected date bounds. XLS dates are **Reported Date**
labels and are preserved without shifting to survey-week-ending dates. A warehouse
contract prevents mixing these two date conventions. Routine scheduled downloads
remain disabled; this command is an explicit bounded fetch.

The verified local service uses `127.0.0.1:55438`, database `additional_market_data`,
login `market_data`, effective role `aaii_writer`, and schema `aaii`. Its actual
settings are in ignored `config/config.yaml`; credentials are in ignored `.env`.
The checked-in example keeps the generic port 55432 and public schema.
These database and Docker identifiers are preserved across the repository rename
so existing volumes and DBeaver connections continue to use the same data.

## Dagster jobs and UI

**[DAGSTER.md](DAGSTER.md) is the single master guide** to current jobs,
schedules/sensors, launch examples, activation controls and maintenance checks.
Open the local UI at <http://127.0.0.1:3030>, or start it from the repository root:

```bash
./scripts/run_local_dagster.sh
```

The launcher loads this project's ignored `.env`; `DAGSTER_PORT` selects a
nondefault port. Job availability and definition defaults are separate from
actual running/scheduled state. See the master guide's dated runtime check and
inspect the UI for current state. [Pipeline runbooks](docs/pipelines.md) retain
source-specific configuration and recovery details.

When changing Dagster definitions, update `DAGSTER.md` in the same change and run
`.venv/bin/python scripts/verify_dagster_docs.py`; CI rejects inventory drift.

## Operations, history and recovery

Optional runtime YAML `operations` settings control `overlap_days` (28),
`max_age_days` (10), `http_attempts` (3), and `scheduled_live_access` (false).
`weekly` is an explicit live invocation using today's New York date and the
configured overlap. `check` performs no provider request and returns nonzero for
stale data, a trailing failure/partial outcome or unavailable publication.

```bash
.venv/bin/python -m market_data_pipeline check --config config/config.yaml
.venv/bin/python -m market_data_pipeline backfill \
  --input data/inbox/sentiment.xls --config config/config.yaml \
  --start 1987-01-01 --end 2026-10-30 --quarantine-invalid
.venv/bin/python -m market_data_pipeline backup --config config/config.yaml \
  --destination data/backups/aaii-example
.venv/bin/python -m market_data_pipeline retain --config config/config.yaml --keep 5
```

Backfill is strict unless quarantine is explicitly requested. Quarantine writes
`quality.json`, promotes valid rows with a **partial** audit outcome, and exits 2.
First-seen is the import time; historical rows do not supply historical vintages.
Backups contain pipeline-owned logical state, Bronze and the committed snapshot;
restore requires an empty root and warehouse. Retention defaults to a dry run.
See [the operation and recovery runbook](docs/deployment/aaii_runbook.md) before
restoring or applying retention. Credentials and local provider workbooks stay ignored.

## Verification and project context

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
```

PostgreSQL tests require a dedicated empty test database; instructions are in
[development](docs/development/README.md). Tests never require a live AAII service.

- [Spec 003: AAII completion](specs/spec003/README.md)
- [Spec 002: executable AAII slice](specs/spec002/README.md)
- [Spec 001: original broader design](specs/spec001/README.md)
- [Roadmap and full source backlog](docs/roadmap.md)
- [Current handoff](docs/handoffs/project_bootstrap.md)
- [Stopping checkpoint and next scoped task](docs/handoffs/aaii_checkpoint.md)
- [Data dictionary](docs/dictionary/data_dictionary.md)
- [Pinned engineering workflow library](docs/development/agent_library.md)

[Available agent libraries](agents/README.md) documents both the pinned local
agent-skills snapshot and the configurable QuantSmith local reference. Quant/data
work should also use the relevant verified QuantSmith specialist guidance;
`MARKET_MEDALLION_QUANTSMITH_PATH` can point to another matching local checkout.

The workflow library is vendored directly under `agents/agent-skills/`, with its
exact upstream revision and content checksum in `agents/agent-skills.lock.json`.
No submodule setup or GitHub refresh is needed. Verify the local snapshot with
`python3 scripts/verify_agent_skills.py`. This project has no runtime imports,
secrets, databases, or deployment identifiers inherited from sibling repositories.

## Repository name and local checkout

The project is named **MarketMedallion**; the GitHub repository and Python
distribution use `market-medallion`. The Python import path
`market_data_pipeline` and existing `market-data` command remain supported.
Installing the package also provides the `market-medallion` command.

The local checkout is `pipelines/market-medallion`. The previous checkout path is
retained as a compatibility symlink for historical absolute Bronze/audit paths
and existing workspace references. New clones can use the new name directly.
