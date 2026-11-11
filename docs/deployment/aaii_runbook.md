# AAII operation and recovery

## Input and local execution

CSV imports accept exports through access you are authorized to use. The bounded
live command also reads AAII's official XLS download; it does not log in or parse
a protected webpage. The current workbook was inspected and loaded on 2026-10-26.
Routine automation and redistribution rights remain unverified.

The supported import is UTF-8 CSV, with one date and three shares per row.
Default headers are `Date,Bullish,Neutral,Bearish`. The manifest specifies
`%m/%d/%Y` and percent units. Both `36.0` and `36.0%` mean 36 percent. Fraction
units require a manifest change and prohibit percent suffixes. Duplicate dates,
missing/non-finite shares, unexpected columns, or invalid totals reject the
entire capture. The default total tolerance is 0.002 in fraction units to allow
display rounding. Input size is limited to 20 MiB.

Convert only the observation table when using a workbook; retain the original
download separately, and do not turn summary averages/highs into observations.
Do not substitute a spreadsheet's file modification time for publication time.

```bash
.venv/bin/python -m market_data_pipeline run \
  --input data/inbox/aaii.csv --root data/aaii
```

Capture time is when this pipeline imports the file. No historical publication
time is inferred. Gold spread changes use the preceding available survey and
include `gap_days`. Percentiles and rolling windows are not implemented.

## Read the committed Parquet snapshot

```python
from pathlib import Path
import pyarrow.parquet as pq
from market_data_pipeline.pipelines.aaii.io.parquet import current_snapshot

snapshot = current_snapshot(Path("data/aaii"))
silver = pq.read_table(snapshot / "silver.parquet")
gold = pq.read_table(snapshot / "gold.parquet")
audit = pq.read_table(snapshot / "audit.parquet")
```

Keep old snapshots for rollback and inspection. Do not glob all snapshot files
as one dataset. The pointer is the publication boundary; never use an incomplete
generation after a failed export. Use the dry-run-first retention command below to remove old managed generations.
Audit Parquet reflects the last published snapshot; inspect warehouse audit and
Bronze receipts for trailing failed/partial runs until the next successful export.

## PostgreSQL

Use `--config config/config.yaml` to resolve typed runtime settings from YAML.
The optional `network` section selects automatic startup proxy/direct fallback
or pins one route, with NO_PROXY rules, timeouts and a config-relative CA bundle.
The selected verified client is reused for all HTTP retries in that run; the next
run tests again. URLs/credentials stay in the worker environment. See the
[outbound HTTP contract](../development/outbound_http.md). Local commands do not
probe. Startup routing failure is recorded in the transport health log.
See `config/postgres.example.yml`: `user` is the authenticated login; optional
`role` selects the effective PostgreSQL role through startup settings, and
`schema` selects its search path. The login needs permission to assume that role;
the role needs privileges in an existing schema. No roles/databases are silently
created by the Python importer. `connect_timeout` and `sslmode` are configurable.
Set `password_env: null` to use libpq's standard credential mechanisms, such as
a properly secured passfile. Default behavior requires the named password variable.

The local Compose service reads database name, user, published port and bind host
from `AAII_POSTGRES_DATABASE`, `AAII_POSTGRES_USER`, `AAII_POSTGRES_PORT` and
`AAII_POSTGRES_BIND_HOST`. Match these with runtime YAML; Compose and the client
file serve different processes. The container's internal port stays 5432.
Changing initialization variables does not rename an already initialized database,
alter its users, or reset its password. Keep existing volumes intact.

Use a dedicated database and a separate filesystem root from a SQLite run.
The role needs CREATE TABLE/INDEX and SELECT/INSERT/UPDATE/DELETE privileges in
its schema. Table creation is automatic for this initial schema; there is no
general schema migration framework yet. Use a service-specific role/search path
and TLS configuration on a remote database. DSNs stay in the worker environment.

For a local persistent service, set `AAII_POSTGRES_PASSWORD` in an ignored `.env`
file or the shell, then start the supplied Compose service:

```bash
docker compose --env-file .env -f resources/compose.postgres.yml up -d --wait
```

Set `MARKET_DATA_POSTGRES_DSN` for the Python process to
`postgresql://market_data:<URI-encoded-password>@127.0.0.1:55432/additional_market_data`,
then run:

```bash
.venv/bin/python -m market_data_pipeline run \
  --input data/inbox/aaii.csv --root data/aaii-postgres --warehouse postgres
.venv/bin/python -m market_data_pipeline gold \
  --root data/aaii-postgres --warehouse postgres
```

The CLI does not automatically load `.env`; export the DSN or inject it through
your runtime's secret mechanism. Reuse this warehouse's root for later runs.
Back up both PostgreSQL and Bronze. Do not delete the Compose volume to perform
routine restarts. `docker compose ... down` preserves the named volume.

### Verified live local target

The completed live run is in the persistent Compose project
`additional-market-data-live`, container `additional-market-data-live-postgres-1`,
on localhost port **55438**. The role `aaii_writer` is NOLOGIN/NOSUPERUSER and owns
schema `aaii`; login `market_data` can assume it. The initial four-date load is recorded in `data/aaii-live/verification.json`.
The 2026-10-30 local historical backfill now contains 2,046 reported dates,
1987-07-24 through 2026-10-22. Three invalid historical rows remain quarantined;
reimport added zero versions. `data/aaii-live/completion-verification.json`
records the backfill, quality report, verified backup/restore and replay.

To restart this specific service without changing its persistent volume:

```bash
docker compose --project-name additional-market-data-live --env-file .env \
  -f resources/compose.postgres.yml up -d --wait
```

For a new bounded fetch, export this project's password environment variable,
then use the README's `run --live --config ... --start ... --end ...` example.
The default CSV manifest must be replaced with the XLS manifest for this command.
For an offline rerun, pass the archived `payload.xls` with `--input` and the same
XLS manifest/start/end selection. Replay preserves original capture selection
and HTTP metadata. The selected dates are reported-date labels, not inferred
survey ending dates or exact publication timestamps.

## Dagster execution

Run `dagster dev` as shown in the README, then launch `aaii_sentiment_job` with
`resources/dagster-aaii.yml`. Point `input_path` to the authorized export when
moving beyond the synthetic demo. For PostgreSQL, set `warehouse: postgres` and
inject the DSN into the worker. All workers must share the persistent Bronze and
Parquet root. One writer topology per warehouse is required.

Both schedules are stopped by default. `aaii_thursday_local_import` schedules
supplied local files. Use atomically delivered files so readers cannot capture
a partially written export. `aaii_weekly_live` uses dynamic overlapping bounds
and requires both `MARKET_MEDALLION_CONFIG` and the YAML access acknowledgement
described below. Validation failures do not retry automatically; other asset
failures retry twice, 30 seconds apart. HTTP retries are separately bounded.
A production run launcher/daemon and external alert destinations are not configured.
For a manual bounded live execution, use `resources/dagster-aaii-live.yml`.
For a local XLS execution, set the XLS manifest plus explicit `start` and `end`.

For an interactive manual update, open **Jobs → aaii_sentiment_job → Launchpad**
in the Dagster UI and edit the run YAML. The worker must have this project's
`.env` loaded, and the existing PostgreSQL container must be running. For example,
this 70-day window ends on November 10, 2026; edit both dates for a later run:

```yaml
ops:
  aaii_sentiment:
    config:
      config_path: config/config.yaml
      manifest_path: manifests/aaii-investor-sentiment-xls.yml
      live: true
      start: '2026-09-01'
      end: '2026-11-10'
```

Click **Launch Run** and inspect its status, logs and materialization metadata.
Manual live windows are limited to 90 days. Changing Launchpad dates affects
only that run and does not activate a schedule. See Dagster's
[run-configuration guide](https://dagster.io/docs/guides/operate/configuration/run-configuration).
The direct `weekly --config config/config.yaml` CLI instead reads
`operations.overlap_days` (7–90) from runtime YAML; it executes the same core
without creating a Dagster run record.

## Replay and failure recovery

```bash
.venv/bin/python -m market_data_pipeline replay \
  --source-root data/aaii --root data/aaii-rebuilt
```

The destination directory and warehouse must be empty. Replay makes no network
requests, validates raw digests, and preserves capture times and IDs. Original
rejections are copied as failed audit rows and returned in `rejected`; the CLI
returns nonzero so they cannot be mistaken for a fully clean reconstruction.

| Outcome | Recovery |
| --- | --- |
| Manifest invalid or input unreadable | Fix the manifest/input; no dataset promotion occurred |
| CSV/DQ failure | Inspect raw capture and `dq_message`; correct the input and import again |
| Warehouse failure before commit | Preserve Bronze, inspect warehouse audit, fix the service and retry |
| Parquet failure after commit (`partial`) | Fix filesystem capacity/permissions and run `export` |
| Missing promotion receipt / `running` audit after interruption | Reconcile run ID, Silver lineage, and warehouse outcome before replay |
| Corrupted Bronze digest or unsupported parser version | Restore trusted archive/parser; replay stops before destination promotion |

```bash
.venv/bin/python -m market_data_pipeline export --root data/aaii
# Add --warehouse postgres for a PostgreSQL root.
```

An export repairs publication, but does not rewrite historical partial status.
Reconcile an interrupted receipt by checking `pipeline_runs` and Silver
`raw_run_id` in the original warehouse. A committed successful zero-change run
has no new Silver rows, so use its audit counts as well. Preserve the original
capture metadata. For an accepted run, the receipt must record `accepted: true`,
`row_count`, and `inserted_versions`; for a known rejection it records
`accepted: false` and its reason. Do not manufacture receipts when the warehouse
outcome is unknown. No automatic crash reconciler is implemented.

## Databricks / Delta

This is an implemented publishing adapter and notebook, **not a live-tested
deployment**. It needs an existing Databricks Spark session with Delta, a Unity
Catalog schema, a volume, and privileges to create/merge tables and create a view.
Build/install the repository wheel on the job cluster. Stage the immutable
snapshot selected by `current.json`, together with the Bronze archive, under
an authorized `/Volumes/<catalog>/<schema>/<volume>/...` path. Copying a snapshot
alone preserves raw IDs/digests but does not make raw payloads available there.

Open `notebooks/publish_aaii_delta.py` and set the `snapshot`, `catalog`, and
`schema` widgets, or invoke:

```python
from pathlib import Path
from market_data_pipeline.pipelines.aaii.io.databricks import publish_delta

publish_delta(
    spark,
    Path("/Volumes/research/market_data/aaii/parquet/snapshots/<snapshot_id>"),
    catalog="research",
    schema="market_data",
)
```

This creates `aaii_silver_versions`, `aaii_pipeline_runs`, and the
`aaii_gold_latest` view in the selected schema. Immutable insert-only merges
preserve all observed states; Gold is calculated from persisted Delta Silver
so an older snapshot cannot overwrite Gold with an older state. One publisher
per target is required. A multi-table failure can be repaired by rerunning.

Live acceptance requires publishing twice without duplicate versions, publishing
a correction and reversion, checking Gold/as-of SQL results and raw lineage,
and retrying a deliberately interrupted publish. No workspace or job is created
by this repository's local commands.


## Weekly acquisition and health

The runtime example includes:

```yaml
operations:
  overlap_days: 28          # allowed 7–90 days
  max_age_days: 10          # allowed 1–90 days
  http_attempts: 3          # allowed 1–5 attempts
  scheduled_live_access: false
```

For an explicit manual weekly acquisition, run `weekly --config config/config.yaml`.
It ends on today's America/New_York date and overlaps the previous 28 days by
default, so changes to already ingested dates are observed again. It does not
assume only the latest week can change. Older corrections require an explicit
wider bounded run or a local historical backfill. `--as-of` sets only the weekly
selection clock; capture time remains actual retrieval time.

The live schedule uses Thursday 10:00 America/New_York. Before deliberately
starting it, verify recurring acquisition permission, inject secrets and
`MARKET_MEDALLION_CONFIG` into the worker/daemon, and set
`operations.scheduled_live_access: true`. It otherwise returns a skip reason,
even if someone enables the schedule. No daemon or live schedule was enabled
by Spec 003. The two schedule definitions should not both publish the same input
unnecessarily. Keep one writer topology on shared durable storage.

HTTP 429/500/502/503/504 responses are archived before retry. Retry-After seconds
or HTTP dates are honored; a requested delay over 30 seconds ends this invocation
instead of retrying early. Other transient attempts use bounded exponential delays.
Connection/read timeouts have sanitized local transport logs; TLS verification
remains enabled. At most `http_attempts` network attempts occur per invocation.

```bash
.venv/bin/python -m market_data_pipeline check --config config/config.yaml
```

Health returns JSON and exits 1 for an uninitialized warehouse, trailing failed
or partial run, unresolved later transport failure, stale latest reported date,
stale successful capture, or missing committed Parquet files. It reads warehouse
and local operation state without fetching data. `max_age_days` is a configurable
age threshold, not a provider publication timestamp or holiday calendar. Arrange
an external monitor/alert destination to run this command in a deployed service;
none is configured here. A historical quarantine remains an explicit partial
status until a later clean run; inspect its quality report instead of treating it
as a transport outage. An export does not clear historical audit outcomes.

## Explicit historical XLS backfill

Use an already authorized local workbook; the command never downloads history.
Back up the existing root/warehouse first. `backfill` lifts the weekly 90-day
selection limit only with explicit start/end dates. Future bounds, duplicate
dates, malformed labels and schema drift still block promotion.

```bash
.venv/bin/python -m market_data_pipeline backfill \
  --input data/inbox/sentiment.xls --config config/config.yaml \
  --start 1987-01-01 --end 2026-10-30
# To explicitly exclude invalid share rows, add --quarantine-invalid.
```

Strict mode rejects selected missing/error/non-finite shares or invalid totals.
The quarantine option retains those original cells in Bronze and writes
`quality.json` with source row, reported date, rule, accepted count and gaps over
eight days. Accepted rows publish to Silver/Gold/Parquet, but audit/result status
is `partial` and CLI exit is **2**. A gap is visible; no missing observation is
invented. All-invalid windows fail. Review/report exclusions before using history.

The local verified import excluded 1987-06-26, 1987-07-17 and 1996-01-11 for
missing/nonnumeric shares. It accepted 2,046 dates, inserted 2,042 new versions
beside the existing four, and inserted zero versions on rerun. No new provider
request was made. Publication timestamps remain null, and new first-seen times
are the actual import time, not dates in the historical workbook.

New explicit backfills archive parser contract `aaii-xls-v2` with selection and
quarantine policy. Legacy `aaii-xls-v1` captures retain their strict bounded replay
contract. Replay reproduces backfill exclusions and returns partial run IDs;
its CLI exits 1 if any run is rejected or partial.

## Backup, restore and retention

```bash
.venv/bin/python -m market_data_pipeline backup --config config/config.yaml \
  --destination data/backups/aaii-2026-10-30
.venv/bin/python -m market_data_pipeline restore \
  --backup data/backups/aaii-2026-10-30 --root data/aaii-restored
```

Backup requires a new directory separate from the source root. The writer lock
and database transaction capture owned Silver versions, derived Gold, audit runs,
date-basis contracts, Bronze and the currently committed Parquet generation.
`backup.json` lists SHA-256 digests of every package file. Retain backups on a
separate secured durable volume/location; a copy on the same disk is only a local
recovery checkpoint. Digests detect accidental change; they are not signatures.
Treat packages as trusted, potentially licensed data. Symlinks are rejected.

This is an application-owned logical backup, **not a PostgreSQL server backup**.
It excludes passwords, config, global roles/grants, unrelated tables, server
settings, operation logs and older snapshot generations. Use your platform's
native backup policy for database-wide recovery; PostgreSQL documents
[pg_dump](https://www.postgresql.org/docs/17/app-pgdump.html) and
[pg_restore](https://www.postgresql.org/docs/17/app-pgrestore.html) for that scope.
No native client or cluster configuration is needed for the application backup.

Restore verifies the complete file inventory before writes. Use an empty target
directory and an empty target warehouse. The command above restores to SQLite;
for PostgreSQL, supply a **separate restore config** pointing to an already
provisioned empty database/schema and a new root. Never point restore at the live
config. Roles/schema/permissions must be provisioned separately. Restore retains
version/run IDs and timestamps, recomputes and compares Gold, adjusts physical
audit Bronze pointers, assigns a distinct target warehouse identity and publishes
fresh Parquet. Original Bronze provenance stays unchanged. On a failed restore,
keep the source/backup intact and retry into another clean target after resolving
the cause. There is no in-place overwrite mode or migration engine.

The original four-date PostgreSQL backup was restored to an isolated PostgreSQL
schema and compared exactly. The completed 2,046-date warehouse was also backed
up, restored to SQLite and replayed offline, with equal Silver/Gold results.
Local evidence and backup locations are in `completion-verification.json`.

```bash
.venv/bin/python -m market_data_pipeline retain --config config/config.yaml --keep 5
# Review the proposed list; only then execute the same command with --apply.
```

Retention always preserves the current pointer's generation and the most recent
additional managed generations up to `keep`. It only removes matching managed
snapshot directories; it never deletes Bronze, warehouse rows or backups.
Dry run is the default; applied deletions have a local operation record. Ensure
consumers no longer use old generations before applying; there is no reader-lease
system. Live verification only performed a dry run. Backup expiry and Bronze
retention are separate operator decisions, not automated here.
