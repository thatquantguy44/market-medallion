# FOMC bounded operations

The FOMC pipeline owns its CLI, source adapters, archive, PostgreSQL namespace and
Dagster definitions under `pipelines/fomc_meeting_minutes/`. Active acquisition
starts in 1990. [T06 acceptance](../../specs/spec017/t06_acceptance.md) records the
bounded local T06 result. The [1993–1994 acceptance](../../specs/spec017/1993_1994_acceptance.md)
is the latest checkpoint; neither certifies full history or text/OCR accuracy.

## Checkout setup and script entrypoint

On macOS or Linux, use Bash, Python 3.12 and an existing PostgreSQL database/login
with permission to create the dedicated FOMC schema and its owned tables. From a
fresh checkout or extracted source distribution, install the tested environment:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
cp config/fomc_meeting_minutes/pipeline.example.yml config/fomc_meeting_minutes/pipeline.local.yml
cp config/fomc_meeting_minutes/operations.example.yml config/fomc_meeting_minutes/operations.local.yml
```

Edit your `pipeline.local.yml` for your own host, port, database, user, root and
network. The example uses port 55432; it does not connect to the maintainer's
local service automatically. Set the password in the environment variable named
by `postgres.password_env` (the example uses `FOMC_POSTGRES_PASSWORD`). Keep
`scheduled_live_access: false`. Each user supplies their own connection settings
and credentials; do not copy another user's local YAML, `.env` or data directory.

The executable script supports the same commands and arguments as the CLI:

```bash
./scripts/run_fomc.sh --help
./scripts/run_fomc.sh validate-config \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml
```

It selects `.venv/bin/python` relative to its checkout. To use another environment
with this project installed, set `FOMC_PYTHON=/absolute/path/to/python` (or an
interpreter name on `PATH`). The script preserves the caller's working directory;
relative `--config`/`--operations` paths resolve there, while paths inside YAML
resolve against that YAML file. Quote paths containing spaces. From another
working directory, invoke the script by its absolute path and supply absolute
config paths as needed.

The script passes through JSON and exit codes. It inherits your environment;
it does not load `.env`, install packages, initialize a database or fetch data
implicitly. `validate-config` and help require no database/password or provider
access. Follow the explicit backup/migration steps below for an existing archive.
For wheel-only installations, use the equivalent `market-medallion fomc` command.

## Configuration and preparation

Use `config/fomc_meeting_minutes/pipeline.local.yml` and
`operations.local.yml`, copied from their checked-in examples and kept ignored.
The pipeline file owns PostgreSQL, root, manifest and run-scoped `network`
settings. The operations file owns finite year/page/document/asset/request/byte/
time/retry limits, pacing, overlap and freshness. Credentials remain in the
pipeline's named environment variables; config validation resolves no secrets
and opens no database or HTTP route. Install the pinned `fomc_text`, `postgres`
and, for jobs, `dagster` extras before live use.

```bash
market-medallion fomc validate-config \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml
```

Before migration, back up the dedicated FOMC schema and filesystem originals/
receipts/derivatives/operation artifacts. Restore the database archive into an
independent disposable database and reconcile table fingerprints and catalog/
original readbacks before continuing. Preserve both the database and filesystem
archive; either alone is insufficient for verified reads. The local T06 evidence
records actual pre-/post-migration restore checks, with backup paths relative to
the ignored FOMC root. Do not use the operational database as a general test DSN.

Migration is explicit, idempotent only for recognized owned components, and makes
no provider request:

```bash
market-medallion fomc migrate \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml
```

T06 adds three tables (`operation_state`, `operation_runs`, `operation_records`)
under `fomc-operations-postgres/1`, bringing the owned schema to 29 tables.
Partial/foreign schemas fail closed. No schedule is started by migration.

## Plan, backfill and refresh

`plan` reads the current inventory without HTTP. It lists known primary references
for the explicit years; it is not a prediction of recursive addenda, revisions,
retry requests or newly released source links. A fresh installation needs all
in-scope inventory pages under the configured page/request budgets before it can
publish a coherent inventory. Inspect the plan and budgets before live use.

```bash
market-medallion fomc plan \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --start-year 1990 --end-year 1990 --mode backfill
market-medallion fomc run \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --start-year 1990 --end-year 1990 --mode backfill --live
```

For a bounded 1993–1994 batch, use the same script after explicit initialization/
migration of your own target:

```bash
./scripts/run_fomc.sh plan \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --start-year 1993 --end-year 1994 --mode backfill
./scripts/run_fomc.sh run \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --start-year 1993 --end-year 1994 --mode backfill --live
./scripts/run_fomc.sh health \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml
```

The local 1990–1994 batches are accepted for listed references; the next core
batch is 1995–1996. The two-year example uses the default `max_years: 2` limit.
Change only the explicit year bounds for another bounded batch; the operation
settings still enforce all finite limits. `migrate`, `recover-capture` and
`recover-run` also work through the same script with their documented arguments.

The source module `python -m market_data_pipeline.pipelines.fomc_meeting_minutes`
accepts the same subcommands. Root CLI aliases also support `fomc`.

`backfill` reuses verified valid originals and fetches missing references.
`refresh` re-fetches selected originals and dependencies to observe corrections.
Both re-fetch the historical index, current calendar and selected year pages;
other verified year pages are carried with their original capture clocks.
A new linked page is acquired under the same finite budgets. Every explicit
HTML addendum and supported content asset must pass before publication. Bundled
artifacts retain all meeting associations; source URLs are discovered, not guessed.

The request ceiling reserves two startup probes, then counts attempted data GETs
including retries. Returned `requests` counts data GETs; `probe_requests` reports
observed startup probes when route selection succeeds. Returned byte counts cover
data response bodies. Probe bodies use the shared client's bounded probe policy.
Each response is capped by its kind and remaining aggregate allowance. The elapsed
budget is checked between work and before commit; a blocking HTTP call retains the
configured client timeout, so this is not a hard process deadline. Numeric/date
Retry-After is honored or the run stops; it is never shortened to fit a retry cap.

One session advisory lock serializes operations before intent/probe. Originals
and rejected attempts are saved before processing. One transaction advances the
inventory/text/chunk pointers and successful operation result together. Failure
keeps the prior pointers and all raw/audit evidence. Repeat as a new bounded run
after resolving the recorded failure; verified originals can be reused. A completed
batch may still contain explicit native-text/OCR review flags and acquisition gaps
outside the selected years.

## Health and recovery

```bash
market-medallion fomc health \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml
```

Health verifies current raw/text/chunk mirrors and pointer coherence and reports
latest operation status, age, source gaps and chunk eligibility. `healthy` means
the last bounded operation is coherent and within the configured freshness limit.
It does not mean every year is acquired or review text is certified. Empty,
not-yet-run, interrupted, stale and failed states remain explicit. Exit code 2
signals a failed/partial/degraded/interrupted result; JSON uses safe error codes.

Capture and operation records are immutable mirrors under `captures/`, `attempts/`
and `operations/<run_id>/`. Recover an interrupted mirror with retained IDs/bytes:

```bash
market-medallion fomc recover-capture \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --capture-id CAPTURE_UUID
market-medallion fomc recover-run \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --run-id RUN_UUID
```

These commands, validation, migration, plan, health and catalog replay never probe
or acquire. Recovery verifies exact immutable mirrors and contiguous record order;
it never guesses a completion or changes catalog pointers. A local completion
proposal for rolled-back catalogs cannot be recovered as success. Preserve it for
investigation and start a new bounded run. The existing text/chunk replay helpers
rebuild selected generations into an independent initialized empty target from
stored artifacts, with no extraction, tokenization or HTTP. Operation receipts
can then be copied unchanged and recovered after their referenced catalogs exist.
Replay of a selected generation does not restore every historical catalog snapshot;
full database backup/restore preserves those snapshots.

## Dagster

The registered `fomc_meeting_minutes_job` runs only
`fomc_meeting_minutes_refresh` through the same bounded source runner. The
user-selected deployment deliverable is this job in the existing code location.
Use the checkout's installed `dagster`, `postgres` and `fomc_text` extras and your
own environment-backed configuration from the setup section above.

For a recent refresh, copy
[`resources/dagster-fomc-refresh.yml`](../../resources/dagster-fomc-refresh.yml)
to an ignored local run config and set `live: true` deliberately. Launch that
config in the Dagster Launchpad for `fomc_meeting_minutes_job`, or execute it
from the checkout root:

```bash
cp resources/dagster-fomc-refresh.yml config/fomc_meeting_minutes/refresh.local.yml
# Edit refresh.local.yml to enable live access after reviewing the configuration.
.venv/bin/dagster job execute \
  -m market_data_pipeline.orchestration.dagster_defs \
  -j fomc_meeting_minutes_job -c config/fomc_meeting_minutes/refresh.local.yml
```

The checked-in examples have `live: false`; they refuse execution before SQL or
an HTTP probe. `recent_years: true` requires `mode: refresh` and omitted/null
`start_year` and `end_year`. It selects the execution-time New York calendar
year plus `overlap_years` (default one prior year), floored at the configured
operating start year and constrained by `max_years`. For example, a 2026 run
selects 2025–2026 with default settings. Year selection never backdates capture
clocks. A partially specified range or mixed recent/explicit selection fails
before service access.

For deliberate historical batches, use
[`resources/dagster-fomc-backfill.yml`](../../resources/dagster-fomc-backfill.yml),
which shows the next 1995–1996 batch with live access disabled. Set explicit
bounds and review them before enabling live access. Ordinary configs omitting
both bounds and `recent_years` retain the original 1990-only backfill default.
This job delivery did not acquire a new historical batch.

A successful materialization requires a completed source operation followed by
healthy no-HTTP archive readback and matching inventory/text/chunk pointers.
Dagster metadata includes the operation ID, commit status, requested years/mode,
request/byte counters, prior/current catalog IDs, chunk coverage and source-gap
outcomes. `full_history_complete` and `text_or_ocr_accuracy_certified` remain
false. A healthy bounded archive may still have unacquired years and review text.

Source failures, budgets, concurrent-operation refusal and SQL publication
failures fail the job without a materialization. The source's immutable evidence
and prior catalog pointers remain available. A health failure after publication
also fails the job, with `operation_committed: true` and the committed operation
and catalog IDs; investigate with `health` and `recover-run` before launching a
new bounded run. Do not assume a failed Dagster run rolled back an already
committed source operation. Unexpected exception details are replaced with safe
phase/error codes in Dagster events. Job-wide and op automatic retries default
to zero; bounded HTTP retries remain source-owned. Explicit retries can reuse
verified originals, and the existing PostgreSQL advisory lock excludes competing
FOMC operations.

The definition `fomc_meeting_minutes_weekday_refresh` remains **STOPPED**. Its
cadence is weekdays at 17:10 America/New_York. Future deliberate activation also
requires `FOMC_CONFIG`, `FOMC_OPERATIONS_CONFIG` and
`scheduled_live_access: true`. Preview uses the same recent-window calculation
without service access; execution rechecks the gates. Keep NAAIM paused.

Job registration and all stopped schedule states were verified locally. No new
persistent worker/service, schedule activation, external alert destination or
off-machine backup destination is installed by this job delivery. External
integrations remain configurable future work. See the
[job acceptance](../../specs/spec017/dagster_job_acceptance.md) for verification.
