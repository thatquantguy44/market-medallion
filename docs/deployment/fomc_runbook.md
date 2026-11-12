# FOMC bounded operations

The FOMC pipeline owns its CLI, source adapters, archive, PostgreSQL namespace and
Dagster definitions under `pipelines/fomc_meeting_minutes/`. Active acquisition
starts in 1990. [T06 acceptance](../../specs/spec017/t06_acceptance.md) records the
bounded local result; it does not certify full history or text/OCR accuracy.

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

The manual `fomc_meeting_minutes_job` invokes the same runner. Its asset config is:

```yaml
ops:
  fomc_meeting_minutes_refresh:
    config:
      config_path: config/fomc_meeting_minutes/pipeline.local.yml
      operations_path: config/fomc_meeting_minutes/operations.local.yml
      start_year: 1990
      end_year: 1990
      mode: backfill
      live: true
      scheduled: false
```

The definition `fomc_meeting_minutes_weekday_refresh` defaults **STOPPED** and is
not activated by this work. Its cadence is weekdays at 17:10 America/New_York.
A future deliberate activation also requires `FOMC_CONFIG`,
`FOMC_OPERATIONS_CONFIG` and `scheduled_live_access: true`. Preview and execution
both check access. The schedule refreshes current year plus `overlap_years`
(default one prior year), floored at 1990. Config loading and preview make no
service request. Do not resume NAAIM or activate any other schedule with FOMC.
