# Dagster job and schedule register

Updated: 2026-11-01, America/New_York. This is the canonical register for
Dagster-owned jobs and schedules in MarketMedallion. It complements the
[pipeline register](../pipelines.md): that register owns source scope and
delivery status; this document owns operational execution details.

## Current local state

The status below was verified in the local Dagster instance on 2026-11-01.
Schedule state is runtime state, so recheck it before assuming a job will run.

| Pipeline | Job | Schedule | Cadence | Local state | Data root / retention | Activation boundary |
| --- | --- | --- | --- | --- | --- | --- |
| AAII Investor Sentiment | `aaii_sentiment_job` | `aaii_thursday_local_import` | Thursday, 10:00 America/New_York | Stopped | Runtime-configured root; snapshot retention is a separate dry-run-first operation | Local file only; no provider request. |
| AAII Investor Sentiment | `aaii_sentiment_job` | `aaii_weekly_live` | Thursday, 10:00 America/New_York | Stopped | Runtime-configured root; snapshot retention is a separate dry-run-first operation | Requires `MARKET_MEDALLION_CONFIG` and `operations.scheduled_live_access: true`; provider entitlement remains separately required. |
| NAAIM delayed public research | `naaim_delayed_capture_job` | `naaim_delayed_daily_live` | Daily, 10:00 America/New_York | Enabled in the local instance; evaluates only while the manually started Dagster service is running | `data/naaim-exposure-public-delayed`; 365-day source-owned retention after a successful capture | Requires `NAAIM_SCHEDULED_LIVE_ACCESS=true`, `NAAIM_DELAYED_ROOT`, and `NAAIM_PERMISSION_REFERENCE`. |

No other Dagster jobs or schedules are defined in the repository. A schedule
being listed or enabled does not establish provider rights, start a persistent
service, or grant a downstream redistribution right.

## Ownership and job behavior

### AAII

AAII owns its job, assets and schedules in
`src/market_data_pipeline/pipelines/aaii/dagster_defs.py`. See the
[AAII runbook](aaii_runbook.md) for run configuration, recovery, PostgreSQL and
retention procedures. Keep both schedules stopped until their specific access
and operational gates are satisfied.

### NAAIM

NAAIM owns its job, asset, schedule, retention and health operations in
`src/market_data_pipeline/pipelines/naaim_exposure_public_delayed/`. The daily
job creates a distinct capture event even if the provider payload is unchanged,
then applies 365-day dry-run-tested retention. It remains limited to the
three-month-delayed internal-research feed and cannot supply QuantMeridian.
See [Spec 007](../../specs/spec007/README.md) for permission and scope limits.

The user confirmed retaining the lagged feed. Outputs and materialization
metadata label it **NAAIM Exposure Index — 3-month-delayed public data** with
`provider_delay_months: 3` and a `delay_notice`. Always show the observation's
`as_of_date` alongside its value. CLI capture/health responses expose
`latest_as_of_date`; a healthy recent capture is not current-week positioning.
Daily polling does not remove NAAIM's three-month source delay.

## Local operation

### Decision: manual local operation

On 2026-11-01, the user selected **manual local operation** rather than a
restart-at-login LaunchAgent or a hosted service. Start the command below when
daily NAAIM automation is intended to run; stopping the process, logging out or
rebooting pauses schedule evaluation. Revisit a durable service only after the
user requests it. The tracked LaunchAgent template is retained for that future
review and is not installed locally.

Load the ignored local environment and inspect schedule state:

```bash
set -a
source .env
set +a
DAGSTER_HOME="$PWD/.dagster" .venv/bin/dagster schedule list \
  -m market_data_pipeline.orchestration.dagster_defs
```

Start the local service when it is intended to remain available:

```bash
set -a
source .env
set +a
DAGSTER_HOME="$PWD/.dagster" .venv/bin/dagster dev \
  -m market_data_pipeline.orchestration.dagster_defs
```

For a future restart-at-login review on macOS, install the tracked LaunchAgent template
after replacing `__PROJECT_ROOT__` with the absolute checkout path. The service
uses `scripts/run_local_dagster.sh`, sources ignored `.env`, writes ignored local
logs under `data/dagster/`, and binds only to `127.0.0.1`. Do not install a
second instance while a manually launched `dagster dev` process is running.
If the checkout remains under macOS's privacy-protected `Documents` location,
the LaunchAgent may be unable to read it; move the durable checkout to a
non-protected development path or grant the service's host process the required
Full Disk Access before installing the agent.

Start or stop a particular schedule deliberately:

```bash
DAGSTER_HOME="$PWD/.dagster" .venv/bin/dagster schedule start \
  naaim_delayed_daily_live -m market_data_pipeline.orchestration.dagster_defs
DAGSTER_HOME="$PWD/.dagster" .venv/bin/dagster schedule stop \
  naaim_delayed_daily_live -m market_data_pipeline.orchestration.dagster_defs
```

For NAAIM, use the ignored `.env` and
`config/naaim-delayed.yml`; committed examples are `.env.example` and
`config/naaim-delayed.example.yml`. Check local health without contacting the
provider:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.naaim_exposure_public_delayed \
  --root data/naaim-exposure-public-delayed --check
```

## Update rule

When adding, removing, enabling, disabling or materially changing a Dagster job
or schedule, update this register and the relevant pipeline row in
`docs/pipelines.md` in the same change. Record the schedule's default state,
actual local state when known, provider boundary, configuration inputs, data
root and retention behavior.
