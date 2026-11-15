# Dagster job and schedule register

Updated: 2026-11-15, America/New_York. This is the canonical register for
Dagster-owned jobs and schedules in MarketMedallion. It complements the
[pipeline register](../pipelines.md): that register owns source scope and
delivery status; this document owns operational execution details.

## Current local state

Verified 2026-11-15 06:23 EST through the running local UI's GraphQL API:

- UI: <http://127.0.0.1:3030>; code location
  `market_data_pipeline.orchestration.dagster_defs` / `__repository__`.
- Eight explicit jobs are loaded across seven pipelines. Dagster also owns the
  implicit `__ASSET_JOB`; it is not another implemented pipeline.
- All eight acquisition/import schedules are STOPPED, including NAAIM.
- FOMC Slack success/failure sensors are RUNNING; the other ten alert sensors
  are STOPPED. Existing stored automation settings were preserved.
- No active or queued runs were present at the recovery check.

The UI had an unreachable user-code server. Reload alone failed; a restart of
this project's manual launcher restored the code location. Startup briefly
reported SQLite metadata read errors for two event-log files; subsequent full
integrity checks passed for both without repair or replacement. No metadata
reset, new job launch, source acquisition or schedule activation was performed.
The underlying cause of the transient metadata reads was not established.
The service is manually launched, rather than an installed LaunchAgent.

The table records the observed state at this timestamp; recheck runtime state
before relying on it. Earlier dated checks below retain historical context.

| Pipeline | Job | Schedule | Cadence | Local state | Data root / retention | Activation boundary |
| --- | --- | --- | --- | --- | --- | --- |
| AAII Investor Sentiment | `aaii_sentiment_job` | `aaii_thursday_local_import` | Thursday, 10:00 America/New_York | Stopped | Runtime-configured root; snapshot retention is a separate dry-run-first operation | Local file only; no provider request. |
| AAII Investor Sentiment | `aaii_sentiment_job` | `aaii_weekly_live` | Thursday, 10:00 America/New_York | Stopped | Runtime-configured root; snapshot retention is a separate dry-run-first operation | Requires `MARKET_MEDALLION_CONFIG` and `operations.scheduled_live_access: true`; provider entitlement remains separately required. |
| NAAIM delayed public research | `naaim_delayed_capture_job` | `naaim_delayed_daily_live` | Daily, 10:00 America/New_York | **Stopped by user decision on 2026-11-03** | `data/naaim-exposure-public-delayed`; 365-day source-owned retention after a successful capture | Do not restart without explicit user direction; then requires `NAAIM_SCHEDULED_LIVE_ACCESS=true`, `NAAIM_DELAYED_ROOT`, and `NAAIM_PERMISSION_REFERENCE`. |
| SEC fails-to-deliver | `sec_fails_to_deliver_job` | `sec_fails_to_deliver_daily_live` | Daily, 08:30 America/New_York | Loaded; schedule **stopped** | Configured pipeline-owned root; immutable Bronze and coherent Parquet retention are operator-managed | Requires local `acquisition.enabled: true`, declared `SEC_FTD_USER_AGENT`, `SEC_FTD_SCHEDULED_LIVE_ACCESS=true`, `SEC_FTD_CONFIG`, and one exact `SEC_FTD_PARTITION`. |
| Nasdaq/NYSE Reg SHO thresholds | `reg_sho_threshold_list_job` | `reg_sho_threshold_lists_daily_live` | Weekdays, 23:30 America/New_York | Loaded; schedule **stopped** | Configured threshold root; immutable Bronze and versioned SQLite state | Requires reviewed provider use, local `acquisition.enabled: true`, `REG_SHO_USER_AGENT`, `REG_SHO_SCHEDULED_LIVE_ACCESS=true`, `REG_SHO_CONFIG`, and one exact `REG_SHO_PARTITION`. |
| FOMC meeting minutes | `fomc_meeting_minutes_job` | `fomc_meeting_minutes_weekday_refresh` | Weekdays, 17:10 America/New_York | Loaded; schedule **stopped** | `data/fomc_meeting_minutes/`; retain originals, revisions, derivatives and operation evidence | Manual runs require explicit `live: true`; scheduled execution additionally requires `FOMC_CONFIG`, `FOMC_OPERATIONS_CONFIG` and `scheduled_live_access: true`. |
| FINRA daily short-sale volume | `finra_short_sale_volume_job` | `finra_short_sale_volume_weekday_refresh` | Weekdays, 09:15 America/New_York | Loaded; schedule **stopped**; isolated manual acceptance is recorded separately | Configured independent root; PRESERVE_ALL dry run | Job/YAML live flags plus `FINRA_SSV_LIVE_ACCESS=true`; scheduled workers require `FINRA_SSV_SCHEDULED_LIVE_ACCESS=true`, explicit config/scope and reviewed calendar. |

Presidential research also owns `presidential_rebuild_job` (offline) and
`presidential_live_fetch_job`, with `presidential_research_refresh` stopped by
default (Monday 09:00 America/New_York). See [Spec016](../../specs/spec016/README.md)
for its independent source-review/activation controls.

FINRA T15 adds `finra_short_sale_volume_job` and
`finra_short_sale_volume_weekday_refresh`, default STOPPED, plus optional stopped
success/failure Slack sensors. The cadence is weekdays 09:15 America/New_York
for the previous reviewed reporting date with two calendar days of overlap.
Worker gates: acquisition YAML and job `live:true`, `FINRA_SSV_LIVE_ACCESS=true`;
scheduled workers also require `FINRA_SSV_SCHEDULED_LIVE_ACCESS=true`.
The schedule needs `FINRA_SSV_CONFIG` and explicit comma-separated
`FINRA_SSV_SCOPES`; optional `FINRA_SSV_OPERATIONS_CONFIG` supplies reviewed
calendar evidence. Unreviewed years skip. Launch example: [SSV](../../resources/dagster-ssv.yml).
This records tested definitions, not an observed service activation. A schedule
being listed or enabled does not establish provider rights, start a persistent
service, or grant a downstream redistribution right.

## Optional Slack status sensors

Each of the six pipelines with Slack integration owns `<pipeline_name>_slack_failure` and
`<pipeline_name>_slack_success` run-status sensors, registered through the root
code location. All twelve definitions default **STOPPED**. YAML flags `slack.on_failure` (default
true) and `slack.on_success` (default false) independently select notifications. Each independently reads optional
`config/<pipeline_name>/alerts.local.yml`; missing/disabled settings send nothing.
Examples contain environment-variable names rather than webhook secrets. See the
[Slack alert runbook](slack_alerts.md) for all names, setup, network policy and
cursor/delivery limitations. On 2026-11-12 the user configured FOMC's webhook and
requested a real Dagster alert test. The manual local service is running on port
3030. As of 2026-11-13, **both FOMC status sensors are RUNNING**, with both flags
enabled. The other eight existing alert sensors and all six then-registered acquisition/import schedules
remained stopped at that historical check. FINRA adds two stopped sensor definitions; current runtime state was not rechecked here. The FOMC test
run `621d5cfc-d83e-46b2-974b-ea189c31b153` failed intentionally before acquisition
(`preflight_failed`, `live: false`); sensor tick 4 succeeded and logged delivery.
No provider acquisition or warehouse mutation was performed by that failure test.
The subsequent live 1990 rerun `cfa76508-63c2-43fa-bf05-52a0cddea51b` succeeded,
reused 16 documents and passed archive health; success-sensor tick 55 delivered
its SUCCESS alert. No acquisition schedule was enabled.

## Ownership and job behavior

### AAII

AAII owns its job, assets and schedules in
`src/market_data_pipeline/pipelines/aaii/dagster_defs.py`. See the
[AAII runbook](aaii_runbook.md) for run configuration, recovery, PostgreSQL and
retention procedures. Keep both schedules stopped until their specific access
and operational gates are satisfied.

### NAAIM

AAII reads `network` from its runtime YAML. NAAIM's optional asset setting
`network_config_path` points to a pipeline-owned network-only YAML; its stopped
schedule can pass `NAAIM_NETWORK_CONFIG`. Both select a proxy/direct route once
before capture and retain it for that run. See
[outbound HTTP settings](../development/outbound_http.md) and the
[NAAIM network example](../../config/naaim_exposure_public_delayed/network.example.yml).
These options do not activate any schedule or resume NAAIM.

NAAIM owns its job, asset, schedule, retention, health operations and local
research export in
`src/market_data_pipeline/pipelines/naaim_exposure_public_delayed/`. The daily
job creates a distinct capture event even if the provider payload is unchanged,
then applies 365-day dry-run-tested retention. It remains limited to the
three-month-delayed authorized-research feed. A separate explicit CLI export
can write a versioned local artifact for QuantMeridian; no job sends data across
repositories and QuantMeridian never contacts NAAIM. See
[Spec 007](../../specs/spec007/README.md) for permission and scope limits.

The user confirmed retaining the lagged feed. Outputs and materialization
metadata label it **NAAIM Exposure Index — 3-month-delayed public data** with
`provider_delay_months: 3` and a `delay_notice`. Always show the observation's
`as_of_date` alongside its value. CLI capture/health responses expose
`latest_as_of_date`; a healthy recent capture is not current-week positioning.
Daily polling does not remove NAAIM's three-month source delay.

### SEC FTD and exchange thresholds

SEC FTD definitions live in `sec_fails_to_deliver/dagster_defs.py`; exchange
threshold definitions live in `reg_sho_thresholds/dagster_defs.py`. Both invoke
one exact configured partition, use source-owned proxy/direct selection, and
default stopped. A definition being loadable does not authorize source access.
The SEC job does not crawl the download page. The threshold job does not invent
a date/market or treat a failed list as empty. See the
[SEC runbook](sec_ftd_runbook.md) and
[threshold runbook](reg_sho_thresholds_runbook.md).

### FOMC

FOMC owns its manual bounded acquisition/publication job and stopped overlapping-
year refresh in `fomc_meeting_minutes/dagster_defs.py`. Both call the source-owned
runner; raw acquisition precedes one coherent inventory/text/chunk promotion.
Manual configs support explicit bounded backfills or opt-in `recent_years: true`
refreshes. Both run source acquisition followed by no-HTTP archive health/readback
before materialization. Metadata exposes operation commit status, catalog IDs,
coverage and gaps; post-commit health failure fails the job while retaining the
committed evidence. Whole-run and op retries default to zero, preserving
source-owned bounded HTTP retries. Live/scheduled gates are rechecked during
execution. Nonsecret [refresh](../../resources/dagster-fomc-refresh.yml) and
[backfill](../../resources/dagster-fomc-backfill.yml) launch examples default to
`live: false`. The 1990-onward scope, finite budgets and actual bounded
acceptance are in the [FOMC runbook](fomc_runbook.md). No FOMC schedule is activated.

## Local operation

### Decision: manual local operation

On 2026-11-01, the user selected **manual local operation** rather than a
restart-at-login LaunchAgent or a hosted service. Start the command below when
daily NAAIM automation is intended to run; stopping the process, logging out or
rebooting pauses schedule evaluation. Revisit a durable service only after the
user requests it. The tracked LaunchAgent template is retained for that future
review and is not installed locally.

### Current decision: NAAIM paused

On 2026-11-03, the user explicitly paused NAAIM. The local
`naaim_delayed_daily_live` schedule is **STOPPED**. Do not run live NAAIM
captures, start this schedule or install a durable service unless the user
explicitly resumes NAAIM. On resumption, run the no-request health check first,
review the delayed-artifact requirement, then deliberately start the schedule.

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
./scripts/run_local_dagster.sh
```

The launcher loads `.env` and binds the UI to `http://127.0.0.1:3030` by default.
Set `DAGSTER_PORT` in `.env` to choose another port. A running process keeps its
existing port until restarted.

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

## Presidential research (PRES)

`presidential_rebuild_job` republishes explicitly selected local captures and
reviewed controls without source requests. `presidential_live_fetch_job` is
separate and all sources are currently blocked pending access review. The
`presidential_research_refresh` defaults **STOPPED**; actual state in any
existing Dagster instance is not assessed. Its optional configuration uses
`PRES_SCHEDULED_LIVE_ACCESS`, `PRES_WORKSPACE`, `PRES_SOURCE_CONFIG` and
`PRES_SOURCE_IDS`, `PRES_REVIEW_ID`, `PRES_PROFILE_ID` and optional
`PRES_NETWORK_CONFIG`; these settings do not override source approval. Workspaces are
operator-selected local paths; retained snapshots and backups are not pruned
by these jobs. See [operations runbook](../../specs/spec016/runbook.md).

## Update rule

When adding, removing, enabling, disabling or materially changing a Dagster job
or schedule, update this register and the relevant pipeline row in
`docs/pipelines.md` in the same change. Record the schedule's default state,
actual local state when known, provider boundary, configuration inputs, data
root and retention behavior.
