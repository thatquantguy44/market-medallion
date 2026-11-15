# Dagster jobs and operations

**Canonical master document for MarketMedallion's implemented Dagster jobs,
schedules, sensors, UI access and operating controls.** Start here when asking
what exists or where to view it. [README](README.md) and [agent guidance](AGENTS.md)
point to this page; the [pipeline register](docs/pipelines.md) owns broader source
scope and planned connectors. A planned connector is not a registered job.

## Open Dagster

Open **<http://127.0.0.1:3030>** on the local host. Browse **Jobs** for definitions
and launch configuration, **Runs** for history, and **Automation** for schedules
and sensors. The code location is `market_data_pipeline.orchestration.dagster_defs`.

Start the manual local service from the repository root when needed:

```bash
./scripts/run_local_dagster.sh
```

The launcher loads this project's ignored `.env`, uses `.dagster/` for local
Dagster metadata, and binds to `127.0.0.1`. Set `DAGSTER_PORT` in `.env` to change
the default port; a running process keeps its existing port until restarted.
The pinned development environment includes Dagster; a minimal installation
needs the `dagster` extra. No LaunchAgent or hosted service is activated here.

A loaded job is available to inspect or launch. Its presence does not mean it
is currently running. Schedules/sensors have separate stored activation states
and provider/live gates. Never infer instance state from a definition's default.

## Jobs

The table lists explicit jobs composed by the root definitions. Dagster's implicit
`__ASSET_JOB` family is generated for assets and is not another pipeline.

<!-- dagster:jobs -->
| Job | Purpose | Config and runbook |
| --- | --- | --- |
| aaii_sentiment_job | Import or explicitly acquire AAII sentiment and publish observed revisions | [Launch config](resources/dagster-aaii.yml), [live example](resources/dagster-aaii-live.yml), [runbook](docs/deployment/aaii_runbook.md) |
| finra_short_sale_volume_job | Bounded admitted TRF acquisition, verified publication and optional research export | [Launch config](resources/dagster-ssv.yml), [pipeline config](config/finra_short_sale_volume/README.md), [runbook](docs/deployment/ssv_runbook.md) |
| fomc_meeting_minutes_job | Bounded FOMC backfill or recent-year refresh, publication and archive health | [Backfill](resources/dagster-fomc-backfill.yml), [refresh](resources/dagster-fomc-refresh.yml), [runbook](docs/deployment/fomc_runbook.md) |
| naaim_delayed_capture_job | Capture the three-month-delayed research feed; **paused by user** | [Config](config/naaim-delayed.example.yml), [contract](specs/spec007/README.md) |
| presidential_live_fetch_job | Gated source refresh and publication; live sources remain blocked pending review | [Source review](specs/spec016/source_access_review.md), [runbook](specs/spec016/runbook.md) |
| presidential_rebuild_job | Rebuild from explicitly selected local captures and reviewed controls | [Runbook](specs/spec016/runbook.md), [contract](specs/spec016/README.md) |
| reg_sho_threshold_list_job | Acquire and validate one configured Nasdaq/NYSE threshold partition | [Config](config/reg_sho_thresholds/README.md), [runbook](docs/deployment/reg_sho_thresholds_runbook.md) |
| sec_fails_to_deliver_job | Acquire and validate one exact configured SEC FTD partition | [Config](config/sec_fails_to_deliver/README.md), [runbook](docs/deployment/sec_ftd_runbook.md) |
<!-- /dagster:jobs -->

Each pipeline owns its implementation under
`src/market_data_pipeline/pipelines/<pipeline_name>/dagster_defs.py`.
[Root composition](src/market_data_pipeline/orchestration/dagster_defs.py) exposes
only implemented definitions. Provider transforms/storage remain callable without
Dagster; see [the orchestration decision](docs/development/adr001-dagster.md).

## Schedules

These are **code defaults**, checked against the definitions. Cron uses the
listed time zone, including daylight-saving changes. See the dated runtime check
below for observed activation. A cadence is not a provider publication guarantee.

<!-- dagster:schedules -->
| Schedule | Job | Cron | Time zone | Default |
| --- | --- | --- | --- | --- |
| aaii_thursday_local_import | aaii_sentiment_job | 0 10 * * 4 | America/New_York | STOPPED |
| aaii_weekly_live | aaii_sentiment_job | 0 10 * * 4 | America/New_York | STOPPED |
| finra_short_sale_volume_weekday_refresh | finra_short_sale_volume_job | 15 9 * * 1-5 | America/New_York | STOPPED |
| fomc_meeting_minutes_weekday_refresh | fomc_meeting_minutes_job | 10 17 * * 1-5 | America/New_York | STOPPED |
| naaim_delayed_daily_live | naaim_delayed_capture_job | 0 10 * * * | America/New_York | STOPPED |
| presidential_research_refresh | presidential_live_fetch_job | 0 9 * * 1 | America/New_York | STOPPED |
| reg_sho_threshold_lists_daily_live | reg_sho_threshold_list_job | 30 23 * * 1-5 | America/New_York | STOPPED |
| sec_fails_to_deliver_daily_live | sec_fails_to_deliver_job | 30 8 * * * | America/New_York | STOPPED |
<!-- /dagster:schedules -->

### Activation controls

Use each linked runbook for exact launch fields and resource limits. Configuration
and worker environment are pipeline-owned; never copy another project's settings.

| Pipeline | Required operating boundary |
| --- | --- |
| AAII | Local import takes an authorized file. Live scheduling requires `MARKET_MEDALLION_CONFIG` and `operations.scheduled_live_access: true`; recurring access remains separately unverified. |
| FINRA short volume | Acquisition YAML and job `live: true`, plus `FINRA_SSV_LIVE_ACCESS=true`. Scheduled workers additionally require `FINRA_SSV_SCHEDULED_LIVE_ACCESS=true`, `FINRA_SSV_CONFIG`, explicit `FINRA_SSV_SCOPES` and a reviewed calendar. The default calendar ends in 2026; API release timing is unknown. |
| FOMC | Manual acquisition requires `live: true`. Scheduling additionally requires `FOMC_CONFIG`, `FOMC_OPERATIONS_CONFIG` and `scheduled_live_access: true`; inspect retained operation evidence on failure. |
| NAAIM | **Paused since 2026-11-03. Do not capture or restart without explicit user direction.** The retained public feed is delayed three months; capture freshness is not current-week positioning. |
| Presidential research | Offline rebuild needs explicit capture/review/profile IDs. Live fetch remains blocked until source-specific review; `PRES_SCHEDULED_LIVE_ACCESS`, workspace/source/review/profile settings do not override that gate. |
| Reg SHO thresholds | Reviewed provider use, local acquisition opt-in, `REG_SHO_USER_AGENT`, `REG_SHO_SCHEDULED_LIVE_ACCESS=true`, `REG_SHO_CONFIG` and an exact `REG_SHO_PARTITION`. |
| SEC FTD | Local acquisition opt-in, `SEC_FTD_USER_AGENT`, `SEC_FTD_SCHEDULED_LIVE_ACCESS=true`, `SEC_FTD_CONFIG` and an exact `SEC_FTD_PARTITION`. |

Starting the UI does not authorize acquisition or enable a stopped schedule.
Keep recurring activation separate from implementation/merge acceptance. Live
access and secrets belong in private configuration/environment. Inspect current
state before starting anything; no automatic enablement is part of documentation
maintenance. FOMC/FINRA use source-owned HTTP retries and zero whole-operation
retries. Data roots, backups and retention are governed by the pipeline runbooks;
there is no shared deletion policy inherited from NAAIM.

## Alert sensors

These are **definition defaults**, independent of acquisition schedules. Each
listed sensor also needs an enabled pipeline alert YAML and the selected status
flag before delivery. Pipeline prefixes match `config/<pipeline_name>/`.
Presidential research has no registered Slack sensors.

<!-- dagster:sensors -->
| Sensor | Type | Default |
| --- | --- | --- |
| aaii_slack_failure | RUN_STATUS | STOPPED |
| aaii_slack_success | RUN_STATUS | STOPPED |
| finra_short_sale_volume_slack_failure | RUN_STATUS | STOPPED |
| finra_short_sale_volume_slack_success | RUN_STATUS | STOPPED |
| fomc_meeting_minutes_slack_failure | RUN_STATUS | STOPPED |
| fomc_meeting_minutes_slack_success | RUN_STATUS | STOPPED |
| naaim_exposure_public_delayed_slack_failure | RUN_STATUS | STOPPED |
| naaim_exposure_public_delayed_slack_success | RUN_STATUS | STOPPED |
| reg_sho_thresholds_slack_failure | RUN_STATUS | STOPPED |
| reg_sho_thresholds_slack_success | RUN_STATUS | STOPPED |
| sec_fails_to_deliver_slack_failure | RUN_STATUS | STOPPED |
| sec_fails_to_deliver_slack_success | RUN_STATUS | STOPPED |
<!-- /dagster:sensors -->

The optional file is `config/<pipeline_name>/alerts.local.yml`, or the path named
by `<PIPELINE_NAME>_ALERTS_CONFIG`. `slack.enabled`, `on_success` and `on_failure`
control delivery independently. Webhooks remain in environment variables.
[Slack configuration and delivery limits](docs/deployment/slack_alerts.md) explain
setup, cursor behavior and failure handling; that guide does not own the inventory.

## Current local state

**Last observed: 2026-11-15 06:23 America/New_York.** The local UI responded at
<http://127.0.0.1:3030> and all eight explicit jobs were loaded. All eight
acquisition/import schedules were STOPPED. Only the two FOMC Slack sensors were
RUNNING; the other ten alert sensors were STOPPED. No active/queued runs were
present at the recovery check. This is a dated observation, not a live status feed.

The unreachable user-code server was recovered by restarting the existing manual
launcher. Two event-log files initially produced SQLite read errors, then passed
full integrity checks without repair/replacement. No metadata reset, job launch,
source acquisition or schedule activation was performed. The transient read cause
was not established. Stored activation settings and history were preserved.

For today's state, inspect the running UI; source/default checks below do not
contact the instance. If the UI has no jobs, check its code-location health first.
Preserve `.dagster/` and identify active/queued runs before restarting. Use a
read-only integrity check before considering metadata recovery; never silently
reset a history database. The manual launcher does not provide restart-at-login
or reboot persistence. The retained LaunchAgent template is a separate future
activation decision.

## Keep this document current

**DAGSTER.md is the only master inventory.** Other guides link here and retain
pipeline-specific configuration or acceptance evidence. The old
[register path](docs/deployment/dagster_jobs.md) is a compatibility pointer.

In every change that adds, removes, renames or materially changes a job, schedule,
sensor, cron/time zone, default status, config path or activation gate:

1. Update this document in the same change, including useful runbook/config links.
2. Keep README/AGENTS links pointing here; update pipeline scope/status in
   [docs/pipelines.md](docs/pipelines.md) and the current
   [master handoff](docs/handoffs/project_bootstrap.md#agent-start-here) when needed.
3. Run the offline inventory check, then follow the handoff's staged review flow:

```bash
.venv/bin/python scripts/verify_dagster_docs.py
.venv/bin/python -m pytest -q tests/test_dagster_docs.py
```

The checker compares the marked tables with the actual root-composed definitions:
job/sensor additions, removals/renames, schedule job/cron/time zone/default changes,
sensor type/default changes, missing navigation and broken local links fail CI.
It imports definitions only: no `.env` loading, Dagster instance access, provider
requests or activation changes. Use `--show-current` for safe definition facts
when repairing a mismatch. Human descriptions, rights/config controls and dated
runtime observations still require an actual review; the checker does not invent
provider permission or runtime health. Full CI runs it after dependencies install.

This single-page contract is the bounded documentation-cleanup scope: preserve
existing definitions/activation, make the current inventory discoverable from root
navigation, and prevent source/document drift. Verification covers invented drift fixtures and the real composed definitions;
runtime observations require a separate actual check.
