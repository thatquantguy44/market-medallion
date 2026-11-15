# SSV-T15 — stopped orchestration acceptance

Traces SSV-NFR03 / SSV-AC15. Built after T14 commit `2b6fc3b` on
`feat/finra-research-orchestration`.

`finra_short_sale_volume_job` invokes the existing bounded admitted source
runner, then verified publication for the requested end date, then optional T14
export. Whole-job/op retries are zero; source-owned finite HTTP retry policy
remains authoritative. No custom untested resource is registered.

The worker requires job `live:true`, acquisition YAML `live:true` and
`FINRA_SSV_LIVE_ACCESS=true`. Scheduled requests also carry `scheduled:true`;
`FINRA_SSV_SCHEDULED_LIVE_ACCESS=true` is checked during worker execution.
Environment gates are rechecked before publication/export. Disable gates to
refuse queued/future runs; cancel a running Dagster run to interrupt in-flight
work. Accepted captures/publications are retained after downstream failures.
Private exceptions are replaced with a fixed phase label outside exception
handling so Dagster cannot serialize their sensitive context.

`finra_short_sale_volume_weekday_refresh` defaults STOPPED, proposes a previous
reviewed reporting day at 09:15 America/New_York weekdays, and refreshes two
calendar days of correction overlap. This is an operating cadence, not an API
release SLA. Missing scope/config/gates or an unreviewed calendar skips. The
default policy ends 2026-12-31. Root composition includes the implemented job
and two optional stopped Slack sensors; disabled source-owned alert YAML is
provided. `resources/dagster-ssv.yml` defaults `live:false`.

21 deterministic Dagster tests cover root definitions, stopped defaults, worker
gates/YAML refusal, scheduled configuration and prior reporting-day/holiday bounds,
calendar expiry/overlap refusal, injected acquisition→publication→export, incomplete
acquisition, revoked gates, export admission before requests, committed-pointer
preservation after export failure and sanitized error events. No provider request,
Slack message, daemon start or schedule activation occurred in T15. Full FINRA
regression count and build/source checks are recorded at T16.

Reviewed [Dagster schedule semantics](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/schedule_definition.py)
and installed pinned Dagster1.12.21 interfaces before implementation.
