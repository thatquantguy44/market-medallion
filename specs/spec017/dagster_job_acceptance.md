# FOMC Dagster job acceptance

Accepted locally 2026-11-12 on `feat/fomc-dagster-job`, based on merged PR #14
(`3bbb0c1426f399b7ec85d6f2eea9ed4d32cb49ef`). Implementation head: `dd3913c`.
The user narrowed deployment to a Dagster job in the existing local Mac/Docker
environment. [Contract](dagster_job_contract.md),
[sanitized evidence](evidence/dagster_job.json) and
[launch/recovery runbook](../../docs/deployment/fomc_runbook.md#dagster) are current.

## Delivered behavior

The existing root code location registers `fomc_meeting_minutes_job`, selecting
only `fomc_meeting_minutes_refresh`. Explicit bounded backfills and the legacy
1990 default remain supported. Opt-in `recent_years: true` with `mode: refresh`
resolves the New York calendar year and configured overlapping prior years at
execution, including UTC/New York New Year boundaries. Mixed/partial bounds,
future floors and closed access gates fail before SQL or HTTP probes.

The job invokes the existing finite source runner, then requires healthy local
archive readback with matching catalog pointers before materialization. Events
expose operation ID, commit status, request/byte counters, catalog IDs, coverage
and source gaps. Partial acquisition/publication failures and post-commit health
failures fail the job with distinct safe metadata. Source originals and operation
evidence remain available; committed data is not falsely reported as rolled back.
Unexpected exception details/context do not enter Dagster events. Run/op automatic
retries default to zero; source-owned retries and the advisory lock remain in force.

Nonsecret [refresh](../../resources/dagster-fomc-refresh.yml) and
[backfill](../../resources/dagster-fomc-backfill.yml) examples validate against the
actual job schema and default to `live: false`. The stopped schedule shares the
recent-year calculation. No new worker service or recurring activation was added.
Alert and off-machine backup destinations remain configurable future work.

## Verification

The repository-local agent-skills workflow was followed: contract, failing
behavioral tests, thin implementation, regression, review and incremental commits.
The pinned workflow snapshot and all 61 adopted QuantSmith Git objects were
verified. The external QuantSmith working tree still differs from the pin and
was left unchanged. Dagster APIs were checked against official documentation and
the installed 1.12.21 runtime; no dependency or schema change was needed.

- **991 regression tests passed, 80 optional skips.** There are 37 added offline
  cases and nine added backend cases. Initial failing checks covered the missing
  recent-window helper, job behavior, launch files, missing-tick handling and prior
  pointer metadata before implementation.
- **313 FOMC tests passed with isolated PostgreSQL enabled**, including 70 backend
  checks. The nine new real Dagster/backend cases exercise acquisition→raw/text/
  chunks→health, repeat/reuse, refresh, retained 429 retries, source rejection,
  request/byte budgets, SQL rollback, post-commit health/exception failures,
  concurrent-run refusal and recent-window audit. Provider transport is offline;
  each test owns a disposable schema rather than the operational schema.
- Actual root `dagster job list` includes the FOMC job and asset. Local schedule
  listing reports all six schedules **STOPPED**, including FOMC and paused NAAIM.
- Launch YAML validation/closed-gate execution, Ruff lint/format, diff checks,
  wheel/source builds, source artifact contents and local documentation links pass.

## Operational readback and limits

No live provider request, migration or further historical backfill was performed.
A no-request health readback remains **healthy** with the same last completed
1993–1994 operation, 146 captures, 29 owned tables and 993 chunks (368 eligible,
625 review-flagged). All three selected catalog IDs match the prior acceptance.
Gaps remain 399 unacquired references, 49 not-listed claims, one unavailable href
and 51 review cases. Complete selected-unit coverage is not full historical
coverage or text/OCR certification; both certification flags remain false.

The accepted deliverable is the observable Dagster job. Persistent worker
installation, schedule activation, external alerting and off-machine backups are
not represented as deployed. The next separately bounded historical operation
remains 1995–1996, preserving source kinds, first-seen clocks and review flags.
