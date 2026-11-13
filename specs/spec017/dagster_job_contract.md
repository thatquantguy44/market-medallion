# FOMC Dagster operating job

User-selected scope: expose and verify the FOMC pipeline as a Dagster job in the
existing local Mac/Docker environment. Reuse `fomc_meeting_minutes_job` and its
asset, registered through the repository code location. Deployment is this job;
external alert and off-machine backup destinations are deferred and configurable
later. No new container stack, daemon installation, data migration or historical
backfill is required by this continuation.

## Behavior and compatibility

- Preserve the job/asset names, explicit bounded-year configurations, live gate,
  schedule gate, 1990 floor and existing manual default of 1990 backfill.
- Add opt-in `recent_years` for `mode: refresh`, selecting the current New York
  year and configured overlapping prior years at execution time. Reject mixed
  explicit bounds, naive clocks, a future operating floor and invalid ranges
  before database access or HTTP probing. Omitted bounds in ordinary mode retain
  the prior 1990 default; partially supplied bounds fail closed.
- Share recent-year arithmetic between the job and stopped weekday schedule.
  Preview remains configuration-only, and execution rechecks live/scheduled gates.
- After source acquisition and coherent publication, run the existing no-HTTP
  archive health/readback. Emit a successful materialization only for a completed
  operation and healthy archive. Source gaps and review counts remain visible;
  health never certifies full history or OCR accuracy.
- Incomplete runs fail the Dagster job with operation ID, safe error code,
  source/request counters and prior/after catalog pointers. Post-commit readback
  failures also fail the job, retain the committed operation/catalogs for recovery,
  and do not guess a rollback. Unexpected exception text/context must not leak
  credentials into Dagster events. Never wrap process interrupts as success.
- Keep source-owned bounded retry/pacing. Disable job-wide automatic run retries
  and op retries by default; a deliberate new run can reuse verified originals.
  The existing PostgreSQL advisory lock serializes FOMC operations across workers.
- Provide checked-in, nonsecret launch YAML for recent refresh and explicit
  historical backfill. Keep live access false in examples and schedules stopped.
  The existing local Dagster instance can load the job; remote integrations remain
  deferred rather than represented as deployed.

## Verification and delivery

Follow repository-local agent-skills: specify first, write failing behavioral
checks, implement thin increments, then review/commit and run regression/build.
Pure tests cover year/overlap/floor/timezone boundaries. Real Dagster executions
cover closed/malformed config, sanitized exceptions, materialization metadata and
retry policy. Isolated PostgreSQL + offline provider fixtures cover the complete
job→acquisition→raw/text/chunk→health flow, repeated backfill, retries, rollback,
source/budget failures, post-commit health failures and concurrent-operation
refusal. Tests never require provider HTTP or mutate the operational schema.

Verify the root code-location job registration, launch YAML, full regression,
isolated FOMC integration, lint/format and source distribution. Document local
readiness honestly: the job and its observable failure states can be verified;
this does not imply an always-on daemon, activated recurrence, external alerts,
off-machine backups, completed history or certified text/OCR.

Official Dagster job/ops/retry APIs are checked against documentation and the
installed 1.12.21 package; fetched online docs label their 1.12 line as 1.12.8:
https://dagster.io/docs/api/dagster/jobs
https://dagster.io/docs/api/dagster/ops
