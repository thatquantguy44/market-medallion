# AAII completion checkpoint and next pipeline

Updated 2026-11-10, America/New_York. Original completion branch: `feature/pipeline-completion`.
The user explicitly selected **finish AAII first; CFTC on the next branch**.
Spec 003 supersedes the previous test-only pickup task. AAII's local pipeline
capabilities are complete; do not restart its scaffold or silently enable downloads.

## 2026-11-10 manual refresh

A user-requested one-time live `aaii_sentiment_job` completed successfully with
reported-date bounds **2026-10-13 through 2026-11-10** (28-day overlap).
Dagster run `3f037d51-a42c-4914-9bef-b35534c05ca6` produced AAII capture
`0ae569cbd96142cf9e2b68b0e14b1923`: four selected rows, two new versions for
October 29 and November 5. Current PostgreSQL and committed Parquet now contain
**2,048 versions / 2,048 reported dates**, through **2026-11-05**.
The post-run no-request health check passes with no reasons.

Capture time is `2026-11-11T02:13:21.054494Z` (November 10 in New York), not
historical publication time. Snapshot `aa3c50eadc1043f6abdaa5e2e9961761`
is selected by `parquet/current.json`. The pre-run logical backup is retained
locally under `data/backups/aaii-before-manual-20261110-021232/`.
Earlier quarantine outcomes remain in historical audit/quality evidence.

Docker Desktop was started and the existing AAII container resumed on its
persistent volume. Its existing password was saved, at the user's request, in
this project's ignored `.env` with owner-only permissions; no secret is included
here. No schedule was activated. NAAIM remains stopped and paused. Runtime data,
run YAML, backups and credentials stay outside Git. The completion evidence
below describes the original October30 baseline.

## Read first

Read root `AGENTS.md`, `README.md`, Specs 002/003, this checkpoint, `tasks/todo.md`,
the master register `docs/pipelines.md`, and `docs/deployment/aaii_runbook.md`. Use the pinned local workflow library via
`agents/agent-skills/skills/using-agent-skills/SKILL.md`. The master handoff and
roadmap retain the complete 14-source backlog; this checkpoint does not replace it.

## Verified completion

- Real synthetic binary XLS fixtures execute through the bounded live Dagster
  path with offline HTTP/clock injection. Tests verify exact bytes/provenance,
  unshifted reported dates, unchanged reruns, corrections/reversions, as-of
  results, materializations and failure preservation of the current publication.
- Configured overlapping weekly bounds, HTTP retries and Retry-After handling,
  health/status checks, and a separately gated stopped live schedule are implemented.
- Explicit local historical XLS backfill is strict by default. Opt-in quarantine
  preserves invalid cells/reasons and publishes accepted rows with partial status.
  Legacy bounded archives replay unchanged; historical policy is versioned.
- Owned-state backups include warehouse records, Bronze and the committed
  snapshot with digests. Empty-target recovery preserves versions/Gold and uses
  a distinct warehouse identity. Dry-run-first snapshot retention protects
  current publication and Bronze. SQLite and real PostgreSQL recovery pass.
- **68 tests passed**, including both isolated PostgreSQL integration tests.
  Ruff, package build, dependency and pinned snapshot checks are recorded in
  `docs/development/verification.md`. Application tests never contact AAII.

## Local data/service left in place

| Setting | Value |
| --- | --- |
| Compose project/container | `additional-market-data-live` / `additional-market-data-live-postgres-1` |
| Endpoint | `127.0.0.1:55438` (internal 5432) |
| Database/schema | `additional_market_data` / `aaii` |
| Login/effective writer | `market_data` / `aaii_writer` |
| Settings/secret | ignored `config/config.yaml`; `AAII_POSTGRES_PASSWORD` in ignored `.env` |
| Bronze/Parquet/evidence | ignored `data/aaii-live/`, `completion-verification.json` |

The initial four-date live load remains intact. An offline backfill from the
already downloaded official workbook now holds **2,046 Silver versions and
2,046 Gold dates**, 1987-07-24 through 2026-10-22. Backfill run
`28556d16d7ee4934bf80e8cd5a521120` inserted 2,042 versions. Rerun inserted zero.
Three rows (1987-06-26, 1987-07-17, 1996-01-11) were explicitly quarantined for
missing/nonnumeric shares; both backfill audits remain partial. Nine gaps over
eight days are recorded, including missing dates. Do not fill them silently.

Verified local backups: `data/backups/aaii-before-backfill-20261031T010011` and
`data/backups/aaii-completed-history`. The initial backup restored into a disposable
PostgreSQL schema; completed history restored and replayed into SQLite exactly.
These same-disk packages are local checkpoints, not an off-machine backup policy.
The disposable PostgreSQL test container is removed after checks; keep the live
service/volume available for DBeaver. No new provider request was made in this scope.
Fresh checkouts contain none of these secrets, volumes or provider workbooks.

## Semantics and operating boundaries

- XLS `survey_date` is the unshifted provider **Reported Date** compatibility
  field. Do not mix it with survey-ending dates; contracts enforce the basis.
- `aaii_gold_latest` means the latest observed version **per ingested date**.
  New weeks append dates; changed dates retain previous Silver versions.
- New historical rows became available at import time. Publication timestamps
  remain null; these are not historical point-in-time publication vintages.
- Read Parquet from `current.json`. Quarantine publishes accepted rows with
  an explicit partial result; `backfill` exits 2 and replay exits 1 for partials.
- `check` reports trailing partial/failed outcomes and stale dates/captures.
  Review the quality report; a later clean acquisition can become healthy.
- Both schedules stay stopped. Recurring permission, daemon/worker deployment,
  durable storage/secret injection and external alert routing remain prerequisites.
- The Delta adapter exists but is not live-tested in a Databricks workspace.
  Mage/Airflow wrappers and rolling research features are optional future scopes.
- Workflow pin remains `f63ec56a3cc936408d792956ae583c3c96a825bd`; never refresh
  or edit vendored files as part of provider work.

## Next scoped work: CFTC on a separate branch

After reviewing/merging the AAII completion branch, create a fresh CFTC branch
from the updated base. Begin with a new numbered spec for **TFF futures-only**,
using Spec 001 as design context. CFTC is not implemented. First verify official
endpoints/dataset IDs, schema, terms, pagination, reporting dates/publication
semantics and revisions. Capture a bounded sample and invented offline fixtures;
then define acceptance criteria before coding. No API identifier or entitlement
is assumed by the existing broad design.

Keep report families and futures-only/combined scopes separate. Preserve contract
codes as strings and use provider-specific Silver/Gold schemas, not AAII's share
schema. Reuse manifest, archive/revision/audit, warehouse and Dagster patterns only
where their contracts fit. Deliver one local end-to-end slice before widening
report families. FINRA short-sale volume and OFR STFM remain subsequent sources.
No CFTC implementation, AAII scheduling or cloud provisioning is included here.

## Repository resumption

Inspect status, branch tracking and recent commits before editing. The user
requested focused commits and push; do not merge or force-push automatically.
The remote is `https://github.com/thatquantguy44/market-medallion.git`.
The actual checkout is `pipelines/market-medallion`; its old path is a compatibility
symlink. Preserve database/volume identifiers and immutable historical metadata.
The subsequent organization task populates `docs/pipelines.md` as the master
register. `docs/handoff.md` points to this checkpoint and the master handoff.
The FX/web specifications are tracked in `specs/README.md`; neither connector
is implemented.
Use `docs/development/verification.md` for current checks and hosted CI evidence.


## Pipeline folder follow-up

The user authorized source organization after completion. All AAII implementation
now lives under `src/market_data_pipeline/pipelines/aaii/`; `shared/` contains only generic
file/YAML helpers. Root CLI and Dagster module paths still work. Internal Python
imports moved with the files; use the documented migration guide. Manifests,
parser strings, raw paths, tables, data semantics and schedule defaults are
unchanged. No live data migration or provider request is needed. The master
register is `docs/pipelines.md`; update it when another pipeline is implemented.


All future pipelines must own a separate snake_case package at
`src/market_data_pipeline/pipelines/<pipeline_name>/`. The latest namespace move
passed 72 offline tests (two PostgreSQL tests skipped), lint/formatting, build and
isolated wheel CLI/Dagster checks; all 21 moved modules retain the same behavior
excluding imports. Use `market_data_pipeline.pipelines.aaii` for pipeline-specific
imports and module commands. The verification document records the current scope.
