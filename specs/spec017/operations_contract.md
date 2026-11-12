# FOMC-T06 — bounded acquisition and operations

Traces FOMC-AC06 and preserves AC02–05/07/12. Start from merged PR #11 on
`feat/spec017-fomc-operations`; scope remains 1990 onward. This slice implements
operator commands, source-owned run receipts, bounded acquisition/publication,
no-request health, and Dagster definitions with a stopped schedule. The first
live acceptance batch is 1990. Full-history acquisition remains separate progress.

## Inputs and bounds

An explicit live flag, valid finite year range/mode, validated pipeline network
settings and initialized owned storage are required before route selection. Local
configuration, plan, health, recovery, replay and schedule preview do not probe.
Operation settings pin maximum selected years, inventory pages, document/addendum
URLs, assets, HTTP requests, total received bytes, elapsed time and attempts.
Reserve two startup route-probe requests, then count every attempted data GET,
including retries. Source-owned pacing/backoff and numeric/date Retry-After obey
finite delay/deadline bounds. Stop rather than retry earlier than Retry-After.
The elapsed budget is checked before work and before publication; individual HTTP
calls retain the run client's configured timeout. It is not an OS hard-time limit.

Follow explicit current/year index links; never synthesize document URLs. Refresh
both indexes and selected historical pages. Reuse verified archived nonselected
inventory pages to preserve the prior source-page union; fetch missing newly linked
pages under the inventory/request budgets. Carried pages retain their capture
clocks and do not acquire a new freshness claim. Unsupported grammar/missing pages
block promotion. Bootstrap may need the complete in-scope inventory page set.

`backfill` reuses verified accepted valid originals while filling selected-year
references; `refresh` re-fetches selected originals/dependencies to discover
revisions. Both refresh inventories. Recurring refresh selects overlapping recent
years and never permanently skips a completed year. Whole bundled artifacts and
all explicit meeting associations remain intact. Acquire declared addenda/assets
recursively under finite limits; unsupported/cyclic dependencies block success.

Every response, including failed/partial bodies, retains its Bronze capture/attempt.
The aggregate byte guard reduces the per-response allowance to preserve the
existing limit-plus-one rejected-prefix contract without exceeding the total.
Only HTTP 429/500/502/503/504 and transport failures are retried; each attempt is
retained. Rejected, malformed or unsupported documents never become successful
catalog inputs. Source-owned unavailable/not-listed outcomes remain distinct.

## Coherent publication and audit

Run intent and append-only progress/final records mirror immutable filesystem
artifacts in separately marked pipeline-owned operation tables. No credentials,
proxy URLs or exception details enter receipts. Capture recovery and operation-
receipt recovery reuse retained bytes/IDs with no network; an interrupted run is
never guessed complete. Retry as a new bounded run can reuse verified originals.

Capture raw responses before the publication transaction. Preview the exact
native inventory union without changing pointers. Link retained primary captures
and inherited dependencies to the new snapshot; preserve known successful capture
history and actual first-seen clocks. Publish inventory, text and chunk pointers
in one outer PostgreSQL transaction. Failure/budget exhaustion leaves all previous
pointers intact; local derivative artifacts remain reusable. Old snapshots and
removed source claims remain queryable, with exclusions recorded in run outcomes.

Health verifies current mirrors and reports archive/text/chunk coverage, source
outcomes, review eligibility, current-pointer coherence, latest run status and
actual run freshness. An empty or interrupted installation is explicit. Health
opens no HTTP route. No completeness/accuracy claim follows merely from a healthy
last bounded run.

## CLI and orchestration

Provide source module and root `fomc` CLI routes: validate-config, migrate, plan,
run, health, recover-capture and recover-run. Migration is explicit and must follow
operator backup/restore verification. Dagster owns a manual bounded job and a
STOPPED weekday refresh schedule. Scheduled access requires a separate false-by-
default operation setting, checked both in preview and execution. No schedule is
activated by implementation or live smoke verification. NAAIM remains paused.

Acceptance: invented offline transport/failure fixtures; real isolated PostgreSQL
atomic publication/recovery tests; CLI/Dagster no-probe gates; a bounded 1990 run
with raw/text/chunk/readback and before/after restore plus replay evidence; then
regression, lint/format, build/artifact checks and reviewed branch/PR tracking.

Official sources checked 2026-11-12:
- https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm
- https://www.federalreserve.gov/disclaimer.htm
- https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3
- https://dagster.io/docs/api/dagster/schedules-sensors

The installed Dagster pin is 1.12.21. The fetched API documentation labels its
1.12 line as 1.12.8; used signatures are additionally checked against the installed
pinned package. Provider notices and existing OCR/native-text review flags remain.
