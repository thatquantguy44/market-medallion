# SSV-T13 — Release health and retention accounting

SSV-NFR02 / SSV-AC13. Implemented on `feat/finra-health-retention`, based on
merged T11–T12 main `12edd0f`. No acquisition, publication, recovery, deletion,
Slack delivery or orchestration is performed by these commands.

## Official evidence reviewed 2026-11-14

- [FINRA reporting calendar](https://www.finra.org/filing-reporting/market-transparency-reporting/holiday-calendar)
  lists ten full closures in 2026 and early closes on November27/December24.
  The default calendar is bounded to 2026, with weekends closed. Early closes
  remain reporting dates. There is no extrapolation to another year, no federal
  banking calendar, and no inference from an empty query.
- [Daily-file site](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files)
  gives a same-day 18:00 Eastern file-posting target, permits later corrections
  and describes legitimate empty ADF files. That target is **not adopted as an
  API SLA**. ADF/TXT remain excluded from the selected API profile.
- [Reg SHO API documentation](https://developer.finra.org/docs#reg-sho-daily-short-sale-volume)
  specifies rolling twelve-month availability without a dataset-specific daily
  release deadline. This availability window is not a local retention limit.
- [Equity-specific terms, section6](https://developer.finra.org/specific-terms-equity-data)
  add no dataset-specific retention condition; the general/API terms and existing
  approved-use admission still apply. No deletion period is approved here.

## Configuration and time

The separate strict `finra-ssv-operations-policy/1` YAML is optional. The checked-in
`config/finra_short_sale_volume/operations.example.yml` matches the default policy.
It holds a bounded calendar interval, explicit closed dates and evidence reference,
nullable API release time/reference, local capture-age threshold and the sole
supported retention mode `PRESERVE_ALL`. Unknown/duplicate keys, invalid dates,
booleans as numeric thresholds and any deletion policy refuse before store reads.
An API deadline requires its own nonempty evidence reference. An operator setting
is an assertion to review, not a FINRA guarantee. Time is America/New_York with
DST; the 24-hour default is a configurable local capture-age threshold.

`health` requires explicit start/end dates (1–366 calendar days) and disjoint
manifest scopes. It does not enforce the live rolling-year acquisition window:
older retained local observations remain inspectable. Missing roots stay absent.

Per-partition output keeps these dimensions separate:

- `coverage`: COMPLETE, VALID_EMPTY or MISSING from committed membership. Empty
  means a verified complete zero-row response, never an imputed symbol value.
- `latest_local_attempt_status`, `last_local_success_at`, `last_capture_at` and
  `stale_capture`: terminal ingestion evidence and actual retrieval/archive age.
- `refresh_status`: LOCAL_ONLY/MISSING without journals, SUCCESS, FAILED, RUNNING
  or AMBIGUOUS from source run evidence. Equal latest start clocks are ambiguous;
  UUID ordering never proves a later refresh. Later no-response/auth/probe failure
  remains visible over valid old Gold. A later successful run resolves failure.
- `last_acquisition_success_at` and `trailing_failure`: source run clocks/IDs,
  without credentials or free-form transport diagnostics. Replay alone does not
  establish a successful original network refresh.
- `release_status`: CLOSED within calendar evidence, UNKNOWN_CALENDAR outside it,
  UNKNOWN_API_TIMING without an API deadline; with evidence, PENDING, OVERDUE or
  OBSERVED. A capture before that deadline remains `pre_release_capture`, including
  valid empties; it cannot satisfy a later overdue check. Publication time remains
  unknown in the original facts; expected release time is separate policy.

Overall health is DEGRADED for stale capture, unresolved raw capture/attempt,
failed/running/ambiguous refresh, overdue release or pending/lagging publication;
UNKNOWN for remaining release uncertainty; PENDING for an otherwise sound pending
release; HEALTHY otherwise. CLI exits2 for DEGRADED/UNKNOWN,0 for PENDING/HEALTHY.
NOT_PUBLISHED is explicit and does not itself fail a local-only pipeline: publishing
is a separate operator command. Invalid/corrupt evidence refuses with CLI exit2.

## Read isolation, integrity and resource bounds

Readers open existing lock files read-only, take nonblocking shared locks in
acquisition → archive → ingestion order, and refuse a busy writer. They use SQLite
read transactions and existing warehouse/catalog verifiers. They never initialize
locks/stores, write parse outcomes, resume interrupted runs or recover pointers.
Bound root/manifest/kind identities, raw bytes/receipts/outcomes, allocator slots,
version/membership hashes, acquisition plans/events/results and frozen typed files
must reconcile. Every catalog generation, including COMMITTED staged work, remains
protected. Current publication lag compares the selected pointer's coverage with
operational partitions; older generations do not make current publication fresh.

Default bounds are1000 captures/runs,10000 total events,50000 filesystem entries,
250000 operational rows and250000 frozen rows across generations,20MiB per raw
capture,128MiB total root files/state and at most1000 generations. CLI overrides
are bounded. Exceeding a limit refuses; no silent truncation. Symlinks, special
files and damaged bound stores refuse. Hashes/accounting include every root file,
including unknown artifacts, SQLite and publication files. Health is linear in
retained evidence within these finite budgets, not a lightweight liveness probe.

## Retention dry run

`retention-dry-run` has **no apply/delete flag**. All files and versions remain
held under PRESERVE_ALL; `deletion_enabled=false`, `deletable_bytes=0`. This is
conservative accounting under existing admitted use, not a new legal retention
opinion. It never inherits NAAIM's365-day rule.

Each raw capture reports payload bytes, current-partition, version-origin,
observation-membership, attempt-audit, acquisition-journal and frozen-publication
reasons where applicable. Collections protect **all original pages in both ordered
passes**, including evidence-only verification pages. Unresolved captures stay held.
Versions retain origin IDs and publication-generation references. File inventory
reports exact bytes/hashes and KEEP reasons; total root bytes count each file once,
including database/catalog/metadata overhead. Logical versions are counted
separately rather than pretending each SQL row has a physical byte allocation.
An unknown artifact is preserved; its presence never creates a deletion candidate.

Deletion, finite retention/backup policy, broader provider coverage and release SLA
certification need their own reviewed scope. T14 interchange and T15 stopped jobs
remain next; G1/G2 are not closed by local health evidence.
