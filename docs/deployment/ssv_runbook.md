# FINRA daily short-sale-volume operations

The selected API profile supports Nasdaq Carteret TRF, Nasdaq Chicago TRF and
NYSE TRF. Only an explicitly acquired date/facility set is verified coverage.
See [delivery acceptance](../../specs/spec012/acceptance.md),
[local command details](ssv_local_runbook.md),
[configuration](../../config/finra_short_sale_volume/README.md) and
[operations contract](../../specs/spec012/operations_contract.md).
The operator launching the run owns its access admission, credentials, bounds and
response to retained failure evidence. The master handoff names the active work.

## Configure and preview

Copy the pipeline and acquisition examples into this project's ignored local
YAMLs. Choose a pipeline-owned root, the verified manifest, accepted Public API
terms/reference and explicit grants. Acquisition requires `acquire` and
`transform`; local inspection/replay uses `local_import` plus `transform`;
provider exports additionally require `internal_export`. Keep the purpose
`personal_noncommercial_internal_research`. Credential fields name environment
variables; credentials stay in the worker environment. Use this project's trusted
`.env` only when explicitly loading it in the invoking shell. The portable script
and libraries do not automatically load it.

```bash
./scripts/run_finra_short_sale_volume.sh validate-config \
  --config config/finra_short_sale_volume/pipeline.local.yml
./scripts/run_finra_short_sale_volume.sh acquisition-plan \
  --config config/finra_short_sale_volume/acquisition.local.yml \
  --start-date 2026-11-09 --end-date 2026-11-09 --scope listed_trfs --overlap-days 0
```

Planning makes no provider request, loads no credentials and creates no root.
Resource limits cover expanded dates, partitions, requests/retries, page sizes,
response/total bytes, wait and duration. Review them against expected counts.
Multi-page partitions require two complete agreeing passes; every original page
is retained. Refresh overlap intentionally re-fetches accepted dates to observe
corrections. It does not skip a date merely because a prior run succeeded.

## Manual execution

CLI acquisition requires acquisition YAML `live:true` and explicit `--live`:

```bash
./scripts/run_finra_short_sale_volume.sh acquire \
  --config config/finra_short_sale_volume/acquisition.local.yml \
  --start-date 2026-11-09 --end-date 2026-11-09 --scope listed_trfs --overlap-days 0 --live
./scripts/run_finra_short_sale_volume.sh publish \
  --config config/finra_short_sale_volume/pipeline.local.yml \
  --trade-date 2026-11-09 --scope listed_trfs
./scripts/run_finra_short_sale_volume.sh export-research \
  --config config/finra_short_sale_volume/pipeline.local.yml \
  --scope listed_trfs --output-use personal_noncommercial_internal_research \
  --destination data/research/ssv-2026-11-09-generation.json
```

The export parent must exist outside the source root. Use a new filename for a
new generation; identical retries are idempotent and differing bytes refuse.
The export verifies the entire committed generation and rejects pending recovery,
wrong scope or disallowed use. It never reads a new mutable-warehouse projection.
Consumer readback:

```python
from market_data_pipeline.pipelines.finra_short_sale_volume.research_export import read
artifact = read("data/research/ssv-2026-11-09-generation.json",
                expected_scopes=["listed_trfs"],
                output_use="personal_noncommercial_internal_research")
```

The source-owned Dagster job `finra_short_sale_volume_job` performs acquisition,
verified end-date publication and optional research export in that order. Use
[the launch example](../../resources/dagster-ssv.yml) with an exact bounded date
range and scope. Job `live:true`, acquisition YAML `live:true` and worker
`FINRA_SSV_LIVE_ACCESS=true` are all required. Optional export use is admitted
before acquisition. `live:false` in the checked-in example deliberately refuses.
Ordinary CI exercises the job using invented injected HTTP, without credentials.

Materialization records the actual acquisition status/counts, committed generation,
row counts, scope, end date and export outcome. A failed downstream step fails the
job even when earlier captures/publication committed. Inspect phase metadata and
retained evidence before retrying; no failed run produces a success materialization.
Private exceptions are replaced with fixed phase messages. HTTP retries belong
to the bounded source runner; Dagster op/job retries are zero.

## Stop and restart

Remove/set false `FINRA_SSV_LIVE_ACCESS` in the worker environment and reload the
code location/workers to refuse queued/future manual or scheduled execution.
For scheduled work also remove `FINRA_SSV_SCHEDULED_LIVE_ACCESS` and stop
`finra_short_sale_volume_weekday_refresh` in Dagster. The worker rechecks both
applicable gates before publication/export. Cancel the actual running Dagster run
to interrupt in-flight work; environment changes alone do not interrupt an active
HTTP request. The finite source duration/request budgets still apply. CLI `acquire`
uses its own YAML/`--live` gates and is not governed by the Dagster environment gate.

After interruption, inspect audit/acquisition journals, health and publication
state using the local CLI. Retained accepted partitions remain valid when a later
attempt fails. Do not delete journals/captures or fabricate a success to clear a
lock. Locks are nonblocking and released on process exit; identify a surviving
writer before restarting. Repair publication with the commands below; re-run the
explicit bounded date range only after classifying the retained source evidence.

## Publication recovery and offline replay

```bash
./scripts/run_finra_short_sale_volume.sh published \
  --config config/finra_short_sale_volume/pipeline.local.yml
./scripts/run_finra_short_sale_volume.sh recover-publication \
  --config config/finra_short_sale_volume/pipeline.local.yml
./scripts/run_finra_short_sale_volume.sh replay --source-root data/ssv-source \
  --config config/finra_short_sale_volume/replay.local.yml
```

Recovery finishes the exact catalog-COMMITTED staged generation, not a projection
of newer ingestion. Corrupt/missing committed bytes block recovery and new
publication; restore a consistent retained copy rather than creating replacement
evidence. Readers follow `parquet/current.json`, never glob every generation.
If an export fails after publication, recover if necessary, verify `published`,
then rerun `export-research` with an admitted unused/identical destination. A
conflicting output is retained unchanged for review.

Replay needs original Bronze **and the verified terminal warehouse**. It is a
reproducible rebuild, not a database-free backup. Choose an absent/empty disjoint
destination root and grant its original input kind. Input/row/byte limits must
admit the original parser profile, including derived collections. Replay retains
original first-seen clocks, versions, membership and audit, with a fresh warehouse
identity. Publication is explicit afterward. It never contacts FINRA or probes a
proxy. Keep the entire root, including raw originals, collection inputs, warehouse,
publication catalog/files and acquisition journal, in a consistent backup made
while writers are quiesced; there is no automated off-machine backup service in
this slice. Never copy a live SQLite file as if it were a consistent backup.

## Health and retention

```bash
./scripts/run_finra_short_sale_volume.sh health \
  --config config/finra_short_sale_volume/pipeline.local.yml \
  --start-date 2026-11-09 --end-date 2026-11-09 --scope listed_trfs
./scripts/run_finra_short_sale_volume.sh retention-dry-run \
  --config config/finra_short_sale_volume/pipeline.local.yml
```

Both commands are offline and preserve source/store bytes. UNKNOWN_API_TIMING is
honest uncertainty, not proof of lateness or an API SLA. The verified calendar is
bounded to 2026 by default; later years need reviewed policy evidence. The daily
file site's18:00 target is not an API deadline. Pending/closed/valid-empty/missing,
stale capture, later failed refresh, publication lag/recovery and corruption are
distinct. Replay with no transport journal reports LOCAL_ONLY. CLI UNKNOWN or
DEGRADED exits2; inspect JSON rather than assuming every nonzero code is corruption.
Retention is PRESERVE_ALL dry run only; every referenced original/verification
page and observed version remains held. No deletion/apply mode is approved.

## Recurring operation and optional alerts

The weekday refresh defaults **STOPPED**. G3 activation is a separate operator
review/decision; this implementation does not enable it. The proposed cadence is
09:15 America/New_York weekdays for the previous reviewed reporting date, with
two calendar days of correction overlap. It is not a release guarantee. Unknown
calendar years, invalid/overlapping scopes and missing activation settings skip.
Set `FINRA_SSV_CONFIG`, comma-separated `FINRA_SSV_SCOPES`, execution and scheduled
live gates only after access/calendar/capacity review. An optional
`FINRA_SSV_OPERATIONS_CONFIG` supplies the reviewed calendar. Restart/reload the
workers after environment changes, then separately start the schedule and verify
an actual tick/run under G3. Scheduled workers recheck the gate at execution.

Optional `finra_short_sale_volume_slack_success` and `_slack_failure` sensors
also default STOPPED. [Alert YAML](../../config/finra_short_sale_volume/alerts.example.yml)
defaults disabled, references `FINRA_SHORT_SALE_VOLUME_SLACK_WEBHOOK_URL` and has
independent success/failure flags. Configure only if desired, inject the secret
and separately enable each sensor. No Slack send is part of the job itself or
this slice's acceptance. See [delivery limitations](slack_alerts.md).

## Source and research limits

Only the selected three TRFs, API source version 1, exact admitted date bounds and
noncommercial internal use are supported. ADF, ORF, consolidated/TXT and older
schemas remain excluded. The API offers a rolling twelve-month window; acquisition
cannot reconstruct unavailable historical vintages. Backfill first-seen is actual
capture time. Unknown provider publication remains null. Symbols are raw source
identifiers, without a permanent-security crosswalk. Coverage/absence never
creates zero trading or a denominator for an unreported symbol. Ratios describe
matched reported off-exchange flow, not short interest, borrow demand, naked
shorting or a causal measure. No QuantMeridian consumer, external distribution,
cloud activation or unattended production operation is delivered here.
