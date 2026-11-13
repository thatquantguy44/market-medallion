# MarketMedallion — master handoff and current pickup point

Last updated: 2026-11-13.

The current repository name is `market-medallion` (MarketMedallion), at
`https://github.com/thatquantguy44/market-medallion`. The local checkout is
`pipelines/market-medallion`; the previous checkout path is a compatibility
symlink for archived absolute paths and existing workspace references. Package
distribution metadata and the request identifier use the new name. The import
path remains `market_data_pipeline`, and both CLI names are supported.
Existing PostgreSQL, roles, Docker projects/volumes and data are preserved.

User-requested outbound HTTP configuration is implemented in
[`shared/http.py`](../../src/market_data_pipeline/shared/http.py), with
[configuration and behavior](../development/outbound_http.md). Live AAII/NAAIM,
SEC FTD and threshold entrypoints select direct/proxy once per run, including
explicit automatic fallback preference, NO_PROXY, timeouts and verified custom
CA support. CLI and Dagster forward pipeline-owned settings. SEC/threshold
validation, health, planning, local ingestion and export make no request. All
new live schedules default stopped.

## 2026-11-13 current pickup — FINRA T06–T08 accepted; T09 next

Branch **`feat/finra-local-revisions`** starts from merged T05 PR #20 / main
`9f211ce`. The user selected the next few working tasks; **SSV-T01–T08 are complete**
for the selected three-TRF API profile. Read [acceptance](../../specs/spec012/t06_t08_acceptance.md),
[storage contract](../../specs/spec012/local_storage_contract.md),
[evidence](../../specs/spec012/evidence/t06_t08_local_conformance.json),
[next-agent handoff](../../specs/spec012/README.md#next-agent-handoff) and
[local runbook](../deployment/ssv_local_runbook.md).

T06 supplies audited SQLite Silver/Gold and strict per-pipeline YAML/CLI/script;
T07 supplies observed corrections/reversions and UTC/sequence cutoffs; T08 supplies
complete membership/removal/return and honest local absence states. Exact decimal
text and capture lineage survive reopen. Partial/rejected/failed refreshes cannot
change committed membership. Attempts retain settings; interrupted STARTED work
must resume before another capture. Root/schema/kind identity and version/result/
membership integrity refuse missing or damaged stores rather than recreating them.

**Verification:** 64 new checks / **379 FINRA tests**; full offline suite
**1,233 passed, 92 optional skips**. Lint/format and package source/config/script
readback pass. Nine invented CLI/script captures reconcile nine audits, eight
versions, eleven complete-membership rows and all cutoff counts. All seven prior
FINRA core/Bronze modules are unchanged. Agent-skills integrity passes; QuantSmith
checkout drift persists, with all 61 pinned guidance objects verified independently.

**Next: SSV-T09**, scope-safe research Gold with denominator-matched aggregation,
prior available trade date and explicit gap/population rules. Define those
semantics before code; preserve fixed ratios, exact quantities and local capture
knowledge. Checkpoint B is accepted for invented input. G1/G2 and real complete
partitions/history remain open; cross-page assembly, Parquet/C07 file journaling,
replay, acquisition, health/export and Dagster remain later tasks. No provider-data
request, credential loading, operational-store change, Slack message or service
activation was part of this slice. NAAIM remains paused and schedules stopped.

Commit/push the completed slice and track the new PR's current-head checks/reviews.
The user's prior commit/push/PR authorization persists; no merge is implied.
Earlier FINRA and FOMC acceptance records below remain historical and unchanged.

## 2026-11-13 accepted checkpoint — FINRA SSV-T05; T06 next

The user selected working implementation on **`feat/finra-daily-short-sale-volume`**,
based on merged main `b580689` (FOMC quality PR #19), delivered in
[PR #20](https://github.com/thatquantguy44/market-medallion/pull/20). **SSV-T01–T05 are complete**
for the selected three-TRF API profile. Read [T05 acceptance](../../specs/spec012/t05_acceptance.md),
[Bronze contract](../../specs/spec012/bronze_contract.md),
[sanitized evidence](../../specs/spec012/evidence/t05_bronze_conformance.json) and
[the next-agent handoff](../../specs/spec012/README.md#next-agent-handoff).

T05 stores exact admitted API-response bytes plus manifest/schema/run/capture
lineage before parsing, and appends immutable sanitized parse/rejection outcomes.
It enforces approved use before I/O, refuses unsafe request identities and
recognizable credential bodies, excludes opaque auth/cookie/proxy evidence,
verifies readback and refuses corruption. UUID retries preserve original clocks
and sequences; concurrent writers and interrupted capture publication are tested.
Local archive observation is distinct from explicit nullable retrieval completion;
provider publication stays unknown and no Silver first-seen is assigned.

**Verification:** 49 Bronze / 315 focused FINRA checks; full offline suite
**1,169 passed, 92 optional skips**. Lint/format, wheel/sdist source-byte readback
and four invented captures pass. Existing five FINRA core modules are unchanged.
Agent-skills integrity passes; QuantSmith checkout drift persists, with all 61
pinned guidance objects verified independently. No library refresh was performed.

**Next: SSV-T06**, one invented complete API partition into audited SQLite
Silver/Gold, with transaction/validation failure preserving prior committed
publication. Follow the task's limits; cross-page assembly/replacements, revision
history, Parquet, replay, acquisition, export and jobs remain later work.
T05 makes no G1/G2, full-history or whole-pipeline production claim. No provider
data request, credential loading, live DB/service operation or Slack message was
needed. Schedules remain stopped; NAAIM remains paused. The earlier FOMC accepted
archive and quality results below remain unchanged.

The implementation is committed and pushed in PR #20. Track its current-head
checks and review threads before merging. The user's prior commit/push/PR
authorization persists; no merge or schedule activation is implied.

## 2026-11-13 accepted checkpoint — FOMC quality Q01–Q03

Branch **`feat/fomc-text-quality`**, from merged main `1bd4ab5`, packages the
three user-selected working slices in [PR #19](https://github.com/thatquantguy44/market-medallion/pull/19).
Read the [contract](../../specs/spec017/quality_contract.md),
[plan](../../specs/spec017/quality_plan.md), [closed tasks](../../specs/spec017/quality_tasks.md)
and [three-slice closeout](../../specs/spec017/quality_remediation_acceptance.md).
Track current-head checks/reviews before merging; no merge or acquisition activation.

- **Q01 accepted:** deterministic offline report over pinned verified catalogs;
  522 versions, 4,018 units, 9,790 chunks and 1,087 unit/issue findings reconcile.
- **Q02 accepted for eight cases:** source/render-hash-bound review covers all
  four observed strata: four charts/images, one sampled text match, one OCR
  candidate and two unresolved source-damaged passages. No native parser defect
  was confirmed. Another 1,079 flagged units remain outside this sample.
- **Q03 bounded no-native-fix path accepted:** working `quality-remediation-plan`
  revalidates the actual pack/reviews and refuses incomplete evidence. All 26
  replacement-character representations have the same 112 literal UTF-8 U+FFFDs
  in their originals as in extracted text. No number was inferred, flag cleared,
  extractor/profile changed or new derivative published. New restore/replay were
  not required or performed; do not report them as passing executions.

The actual portable commands are `quality-report`, `quality-review-plan`,
`quality-review-pack`, `quality-review-record`, `quality-review-check` and
`quality-remediation-plan`, all through `scripts/run_fomc.sh`. Q01/report and pack
export use the owned PostgreSQL config; plan/record/check/assessment are local
and do not connect to PostgreSQL. Consult the acceptance records for exact flags.

Final read-only qualification matches **all 29 tables, 1,081 capture originals/
receipts/clocks, 10,207 canonical files and all pointers** to the accepted
backfill's last restore-verified checkpoint; health remains healthy. All seven
old extractor/acquisition module hashes remain exact. All 167 review-required
representations and 1,520 flagged chunks remain explicit. Tests: **1,120 offline
passed (92 optional skips), all 395 FOMC checks with isolated PostgreSQL, and 51
focused checks**. Lint/format, build/source-byte checks and actual scripts pass.

Ignored report/review artifacts live under
`data/fomc_meeting_minutes/quality/4e4d302658dd7040b91a78cdd08adb25ba68181a359441921c0674c426f05001/`.
Its `review-plan.json` and `review-34a710fc4639429c5b4a75a2a3d234e71439bb89471a828f4b630e2fe7878bda/`
contain the source pack, PNG evidence, `reviews.json`, `review-check.json` and
`remediation.json`. The temporary review browser is closed; no new service was
installed. Raw documents and rendered evidence stay ignored; sanitized evidence
is committed alongside each slice's acceptance.

Next scoped work: additional source-page review beyond the sample, verified
source reconciliation for damaged HTML, and an explicit OCR/text-layer policy.
No OCR engine/service or new publication is selected. Deliberate recent-year
refreshes remain the path for later releases/corrections. T07 optional embeddings
stay separate. Only the two configured FOMC status sensors run; all acquisition
schedules remain stopped, NAAIM paused and FINRA at SSV-T05. QuantSmith checkout
drift remains reported; pinned Git guidance was verified without repinning.

## 2026-11-13 accepted checkpoint — released 1997–2026 backfill

The user-requested 1997–2026 expansion passed all 21 bounded Dagster batches and
final per-year reconciliation on the existing `feat/fomc-1995-1996-backfill`
branch in merged [PR #18](https://github.com/thatquantguy44/market-medallion/pull/18).
Read [acceptance](../../specs/spec017/1997_2026_acceptance.md),
[contract](../../specs/spec017/1997_2026_contract.md) and
[evidence](../../specs/spec017/evidence/1997_2026_dagster_backfill.json).
Its final checks passed before merge; recurring acquisition remains stopped.

All 390 unique selected primary URLs are
archived. The current archive has 1,081 captures,
522 text representations, 3,200 PDF pages,
25,537,392 characters and 9,790 chunks
(8,270 eligible, 1,520 review-flagged).
All 172 baseline receipt bytes/clocks and append-only rows are intact. No
unacquired inventory references remain. Other source outcomes are
`{"not_listed": 49, "review_required": 167, "unavailable": 1}`; original-byte coverage does
not certify source availability or extraction/OCR quality. Latest listed 2026
meeting: 2026-09-16, explicit release
2026-10-07. October/December minutes have no listed link.

All 32 schema/filesystem checkpoints were restored and all 21 selected-catalog
replays passed with HTTP/transforms disabled; the final read-only audit confirmed
all 29 tables, original readbacks, exact source spans/citations and retained
archive checksums/receipts. Each requested Dagster run has acknowledged Slack
success. A separate completed 2025–2026 operation was explicitly retained in the
1999–2000 checkpoint pointer chain; subsequent verification snapshots hold the
source advisory lock. Historical HTML and asset fixes were needed; all 194
pre-fix selected extraction results/IDs remain exact. Capture-time semantics,
schema and dependencies are unchanged.

Durable ignored local records are under
`data/fomc_meeting_minutes/1997-2026-backfill-review/` (`state.json`, per-batch
`verified.json`/`checkpoint.json`, `final-acceptance.json`). All batches are
accepted; **do not relaunch the completed bulk run**. Backups and their verification
receipts remain under the ignored FOMC `backups/` directory. Scratch targets were
removed. Both FOMC sensors remain running; all six acquisition/import schedules
and the other eight status sensors remain stopped. NAAIM stays paused.

Next FOMC work: flagged extraction/page-quality review and deliberate bounded
recent-year refresh after the next official release. Preserve captured revisions
and first-seen semantics. FINRA stays at SSV-T05; embeddings remain independent.

## 2026-11-13 accepted checkpoint — bounded FOMC 1995–1996 accepted

The user authorized the next historical batch after merged PR #17. Current branch:
`feat/fomc-1995-1996-backfill`, from main `f3925f8`, in
[PR #18](https://github.com/thatquantguy44/market-medallion/pull/18). Track its
[checks](https://github.com/thatquantguy44/market-medallion/pull/18/checks) and
[review threads](https://github.com/thatquantguy44/market-medallion/pull/18/files)
against the current head before merging; no merge or acquisition activation is implied.
The requested originals/text/chunks/health/Slack/restore/replay gates are explicitly
closed in the [acceptance checklist](../../specs/spec017/1995_1996_acceptance.md#acceptance-gate-checklist).
A fresh read-only audit reconfirmed all current tables, originals, text/chunk spans,
retained archive checksums/restore receipts and the exact Slack reaction, with no
provider HTTP or source mutation. This gate review preceded the expanded backfill above. Read
[acceptance](../../specs/spec017/1995_1996_acceptance.md),
[contract](../../specs/spec017/1995_1996_contract.md),
[evidence](../../specs/spec017/evidence/1995_1996_dagster_backfill.json) and the
[runbook](../deployment/fomc_runbook.md).

The existing Dagster job acquired all **16 listed 1995–1996 HTML minutes**, eight
per year, using unchanged finite settings and source code. Run
`12d67c3f-4aab-41a1-bdb1-83a5c0532d9c` and operation
`db6830d0-2415-46ff-8279-6ed9351e6c17` completed in **20 data GETs plus two
probes, 1,155,323 bytes**. Four inventories refreshed; 29 verified nonselected
pages retained their old clocks. All three pointers and the successful result
committed together; archive health passed. Slack success-sensor tick **147**
acknowledged this exact run. Both FOMC alert sensors remain RUNNING; the other
eight alert sensors and all six acquisition/import schedules remain STOPPED.

Current archive: **172 captures** (61 inventories, 89 documents, 22 PNGs),
**89 text representations, 624 PDF pages, 689 units and 2,771,872 characters**.
Chunks: **1,193 total, 568 eligible and 625 review-flagged**. New HTML minutes add
599,726 characters and 200 eligible chunks; native outcomes are 38 extracted and
51 review-required. Current inventory is unchanged: 33 pages, 371 meeting claims,
470 references and 561 associations. Gaps: 383 unacquired references, 49 not-listed
claims, one unavailable 1994 href and 51 review cases. Complete history and
text/OCR accuracy remain uncertified; no OCR/model service was called.

All **152 prior originals/receipt bytes/clocks and append-only rows** are intact.
The six intervening inventory captures from live 1990 reruns explain the increase
from the older 146-capture acceptance to the pre-batch 152. Fresh before/after
backups restored all **29 tables** and exact canonical file hashes (1,337/1,546
files). Actual empty-target replay preserved all 172 originals, selected catalogs,
chunk rows/eligibility and the latest operation audit with HTTP and transforms
disabled. Offline health/plan/recovery passed; disposable databases/roots were
removed and other operational namespaces remained unchanged. Backups, configs,
credentials and raw/derived data remain ignored.

Verification: **1,050 regression tests passed, 80 optional skips; all 313 FOMC
checks passed with isolated PostgreSQL enabled**, including 70 backend checks.
No runtime, dependency, schema or provider semantic change was needed. The local
workflow snapshot and 61 pinned QuantSmith Git objects were verified; external
working-tree drift was left untouched. **Next core batch: bounded 1997–1998**
through the same job or script with the same recovery gates. Keep NAAIM paused,
embeddings independently selected and FINRA at SSV-T05.

## 2026-11-13 accepted checkpoint — configurable success and failure alerts

The user requested success alerts through configuration and a real rerun.
`slack.on_success` defaults false; `slack.on_failure` defaults true, preserving
existing YAML behavior. Each pipeline now owns two stopped-by-default status
sensors. FOMC's ignored alert YAML enables both flags, and both FOMC sensors are
**RUNNING**. All six schedules and the other eight alert sensors remain **STOPPED**.
The existing failure sensor name/cursor is retained. Current work stays on
`feat/pipeline-slack-alerts` in [PR #17](https://github.com/thatquantguy44/market-medallion/pull/17).

Live run `cfa76508-63c2-43fa-bf05-52a0cddea51b` succeeded for the same manual
1990 backfill: all 16 originals reused, three data GETs plus two probes, 350,095
bytes, coherent promotion and healthy archive readback. Success-sensor tick 55
was SUCCESS and logged acknowledgement-backed `slack_alert_delivered` for that
exact run with `status=success`. Local evidence is ignored
`data/dagster/fomc-success-alert-rerun.json`. The prior live rerun
`1187ee91-bfd8-4a8e-8b71-0434648f14da` also succeeded before success alerts
were enabled. Historical coverage remains bounded through the accepted 1990–1994
batches; chunks remain 993 (368 eligible, 625 review-flagged). Next historical
batch remains 1995–1996. NAAIM acquisition remains paused.

Verification: **1,050 regression tests passed, 80 optional skips; 313 FOMC checks
passed with isolated PostgreSQL enabled**. Status flag gates, defaults, labels,
real success event/cursor behavior and legacy failure routing pass. See
[Spec018](../../specs/spec018/README.md) and the
[updated runbook](../deployment/slack_alerts.md). Secrets stay ignored; no
acquisition schedule, dependency, storage schema or provider semantic was changed.

## 2026-11-12 accepted checkpoint — optional pipeline-owned Slack alerts

**Subsequent operational verification:** the user configured the FOMC webhook and
requested a real job-driven alert. The manual local Dagster UI/daemon is running
on `http://127.0.0.1:3030`. FOMC's alert YAML is enabled and its failure sensor is
**RUNNING**; the other four sensors and all six schedules remain **STOPPED**.
Intentional test run `621d5cfc-d83e-46b2-974b-ea189c31b153` failed at the closed
live-access gate (`preflight_failed`) before source acquisition/database access.
Sensor tick 4 completed successfully, associated that exact run and logged
`slack_alert_delivered`. Ignored local evidence is
`data/dagster/fomc-alert-test-result.json`; webhook/config secrets remain ignored.
No LaunchAgent or durable installation was added. The earlier disabled-only
feature acceptance below remains historical; this paragraph owns current runtime
alert state.

PR #16 is merged; local Dagster defaults to port 3030. The user requested a new
branch for configurable Slack alerts with separate optional YAML per pipeline.
Current branch: `feat/pipeline-slack-alerts`, based on merged main, in
[PR #17](https://github.com/thatquantguy44/market-medallion/pull/17). Track its
[checks](https://github.com/thatquantguy44/market-medallion/pull/17/checks) and
review threads against the current head before merging. See
[Spec018](../../specs/spec018/README.md) and the
[Slack runbook](../deployment/slack_alerts.md).

Each of the five existing Dagster jobs owns a stopped `<pipeline>_slack_failure`
sensor and `config/<pipeline>/alerts.example.yml`. Users may copy it to ignored
`alerts.local.yml`, set `slack.enabled`, provide the named webhook environment
variable and deliberately start only that alert sensor. Missing/disabled config
makes no probe; explicit broken config is a visible safe sensor error. Import
loads no local file or secret. Alerts contain only job/pipeline/run IDs and an
optional Dagster link, use the shared HTTP route with redirects blocked, and
require a bounded acknowledgement. Delivery is best-effort with one POST, no
automatic retry/outbox, and documented Dagster cursor consumption/replay limits.

Verification: **1,038 regression tests passed, 80 optional skips; all 313 FOMC
checks passed with isolated PostgreSQL enabled**. Forty-seven added checks cover
config gates, routing, redaction, real failure/pre-step/success/unrelated-job
status events and cursor behavior. Root registration reports all five sensors
stopped, and all six schedules remain stopped. No real webhook was configured
or message sent; no service restart, acquisition or migration was performed.
NAAIM remains paused; the next historical FOMC batch remains 1995–1996.

## 2026-11-12 accepted checkpoint — local Dagster launcher port

PR #15 is merged. The user requested a different default port for
`scripts/run_local_dagster.sh`. The follow-up branch is
`fix/dagster-default-port` in
[PR #16](https://github.com/thatquantguy44/market-medallion/pull/16); track its
[checks](https://github.com/thatquantguy44/market-medallion/pull/16/checks) and
review threads on the current head. The launcher defaults to `127.0.0.1:3030`, with
`DAGSTER_PORT` configurable in ignored `.env` or the inherited environment.
README and the job register use the launcher consistently. Port 3001 was already
occupied during verification; 3030 had no listener. Syntax and isolated launcher
smoke checks passed for default, `.env`, inherited and empty-value fallback.
No running service was restarted or schedule activated; the default applies at
the next launch. The accepted FOMC archive/job state below remains unchanged.

## 2026-11-12 accepted checkpoint — FOMC Dagster job delivery

PR #14 merged at `3bbb0c1426f399b7ec85d6f2eea9ed4d32cb49ef`. The user first
selected a local Mac/Docker productionization target, then narrowed deployment
to **a Dagster job**. Backup and alert destinations should be configurable later.
The job delivery branch was `feat/fomc-dagster-job`, merged through
[PR #15](https://github.com/thatquantguy44/market-medallion/pull/15).
Track its [checks](https://github.com/thatquantguy44/market-medallion/pull/15/checks)
and [review threads](https://github.com/thatquantguy44/market-medallion/pull/15/files).
Acceptance head: `e73640e4df41b8cabfb1000c52e4abd041466faf`; on continuation
recheck the current head SHA, check results and unresolved threads. No merge or
schedule activation is implied. [Job acceptance](../../specs/spec017/dagster_job_acceptance.md),
[contract](../../specs/spec017/dagster_job_contract.md),
[evidence](../../specs/spec017/evidence/dagster_job.json) and
[runbook](../deployment/fomc_runbook.md#dagster) record the bounded delivery.

Reuse the root-registered `fomc_meeting_minutes_job`. It supports explicit
backfills and opt-in execution-time recent refresh, followed by no-HTTP archive
health before materialization. Metadata distinguishes an incomplete operation
from a committed operation requiring health review, retains catalog/run IDs and
coverage/gaps, and redacts unexpected exception context. Job/op automatic retries
default to zero; source retries and the PostgreSQL advisory lock remain in force.
Refresh/backfill launch examples validate and keep `live: false` by default.

Verification: **991 regression tests passed, 80 optional skips; 313 FOMC checks
passed with PostgreSQL enabled**, including 70 backend checks. Root job
registration, all six stopped schedule states, lint/format and package checks
pass. No-request operational health remains healthy at 146 captures, 29 tables
and 993 chunks, with the same last 1993–1994 operation and catalog selections.
No provider request, migration or additional historical batch was performed.
No worker service, recurrence, alert endpoint or remote backup destination was
installed. NAAIM remains paused, embeddings unselected and FINRA at SSV-T05.
Next historical batch remains 1995–1996 through the script or explicit job config.

## 2026-11-12 historical checkpoint — PR #14 covers 1992–1994 (accepted locally)

The user requested more work in the same PR. Keep `feat/fomc-1992-backfill` and
[PR #14 — 1992–1994 FOMC backfills and recovery](https://github.com/thatquantguy44/market-medallion/pull/14).
The added 1993–1994 batch is accepted. [Acceptance](../../specs/spec017/1993_1994_acceptance.md),
[evidence](../../specs/spec017/evidence/1993_1994_script_backfill.json) and
[runbook](../deployment/fomc_runbook.md) are current. Follow the PR's
[live checks](https://github.com/thatquantguy44/market-medallion/pull/14/checks)
and [review discussion](https://github.com/thatquantguy44/market-medallion/pull/14/files);
on continuation verify the current head SHA, checks and unresolved review threads.
No merge or schedule activation is implied.

The unchanged script ran explicit **1993–1994** backfill under existing finite
settings. All 16 listed HTML minutes references are archived/verified, eight per
year: **15 acquired and one reused**. Four inventories were refreshed and 29
verified pages carried with their old clocks. The run made **19 data GETs plus
two probes**, receiving **1,145,768 bytes**. Eleven not-listed claims and the
unavailable December 30, 1994 href remain explicit; no replacement was guessed.
All three pointers and the successful operation result committed together.
All **127 prior originals/receipts/clocks** and old append-only rows are intact.

The archive now has **146 captures** (51 inventories, 73 documents, 22 PNGs),
**624 PDF pages**, **673 text units**, **2,172,146 characters** and **993 chunks:
368 eligible, 625 review-flagged**. The added HTML minutes contribute 202 eligible
chunks. Native outcomes are 22 native-extracted and 51 review-required. Coverage
reconciles 2,170,946 unit characters and 1,200 separators. Current normalized
inventory is unchanged: **33 pages, 371 meeting claims, 470 references and 561
associations**. Gaps remain: 399 unacquired references, 49 not-listed claims,
one unavailable href and 51 review cases. Full history and accuracy review remain
incomplete; no OCR/model service was called and no clock was backdated.

Fresh pre-/post-run backups both restored all **29 tables**, matching exact row
fingerprints, originals, selected catalogs and operation audit. Offline replay
preserves all 146 receipt bytes, selected catalogs/chunk eligibility and the latest
1993–1994 operation's intent/events/result, with HTTP/extraction/tokenization/
chunking disabled. Full backup retains historical catalog/operation snapshots.
Health, plan and run recovery also passed with HTTP disabled. Disposable targets
were removed; other namespaces remain unchanged. Local configuration, credentials,
originals, derivatives and backups stay ignored. No runtime/config/schema or
dependency change was needed for the additional years.

Verification: **954 regression tests passed, 71 optional skips; 267 FOMC checks
passed with PostgreSQL enabled**, including 61 backend checks. Bash, Ruff, format,
build, diff, local-link and source-artifact checks pass. The local workflow pin
and all 61 adopted QuantSmith guidance objects were verified without editing the
external checkout, which still differs from its configured pin.

**Next core operation: bounded 1995–1996 through the same script**, then health/
readback and recovery gates. Preserve source-product distinctions, first-seen
clocks and review flags. Keep all schedules stopped, embeddings unselected,
NAAIM paused and FINRA at SSV-T05.

## 2026-11-12 bounded FOMC 1992 accepted checkpoint

[PR #14 — Bounded 1992 FOMC backfill and recovery](https://github.com/thatquantguy44/market-medallion/pull/14)
is open from `feat/fomc-1992-backfill` to `main`. Acceptance/evidence head at
creation: `5274279ae741e8d0527a197963821f7acac3ed22`.
Track [live checks](https://github.com/thatquantguy44/market-medallion/pull/14/checks)
and [review discussion](https://github.com/thatquantguy44/market-medallion/pull/14/files).
On continuation verify the current head SHA, CI results and unresolved review
threads before advancing. The handoff does not imply approval to merge or
activate schedules.

The selected 1992-only continuation is on `feat/fomc-1992-backfill`, from merged
PR #13 at `41e9d0c06a4669b1ef6e9aa308c93f829034176f`.
[Acceptance](../../specs/spec017/1992_acceptance.md),
[evidence](../../specs/spec017/evidence/1992_script_backfill.json) and
[runbook](../deployment/fomc_runbook.md) are current. The existing
[`scripts/run_fomc.sh`](../../scripts/run_fomc.sh) ran with unchanged finite
operation/network settings. No runtime/schema/config/dependency change was needed.

The bounded run acquired **16 primary 1992 representations** and refreshed three
inventory pages, carrying 30 verified nonselected pages with old capture clocks.
It made **19 data GETs plus two startup probes**, receiving **9,401,386 bytes**.
Four 1992 not-listed outcomes remain explicit. All three selected pointers and
the successful operation result committed together. All **108 prior original
receipts/bytes/clocks** and old append-only database rows are preserved.

The archive has **127 captures** (47 inventories, 58 documents, 22 PNGs),
**624 PDF pages**, **658 text units**, **1,579,673 characters** and **791 chunks:
166 eligible, 625 review-flagged**. Coverage reconciles 1,578,473 source-unit
characters and 1,200 separators. Fifty-one representation review cases, 414
unacquired references, 49 not-listed claims and one unavailable 1994 href remain
explicit. Full history and text/OCR accuracy are incomplete; no OCR/model service
was called. Current normalized inventory is exactly unchanged from 1991: **33
pages, 371 meeting claims, 470 references and 561 associations**. Earlier source
snapshots and capture clocks remain queryable; no publication time was inferred.

Fresh pre-/post-batch backups both restored all **29 tables**, with exact row
fingerprints, originals, selected catalogs and operation audit. Actual empty-
target replay preserves all 127 receipts, selected catalogs/chunk eligibility
and the latest 1992 operation audit with HTTP/extraction/tokenization/chunking
disabled. Full backup retains historical catalog/operation snapshots. Health,
plan and run recovery also passed with HTTP disabled. Disposable targets were
removed; other namespaces remain unchanged. Local configs, originals,
derivatives and backups remain ignored.

Verification: **954 regression tests passed, 71 optional skips; 267 FOMC checks
passed with PostgreSQL enabled**, including 61 isolated backend checks. Bash,
Ruff, format, build, diff, local-link and source-artifact checks pass. The pinned
workflow snapshot and all 61 QuantSmith guidance objects were verified; the
external checkout still differs from its pin and was left unchanged.

The added 1993–1994 batch is accepted above; the next core batch is 1995–1996.
Preserve distinct action/policy products and modern minutes through the transition. Keep all schedules stopped, embeddings optional
and unselected, NAAIM paused and FINRA at SSV-T05.

## 2026-11-12 FOMC script and 1991 accepted checkpoint (merged)

[PR #13 — Reusable FOMC runner script and 1991 backfill](https://github.com/thatquantguy44/market-medallion/pull/13)
merged into `main` at `41e9d0c06a4669b1ef6e9aa308c93f829034176f`. Acceptance/evidence head at
creation: `6fd3c5b9594eb933db48117a1c93b08e49d41b53`.
Track its [live checks](https://github.com/thatquantguy44/market-medallion/pull/13/checks)
and [review discussion](https://github.com/thatquantguy44/market-medallion/pull/13/files).
On continuation verify the latest head SHA, CI results and unresolved review
threads; the handoff does not imply approval to merge or activate schedules.

User-selected continuation is on `feat/fomc-runner-script`, based on merged
PR #12 at `1aa49549890dba51445341e304fdea83c9c2c7e8`. The executable
[`scripts/run_fomc.sh`](../../scripts/run_fomc.sh) delegates to the source CLI,
selects checkout `.venv` or explicit `FOMC_PYTHON`, and preserves caller paths,
arguments, JSON and exit codes. It does not source `.env`, install dependencies,
migrate or enable live access implicitly. Other users supply their own config
and environment-backed credentials using the [setup/runbook](../deployment/fomc_runbook.md).
The script is included in source distributions; wheel users retain the console CLI.
[Acceptance](../../specs/spec017/1991_acceptance.md),
[contract](../../specs/spec017/scripts_contract.md) and
[evidence](../../specs/spec017/evidence/1991_script_backfill.json) are current.

The same script completed the **1991** batch: 16 primary representations acquired,
three inventories refreshed, 19 data GETs plus two probes, 9,439,345 received bytes.
All three catalog pointers advanced together. The archive now has **108 captures**
(44 inventories, 42 documents, 22 PNGs), **434 PDF pages**, **468 text units**,
**1,239,281 characters** and **601 chunks: 166 eligible, 435 review-flagged**.
Coverage accounts for 1,238,429 unit characters and 852 separators separately.
Thirty-five representation review cases, 430 unacquired references, 49 not-listed
claims and one unavailable 1994 href remain explicit. Accuracy and full history
remain incomplete; no OCR/model service was invoked.

The latest captured calendar again contains eight 2027 planning rows absent from
T06's capture. Current inventory is **33 pages, 371 meeting claims, 470 references
and 561 associations**. Both earlier snapshots remain queryable with actual clocks;
this observed variation does not infer meeting creation or cancellation.
All 89 prior original receipts/bytes/clocks and old append-only rows are preserved.
No schema migration was needed. Pre-/post-batch backups both restored **29 tables**.
Offline replay preserves all 108 receipts, selected catalogs/chunk eligibility and
the latest operation's intent/events/result with HTTP/extraction/tokenization/
chunking disabled. Full backup retains old catalog/operation snapshots; replay
selects the latest generation. Disposable targets were removed; other namespaces
remain unchanged. Health/plan/run-recovery also passed with HTTP disabled.

Verification: **954 regression tests passed, 71 optional skips; 267 FOMC checks
passed with PostgreSQL enabled**, including 61 isolated backend checks. Bash
syntax, Ruff, format, build, diff and source-artifact checks pass. The two new
script checks cover other working directories, paths with spaces, interpreter
selection, no-service validation and closed access gates.

The 1992 continuation is accepted above; the current next core batch is 1995–1996.
Keep all FOMC schedules stopped, embeddings independently selected, NAAIM paused
and FINRA at SSV-T05. Local config, bytes, derivatives and backups remain ignored.
The external QuantSmith checkout still differs from its pin; all 61 adopted
pinned Git objects were hash-verified without editing that checkout.

## 2026-11-12 FOMC-T06 accepted checkpoint (merged)

[PR #12 — Complete FOMC T-06 bounded acquisition and operations](https://github.com/thatquantguy44/market-medallion/pull/12)
merged into `main` at `1aa49549890dba51445341e304fdea83c9c2c7e8`. Accepted implementation/
evidence head at creation: `b0b7b12e18d8e73c033639270e4d3a6228360afd`.
Track [live checks](https://github.com/thatquantguy44/market-medallion/pull/12/checks)
and [review discussion](https://github.com/thatquantguy44/market-medallion/pull/12/files);
on continuation re-read the latest head SHA, CI results and unresolved review
threads before advancing. Runtime/CI state is live, not an implied approval from
this handoff. Merging remains unrequested.

T01–T05 merged through [PR #11](https://github.com/thatquantguy44/market-medallion/pull/11)
at `79c499420faf9602f7ac2a50cafca0596c5b12fc`. T06 is implemented on
`feat/spec017-fomc-operations`; [acceptance](../../specs/spec017/t06_acceptance.md),
[contract](../../specs/spec017/operations_contract.md),
[evidence](../../specs/spec017/evidence/t06_operations.json) and
[runbook](../deployment/fomc_runbook.md) are current.

T06 adds finite source acquisition/retries, overlapping inventory refresh,
immutable operation audit/recovery, coherent inventory/text/chunk promotion,
operator CLI, no-request health and a manual Dagster job with a **STOPPED**
weekday schedule. Live/scheduled gates are checked before service/probe and again
at execution. No schedule was activated. NAAIM remains paused; FINRA stays at SSV-T05.

The bounded 1990 batch acquired 14 PDFs and reused two originals: 17 data GETs,
two startup probes and 7,800,834 response bytes. The archive has **89 captures**
(41 inventory, 26 documents, 22 PNGs). The selected generation has **26
representations**, **239 PDF pages**, **273 units**, **897,899 text characters**
and **406 chunks: 166 eligible, 240 review-flagged**. All 897,405 source-unit
characters are covered; 494 separators are accounted for separately. Nineteen
representation review cases, 446 unacquired references, 41 not-listed claims and
one unavailable 1994 href remain explicit. Full history and text/OCR accuracy
review remain incomplete; no OCR/model service was invoked.

The refreshed calendar removed its eight 2027 planning placeholders. Exact raw
comparison confirms the change. The current 33-page inventory has **363 meeting
claims, 470 references and 553 associations**; the prior 371/561 snapshot and all
old document claims remain queryable. This does not infer cancelled meetings.
All 72 prior original receipts/bytes/clocks and every old append-only table row
are preserved; the three current pointers intentionally changed together.

Fresh backups restored all 26 pre-migration and 29 post-operation tables exactly.
Actual empty-target replay preserves all 89 captures, selected catalogs/chunk
eligibility and the operation intent/events/result with HTTP/extraction/
tokenization/chunking disabled. Disposable databases/root were removed; other
namespaces remain unchanged. Local config, source bytes, derivatives and backups
remain ignored. QuantSmith's working checkout differs from its pin; adopted
guidance was read from hash-verified pinned Git objects (all 61 verified).

Verification: **952 tests passed, 71 optional skips; 265 FOMC checks passed with
PostgreSQL enabled**, including 61 isolated backend checks. Ruff, format, build,
diff and artifact checks pass. CLI health/plan/run-recovery also passed against
actual local state with HTTP disabled.

The 1991 continuation is accepted above; the current next core batch is 1995–1996.
Continue year batches and review separately. T07 embeddings remain optional and
independently selected. The user requested commit/
push, PR creation and tracking; inspect the current branch PR's head, checks and
review threads on continuation. Merging remains unrequested.

## 2026-11-10 FINRA continuation and AAII manual refresh

Spec011 T17/G2 merged through PR #8; AAII's refresh documentation merged through
PR #9. FINRA **SSV-T01–T04 are complete for source version 1 and the selected
three-TRF Public API profile**. Approved-use admission, strict manifest/schema/
version validation, exact facility resolution, disjoint scope selection and
explicit rolling acquisition-date guards, a pure receipt-bound JSON parser,
shared quantity validation/text readback and per-observation ratios are implemented.
Reusable acquisition, warehouse, export and job remain absent.
**Next: SSV-T05**, immutable raw/rejected-capture archiving and redacted receipts.
Start with the [Spec012 next-agent handoff](../../specs/spec012/README.md#next-agent-handoff),
which records commit `9a506d3`, planned files, scope limits and verification.
T04's numeric helpers do not complete aggregate/temporal Gold or publication.

The user confirmed Public credentials and accepted API terms for personal
noncommercial internal research. Keep the evidence reference
`USER_ATTESTED_PUBLIC_API_TERMS_2026_11_10` as user attestation, not independent
license documentation. Credentials and tokens remain excluded from Git/logs.

The [T02 aggregate evidence](../../specs/spec012/evidence/t02_source_contract.json)
reconciles six November 9 rows to NQTRF/Q, NCTRF/B and NYTRF/N. Source dataset
version 1 is active, and 252 available partition dates span 2025-11-10 through
2026-11-10. These are partial samples, not complete date/facility history. The
market D query returned HTTP 204 / Record-Total 0 with no ADF identifier. Its initial
checker rejection is retained; offline review reconciled all raw digests without
refetching or relabeling that failed check as a successful pipeline capture.

T03's first offline parse exposed an incorrect integer-only assumption: all six
retained rows contain fractional short or total quantities. The
[API amendment](../../specs/spec012/api_contract.md#ssv-t03-fractional-quantity-amendment)
and [sanitized evidence](../../specs/spec012/evidence/t03_parser_conformance.json)
record the rejection and correction. Whole/fractional quantities now remain exact
Decimal values, without rounding/truncation. All six unchanged rows and ten
fractional values reconcile; every sampled page remains partial. T03 made no
provider-data request, assigned no clocks and promoted no observations.

T04's [numeric contract](../../specs/spec012/data_contract.md#ssv-t04-arithmetic-and-quantity-text-boundary)
pins ratio method `finra-short-sale-volume-ratios/1`, unit 1, 34 significant digits
and half-even rounding. Quantities retain every digit and scale through bounded
Decimal text/JSON readback. Valid zero total yields null ratios; invalid numerator/
denominator triples reject. Caller decimal settings/flags remain unchanged.
[Offline evidence](../../specs/spec012/evidence/t04_semantics_conformance.json)
checks all 18 quantities and 12 ratios in the unchanged six-row sample. It adds
no source requests, clocks, promoted observations or coverage claims.

The [API contract](../../specs/spec012/api_contract.md) records exact schema,
version and scope behavior. `listed_trfs` is only the three reviewed TRFs;
ADF, consolidated TRF/ADF, ORF, TXT, mock and other versions remain excluded.
G0 is ready for this explicit profile; G1/G2 and full source conformance remain
open. The rolling twelve-calendar-month guard is for new acquisition, not a
current-day filter on archived local imports/replay. Contract-check receipt
`captured_at` records request start, not an assigned Silver first-seen clock.
No observations have been promoted. NAAIM stays paused; schedules stay stopped.

Verification: **748 offline tests passed, 10 optional integrations skipped**;
266 focused FINRA tests pass (71 semantics + 111 parser + 63 registry + 21 admission). Ruff
lint/format and source distribution/wheel builds pass. The six retained samples,
source metadata, version headers and unchanged raw hashes reconcile to the
implemented registry and parser. Core verification does not establish integrated HTTP,
warehouse, export or job behavior for this new pipeline.

QuantSmith's external working-tree pin still fails verification. Previously
adopted guidance was read from its pinned Git object and hash-checked without
refreshing its checkout or this repository's reference.

The user-requested AAII manual Dagster refresh succeeded for October 13 through
November 10. It added October 29 and November 5, bringing local PostgreSQL and the
committed Parquet snapshot to **2,048 reported dates through 2026-11-05**.
The no-request health check passes. The pre-run backup and the full sanitized
run/snapshot identifiers are recorded in the [AAII checkpoint](aaii_checkpoint.md#2026-11-10-manual-refresh).
Credentials and provider bytes remain ignored. This was a single manual run;
recurring acquisition permission and production operation are still separate.

## 2026-11-05 Spec011 checkpoint — accepted delivery evidence

**T17/G2 is complete for the bounded native-source internal-research delivery**
on `chore/spec011-t17-acceptance`, from merged `main` `5b092af` (PR #7).
The [acceptance record](../../specs/spec011/acceptance.md) is the current
A01–A15 evidence map, source-use decision and reproducibility guide.

The user confirmed existing Nasdaq/NYSE permission under a verbal agreement
and declined a document reference. Record this as
`USER_ATTESTED_VERBAL_PROVIDER_PERMISSION`, not independently verified provider
license documentation. The accepted scope is the ten selected October26–30,
2026 Nasdaq/NASDAQ and NYSE/NYSE files, retained local copies and internal
research. No recurring activation or public redistribution was inferred.

Both approved September SEC archives reconcile all 117,025 facts and their
source fields/raw lineage against retained facts and Gold. Original digests,
current pointer and snapshot files remain unchanged; population coverage stays
PARTIAL/NOT_CERTIFIED. All ten authentic threshold files now reconcile every
source field, Bronze digest, normalized row and coverage/audit capture clock:
404 Nasdaq plus four NYSE rows, with a valid empty NYSE October30 list.
The real ten-capture threshold history replays without network, exports 408
membership rows, ten coverage rows and 93 observed episodes, and stages typed
Parquet for Delta. Raw files and the verified ZIP bundle remain ignored under
`data/spec011-t17/thresholds/`; sanitized aggregate evidence is committed under
`specs/spec011/evidence/`. Knowledge clocks remain actual November captures.

Real conformance found and fixed a NYSE acquisition issue: its Download route
returns HEAD404 and rejects the explicit text/plain preference. The source-owned
adapter probes the published source page, uses unrestricted content negotiation,
then reuses the selected run-scoped client for the exact configured GET.
Regression fixtures prove bytes/provenance, startup rejection and manifest gates;
shared route fallback and TLS policy are unchanged. Initial failures are retained
as failed attempts, not mislabeled empty lists.

Final full verification with actual disposable PostgreSQL17.6 and local
OpenJDK17 / Spark4.0.1 / Delta4.0.1 enabled together: **492 passed, zero skips**.
Ruff lint/format, dependency checks and sdist/wheel builds pass. All test databases
were isolated and removed. agent-skills integrity verifies; QuantSmith's external
revision/tree still differs from its pin and was left untouched/unverified.

**Next source-build work: Spec012 FINRA daily short-sale volume**, after this
closeout is reviewed/merged. Begin with its existing requirements/data contract,
provider-use/schema review and selected fixture/sample plan. Spec011's optional
T14 joined projection remains explicitly deferred; its two native outputs are
independent. G3 recurring operation is not activated; G4 consumer UI/public
presentation remains deferred. Databricks workspace publication was not run.
Checked-in acquisition settings, the local sample acquisition config and all
schedules remain disabled/stopped. NAAIM remains paused.

### Safe stopping boundary

The closeout branch contains the NYSE fix, offline regression/acceptance tests,
reproducible sample-review scripts and the evidence/status updates. Review it
against merged `main`; do not treat G2 as a request to activate a service.
No production database, cloud object, Databricks write or NAAIM resume occurred.

## Presidential pipeline port and PRES plan — Spec016

The 2026-11-04 request is documented in [Spec016](../../specs/spec016/README.md),
with [data contract](../../specs/spec016/data_contract.md),
[cross-repository plan](../../specs/spec016/plan.md),
[task tracker](../../specs/spec016/todo.md) and an inspection checksum inventory.
It covers the research website donor → MarketMedallion canonical pipeline →
QuantMeridian PRES, including the five frozen datasets, reviewed-control evidence,
atomic publication, paired/inherited/cycle views and replay provenance.
All port/integration tasks remain planned. Donor baseline verification: 29 offline
tests passed. Next scoped Spec016 task is T01; no new package, source capture,
schedule, deployment or consumer change was made by this specification. The
existing securities-finance build sequence and NAAIM pause remain unchanged.

## Immediate operational priority — NAAIM paused

On **2026-11-03**, the user paused NAAIM work. The local Dagster schedule
`naaim_delayed_daily_live` has been deliberately stopped and verified as
**STOPPED**. Do not start that schedule, run a NAAIM live capture, or refresh
the downstream research artifact until the user explicitly asks to resume it.

When resuming, first review the current artifact/report need, run the local
no-request health check, and then start the schedule deliberately using the
command in the [Dagster job register](../deployment/dagster_jobs.md). Keep the
three-month delay and required NAAIM attribution intact. Configured artifact
export after a successful capture is the first NAAIM technical task only after
that explicit resume.

## Next source-build priority — securities-finance program

The user requested a comprehensive specification/plan/task/roadmap set for
five workstreams on 2026-11-03. The complete planning set is at
[docs/securities_finance/](../securities_finance/README.md). Build in this order:

1. [Spec011](../../specs/spec011/README.md): SEC outstanding FTD balances plus
   separately owned Nasdaq/NYSE Reg SHO threshold history.
2. [Spec012](../../specs/spec012/README.md): FINRA facility-scoped daily short
   volume and matching denominators.
3. [Spec013](../../specs/spec013/README.md): FINRA delayed ATS/non-ATS weekly
   venue/participant activity.
4. [Spec014](../../specs/spec014/README.md): public SEC N-PORT fund holdings
   and lending disclosures.
5. [Spec015](../../specs/spec015/README.md): NY Fed primary-dealer financing,
   positions, transactions and fails.

**Current checkpoint:** Spec011's offline SEC parser/manifest and local
inspection commands are implemented on `feat/spec011-sec-ftd-parser`. Both
approved local September2026 archives reconcile all 117,025 observations and
their count/quantity trailers. Version2 pins ASCII/LF and explicit Sep1–14 /
Sep15–30 bounds; no broader completeness or provider authentication is claimed.
**Current delivery: S11-T17/G2 accepted for the bounded internal-research scope** in
[Spec011 tasks](../../specs/spec011/todo.md). SEC T03–T07/T19/T21 and threshold
T20/T22 are complete: immutable Bronze, SQLite/PostgreSQL persistence,
revisions/withdrawals, cutoff Gold, offline replay, coherent Parquet publication
and SEC Delta publication.
T06 preserves original clocks, sequence, IDs, terminal outcomes and archived
proof, including historical rejections. New captures seal `outcome.json`;
legacy captures reconcile against the read-only original ledger. Missing or
unresolved evidence blocks replay. A private verified staging root is renamed
into a separate empty destination with a new warehouse identity.

T06 full suite: **419 passed, 2 existing opt-in PostgreSQL skips**. An isolated
real September rebuild has identical 117,025 fact versions, four coverage rows
and four audits, with cutoff counts 0/50,332/117,025. All source-file digests
remain unchanged. Ignored evidence: `data/sec_ftd/t06-verification.json`; config
`config/sec_fails_to_deliver/t06-verification.local.yml` deliberately has unusable
live paths/CA. A finalized checksummed inventory pins every capture/outcome for
database-free replay; missing inventory or capture blocks that route. Bounded
exact-URL SEC acquisition, no-request health, native research export and stopped
Dagster definitions are implemented. Checked-in acquisition settings remain
disabled and no live schedule was enabled.

T07 writes exact-schema facts, current Gold, coverage and audit into immutable
snapshots and records STAGED/COMMITTED/PUBLISHED database state. Only atomic
`parquet/current.json` selects consumers. Failure tests cover partial files,
rename, pointer and watermark transitions, including recovery before a newer
capture. The ignored real T06 root now has a verified snapshot with 117,025
facts/Gold rows and four coverage/audit rows; quantity/price are exact decimal128
and clocks are UTC. T19 uses native PostgreSQL types, environment-only password,
root/database identity and session advisory serialization. Three actual
PostgreSQL17.6 integration cases cover revision/reversion, equal clocks/cutoffs,
withdrawal, rollback/failed promotion, concurrent retry, wrong root, Parquet,
SQLite→PostgreSQL replay and identity-verified failed-replay cleanup. No retained
production database was provisioned.
Final verification with all PostgreSQL integrations enabled is **430 passed**;
lint, format, documentation links and package builds pass.
On 2026-11-03 the user required PostgreSQL and Databricks compatibility too.
SEC T19 and threshold T20 are implemented and verified on disposable
PostgreSQL17.6; SEC T21 has actual local Delta evidence. T22 now passes local
engine verification; T17/G2 is closed for the attested bounded scope. Pin exact numeric text, checked
decimal ranges and UTC/date types before storage; never coerce FTD balances
to floats/int64. See [the platform contract](../../specs/spec011/data_contract.md#9-storage-and-platform-compatibility).
T18 is complete: config foundation is implemented in
[`config/sec_fails_to_deliver/`](../../config/sec_fails_to_deliver/README.md):
strict loader, `validate-config`, config-based local `inspect` and a semantic
storage fingerprint. The user selected PostgreSQL by default and Databricks
disabled until configured. Connection settings in the example are illustrative;
do not reuse AAII credentials. Both source packages now own storage schemas and
warehouse Protocols. Exact codecs pass actual SQLite/Parquet round trips,
native PostgreSQL bindings, configurable decimal limits and Spark schema tests.
The SEC parser preserves original numeric tokens. All117,025 approved September
observations pass numeric/date preflight; ignored report:
`data/sec_ftd/storage-types-202609.json`. T04 also imports all 117,025 facts
into isolated `data/sec_fails_to_deliver/t04-verification`, with ignored config
`config/sec_fails_to_deliver/t04-verification.local.yml`. Reopen, exact trailer
totals, all three Bronze captures and an unchanged repeat (zero new facts) pass.
First-seen is actual November3 acquisition completion; population coverage
remains PARTIAL/NOT_CERTIFIED. Full suite **316 passed, 2 existing opt-in
PostgreSQL skips**; Ruff, changed-file format and sdist/wheel build pass.
Evidence: `data/sec_ftd/t04-verification.json` and
[the local runbook](../deployment/sec_ftd_runbook.md). PostgreSQL remains the
operational default and has no SQLite fallback. SEC Delta is implemented; T22
threshold parity is verified; T17/G2 is now accepted for the bounded scope. Interrupted
STARTED captures remain visible; do not recreate a missing/mismatched bound store or claim automatic replay.
T05 extends that checkpoint: **346 passed, 2 skipped**; a fourth unchanged
capture leaves 117,025 fact versions, four audits and PARTIAL real coverage.
Receipt2 archives optional reviewed completeness evidence and reads legacy
receipt1. Corrections/reversions, cutoff queries and evidence-gated withdrawals
are implemented. Ignored evidence: `data/sec_ftd/t05-verification.json`.
Preserve unknown publication/retrieval clocks and actual ingestion
first-seen. Inputs/report stay ignored in `data/sec_ftd/`; commands, hashes and
coverage limits are in the [source checkpoint](../../specs/spec011/source_review.md).
The documentary source matrix and selected first partitions are recorded.
Each numbered directory owns its
`README.md`, `data_contract.md`, `plan.md` and `todo.md`. The
[program roadmap](../securities_finance/roadmap.md) tracks delivery gates and
the [common contract](../securities_finance/contract.md) supplies lineage,
knowledge-clock, revision/removal, replay, export and manual-operation rules.
The SEC source package contains parser/config/inspection, SQLite/PostgreSQL
ingestion/Gold, replay, Parquet, research export and Delta publication. The
threshold package contains independent Nasdaq/NYSE acquisition/parsing,
SQLite/PostgreSQL history, replay, episodes, research export and stopped Dagster
definitions. The other four source packages are absent. Threshold Delta is
verified; final bounded source-evidence/use acceptance is recorded in T17. Do not claim
source access, historic coverage or consumer features from fixture tests.
CFTC, Cboe, Nasdaq breadth, OCC, OFR and other
existing candidates remain queued behind this user-selected sequence.

**Current stopping point:** AAII's local completion scope under Spec 003 is
implemented on `feature/pipeline-completion`: offline XLS/Dagster integration,
weekly bounds/retries/status, historical backfill, backups/restore and retention.
Read [the AAII checkpoint](aaii_checkpoint.md) for evidence and operating limits.
The user selected CFTC on a separate subsequent branch and its provider contract
is now recorded in Spec 006. NAAIM Exposure Index has a local delayed-public
research slice in Spec 007. Recurring AAII permission, production orchestration/alerts
and live Databricks verification remain open.

On 2026-11-01/02, the user confirmed NAAIM daily automated pulls, 365-day
retention and the attributed local research-report/display scope. A local
read-only health check and source-owned research export are implemented. The
10:00 America/New_York schedule is now **stopped** by the 2026-11-03 pause;
manual local operation remains the chosen service model. The LaunchAgent
template is documented but not installed. NAAIM must not be resumed as part of
the new securities-finance program; broader/public/current-data scope is separate.

**New-source discovery queue:** The user added three separate potential
pipelines on 2026-11-01: Cboe published daily put/call ratios (Spec 009),
Nasdaq Trader daily advances/declines/unchanged breadth counts (Spec 010), and
OCC filtered options put/call volumes (Spec 008). Their provisional discovery
order was **Cboe → Nasdaq → OCC** and is now deferred behind Specs011–015.
For each, the eventual next delivery is a reviewed
provider/access/schema contract and invented-fixture plan—not a live connector,
bulk history, schedule or product output. This queue does not displace the
NAAIM pause or remove the reviewed CFTC contract.

The user subsequently authorized pipeline-owned source folders and a master
pipeline document. AAII now lives under `src/market_data_pipeline/pipelines/aaii/`; common
file/YAML helpers are under `shared/`. Existing root CLI/Dagster entrypoints
forward to AAII. Every future pipeline must use its own subpackage under
`src/market_data_pipeline/pipelines/`. Read [the master pipeline register](../pipelines.md) and
[import migration guide](../development/pipeline_layout.md) before adding a source.
The register includes the tracked FX/web-traffic specifications. FX initial scope
is accepted; the [completed overlap/gap review](../reviews/spec004_macro_medallion/README.md)
confirms all nine ECB FX currencies already exist in MacroMedallion. Keep ingestion
owned there and prioritize temporal correctness, exact values/metadata and FX Gold
outputs. No local FX connector is implemented. Web-traffic source selection
remains draft. Spec 007 implements only NAAIM's permission-gated, delayed
public-research slice and attributed local QuantMeridian artifact; broader
subscription/API/public distribution remains separately scoped and NAAIM is paused.

The workflow library is now a complete local snapshot at `agents/agent-skills/`,
pinned by `agents/agent-skills.lock.json` to revision
`f63ec56a3cc936408d792956ae583c3c96a825bd`. It replaces the old submodule;
normal use and checksum verification require no network refresh.

The user also authorized a configurable local QuantSmith guidance reference.
`agents/README.md` is the library discovery/routing document; root `AGENTS.md`
directs agents to both libraries. `agents/quantsmith.reference.json` records
revision `41c56458f476a6c32fa922f7b8c70bb92e4fd21e` and checksums for 61
adopted guidance files. Verify with `scripts/verify_quantsmith.py`; the path
can be overridden with `MARKET_MEDALLION_QUANTSMITH_PATH`. The external checkout
has unrelated drafts; those remain untouched and outside the guidance pin.
Relevant QuantSmith roles should inform new pipeline work while preserving this
repository's source contracts and captured-first-seen/revision semantics.

The user requested a sister repository named
`additional-market-data-pipelines`, beside `fred-bronze-to-gold-pipeline`.
They selected positioning and market structure as the main spec, requested the
same general repository format, and retained disclosures/insider activity and a
broader alternative-data mix for later.

Completed: independent repository scaffold and original design, plus the first
AAII slice under Spec 002. The implementation has a strict local CSV manifest,
immutable Bronze, observed Silver revisions, Gold/as-of features, audit,
SQLite/PostgreSQL persistence, typed Parquet snapshots, replay, CLI, and Dagster
asset/job/stopped schedule. A Databricks publisher and notebook are implemented
but not live-tested in a workspace. A bounded official AAII XLS load is now
verified in persistent local PostgreSQL; no recurring download or cloud deployment is active.

The user also requested that every additional source discussed be retained in
the handoff and roadmap. The [master roadmap](../roadmap.md) is the canonical
source backlog, with official references, proposed access methods, research
uses, limitations, and stable backlog IDs for all 14 additions.

## Retained source inventory

Current sequence: AAII is implemented locally; CFTC has a provider contract;
NAAIM Exposure Index has a manual-only local delayed-research slice. The remaining
Spec 001 sources (CFTC positioning → FINRA short-sale volume → OFR Short-term
Funding Monitor) remain planned, as does SEC fails-to-deliver. See the
[source catalog](../catalog/README.md) for those original four sources.

Additional positioning and market-structure candidates:

- `nyfed-primary-dealers`: NY Fed Primary Dealer Statistics.
- `ofr-hedge-fund-monitor`: OFR Hedge Fund Monitor, distinct from STFM.
- `finra-otc-transparency`: FINRA ATS/non-ATS activity, distinct from short-sale
  volume.
- `sec-nmfp`: SEC money-market-fund portfolio holdings.
- `dtcc-public-swaps`: DTCC public swap/security-based-swap dissemination.
- `sec-midas`: SEC Market Structure Analytics / MIDAS published datasets.
- `aaii-investor-sentiment`: first implemented local CSV slice for weekly bullish,
  neutral, and bearish shares. The current official XLS layout and bounded live
  load are verified; recurring entitlement, membership requirements, and reuse
  still require review. Fixtures are synthetic; no official
  public API or free full-history entitlement has been verified.

Later company/holdings candidates:

- `sec-nport`: SEC fund portfolio holdings, derivatives, and securities lending.
- `fdic-bankfind`: FDIC bank financials, deposits, branches, and history.

Later alternative-data candidates:

- `usda-mymarketnews`: USDA agricultural reports and prices.
- `stb-rail-service`: Surface Transportation Board rail performance.
- `epa-clean-air-markets`: EPA plant emissions and operating data.
- `usace-corps-locks`: USACE vessel movements, lockage times, and delays.
- `usaspending`: Federal awards, recipients, and award transactions.

Company insider filings and filing-event metadata also remain future themes.
The other additions are a research backlog; connector access and functionality
have not been validated by live extraction. All 14 IDs remain in the roadmap.

Additional options and breadth candidates, each with its own numbered source
contract and no connector:

- `cboe-published-put-call-ratios`: [Spec 009](../../specs/spec009/README.md),
  first discovery priority; public daily labelled ratios, with terms/date/history
  contract still to verify.
- `nasdaq-daily-market-breadth`: [Spec 010](../../specs/spec010/README.md),
  second; 2026 advances/declines/unchanged fields observed, with history/universe
  contract still to verify.
- `occ-options-volume-query`: [Spec 008](../../specs/spec008/README.md), third;
  documented query parameters, with CSV schema/result completeness/terms still
  to verify.

## NAAIM delayed-feed presentation

On 2026-11-01 the user confirmed continuing with the public feed's three-month
lag. Gold latest, capture/health CLI responses and Dagster materialization
metadata now identify it as **NAAIM Exposure Index — 3-month-delayed public
data**, with `provider_delay_months: 3` and a human-readable notice. Observation
dates remain separate from capture freshness. Daily capture and 365-day
retention continue within the existing internal-research scope.

## Next work

Preserve the NAAIM pause and all disabled acquisition/schedule defaults. Spec011
T17 and Spec012 T01–T04 are merged. FOMC T01–T06 and the bounded 1990–1996
batches are accepted locally and PR #14 is merged. The current deployment slice
is the verified Dagster job on `feat/fomc-dagster-job`; see the current pickup.
The next historical operation is bounded 1997–1998 through the script or job. Read the current pickup above and Spec017 tasks.
When FINRA resumes, its next task is SSV-T05 immutable raw/rejected-capture
archiving and redacted lineage receipts. G0 and parser
Checkpoint A are ready for this scope; local publication and G1/G2 remain open.
Read Spec012's README, data contract, plan, tasks, API contract and source review
before further implementation.
Spec011's selected source conformance and A01–A15
backend/recovery evidence are complete; wider source coverage, recurring
operation, Databricks workspace activation, public distribution and the optional
joined projection remain outside this accepted delivery.

Verify the repository-local workflow library before engineering. QuantSmith's
external checkout still differs from its pin; review that drift separately and
do not silently refresh it or treat unverified guidance as checked evidence.

The T02 September sample reconciliation is complete. Older recommendations below
are historical context; Specs011→012→013→014→015 govern the requested next builds.
The documentation branch is `docs/securities-finance-program` (planning commit
`1b2e73b`); the
review branch is `feat/spec011-sec-ftd-parser`. No schedule has been enabled;
checked-in acquisition settings remain false.

Current contracts: Specs 002 and 003, with plan/tasks under `tasks/`. Run the README demo
and use `docs/deployment/aaii_runbook.md` for PostgreSQL, Dagster, Parquet, replay,
and Delta publishing. Local tests cover validation, revisions/reversion, as-of
changes, raw retention, replay, CLI, and Dagster. PostgreSQL has a real opt-in
test; use a dedicated empty test database. `origin/main` is configured for this
repository; the user requested a commit/push series at closeout. No cloud
deployment or recurring download is active.

Runtime YAML controls PostgreSQL host/port/database/login, role/schema, SSL,
timeout and environment-only passwords. Optional operations settings control
weekly overlap, age threshold, HTTP attempts and the scheduled-access gate.
The original official XLS download and four-date PostgreSQL load were preserved.
An explicit offline historical backfill now holds 2,046 valid reported dates,
1987-07-24 through 2026-10-22. Three invalid rows remain quarantined with partial
audit status. The current data/backup evidence is in ignored
`data/aaii-live/completion-verification.json`. Same-payload rerun added zero
versions; verified backup/restore and replay reproduce Silver/Gold exactly.

The persistent service is unchanged: localhost 55438, database
`additional_market_data`, schema `aaii`, effective role `aaii_writer`, Compose
project `additional-market-data-live`. Settings are ignored `config/config.yaml`
and `.env`. No new provider request or recurring schedule was enabled.

Next source: AAII merged through PR #2. CFTC TFF futures-only has a verified
provider contract in [Spec 006](../../specs/spec006/README.md), but no connector.
The user selected NAAIM's delayed public research capability on
`spec007-naaim-exposure`; [Spec 007](../../specs/spec007/README.md) records its
weekly methodology and the explicit live-permission boundary. Preserve
provider-specific grain. Its local Bronze/Silver/delayed-latest slice, one
bounded manual smoke request, health check and attributed local research export
are verified. NAAIM is paused; wider Program Partner/API/public use remains
separate. The securities-finance sequence above governs the next source build.
Next AAII operational work: establish recurring acquisition permission and reuse
terms before scheduling; deploy durable workers, external monitoring/alerts and
off-machine backups as appropriate. Live Delta requires a selected workspace.
Mage/Airflow wrappers remain optional; Dagster is the working orchestrator.

The earlier sequence, NY Fed Primary Dealer Statistics → SEC N-MFP →
FINRA ATS/non-ATS → OFR Hedge Fund Monitor, is superseded for next work by the
five-stage program. Keep DTCC
and MIDAS queued for later evaluation and retain all original source IDs.

Use the roadmap's promotion checklist before implementing each candidate:
verify access/terms, coverage and schema, publication/revision semantics, then
capture fixtures and specify acceptance criteria. Preserve provider-specific
grains and original availability timestamps. Update this handoff and the
roadmap when status or priorities change so the same inventory stays visible.
