# MarketMedallion — master handoff and current pickup point

Last updated: 2026-11-10.

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

## 2026-11-10 current pickup and AAII manual refresh

Spec011 T17/G2 merged through PR #8; AAII's refresh documentation merged through
PR #9. FINRA **SSV-T01–T03 are complete for source version 1 and the selected
three-TRF Public API profile**. Approved-use admission, strict manifest/schema/
version validation, exact facility resolution, disjoint scope selection and
explicit rolling acquisition-date guards and a pure receipt-bound JSON parser
are implemented. Reusable acquisition, warehouse, export and job remain absent.
**Next: SSV-T04**, exact decimal quantity arithmetic, subset/zero cases and
lossless serialization; T03's parsing checks do not complete Gold semantics.

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

The [API contract](../../specs/spec012/api_contract.md) records exact schema,
version and scope behavior. `listed_trfs` is only the three reviewed TRFs;
ADF, consolidated TRF/ADF, ORF, TXT, mock and other versions remain excluded.
G0 is ready for this explicit profile; G1/G2 and full source conformance remain
open. The rolling twelve-calendar-month guard is for new acquisition, not a
current-day filter on archived local imports/replay. Contract-check receipt
`captured_at` records request start, not an assigned Silver first-seen clock.
No observations have been promoted. NAAIM stays paused; schedules stay stopped.

Verification: **677 offline tests passed, 10 optional integrations skipped**;
195 focused FINRA tests pass (111 parser + 63 registry + 21 admission). Ruff
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
T17 is merged. Continue Spec012 SSV-T04 on `feat/spec012-finra-access-review`:
SSV-T01–T03 are complete for the selected source version 1 three-TRF API profile.
Next is decimal quantity arithmetic and lossless serialization. G0 and parser
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
