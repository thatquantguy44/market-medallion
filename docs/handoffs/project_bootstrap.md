# MarketMedallion — master handoff and current pickup point

Last updated: 2026-11-05.

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

## 2026-11-05 latest Spec011 checkpoint — authoritative pickup

The active branch `feat/spec011-sec-ftd-parser` is pushed through `68c6785`.
Its latest implementation commit is `2ee5616` for SEC T21 Delta publication;
`68c6785` separately restores the reviewed QuantSmith guidance reference.
The branch also contains implementation for exact configured SEC ZIP acquisition,
SEC health, `sec-ftd-research/1` export and stopped Dagster; independent Nasdaq/NYSE
manifests/parsers; versioned threshold SQLite membership/coverage/audit;
corrections, withdrawals and cutoff Gold; bounded threshold acquisition,
health/plan/CLI; gap/censoring episodes; dated identity mapping; native research
export; and stopped threshold Dagster definitions.

Official checks on November4 pinned the SEC ZIP route, Nasdaq's current six-field
file plus trailer, and NYSE's public `selectedDate`/`market` download route and
six-field/trailer responses. No provider rows are committed. Checked-in configs
keep acquisition disabled; definitions default stopped; NAAIM remains paused.

This is **not final Spec011 closure**. Deterministic threshold replay and T20
PostgreSQL parity pass, and T21 SEC Delta now has an actual OpenJDK17,
Spark4.0.1 and Delta Lake4.0.1 round trip. The exact next engineering work is
T22 threshold Delta. Databricks remains owner-disabled until a
catalog, schema and staging volume are supplied. Nasdaq/NYSE intended retention,
automation and research-display use must also be accepted before live activation.
Do not begin Spec012 or mark G2 complete while those gates remain.
The T21 engine run proves idempotent revisions, exact large quantities/decimals,
and recovery after a partial multi-table publish while the prior pinned
generation stays readable. Unity Catalog code and a notebook are included; no
Databricks workspace smoke, cloud object or schedule was created. Final offline
verification is **461 passed, 8 opt-in skips**; the separate enabled Delta suite
is **3 passed**. Ruff, dependency checks, agent-skills integrity and sdist/wheel
build pass. At T21 closeout the read-only QuantSmith checkout differed from its
recorded pin, so that slice did not rely on it. The dedicated 2026-11-05 library
maintenance change below subsequently restored a verified 61-file reference.

### Safe stopping boundary

This branch is ready for review into `main`. All repository changes are committed
and pushed; there is no pending local implementation work to preserve. SEC T21
is complete, but threshold T22 has not started. No provider capture, Databricks
workspace write, cloud object, schedule activation or NAAIM resume is part of
this stopping point. After this branch merges, start T22 on a fresh branch such
as `feat/spec011-threshold-delta`; do not continue piling it onto this branch.

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
**Exact next numbered task: S11-T22 threshold Delta publication** in
[Spec011 tasks](../../specs/spec011/todo.md). SEC T03–T07/T19/T21 and threshold
T20 are complete: immutable Bronze, SQLite/PostgreSQL persistence,
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
PostgreSQL17.6; SEC T21 has actual local Delta evidence. Threshold T22 still
gates G2. Pin exact numeric text, checked
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
threshold parity remains open. Interrupted STARTED captures remain visible; do
not recreate a missing/mismatched bound store or claim automatic replay.
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
definitions. The other four source packages are absent. Threshold Delta and
final provider-use acceptance remain open. Do not claim
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

**Next-agent pickup point:** preserve the NAAIM pause at the top of this file;
read the [program map](../securities_finance/README.md),
[roadmap](../securities_finance/roadmap.md) and Spec011's four documents.
Verify both agent-library references before implementation; QuantSmith is now
pinned at `41c56458f476a6c32fa922f7b8c70bb92e4fd21e` with 61 adopted files.
Continue S11-T22 threshold Delta. SEC T03–T07/T19/T21 and threshold T20 are
verified; T08/T09 and the physical/local NYSE slice are implemented;
T18 is complete for both packages. T22 actual Delta integration evidence
remains required before Spec011 G2. Provider-use acceptance is still open.
The bounded T22 sequence is:

1. Inspect the committed threshold snapshot/research boundary and the SEC T21
   generation design, but keep the new publisher entirely under
   `pipelines/reg_sho_thresholds/`; do not import SEC-owned schemas or tables.
2. Publish explicit-schema membership, coverage, episode and audit tables with
   provider/market identity, exact source revision keys, source-root binding,
   monotonic capture ordering and one checksummed version-pinned generation.
3. Add an actual local Spark/Delta round trip covering repeat idempotency,
   correction/withdrawal, complete-empty versus missing/failed coverage,
   cutoff ordering and an injected partial-table failure that leaves the prior
   generation readable.
4. Add the source-named notebook/config/runbook path, while keeping Databricks
   disabled and every Dagster schedule stopped by default.
5. Reconcile T22 against A15, then obtain the outstanding Nasdaq/NYSE
   retention/automation/research-use acceptance and close T17/G2. Only after
   those gates should the sequence advance to Spec012.

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
