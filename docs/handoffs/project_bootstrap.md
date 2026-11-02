# MarketMedallion — master handoff and current pickup point

Last updated: 2026-11-01.

The current repository name is `market-medallion` (MarketMedallion), at
`https://github.com/thatquantguy44/market-medallion`. The local checkout is
`pipelines/market-medallion`; the previous checkout path is a compatibility
symlink for archived absolute paths and existing workspace references. Package
distribution metadata and the request identifier use the new name. The import
path remains `market_data_pipeline`, and both CLI names are supported.
Existing PostgreSQL, roles, Docker projects/volumes and data are preserved.

**Current stopping point:** AAII's local completion scope under Spec 003 is
implemented on `feature/pipeline-completion`: offline XLS/Dagster integration,
weekly bounds/retries/status, historical backfill, backups/restore and retention.
Read [the AAII checkpoint](aaii_checkpoint.md) for evidence and operating limits.
The user selected CFTC on a separate subsequent branch and its provider contract
is now recorded in Spec 006. NAAIM Exposure Index has a local delayed-public
research slice in Spec 007. Recurring AAII permission, production orchestration/alerts
and live Databricks verification remain open.

On 2026-11-01, the user reported oral NAAIM permission by phone. The current
NAAIM slice records that only as a nonsecret, user-confirmed manual-run
reference; scope, frequency, retention, transformations, redistribution and
QuantMeridian terms have not been supplied in writing. A local read-only health
check now covers capture freshness, version retention and output availability.
The user then confirmed daily automated pulls and 365-day retention. A 10:00
America/New_York Dagster schedule is enabled in the local instance, but the user
selected manual operation: it evaluates only while an operator runs local
`dagster dev`. A restart-at-login LaunchAgent is documented but deliberately not
installed; revisit only on request. Keep downstream distribution disabled until
provider terms are documented.

**New-source discovery queue:** The user added three separate potential
pipelines on 2026-11-01: Cboe published daily put/call ratios (Spec 009),
Nasdaq Trader daily advances/declines/unchanged breadth counts (Spec 010), and
OCC filtered options put/call volumes (Spec 008). Their provisional discovery
order is **Cboe → Nasdaq → OCC**. For each, the next delivery is a reviewed
provider/access/schema contract and invented-fixture plan—not a live connector,
bulk history, schedule or product output. This queue does not displace the
NAAIM written-permission evidence prerequisite or the reviewed CFTC contract.

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
public-research slice; it does not authorize subscription, API use, scheduling
or QuantMeridian integration.

The workflow library is now a complete local snapshot at `agents/agent-skills/`,
pinned by `agents/agent-skills.lock.json` to revision
`f63ec56a3cc936408d792956ae583c3c96a825bd`. It replaces the old submodule;
normal use and checksum verification require no network refresh.

The user also authorized a configurable local QuantSmith guidance reference.
`agents/README.md` is the library discovery/routing document; root `AGENTS.md`
directs agents to both libraries. `agents/quantsmith.reference.json` records
revision `5deea85438f6d46a8a14ddb85e3821a65fa62f4e` and checksums for 61
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

**Next-agent pickup point:** Read [the Dagster job register](../deployment/dagster_jobs.md)
and [Spec 007](../../specs/spec007/README.md). Keep NAAIM in the selected manual
operation model; do not install a durable service. The remaining non-code NAAIM
gates are written provider confirmation and a user-selected backup/alert
destination. Once those are pending or resolved, the next source-development
task is Spec 009: Cboe published put/call-ratio provider-contract review,
followed by Spec 010 then Spec 008. Start each on a fresh branch after the
current PR is merged.

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
bounded manual smoke request and read-only health check are verified; retain
written permission evidence before a schedule, persistence expansion or
downstream use. QuantMeridian remains a separate Program Partner/API scope.
Next AAII operational work: establish recurring acquisition permission and reuse
terms before scheduling; deploy durable workers, external monitoring/alerts and
off-machine backups as appropriate. Live Delta requires a selected workspace.
Mage/Airflow wrappers remain optional; Dagster is the working orchestrator.

After the initial pipeline slice, the recommended order for the new additions
is NY Fed Primary Dealer Statistics → SEC N-MFP → FINRA ATS/non-ATS → OFR Hedge
Fund Monitor. Keep DTCC and MIDAS queued for later evaluation. This ordering does
not replace the current AAII-first implementation priority.

Use the roadmap's promotion checklist before implementing each candidate:
verify access/terms, coverage and schema, publication/revision semantics, then
capture fixtures and specify acceptance criteria. Preserve provider-specific
grains and original availability timestamps. Update this handoff and the
roadmap when status or priorities change so the same inventory stays visible.
