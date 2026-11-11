# Master pipeline register

Updated: 2026-11-11, America/New_York.

This is the master index for **pipeline ownership, implementation status and
next work**. It tracks all concrete pipelines/candidates currently recorded in
this repository. The [research roadmap](roadmap.md) retains source-selection
rationale, access reviews and the original 14-addition inventory. Numbered specs
define contracts; [handoffs](handoffs/project_bootstrap.md) record pickup details.
A draft spec or proposed package name does not mean a connector exists.
The [Dagster job and schedule register](deployment/dagster_jobs.md) is the
canonical operational inventory.

## Current build sequence and status

On 2026-11-11 the user selected FOMC implementation with the existing PostgreSQL
location and a pipeline-owned config folder. T01/T02 inventory/config/catalog
foundation is implemented; [FOMC-T03](../specs/spec017/todo.md) is the immediate
pickup. FINRA remains at SSV-T05 for its next continuation, and the remaining
securities-finance sequence below is preserved.

AAII local completion and package cleanup merged through PR #2. SEC FTD and
Reg SHO thresholds now satisfy Spec011 G2 for the bounded native-source
internal-research scope, with the permission attestation and exclusions in
[the acceptance record](../specs/spec011/acceptance.md). CFTC TFF futures-only has a reviewed provider
contract but no package or connector. NAAIM's delayed public research feed has
a local operational slice and a narrow, attributed current-vintage research
artifact for the separately owned QuantMeridian local consumer. The FX,
web-traffic, OCC, Cboe and Nasdaq drafts do not silently change the backlog.

The current user-selected build program is **Spec011 SEC FTD + Reg SHO →
Spec012 FINRA short volume → Spec013 FINRA OTC transparency → Spec014 SEC
N-PORT → Spec015 NY Fed primary dealers**. Full requirements/data contracts,
plans and source tasks live in the numbered specs; the
[program roadmap](securities_finance/roadmap.md) owns delivery gates. SEC FTD
now includes bounded exact-URL acquisition, health, stopped Dagster and a native
research exchange. The threshold package now implements independent Nasdaq/NYSE
parsers, versioned SQLite/PostgreSQL state, acquisition/operator commands, episodes,
export, stopped Dagster definitions and verified local Spark/Delta publication. FINRA short volume now has admission, schema/version and facility/date registry
checks, a pure receipt-bound JSON parser, exact quantity text readback and
per-observation ratios; its connector and the
other three later source packages remain absent.
The former Cboe → Nasdaq breadth → OCC discovery
queue is retained but deferred behind this program. NAAIM remains paused.

| Pipeline ID | Purpose | Status | Package under `src/market_data_pipeline/pipelines/` | Contract / evidence | Next scoped work |
| --- | --- | --- | --- | --- | --- |
| `fomc-meeting-minutes` | Complete official current/historical FOMC minutes and typed predecessor documents | T01/T02 normalized inventory, own config and PostgreSQL inventory snapshots implemented; 80 focused checks including five real schema-isolated PostgreSQL tests; no source archive/text/chunks/job yet | [`fomc_meeting_minutes/`](../src/market_data_pipeline/pipelines/fomc_meeting_minutes/) | [Spec017](../specs/spec017/README.md), [config](../config/fomc_meeting_minutes/README.md), [backend evidence](../specs/spec017/evidence/postgres_inventory.json) | FOMC-T03 native HTML inventory and immutable document archive, including PostgreSQL byte storage. Six FOMC metadata tables are initialized empty in the existing database; embeddings remain optional later work. |
| `presidential-research-data` | Reviewed political-control and public equity-return research inputs for QuantMeridian PRES | Spec016 draft and multi-repository plan saved; donor standalone pipeline exists in research website; no target port, jobs or consumer additions implemented | Proposed `presidential_research/`; absent | [Spec016](../specs/spec016/README.md), [integration plan](../specs/spec016/plan.md), [tasks](../specs/spec016/todo.md) | Review contract and start T01 baseline when implementation is selected; preserve existing source-build priority and NAAIM pause. |
| `aaii-investor-sentiment` | Weekly investor sentiment shares and bull/bear spread | Implemented locally; recurring acquisition gated | [`aaii/`](../src/market_data_pipeline/pipelines/aaii/) | [Spec 002](../specs/spec002/README.md), [Spec 003](../specs/spec003/README.md), [checkpoint](handoffs/aaii_checkpoint.md), [runbook](deployment/aaii_runbook.md) | Local completion merged; November 10 manual Dagster refresh is healthy. Establish recurring access and production operations before scheduling. |
| `naaim-exposure-public-delayed` | Three-month-delayed active-manager U.S. equity-market exposure for authorized research | Local Bronze/Silver/delayed-latest slice, manual smoke, health check, stopped-by-user daily Dagster schedule and versioned research export verified; each scheduled run retains 365 days; every output explicitly labels the 3-month provider delay | [`naaim_exposure_public_delayed/`](../src/market_data_pipeline/pipelines/naaim_exposure_public_delayed/) | [Spec 007](../specs/spec007/README.md), [job register](deployment/dagster_jobs.md) | **Paused.** Do not capture or restart the schedule until the user resumes NAAIM. On resumption, automate export after a successful capture, then retain attribution/delay labels. No warehouse or public service is added. |
| `sec-fails-to-deliver` | Security-level reported outstanding settlement failures | T03–T07 immutable Bronze, SQLite persistence, revisions/withdrawals, cutoff Gold, deterministic replay and coherent typed Parquet implemented; T19 PostgreSQL17.6 parity and T21 Spark4.0.1/Delta4.0.1 parity verified; bounded exact-URL acquisition, no-request health, stopped Dagster and `sec-ftd-research/1` export implemented; real isolated snapshot has 117,025 facts/Gold rows | [`sec_fails_to_deliver/`](../src/market_data_pipeline/pipelines/sec_fails_to_deliver/) | [Spec011](../specs/spec011/README.md), [checkpoint](../specs/spec011/source_review.md), [config](../config/sec_fails_to_deliver/README.md), [runbook](deployment/sec_ftd_runbook.md) | Keep live acquisition/schedule and Databricks target disabled until explicitly configured. T17/G2 bounded delivery is complete and merged through PR #8; Spec012 remains the next securities-finance continuation after the selected FOMC work. Broader history and G3/G4 remain separate. |
| `reg-sho-thresholds` | Nasdaq/NYSE threshold membership and observed episodes | Independent Nasdaq/NYSE manifests/parsers, SQLite/PostgreSQL revisions/withdrawals/coverage and deterministic replay, exact-URL acquisition, health/plan/CLI, gap/censoring episodes, stopped Dagster and `reg-sho-thresholds-research/1` export implemented; PostgreSQL17.6 parity verified; NYSE public route/schema sample reconciled; T22 typed history/episode staging and coherent Delta generations verified on Spark4.0.1/Delta4.0.1 | [`reg_sho_thresholds/`](../src/market_data_pipeline/pipelines/reg_sho_thresholds/) | [Spec011](../specs/spec011/README.md), [config](../config/reg_sho_thresholds/README.md), [runbook](deployment/reg_sho_thresholds_runbook.md), [contract](../specs/spec011/data_contract.md) | T17/G2 complete for ten authentic NASDAQ/NYSE October26–30 files on operator-attested verbal permission; 408 rows and complete-empty evidence reconcile. Keep broader use, Databricks and recurring schedules separately gated. |
| `finra-short-sale-volume` | Facility-scoped short-sale volume and matched denominators | SSV-T01–T04 complete for the user-attested three-TRF Public API profile; version 1 and six retained rows reconcile offline with exact Decimal shares, quantity text readback and pure per-observation ratios; 266 admission/registry/parser/semantics tests; connector, jobs and outputs absent; delivery2 | [`finra_short_sale_volume/`](../src/market_data_pipeline/pipelines/finra_short_sale_volume/), source contract/registry/pure parser/numeric helpers | [Spec012](../specs/spec012/README.md), [source review](../specs/spec012/source_review.md) | Public API selected for personal noncommercial internal research; [API contract](../specs/spec012/api_contract.md) records the JSON amendment. Next: SSV-T05 immutable raw/rejected-capture archiving and redacted receipts. Ratio method 1 uses 34 significant digits / half-even; published Gold and persistence remain absent. The API amendment records fractional source quantities; the integer-only assumption is superseded. G0 ready for source version 1 and the three verified TRFs; ADF/consolidated/ORF/TXT excluded. Six sampled rows do not establish complete coverage; schedules remain stopped. |
| `finra-otc-transparency` | Delayed ATS/non-ATS weekly activity and venue concentration | Specified; connector absent; delivery3 | Proposed `finra_otc_transparency/`; absent | [Spec013](../specs/spec013/README.md) | After012: select complete weekly product/tier contracts and admit participant identity. |
| `sec-nport` | Public fund holdings, lending and collateral disclosures | Specified; connector absent; delivery4 | Proposed `sec_nport/`; absent | [Spec014](../specs/spec014/README.md) | After013: pin archive tables, filing hierarchy and actual public disclosure regime. |
| `nyfed-primary-dealers` | Aggregate dealer positions, transactions, financing and fails | Specified; connector absent; delivery5 | Proposed `nyfed_primary_dealers/`; absent | [Spec015](../specs/spec015/README.md) | After014: inventory overlap, select series/regime and pin units/suppression definitions. |
| `cboe-published-put-call-ratios` | Cboe-labelled daily total, index and equity put/call ratios | Draft provider contract; connector absent; deferred | Proposed `cboe_put_call_ratios/`; absent | [Spec 009](../specs/spec009/README.md) | First in deferred options/breadth discovery queue: verify terms/date/history then fixtures. |
| `nasdaq-daily-market-breadth` | Nasdaq Trader daily advances, declines and unchanged counts | Draft provider contract; connector absent; deferred | Proposed `nasdaq_daily_market_breadth/`; absent | [Spec 010](../specs/spec010/README.md) | Second in deferred options/breadth queue: verify year coverage, universe, terms and file stability; then fixtures. |
| `occ-options-volume-query` | Filtered OCC options put/call volume query results | Draft provider contract; connector absent; deferred | Proposed `occ_options_volume_query/`; absent | [Spec 008](../specs/spec008/README.md) | Third in deferred options/breadth queue: verify schema/completeness, terms/history limits and selected-population semantics; then fixtures. |
| `cftc-cot` | Trader positioning, starting with TFF futures-only | Provider contract drafted; connector absent | Proposed `cftc_tff/`; absent | [Spec 006](../specs/spec006/README.md), [Spec 001](../specs/spec001/README.md), [catalog](catalog/README.md#cftc) | Retain the reviewed TFF contract; begin fixtures and a source-owned vertical slice when reprioritized. |
| `ofr-stfm` | Short-term funding and repo measures | Planned | Proposed `ofr_stfm/`; absent | [Spec 001](../specs/spec001/README.md), [catalog](catalog/README.md#ofr) | Select series from metadata and verify provenance/units before implementation. |
| `ecb-fx-reference` | Daily official FX reference rates, ECB first | Existing MacroMedallion coverage confirmed; no local connector | Proposed `fx_reference_rates/`; absent and deferred | [Spec 004](../specs/spec004/README.md), [overlap/gap review](reviews/spec004_macro_medallion/README.md): all nine currencies already stored upstream | Keep ECB ingestion in MacroMedallion; prioritize captured revisions/as-of/replay, exact values/metadata, then FX Gold outputs there. |
| `company-web-traffic` | Company/competitor website visit estimates | Draft; acquisition source unresolved | Proposed `company_web_traffic/`; absent | [Spec 005](../specs/spec005/README.md) | Verify a suitable free source first; paid Similarweb is an optional later decision. |

The [specification index](../specs/README.md) records the FX/web contracts and
user decisions. These documentation additions do not implement either connector
or change the current build priority. Provider alternatives such as Bank of
Canada, BIS, Similarweb and DNS rankings are options within those drafts, not additional implemented
pipelines or interchangeable fallbacks.

## Remaining source candidates

All entries below are **research candidates with no code package or implemented
connector**. Promote one only after the roadmap's access/schema/timing review,
numbered spec and offline-fixture acceptance steps. These entries retain the
original IDs; their research detail remains in the linked roadmap sections.

| Pipeline ID | Dataset / purpose | Reference |
| --- | --- | --- |
| `ofr-hedge-fund-monitor` | Aggregate hedge-fund leverage, liquidity and financing | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `sec-nmfp` | Money-market-fund portfolio holdings | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `dtcc-public-swaps` | Public swap transaction dissemination | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `sec-midas` | Published market-structure metrics and datasets | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `fdic-bankfind` | Bank financials, branches and deposits | [Company/holdings backlog](roadmap.md#later-theme-company-disclosures-and-insider-activity) |
| `usda-mymarketnews` | Agricultural market reports and prices | [Alternative-data backlog](roadmap.md#later-theme-broader-free-alternative-data) |
| `stb-rail-service` | Rail service and freight congestion | [Alternative-data backlog](roadmap.md#later-theme-broader-free-alternative-data) |
| `epa-clean-air-markets` | Power-plant emissions and operating data | [Alternative-data backlog](roadmap.md#later-theme-broader-free-alternative-data) |
| `usace-corps-locks` | Inland-waterway vessel movements and delays | [Alternative-data backlog](roadmap.md#later-theme-broader-free-alternative-data) |
| `usaspending` | Federal awards and recipient exposure | [Alternative-data backlog](roadmap.md#later-theme-broader-free-alternative-data) |

SEC insider filings and filing-event metadata remain broader discovery themes;
no concrete new connector contract or package has been selected for them.

## AAII delivery and operations

| Capability | Current evidence / limitation |
| --- | --- |
| CSV and bounded official XLS | Implemented; exact bytes, selection, HTTP provenance and observed revisions retained |
| SQLite and PostgreSQL | Tested against both; local PostgreSQL on port 55438 holds 2,048 reported dates through 2026-11-05 after the November 10 manual Dagster refresh |
| Historical backfill | Implemented; three invalid rows explicitly quarantined; partial audit and quality report retained |
| Parquet | Committed typed snapshots, `current.json` publication and dry-run-first retention |
| Recovery | Owned-state/Bronze backup, empty-target restore and offline replay verified |
| Dagster | Pipeline-owned asset/job, local and live schedules; both schedules stopped |
| Freshness and retries | Configurable overlapping weekly bounds, bounded HTTP attempts and local health command |
| Databricks/Delta | Publisher/notebook implemented; live workspace behavior unverified |
| Production operations | Recurring acquisition rights, daemon/workers, external alerts and off-machine backup policy remain open |

No pipeline is represented as a deployed production service. Raw provider data,
local runtime config, passwords and backup packages remain outside Git.

AAII and NAAIM HTTP entrypoints now use configurable startup proxy/direct
selection, with one selected client reused for the run. Both CLI and Dagster
forward pipeline-owned settings; see [outbound HTTP](development/outbound_http.md).
SEC FTD and threshold live entrypoints use the same run-scoped routing; their
validation, health, plan, local-run and export commands make no requests.
Provider permissions, NAAIM's pause and all stopped schedule defaults still apply.

## Package ownership and maintenance

Every implemented pipeline gets its own Python subpackage immediately beneath
`src/market_data_pipeline/pipelines/`, using a valid snake_case name. New provider
packages must not be created directly under `src/market_data_pipeline/`. Each
pipeline owns its manifest contract, parsers, transforms, acquisition, tables,
publication, recovery, CLI and scheduler assets. Only provider-independent helpers belong in `shared/`.
See the [layout and import migration guide](development/pipeline_layout.md).

Update this register whenever a pipeline is added, scoped, implemented, deployed
or retired. Record the stable pipeline ID, actual package path, spec, verification,
operational status and next task. Keep dataset grain, date basis, access policy
and revisions source-specific. An absent package stays explicitly absent; avoid
placeholder connectors that make planned sources look implemented. Future
manifests/resources/fixtures remain source-scoped in their existing directories.
