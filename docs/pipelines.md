# Master pipeline register

Updated: 2026-11-01, America/New_York.

This is the master index for **pipeline ownership, implementation status and
next work**. It tracks all concrete pipelines/candidates currently recorded in
this repository. The [research roadmap](roadmap.md) retains source-selection
rationale, access reviews and the original 14-addition inventory. Numbered specs
define contracts; [handoffs](handoffs/project_bootstrap.md) record pickup details.
A draft spec or proposed package name does not mean a connector exists.

## Current build sequence and status

AAII is the only fully implemented pipeline. Its local completion and package
cleanup merged through PR #2. CFTC TFF futures-only has a reviewed provider
contract but no package or connector. NAAIM's delayed public research feed has
a local, manual-only slice; written permission evidence remains an operational
gate. The FX, web-traffic, OCC, Cboe and Nasdaq drafts do not silently change
the backlog.

The provisional **discovery** order among the three new options/breadth
proposals is Cboe published ratios → Nasdaq daily breadth → OCC granular
volumes. A delivery at this stage means an approved provider contract and
invented-fixture plan, not a live connector or schedule.

| Pipeline ID | Purpose | Status | Package under `src/market_data_pipeline/pipelines/` | Contract / evidence | Next scoped work |
| --- | --- | --- | --- | --- | --- |
| `aaii-investor-sentiment` | Weekly investor sentiment shares and bull/bear spread | Implemented locally; recurring acquisition gated | [`aaii/`](../src/market_data_pipeline/pipelines/aaii/) | [Spec 002](../specs/spec002/README.md), [Spec 003](../specs/spec003/README.md), [checkpoint](handoffs/aaii_checkpoint.md), [runbook](deployment/aaii_runbook.md) | Review/merge completion; establish recurring access and production operations before scheduling. |
| `naaim-exposure-public-delayed` | Three-month-delayed active-manager U.S. equity-market exposure for internal research | Local Bronze/Silver/delayed-latest slice, manual smoke, local health check and stopped daily Dagster schedule verified; each scheduled run retains 365 days | [`naaim_exposure_public_delayed/`](../src/market_data_pipeline/pipelines/naaim_exposure_public_delayed/) | [Spec 007](../specs/spec007/README.md) | Document the oral daily/365-day authorization in writing, configure the three required runtime environment values, then deliberately enable the stopped schedule; do not add a warehouse or QuantMeridian integration. |
| `cboe-published-put-call-ratios` | Cboe-labelled daily total, index and equity put/call ratios | Draft provider contract; connector absent | Proposed `cboe_put_call_ratios/`; absent | [Spec 009](../specs/spec009/README.md) | First among the new proposals: verify terms, date selection, source shape and historical continuity; then create invented fixtures. |
| `nasdaq-daily-market-breadth` | Nasdaq Trader daily advances, declines and unchanged counts | Draft provider contract; connector absent | Proposed `nasdaq_daily_market_breadth/`; absent | [Spec 010](../specs/spec010/README.md) | Second: verify coverage by year, eligible universe, terms and file stability; then create invented fixtures. |
| `occ-options-volume-query` | Filtered OCC options put/call volume query results | Draft provider contract; connector absent | Proposed `occ_options_volume_query/`; absent | [Spec 008](../specs/spec008/README.md) | Third: verify query schema, result completeness, terms, rate/history limits and selected-population semantics; then create invented fixtures. |
| `cftc-cot` | Trader positioning, starting with TFF futures-only | Provider contract drafted; connector absent | Proposed `cftc_tff/`; absent | [Spec 006](../specs/spec006/README.md), [Spec 001](../specs/spec001/README.md), [catalog](catalog/README.md#cftc) | Retain the reviewed TFF contract; begin fixtures and a source-owned vertical slice when reprioritized. |
| `finra-short-sale-volume` | Facility-scoped short-sale volume and matched denominators | Planned | Proposed `finra_short_sale_volume/`; absent | [Spec 001](../specs/spec001/README.md), [catalog](catalog/README.md#finra) | Confirm current extraction route, scope, terms and fixtures after CFTC. |
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
| `sec-fails-to-deliver` | Settlement-failure balances | [Original catalog](catalog/README.md#sec-settlement-failures), [Spec 001](../specs/spec001/README.md) |
| `nyfed-primary-dealers` | Dealer positions, transactions and financing | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `ofr-hedge-fund-monitor` | Aggregate hedge-fund leverage, liquidity and financing | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `finra-otc-transparency` | ATS/non-ATS activity and venue concentration | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `sec-nmfp` | Money-market-fund portfolio holdings | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `dtcc-public-swaps` | Public swap transaction dissemination | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `sec-midas` | Published market-structure metrics and datasets | [Positioning backlog](roadmap.md#additional-positioning-and-market-structure-sources) |
| `sec-nport` | Fund holdings, derivatives and securities lending | [Company/holdings backlog](roadmap.md#later-theme-company-disclosures-and-insider-activity) |
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
| SQLite and PostgreSQL | Tested against both; local PostgreSQL on port 55438 holds 2,046 reported dates through 2026-10-22 |
| Historical backfill | Implemented; three invalid rows explicitly quarantined; partial audit and quality report retained |
| Parquet | Committed typed snapshots, `current.json` publication and dry-run-first retention |
| Recovery | Owned-state/Bronze backup, empty-target restore and offline replay verified |
| Dagster | Pipeline-owned asset/job, local and live schedules; both schedules stopped |
| Freshness and retries | Configurable overlapping weekly bounds, bounded HTTP attempts and local health command |
| Databricks/Delta | Publisher/notebook implemented; live workspace behavior unverified |
| Production operations | Recurring acquisition rights, daemon/workers, external alerts and off-machine backup policy remain open |

No pipeline is represented as a deployed production service. Raw provider data,
local runtime config, passwords and backup packages remain outside Git.

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
