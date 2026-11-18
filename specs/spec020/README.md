# Spec 020 — ICI weekly ETF issuance and long-term mutual fund flows

**Status:** draft requirements; no connector, source acquisition, warehouse tables, fixtures, jobs, or delivery are implemented. Drafted 2026-11-18 (UTC).
**Capability ID:** `ici-weekly-etf-fund-flows`.
**Proposed source-owned package:** `src/market_data_pipeline/pipelines/ici_weekly_flows/`.
This proposal does not change the current Spec013 OTC-T08 implementation priority or resume NAAIM.

## Request, use case, and research question

Add an ICI flow pipeline to MarketMedallion so a local researcher can query what ICI reported for a given week, fund type and category, and what the pipeline had actually observed by a chosen cutoff.

The research question is whether reported aggregate ETF net issuance and long-term mutual fund net new cash flow provide useful context for fund demand. Building the dataset does not establish a trading signal or predictive value.

## Source evidence reviewed on 2026-11-18

| Official source | Verified observation | Remaining gate |
| --- | --- | --- |
| [Weekly ETF page](https://www.ici.org/research/statistics/etfs/weekly-estimated-etf-net-issuance) and [ETF release](https://www.ici.org/research/stats/etf_flows) | A weekly release and linked XLS summary are listed. The release labels ETF **estimated net issuance** in millions of dollars, shows several week-ended columns, and notes prior-week revisions and rounding. | Admit the exact licensed XLS/HTML route, workbook schema, access method and historical coverage before coding. |
| [Weekly mutual fund page](https://www.ici.org/research/statistics/mutual-funds/weekly-estimated-longterm-mutual-fund-flows) and [mutual fund release](https://www.ici.org/research/stats/flows) | A weekly release and linked XLS summary are listed. The release labels **estimated net new cash flows** for long-term mutual funds, in millions of dollars, and notes prior-week revisions. | Confirm the selected artifact, table shape and categories with source-shaped samples. |
| [Combined release](https://www.ici.org/research/stats/combined_flows) | ICI defines mutual fund flow as new sales less redemptions plus net exchanges; ETF net issuance is gross issuance less gross redemptions. It says weekly values are estimates and monthly actuals differ. | Treat as a possible reconciliation source only after separate access/schema review; do not ingest it as a third fact series in v1. |
| [ICI permissions](https://www.ici.org/permissions) and [copyright policy](https://www.ici.org/copyright-and-linking-policies) | ICI says underlying statistical-publication data are available to members and subscribers only and are not to be republished; website reuse has restrictions. | Source owner must document authorization for the precise acquisition, storage, internal use and any export. Public page visibility alone does not clear this gate. |

The releases exclude mutual funds primarily investing in other mutual funds and ETFs primarily investing in other ETFs. The ETF release includes some commodity ETFs not registered under the 1940 Act. Category definitions and scope must travel with the data. The observed release dates are examples, not a guaranteed release clock or historical publication-vintage API.

## Scope

**Must have, first local release:** Two independent weekly ICI populations: long-term mutual funds and ETFs. Admit authorized, operator-supplied source files first; preserve exact source bytes and receipts. Provide strict parsing, versioned observations, cutoff queries, local PostgreSQL primary, coherent typed Parquet research snapshot, network-free replay, and no-request health. A bounded automated fetch is a separate gate after access and source conformance are documented.

**Should have after the first local release:** An attributed, rights-compatible versioned local research artifact; stopped-by-default Dagster job and schedule; bounded overlap refresh for newly authorized captures.

**Out of scope:** Monthly actual mutual fund flows, monthly ETF data, money market funds/assets, fund-level or ticker-level flows, the combined ICI report as a third authoritative series, a public feed, redistribution, an inferred release timestamp, and a trading signal. No N-PORT or existing pipeline semantics change.

## Functional requirements

| ID | Priority | Requirement | Verification |
| --- | --- | --- | --- |
| REQ-01 | Must | Record the admitted source artifact, use rights, attribution, historical scope and exact permitted operations before acquisition. | A dated source/access matrix names the permission basis and an operator-approved route; absent approval blocks source requests. |
| REQ-02 | Must | Keep ETF estimated net issuance and mutual fund estimated net new cash flow as distinct measures and populations. | A query/export carries `fund_type`, `measure`, frequency, currency, unit and estimate status; neither measure is relabelled as the other. |
| REQ-03 | Must | Archive exact authorized input bytes and nonsecret provenance before parsing, including source URL or local origin, checksum, capture time and parser/schema version. | An invalid file still has a traceable rejected capture; offline replay uses the archived bytes. |
| REQ-04 | Must | Parse only an admitted source-shaped schema. Preserve every recognized category label, week-ended date, signed decimal value and source location; reject duplicate keys, bad dates/numbers, unsupported layout and unknown required columns. | Invented fixtures cover valid, malformed, changed-layout and duplicate cases without value guessing. |
| REQ-05 | Must | Preserve category hierarchy and definition era. Distinguish total, parent and child categories so totals are not double-counted. | Category metadata and coverage tests reject unreviewed additions, removals or definition changes; no parent-plus-child sum is published. |
| REQ-06 | Must | Retain each observed correction, reversion and removal with an immutable receipt and capture-order first-seen time. Identical captures are idempotent. | Two captures containing different values for one week remain separately queryable; replay reproduces their order and cutoffs. |
| REQ-07 | Must | Expose reported week-ended date, source-stated release date if present, actual capture time and first-seen time as separate clocks. Unknown publication time remains null. | Cutoff queries never expose a version captured after the cutoff or infer a release timestamp from week-end. |
| REQ-08 | Must | Publish a coherent local PostgreSQL/Parquet snapshot of source levels and coverage. A partial or rejected capture cannot advance its complete-publication pointer. | Interrupted publication recovers to one complete generation; output reconciles to admitted observations. |
| REQ-09 | Must | Support bounded, network-free replay and a local health view showing last capture, last observed week by population, revisions, missing weeks and incomplete/rejected captures. | Empty-target replay matches logical records, clocks and snapshot hashes; health makes no HTTP call. |
| REQ-10 | Should | Refresh an authorized overlapping week window without assuming an append-only provider file. | A correction to an older displayed week is captured without overwriting its prior version. |
| REQ-11 | Should | Supply an attributed local artifact only for an approved audience/use, with method and input-version IDs. | The artifact states ICI source, estimates, populations, units, dates, capture cutoff and rights limits. |
| REQ-12 | Should | Add source-owned Dagster definitions with ingestion/schedule stopped by default. | Definition loading performs no ICI request; current NAAIM and other activation states remain unaffected. |

## Data contract

The **observation grain** is `(provider=ICI, source_series, fund_type, measure, week_ended, category_code, definition_era)` per observed version. `source_series` identifies the separately admitted ETF or mutual-fund publication; a receipt ties each value to its exact source artifact and row/cell. `category_code` is a reviewed local mapping to ICI's source label, with parent relationship and effective definition era.

Store source numeric text and a lossless decimal value in **millions of U.S. dollars** only after verifying the artifact's stated unit. Negative values represent outflow/net redemptions under the source's measure. Null, missing, suppressed, and malformed input are distinct states; none becomes zero. Date labels remain week-ended dates. Preserve source-stated notes and coverage limitations, including rounding and excluded fund populations. Rounding means a sum of published components need not equal the published total exactly.

Bronze owns immutable files and receipts; Silver owns typed observations, category versions and capture membership; Gold exposes source levels plus explicitly labelled optional week-over-week changes within a compatible fund type/category/definition era. A cross-population comparison may show ETF and mutual fund levels side by side for the **same week-ended date** after both sources are observed. It must retain each source's distinct measure and availability clock. Do not silently use the latest ETF week with an older mutual fund week or treat the combined ICI total as an additive validation identity without reviewing its methodology.

## Non-functional requirements

| ID | Priority | Constraint | Verification |
| --- | --- | --- | --- |
| NFR-01 | Must | Python 3.12 pure transforms, source-owned package and existing locked dependencies first; PostgreSQL uses the project's `additional_market_data` database with a separate schema/role after provisioning review. | Offline source/wheel behavior and schema isolation pass. |
| NFR-02 | Must | No automated HTTP or credential use before REQ-01 admission. Later HTTP uses the run-scoped shared client, explicit bounds, timeouts and retry budget. | Disabled/local/replay/health paths make zero requests; transport tests bound failures and size. |
| NFR-03 | Must | Historical backfill uses actual capture first-seen time; it cannot manufacture historical knowledge vintages. | Cutoff/replay fixtures prove backfilled rows remain unavailable before capture. |
| NFR-04 | Must | Parsing and publication fail closed on source drift, incomplete artifacts or unreviewed category changes. | Prior committed snapshot stays readable after each injected failure. |
| NFR-05 | Must | Preserve-all raw and version history until a rights-compatible retention policy is explicitly set. Keep credentials and restricted source bytes out of Git, logs and public artifacts. | Repository and log review finds metadata only; retention dry run shows no deletion. |
| NFR-06 | Should | Define a freshness expectation only after observing the chosen artifact's actual release behavior and holiday exceptions; cost/capacity limits come from the permitted source route. | Runbook records measured schedule, maximum file/row/window sizes and alert thresholds before schedule activation. |

## Constraints & Risks

| ID | Risk / constraint | Treatment |
| --- | --- | --- |
| RISK-01 | ICI rights may preclude automated capture, internal redistribution or a research artifact. | REQ-01 is a source-owner gate; local fixture work can proceed independently. No live fetch or sharing follows from this draft. |
| RISK-02 | Weekly estimates are revised, and monthly actuals have different methodology/frequency. | Retain overlap versions and label estimate status; do not replace weekly history with monthly data. |
| RISK-03 | The two weekly publications can have different latest week-ended dates or release dates. | Partition health and cutoff visibility by source; join only on the same reported week. |
| RISK-04 | Category or workbook layouts may change, and parent/child categories overlap. | Pin a schema/definition era from authorized samples; stop on unknown layout and prevent hierarchical double counting. |
| RISK-05 | Current public releases do not establish full historical vintages or precise intraday publication times. | Use capture first-seen for point-in-time queries; keep unknown publication time null. |

## Dependencies, assumptions and owners

- **Source/data owner:** Confirm ICI subscription or permission terms for file access, local storage, internal research, retention, artifact output and any automation. Identify approved audience. No such authorization was verified during this draft.
- **Data engineer:** Obtain authorized source-shaped ETF and mutual fund artifacts, their current schemas and category definitions; document the actual historical coverage and revision presentation.
- **Warehouse/operator:** Select an isolated ICI schema/writer in `additional_market_data`, storage bounds, backup scope and release/freshness policy.
- **Research reviewer:** Approve the interpretation of ETF issuance versus mutual fund flows, category hierarchy, exclusions and any derived comparisons.
- **Assumption:** The initial user need is aggregate **weekly** flow context. Monthly actuals and fund-level data require a separate scope decision.

## Open Questions

1. Is the intended research output the two separate weekly series, a licensed combined series, monthly actuals, or a staged combination? This draft assumes the two separate weekly series.
2. Which ICI access/subscription agreement covers automated retrieval, storage, internal sharing and retention for this project? Who signs off on it?
3. Which exact XLS or other permitted artifacts should be admitted, and what historical date range and schema/category eras do they actually expose?
4. What internal consumer needs the data, and is a local export permitted for that audience?
5. What release/freshness target and bounded backfill window are useful after observing real release behavior and cost?

## Acceptance Criteria

| ID | Evidence required | Trace |
| --- | --- | --- |
| AC-01 | Dated source/rights matrix and two admitted source-shaped artifacts, or an explicit blocked-source result with fixture-only work. | REQ-01, NFR-02, RISK-01 |
| AC-02 | Offline fixtures validate exact grain, signed decimals, units, week dates, categories, hierarchy, drift rejection and no silent zero/total double count. | REQ-02, REQ-04, REQ-05 |
| AC-03 | Bronze receipts and Silver versions prove identical capture, correction, reversion, removal and equal-clock ordering. | REQ-03, REQ-06, REQ-10 |
| AC-04 | Cutoff and replay tests prove source release/capture/first-seen separation, including historical backfill and differing latest weeks. | REQ-07, REQ-09, NFR-03 |
| AC-05 | PostgreSQL/Parquet reconciliation and injected failure recovery preserve the prior committed generation and isolate the ICI schema. | REQ-08, NFR-01, NFR-04 |
| AC-06 | No-request health reports freshness/coverage separately for ETF and mutual fund sources; any automation is stopped by default. | REQ-09, REQ-12, NFR-06 |
| AC-07 | If rights permit, an attributed local artifact and runbook identify exact audience, limits, evidence, costs, recovery and unresolved coverage. | REQ-11, NFR-05 |

**Definition of done:** AC-01 through AC-06 are evidenced for an authorized local dataset, and AC-07 is evidenced only if its rights gate clears. This draft is the specification step; implementation, live access, scheduling and consumer publication each remain separate decisions.
