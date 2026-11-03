# Market-data source catalog

**Current build priority (2026-11-03):** the
[five-stage securities-finance program](../securities_finance/README.md),
Specs011–015. The older catalog below preserves original source rationale;
its former priority labels are superseded by that program. Full source
contracts/plans/tasks now exist for SEC FTD + Reg SHO, FINRA short volume,
FINRA OTC transparency, SEC N-PORT and NY Fed primary dealers. Spec011 now has
a local SEC Bronze/SQLite/audit/Gold slice; later connectors remain planned.
Its parser/manifest/local inspection slice reconciles two approved
September2026 archives. T18 type contracts/serialization are implemented for both
source packages; T03–T05 SEC persistence, observed revisions, cutoff Gold and
evidence-gated withdrawals are complete. Next is S11-T06 offline replay.
PostgreSQL and Databricks are required Spec011 targets, not implemented yet.
NAAIM remains paused.

Researched against official provider pages on 2026-10-26. These are source
candidates, except for AAII's implemented authorized-local-CSV slice.
Automated provider retrieval remains unverified. “Under-utilized” is a research theme;
adoption and investment usefulness have not been measured. Free access does not
establish unrestricted redistribution rights. Verify terms for each dataset.

This catalog describes the original four source candidates and the AAII
Investor Sentiment Survey addition. The
[master roadmap and source backlog](../roadmap.md) retains all 14 additional
sources discussed, with official links, access methods, potential uses, and
implementation considerations. The [master handoff](../handoffs/project_bootstrap.md)
lists the complete inventory and current priorities. Other sources remain unimplemented.

| Priority | Dataset | Proposed research use | Access evidence | Important limit |
| --- | --- | --- | --- | --- |
| First local slice | AAII Investor Sentiment Survey | Opinion shares and bull/bear spread | CSV import and a bounded official XLS live load are verified; routine automation remains unverified | Local captures are not historical publication vintages |
| Planned | CFTC Commitments of Traders, TFF and Disaggregated | Category positioning and changes | CFTC documents a public API generally usable without a token | Report families and futures-only/combined reports differ |
| Next | FINRA short-sale volume | Reported short-trading activity | Public catalog links downloads and a query API; confirm current free automated route before coding | Scope and denominators must match; short volume is not short interest |
| Funding extension | OFR Short-term Funding Monitor | Funding rates and repo conditions | Official API explicitly needs neither tokens nor registration | Review per-series provenance, units, and underlying-source terms |
| Candidate | SEC fails-to-deliver | Settlement-failure balances | Public ZIP downloads of pipe-delimited records | Outstanding balances are not daily flows or proof of abusive shorting |

## CFTC

The reports break down open interest by trader classifications. TFF covers
financial contracts; Disaggregated covers agriculture and natural resources.
Keep these universes and classifications separate. Verify actual API field names,
dataset identifiers, release timing, and corrections during implementation.

Sources: [COT documentation](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm),
[public reporting report families](https://publicreporting.cftc.gov/stories/s/r4w3-av2u),
[user guide](https://publicreporting.cftc.gov/stories/s/COT-Help/p2fg-u73y/).

## FINRA

Retain reporting-facility coverage. Avoid joining a venue-specific numerator to
market-wide volume. Current download availability, automation conditions, and
terms remain an implementation prerequisite; the website catalog alone is not a
validated extraction contract.

Sources: [short-sale volume catalog](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data),
[interpretation guidance](https://www.finra.org/rules-guidance/notices/information-notice-051019).

## OFR

The STFM API offers metadata discovery and time-series retrieval as JSON at
`https://data.financialresearch.gov/v1`. Start from metadata rather than guessing
series mnemonics. Select measures after checking overlap with existing macro
coverage in the sister project.

Source: [official STFM API documentation](https://www.financialresearch.gov/short-term-funding-monitor/api/).

## SEC settlement failures

The SEC publishes files twice per month with a lag. Preserve both settlement
date and observed availability. Its notes explain that balances include prior
outstanding failures and that fails can arise from long or short sales. The
price field can be missing and refers to the preceding day's close.

Source: [SEC data and field definitions](https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data).

## AAII Investor Sentiment Survey

Backlog ID: `aaii-investor-sentiment`. Status: authorized-local-CSV slice
implemented under Spec 002; automated-access review still required.
AAII reports weekly bullish, neutral, and bearish percentages for
individual investors' six-month stock-market outlook, with results published on
Thursdays. The survey dates to 1987. These are stated opinions, not observed
positions or trade flows.

Potential derived features: bullish-minus-bearish spread in percentage points,
week-over-week changes, and rolling percentiles using only observations available
at the calculation cutoff. Predictive usefulness remains a research question.

Start from the official results and historical download pages. AAII describes
full historical data as a member benefit; verify current membership/cost,
automation permissions, and reuse terms before treating this as a free source.
No official public API is verified. The official XLS endpoint and its current layout were verified by a bounded live run. Preserve
survey-period labels separately from publication/retrieval timestamps; check
percentage units, rounding, missing weeks, and revisions when creating fixtures.

CSV tests use synthetic values. A subsequent explicitly authorized live request
on 2026-10-26 downloaded the official XLS file successfully. Its SENTIMENT sheet
has a row-4 Date/Bullish/Neutral/Bearish header, Excel date cells and fractional
shares; the preceding row labels the dates Reported Date. Four observations in
the October 1–26 window were saved to PostgreSQL with original-byte lineage.
Earlier access challenges are not treated as a permanent access restriction;
no bypass or member login was used. Recurring entitlement remains unverified. The reviewed
[terms](https://www.aaii.com/privacy/tos) do not establish automation or
redistribution permission for this pipeline. See the
[runbook](../deployment/aaii_runbook.md) for the supported local workflow.

Sources: [official survey page](https://www.aaii.com/sentiment-survey),
[historical results and download page](https://www.aaii.com/sentimentsurvey/sent_results),
[survey methodology](https://www.aaii.com/latest/article/16502-the-aaii-investor-sentiment-survey).

## Connector readiness checklist

Before enabling any provider: verify current endpoints, response fixtures,
pagination/archives, provider identifiers, units, null markers, release schedule,
correction behavior, HTTP limits, authentication, attribution, and usage terms.
Persist that metadata with the manifest. No dataset is production-ready solely
because it appears in this catalog.
