# Spec 010 — Nasdaq Trader daily market-breadth counts

Status: **draft provider contract; connector not implemented**. Created
2026-11-01 on branch `spec007-naaim-exposure`.

Capability ID: `nasdaq-daily-market-breadth`. This capability covers Nasdaq
Trader's daily-file counts of advances, declines and unchanged issues. It is a
count-based breadth dataset, not a price-return index, constituent history,
exchange-wide consolidated market breadth or a trading signal.

## Objective

Create a traceable local research dataset from Nasdaq Trader daily files that
preserves the date label, count fields, complete raw line/file and observed
availability. The initial candidate source is the annual 2026 text file, whose
header and rows expose `Advances`, `Declines` and `Unchanged` alongside Nasdaq
market statistics. Earlier-year availability and consistent schema are explicit
implementation gates.

The first slice is local and fixture-driven. It does not authorize annual-file
bulk backfill, a recurring downloader, a production database, redistribution or
QuantMeridian integration.

## Officially reviewed provider evidence

Nasdaq Trader's Daily Market Statistics page identifies the daily-files
collection. The current `daily2026.txt` file is CSV-like text with a `Date`
column and the fields `Advances`, `Declines` and `Unchanged`; the reviewed
records use values such as 2,959 advances, 1,890 declines and 168 unchanged
for a daily observation. The file also contains index, trade, volume, dollar
volume and market-value columns, none of which enters the initial breadth
contract. Historical URL coverage, annual-file naming stability, publication
time, correction behavior, terms and automation/reuse rights are not yet
verified.

## Proposed source and data contract

| Contract item | Initial decision / open verification |
| --- | --- |
| Provider | Nasdaq Trader. |
| Source route | Daily Market Statistics annual text/CSV file, beginning with `daily2026.txt`; do not generalize a year URL pattern until older years are verified. |
| Grain | `(source, dataset, observation_date)` for one daily file row. |
| Required fields | `Date`, `Advances`, `Declines`, `Unchanged`; all three counts must parse as nonnegative integers. |
| Scope | Nasdaq daily-file breadth counts exactly as published. The provider's eligible-universe definition must be documented before calling the counts Nasdaq-wide or U.S.-market-wide. |
| Date semantics | Treat parsed date as an exchange/statistics date label; `provider_published_at` is null until independently documented. |
| History | Earlier-year retention and schema consistency are unverified. No history claim or backfill is part of this spec. |
| Current access | A public 2026 file is observed; permission for automated retrieval, retention and reuse remains to be reviewed. |

## Data lifecycle

### Bronze

Archive exact annual-file bytes, requested source URL, status, nonsecret
headers, SHA-256, parser version and retrieval clock. If a year file is later
shown to update in place, each fetch remains its own receipt; a capture is never
silently replaced.

### Silver and observed revisions

Parse a strict header-based row contract. Reject missing required fields,
duplicate dates, malformed dates, fractional/negative counts and accidental
column-order/schema drift. Store the raw row, count values, retrieval time and
first-seen time. An unchanged observation is idempotent; a different observed
row for the same date becomes a new immutable version.

### Gold, first slice only

Expose counts plus safe, labelled derived measures only after review: net
breadth `advances - declines`, and advance share
`advances / (advances + declines + unchanged)` when the denominator is positive.
Each output must retain scope and denominator. Do not infer index performance,
constituent coverage, cross-exchange breadth or predictive value.

## Boundaries

Always:

- preserve exact files, provider fields, source URL and availability clocks;
- retain raw counts rather than only derived breadth metrics;
- use invented offline fixtures for all parsing and version behavior;
- make file year, coverage and eligible-universe caveats visible in outputs.

Ask first:

- live acquisition, schedule, multi-year backfill, production persistence,
  redistribution or QuantMeridian output;
- relying on a year URL pattern, combining files or adding additional columns;
- interpreting advance/decline counts as a complete market universe.

Never:

- convert absent/invalid counts to zero;
- backdate first-seen time or assign a publication timestamp from the date
  label;
- label a derived measure as an index return, sentiment score or advice.

## Delivery sequence and acceptance criteria

**Relative discovery priority: 2 of the three new proposals.** The 2026 field
contract is visible, but coverage and universe semantics need more verification
than Cboe's labelled daily ratios. This ranking does not displace active NAAIM
permission evidence or the reviewed CFTC work.

1. Verify current Nasdaq terms, historical year availability, update/correction
   behavior, file naming and the documented breadth universe.
2. Capture a permitted source-shaped sample from at least two years or record
   a single-year coverage boundary; establish the CSV dialect and date format.
3. Add invented fixtures for valid row(s), a changed row, duplicate date,
   missing column, malformed date/count, empty file and HTTP failures.
4. Implement a local source-owned Bronze/Silver/latest slice from fixtures;
   request a separately approved bounded live smoke run only after review.

Implementation requires reviewer acceptance of:

- [ ] verified universe, date semantics and year-coverage contract;
- [ ] current terms, attribution, automation and retention treatment;
- [ ] strict schema/CSV dialect and correction policy;
- [ ] derived-measure denominator/scope treatment and fixture-only test plan;
- [ ] package name `nasdaq_daily_market_breadth` under
  `src/market_data_pipeline/pipelines/`.

## Official sources reviewed

- [Nasdaq Trader Daily Market Statistics](https://www.nasdaqtrader.com/Trader.aspx?id=DailyMarketFiles)
- [Nasdaq Trader 2026 daily file](https://www.nasdaqtrader.com/dynamic/dailyfiles/daily2026.txt)
