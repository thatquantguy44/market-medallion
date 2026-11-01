# Spec 008 — OCC options put/call volume query

Status: **draft provider contract; connector not implemented**. Created
2026-11-01 on branch `spec007-naaim-exposure`.

Capability ID: `occ-options-volume-query`. This capability covers granular,
reported options put/call **volume** from OCC's Volume Query service. It is not
open interest, a published put/call ratio, an options-trading recommendation, or
a substitute for exchange-specific volume.

## Objective

Create a traceable local research dataset of OCC options volume, optionally
filtered by report date, product kind, underlying/symbol, account type and
put/call side. Preserve the provider response, selected filters, report-date
label, retrieval time and every observed correction. Any implementation must
make the selected population explicit; it must not label a filtered subset as
the whole options market.

This is a future, source-owned Bronze → Silver → Gold slice. It does not
authorize a schedule, mass historical extraction, a production database,
redistribution, or a QuantMeridian export.

## Officially reviewed provider evidence

OCC documents an Options Volume Query batch CSV interface with these relevant
parameters: `reportDate`, `format=csv`, `volumeQueryType=O`, `symbolType`,
`symbol`, `reportType` (daily/weekly/monthly), `accountType`, `productKind`,
`porc` (put/call/both), and `contractDt`. Account types include customer, firm
and market maker; product kinds include options on debt, indexes and stocks.
The live request/response schema, pagination, rate limits, terms, attribution,
and the claimed rolling 24-month availability remain unverified implementation
gates. The public UI's stated coverage must be captured in a dated contract
review rather than assumed from this proposal.

## Proposed source and data contract

| Contract item | Initial decision / open verification |
| --- | --- |
| Provider | The Options Clearing Corporation (OCC). |
| Source route | Documented batch CSV Volume Query endpoint; parameter values must be URL-encoded and retained in the Bronze receipt. |
| Dataset scope | Options only (`volumeQueryType=O`); futures volume is explicitly excluded. |
| Grain | To be established from a permitted source-shaped response. The candidate logical key includes report date, report type, product kind, account type, put/call side, symbol identity, contract date and every provider identity field needed to distinguish rows. |
| Date semantics | `reportDate` is a provider report-date label. It is not a publication timestamp. Store it as a date only after fixture-backed parsing. |
| Values | Preserve provider volume counts as nonnegative integers; preserve source fields rather than reconstructing volume from ratios. |
| History | The public query is reported to offer a rolling 24-month window. Verify available dates and whether queries are limited by result size before promising history or a backfill. |
| Current access | Public documentation exists; automation, reuse, rate and retention rights are **not yet approved**. |

## Data lifecycle

### Bronze

Archive exact CSV bytes and a receipt before parsing. The receipt must contain
the nonsecret selected parameters, canonical endpoint, response status,
nonsecret response headers, SHA-256, parser version and UTC retrieval time.
Record an error body/metadata when safely available. Never log credentials or
invent credentials for a public route.

### Silver and observed revisions

Normalize only a source-shaped, fixture-backed schema. Keep raw provider
identifiers, the complete requested filter set, report-date label,
`retrieved_at` and `first_seen_at`. Identical logical observations create no
new version; changed normalized observations create an immutable observed
version. Historical rows first obtained by this pipeline are known from their
capture time, not from their report date.

### Gold, first slice only

If source semantics permit, expose bounded aggregates only for the exact
population stated in the query, such as daily put volume, call volume and
`put_volume / call_volume` when call volume is positive. Gold must retain the
dimensions used for aggregation and label the metric as OCC query-derived. It
must not call the result a Cboe ratio, all-market ratio, sentiment signal, or
investment recommendation.

## Boundaries

Always:

- retain the exact provider payload, parameters, raw identifiers and
  report-date/retrieval/first-seen clocks;
- use invented offline CSV fixtures, including parser drift and error cases;
- validate schema, integral nonnegative volume, date format, duplicate logical
  keys and selected scope before producing Silver;
- distinguish puts/calls and account/product filters from a market-wide total.

Ask first:

- any live request, automated cadence, historical backfill, production
  persistence, redistribution, additional OCC dataset, or derived signal;
- treating a UI coverage statement as a reusable/automatable data license;
- collapsing account types, product kinds or symbols into a total.

Never:

- claim open interest, trade direction, investor sentiment or market-wide
  coverage from a filtered volume response;
- overwrite observed historical values or fabricate publication timestamps;
- use another provider's ratio as validation for an OCC-derived metric;
- copy secrets, databases, deployment configuration or data from another
  repository.

## Delivery sequence and acceptance criteria

**Relative discovery priority: 3 of the three new proposals.** The granular
query is valuable but has the most access, result-shape and aggregation risk.
It does not displace active NAAIM evidence work or the reviewed CFTC contract.

1. Review current OCC terms, documented history/row limits and a bounded query
   response; record the evidence date and allowed automation/reuse scope.
2. Capture a permitted, source-shaped sample; define the exact CSV columns,
   identity and selected filter semantics.
3. Add invented fixtures for a valid response, no records, duplicate key,
   changed value, malformed count/date, schema drift, `429` and server failure.
4. Implement and verify a local source-owned Bronze/Silver/Gold slice from
   fixtures before requesting one separate bounded live smoke run.

Implementation requires reviewer acceptance of:

- [ ] the precise source schema and logical grain;
- [ ] current terms, attribution, rate and retention treatment;
- [ ] a history/result-size plan that cannot silently omit rows;
- [ ] clear scope labels for every output and an offline-only test strategy;
- [ ] package name `occ_options_volume_query` under
  `src/market_data_pipeline/pipelines/`, without changing AAII or NAAIM.

## Official sources reviewed

- [OCC Volume Query](https://www.theocc.com/market-data/market-data-reports/volume-and-open-interest/volume-query)
- [OCC Volume Query Batch Processing](https://www.theocc.com/market-data/market-data-reports/other-market-data-info/batch-processing/volume-query-batch-processing)
