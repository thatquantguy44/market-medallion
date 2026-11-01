# Spec 009 — Cboe published daily put/call ratios

Status: **draft provider contract; connector not implemented**. Created
2026-11-01 on branch `spec007-naaim-exposure`.

Capability ID: `cboe-published-put-call-ratios`. This capability covers Cboe's
published daily options-market ratio values. It is separate from raw OCC volume
and must not be rebuilt from another provider's data.

## Objective

Create a traceable local research dataset of Cboe's explicitly published daily
put/call-ratio series, initially including total, index and equity labels where
the provider page presents them. Preserve the page response, provider labels,
requested/displayed date, retrieval time, terms notice and observed revisions.

The first slice is local and fixture-driven. It cannot assume a stable API,
automated use permission, a complete history, a schedule, redistribution or a
QuantMeridian output.

## Officially reviewed provider evidence

The current Cboe Daily Market Statistics page presents a date input and a
Ratios table including `TOTAL PUT/CALL RATIO`, `INDEX PUT/CALL RATIO` and
`EQUITY PUT/CALL RATIO`, along with other product-specific labels. Cboe says
the summary data are furnished for visitor convenience, without a guarantee of
accuracy, and subject to its website terms. Its legacy Historical Data route is
available, but its applicable ratio archives and continuity after 2019 have not
been verified. The implementation must not infer a public API, durable HTML
contract, bulk-history entitlement or reuse right from page visibility.

## Proposed source and data contract

| Contract item | Initial decision / open verification |
| --- | --- |
| Provider | Cboe. |
| Source route | Daily Market Statistics page, with the exact date-selection request and rendered/source response to be characterized before code. |
| Grain | `(source, dataset, observation_date, published_ratio_label)`; provider labels are retained verbatim and are not remapped to a generic source-independent taxonomy in the first slice. |
| Core fields | Observation date, ratio label, decimal ratio value, raw response reference, retrieval time and first-seen time. |
| Date semantics | An observation date is accepted only when the selected/requested date is explicit and agrees with the provider display. A page default/current date is not sufficient evidence for a historical row. |
| History | Historical coverage, archive scope, available file formats and gaps after 2019 are open questions. No backfill is authorized by this spec. |
| Current access | Public daily display observed; automation, reuse, attribution and rate policies require review against Cboe terms before any request. |

## Data lifecycle

### Bronze

Keep the exact page/document response and a receipt with the requested date,
final URL, nonsecret headers, status, SHA-256, parser version and UTC retrieval
time. Retain date-selection context because the same display route may otherwise
be ambiguous. Do not retain credentials or bypass access controls.

### Silver and observed revisions

Parse only an exact, reviewed fixture shape. Store decimal strings/decimals
without rounding; reject unknown required table structure, duplicate
`(date, label)` rows, invalid dates and nonfinite/nondecimal values. A repeated
unchanged row is idempotent; an observed changed ratio becomes a new immutable
version. `provider_published_at` remains null unless Cboe supplies row-specific
evidence.

### Gold, first slice only

Publish the latest observed value for each provider label with source
attribution and delayed/availability metadata. Do not combine labels,
recalculate a ratio from unrelated volumes, rank sentiment, or produce trading
signals. Any normalized “total/index/equity” convenience mapping needs a later
reviewed crosswalk and must preserve the original label.

## Boundaries

Always:

- identify values as Cboe-published daily ratios and retain Cboe's exact label;
- preserve response lineage, selected date, retrieval time and observed
  revisions;
- characterize behavior with invented fixtures before a live request.

Ask first:

- live retrieval, scheduling, archive downloads, bulk backfill, production
  persistence, redistribution or QuantMeridian integration;
- adding product-specific labels beyond the reviewed initial set;
- treating Cboe's daily display as a stable API or using a source other than
  the documented Cboe page/archives.

Never:

- represent a Cboe ratio as OCC volume, a universal options-market measure, or
  an investor-positioning observation;
- fabricate history or an exact publication timestamp;
- overwrite an observed correction or hide an unavailable date.

## Delivery sequence and acceptance criteria

**Relative discovery priority: 1 of the three new proposals.** It has a small,
clearly labelled daily payload, so the next deliverable is a narrow provider
contract—not production ingestion. This ranking does not displace active NAAIM
permission evidence or the reviewed CFTC work.

1. Verify Cboe terms, date-selection behavior, source rendering and current
   history/archive continuity with one separately authorized bounded read.
2. Record a source-shaped sample and exact initial label set; explicitly decide
   whether each label is in scope.
3. Add invented fixtures for valid values, absent ratio, changed value, date
   mismatch, schema drift, empty response and relevant HTTP failures.
4. Implement a source-owned local Bronze/Silver/latest slice from fixtures;
   request a live smoke run only after review.

Implementation requires reviewer acceptance of:

- [ ] date-selection and observation-date evidence;
- [ ] terms, attribution, automation and retention treatment;
- [ ] exact labels, decimal behavior and history-gap policy;
- [ ] fixture-only test plan and observed-version semantics;
- [ ] package name `cboe_put_call_ratios` under
  `src/market_data_pipeline/pipelines/`.

## Official sources reviewed

- [Cboe Daily Market Statistics](https://www.cboe.com/markets/us/options/market-statistics/daily)
- [Cboe Historical Data](https://www.cboe.com/us/options/market_statistics/historical_data/)
