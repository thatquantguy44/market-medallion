# Spec 007 — NAAIM Exposure Index

Status: **initial local slice, one bounded live smoke, local health check and a
stopped daily schedule verified; explicit live permission reference remains
required**. Created
2026-10-31 on branch
`spec007-naaim-exposure`.

Capability ID: `naaim-exposure-index`. The active implementation target is the
separate delayed-public research sub-capability
`naaim-exposure-public-delayed`, using package
`naaim_exposure_public_delayed`. This is the next source selected by the user
after the CFTC TFF provider-contract discovery. It is a distinct NAAIM pipeline,
not an AAII extension and not a QuantMeridian-owned ingester.

## Objective

Provide a licensed, traceable weekly NAAIM Exposure Index dataset from
MarketMedallion for a future QuantMeridian integration. The dataset represents
the average U.S. equity-market exposure that participating NAAIM active
investment managers report; it is not an investor-opinion survey, portfolio
holdings disclosure, market forecast, or trading recommendation.

The initial implementation must preserve the provider payload, source terms,
as-of date, retrieval time and observed revisions. It may expose only fields
and derived outputs explicitly permitted by NAAIM's subscription agreement.
QuantMeridian consumes a documented, licensed output after MarketMedallion has
validated the upstream contract; it must not obtain the provider credentials or
scrape the provider's public website itself.

## Active implementation target — delayed public research only

The user confirmed continuing with the delayed feed on 2026-11-01. The display
name is **NAAIM Exposure Index — 3-month-delayed public data**. Gold latest,
CLI capture/health responses and Dagster materialization metadata explicitly
state `provider_delay_months: 3` and carry a human-readable `delay_notice`.
`as_of_date` (or `latest_as_of_date` in operational responses) identifies the
observation. Capture freshness measures acquisition health only; it does not
mean current-week data. NAAIM imposes the source delay; the pipeline adds none.
These additive labels do not change provider values, observed revisions,
365-day capture retention, internal-research scope or access permissions.


The user selected a distinct, **three-month-delayed**, internal research
capability: `naaim-exposure-public-delayed` in the future package
`naaim_exposure_public_delayed`. It is not the licensed-current-data pipeline
described elsewhere in this document. It must never supply QuantMeridian,
customer-facing displays, redistribution, a recurring schedule or a bulk
backfill.

NAAIM's public material does not document a stable public-display DOM/payload
contract or grant automated retrieval permission. The existing parser, invented
fixtures and one manual live request are limited to the user-confirmed,
permission-referenced delayed research slice. No recurring acquisition,
persistence expansion or downstream presentation may be added until NAAIM
provides written scope covering automation frequency, retention, internal use
and allowed transforms, plus an approved source-shaped example containing the
visible value, as-of date, units and failure behavior. A login, personal/member
password, hidden endpoint or paywall bypass is never in scope.

The current task breakdown is [plan.md](plan.md) and [todo.md](todo.md). The
licensed Program Partner/API route remains a separately gated future capability.

The manual, one-page command is:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.naaim_exposure_public_delayed \
  --root data/naaim-public-delayed-smoke-YYYY-MM-DD \
  --permission-reference '<nonsecret written-permission reference>'
```

It has no schedule, retry loop, bulk-history option or QuantMeridian export. A
bounded smoke run on 2026-11-01 accepted 131 delayed rows into ignored local
pipeline data; the raw provider payload and values remain outside Git.

The local, read-only health command makes no provider request and needs no
permission reference:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.naaim_exposure_public_delayed \
  --root data/naaim-public-delayed-smoke-YYYY-MM-DD --check
```

It reports capture freshness, retained-version count and whether the attributed
delayed latest output is available. Freshness is based on local capture time,
not the intentionally delayed observation date.

### Permission evidence status

On 2026-11-01, the user reported NAAIM granted permission by phone. The manual
smoke receipt records the nonsecret reference
`user-confirmed-naaim-permission-2026-11-01`; no call recording, provider email
or written terms are stored in this repository. The exact automation frequency,
retention, permitted transformations, redistribution and QuantMeridian scope
remain undocumented. The user subsequently confirmed daily automated pulls and
365-day retention on 2026-11-01. The stopped schedule enforces that cadence and
retention locally, but written provider terms are still required before any
scope expansion, API access or downstream display.

### Verified delayed-display table contract

One bounded, authorized read on 2026-10-31 established that the public NAAIM
page embeds `https://index.naaim.org/embeddable/table`. It returns static UTF-8
HTML containing exactly one `table-naaim` table with these columns, in order:

```text
Date, NAAIM Number, Bearish, Quarter 1, Quarter 2, Quarter 3, Bullish, Deviation
```

`Date` uses `%m/%d/%Y`; the numeric cells are decimal text. The parser rejects
header/order drift, malformed dates or numerics, missing cells, duplicate dates
and empty tables. Fixtures reproduce only the shape with invented values; no
live NAAIM values are committed. The table's published field labels are retained
verbatim in Bronze/Silver parsing; their units and any derived-transform rights
remain limited by NAAIM's written permission.

NAAIM exposes three public embeds:

- [`/embeddable/number`](https://index.naaim.org/embeddable/number) displays a
  current delayed number but no associated as-of date, so it is not a standalone
  ingestion source.
- [`/embeddable/chart`](https://index.naaim.org/embeddable/chart) is a visual
  presentation surface and is not parsed by this pipeline.
- [`/embeddable/table`](https://index.naaim.org/embeddable/table) is the sole
  canonical source because each row contains its date and the complete published
  metric set.

## Verified provider and access contract

| Contract item | Decision / evidence |
| --- | --- |
| Provider | National Association of Active Investment Managers (NAAIM) |
| Dataset | NAAIM Exposure Index, a weekly average of participating managers' reported U.S. equity-market exposure. |
| Observation basis | Participants report overall U.S. equity exposure at Wednesday market close; NAAIM updates the index weekly. |
| Verified statistics | The weekly index average; NAAIM also says the product includes standard-deviation and quartile statistics. Exact API field names, units and availability require credentialed API documentation. |
| Reported range | The public methodology describes responses from -200% leveraged short through +200% leveraged long. This is respondent exposure context, not a validation range to invent for an unreviewed API field. |
| Public display | Three-month-delayed, informational display only. It is not a current-data acquisition route and must not be scraped or used for QuantMeridian. |
| Current/history access | Subscribers receive current weekly values and more than twenty years of history. |
| API access | NAAIM confirms an API access option for organizations integrating the index into applications, websites or research platforms. No endpoint, schema, authentication flow, rate limit, revision policy or API terms were published in the reviewed public material. |
| Required entitlement | A QuantMeridian integration, redistribution or republication requires NAAIM's **Program Partner** subscription under the published subscription page. Password-only/membership access is not treated as an integration license. |
| Attribution | Published or redistributed data must state: `Source: NAAIM Exposure Index® (National Association of Active Investment Managers).` The provider says data must not be modified or presented to misrepresent it or imply NAAIM endorsement. |

The Program Partner plan currently advertises an API key or password and rights
for commercial platform use and redistribution. That public description is not
the API contract. Do not infer a URL, request format, pagination rule, response
schema, historical-retention guarantee, or permitted transformation from it.

## Semantics, timing and interpretation

- `as_of_date` is the provider-confirmed Wednesday market-close date, once the
  credentialed schema demonstrates how NAAIM represents it. It is not the
  retrieval date, an arbitrary week-ending convention, or a Friday publication
  date.
- `provider_published_at` remains null until the credentialed documentation or
  response supplies a reliable publication timestamp. A weekly cadence does not
  establish a historical availability timestamp.
- `retrieved_at` and `first_seen_at` are UTC MarketMedallion clocks. Historical
  imports are first known at capture time, never backdated to the source date.
- Preserve every observed normalized change as a new version. NAAIM's public
  material reviewed here does not state its correction/revision policy, so an
  absence of a stated policy never authorizes overwriting history.
- Preserve the provider's stated limitations: one composite respondent value can
  conceal conflicting strategies, participant styles vary, and sample size may
  be limited. Do not represent the series as comprehensive market positioning.

## Proposed source-owned lifecycle

The local research slice is under
`src/market_data_pipeline/pipelines/naaim_exposure_public_delayed/`. It owns a
strict HTML parser, explicit permission-gated one-page acquisition boundary,
immutable local Bronze receipt, typed Silver snapshot and attributed delayed
latest output, a read-only local health check, and a stopped daily Dagster
schedule. The schedule requires `NAAIM_SCHEDULED_LIVE_ACCESS=true`,
`NAAIM_DELAYED_ROOT` and `NAAIM_PERMISSION_REFERENCE` at runtime, runs at
10:00 America/New_York, and applies 365-day dry-run-tested source-owned
retention after a successful capture. It has no database adapter, backfill or
QuantMeridian export.

### Bronze

Archive exact credentialed response bytes before parsing, with the approved
request shape, redacted query metadata, nonsecret response headers, status,
SHA-256, parser version, run ID and UTC retrieval time. Never retain an API key,
password, cookie, Authorization header, or credential-bearing URL in Bronze,
logs, manifests, fixtures or Git.

Each request/page is a receipt. An ambiguous timeout, authentication failure,
unexpected schema, missing expected page, or unhandled `429`/5xx yields a
visible failed or partial audit outcome and cannot advance a watermark.

### Silver

Retain the complete licensed raw record and construct a typed observation only
after the credentialed schema is approved. The minimum future interchange grain
is:

```text
(source, dataset, as_of_date, metric, observed_version)
```

where `source = naaim`, `dataset = exposure-index`, and `metric` is an explicit
provider-supported measure such as the index average. The API documentation
will determine provider IDs, exact date encoding, whether values are percentages
or percentage points, available dispersion/quartile metrics, null behavior and
the true uniqueness key. No field name or numeric range is guessed in code.

### Gold and QuantMeridian boundary

The first permitted Gold output is a licensed, attributed latest observation
and an observed-as-of observation for each provider-supported metric. A
week-over-week change may be added only after NAAIM confirms that it is a
permitted presentation of the licensed data; it must use the previous available
observation and retain the prior date and calendar gap.

The future QuantMeridian adapter is deliberately outside this repository. Once
access is approved, MarketMedallion will publish a versioned, source-attributed
exchange contract containing at least the source/dataset, provider metric name,
as-of date, value and unit, `provider_published_at` when known, `retrieved_at`,
`first_seen_at`, version identity and Bronze lineage. QuantMeridian must retain
the required NAAIM attribution wherever values are displayed or redistributed.
No cross-repository database connection, secret, schema, migration, runtime
configuration, deployment identifier or Git history is created by this spec.

## Delivery gates

### Required before code or a live request

1. The account owner obtains or confirms Program Partner rights that cover the
   intended QuantMeridian audience, display, retention and redistribution.
2. NAAIM supplies the credentialed API documentation and a redacted sample
   response covering endpoint/base URL, authentication, field dictionary, units,
   date semantics, pagination/history scope, rate limits, errors and revisions.
3. A reviewer approves the schema, attribution placement and which transforms
   are permitted by the agreement.
4. The source contract is updated with the verified details, then provider-shaped
   **invented** offline fixtures cover valid observations, missing fields,
   changed historical rows, auth failures, throttling/timeouts and schema drift.

### Implementation sequence after approval

1. Add the source-specific package and manifest validation using only invented
   fixtures and injected transport.
2. Prove local Bronze → Silver → licensed Gold, audit, replay and observed-as-of
   behavior in SQLite first.
3. Add approved Parquet/PostgreSQL output and a stopped schedule only after
   local semantics pass.
4. Request a separate explicit, bounded live smoke run; compare its response
   with the contract and archive only the approved scope.
5. Specify and test the QuantMeridian-side adapter in that repository after the
   MarketMedallion interface and entitlement are accepted.

## Boundaries

Always:

- show the required NAAIM source attribution with every permitted downstream
  presentation;
- preserve licensed raw lineage, source timing, observed revisions and nulls;
- keep NAAIM implementation and credentials isolated in `naaim_exposure`.

Ask first:

- purchasing/subscribing, accepting commercial terms, using credentials,
  contacting NAAIM, or executing a live request;
- adding a warehouse/schedule/dependency, selecting transformed metrics, or
  changing QuantMeridian's schema/UI; and
- storing, redistributing, republishing or exposing derived metrics outside the
  agreed rights.

Never:

- scrape the public delayed page, bypass authentication, or use a personal or
  member password as a product-integration credential;
- imply that NAAIM endorses QuantMeridian or alter/present data misleadingly;
- treat the index as holdings, a forecast, a representative market census or
  investment advice; or
- copy a sibling repository's credentials, runtime configuration, database or
  deployment state.

## Acceptance criteria

This provider-discovery slice is complete when the following are true:

- [x] Official NAAIM methodology, cadence, public-delay and high-level API
  availability were reviewed.
- [x] The commercial redistribution/attribution boundary for a QuantMeridian
  integration was recorded.
- [x] The public delayed display was excluded from QuantMeridian and all
  customer-facing redistribution.
- [x] A bounded public table contract was characterized with invented fixtures;
  the local Bronze/Silver/delayed-latest fixture slice passes tests.
- [x] A local-only health check reports capture freshness, version count and
  attributed delayed-output availability without contacting NAAIM.
- [ ] Program Partner entitlement and credentialed API documentation are
  available and reviewed.
- [ ] Exact schema, units, identity, revisions, rate limits and retention terms
  are added to this spec.
- [ ] Offline-only fixtures and a source-owned vertical slice pass verification.
- [ ] A separate QuantMeridian implementation spec accepts the published
  MarketMedallion exchange contract and attribution requirements.

## Official sources reviewed

- [NAAIM Exposure Index methodology and public-delay notice](https://naaim.org/programs/naaim-exposure-index/)
- [NAAIM Exposure Index FAQs](https://naaim.org/programs/naaim-exposure-index/faqs/)
- [NAAIM Exposure Index subscription and redistribution terms](https://members.naaim.org/ap/Membership/Application/GrZAe6L1)
