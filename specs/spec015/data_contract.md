# NY Fed primary-dealer statistics data contract

Status: **proposed normalized contract; runtime absent**. Reviewed 2026-11-03.
Owner/capability: `nyfed-primary-dealers`.
Proposed artifact version: `nyfed-primary-dealers-research/1`.
Read the [spec](README.md) and
[common contract](../../docs/securities_finance/contract.md).

## Official interfaces reviewed

The [official API schema](https://markets.newyorkfed.org/static/docs/markets-api.yml),
rendered by the [interactive documentation](https://markets.newyorkfed.org/static/docs/markets-api.html),
documents these routes on `https://markets.newyorkfed.org`:

| Route | Documentary meaning | Planned use |
| --- | --- | --- |
| `/api/pd/list/seriesbreaks.json` | Definition eras and date bounds. | Version metadata; do not hardcode the current-era ID. |
| `/api/pd/list/timeseries.json` | Series/key ID descriptions and break membership. | Explicit allowlist/catalog admission. |
| `/api/pd/list/asof.json` | Source dates with associated break. | Validate planned date partitions. |
| `/api/pd/get/asof/{date}.json` | Observations for one source date. | Initial bounded acquisition unit. |
| `/api/pd/latest/{seriesbreak}.json` | Latest observations in an era. | Discovery/health comparison after characterization. |
| `/api/pd/get/{timeseries}.json` | Selected series across eras. | Optional bounded-response history strategy. |
| `/api/pd/get/{seriesbreak}/timeseries/{timeseries}.json` | Selected series in a specified era. | History only under explicit size/coverage bounds. |

The schema also offers a broad all-surveys CSV. Its existence does not make
an unbounded default request appropriate. No arbitrary start/end query string,
pagination parameter, API key or undocumented unit field is assumed here.
At implementation, capture the official schema checksum and prove actual
response behavior before choosing a transport mode.

## Upstream field evidence

Documented metadata has `label`, `seriesbreak`, `startdate`, `enddate` for
eras and `seriesbreak`, `keyid`, `description` for series. JSON observations
contain `asofdate`, `keyid`, `value` under the primary-dealer response envelope.
`value` is documented as text. The exact empty/suppressed marker, numeric scale,
unit mapping and any additional payload attributes require current samples.

The [statistics page](https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics)
defines source caveats. Preserve the documented distinctions among positions,
transactions, financing, cumulative weekly fails and MBS settlement-cycle
measures. The [reporting forms](https://www.newyorkfed.org/markets/primarydealers)
are the field-level authority where API descriptions do not state units/basis.

## Proposed normalized metadata

**Series era** grain: `(provider, seriesbreak, metadata_version)`.
Keep original era ID/label, inclusive/exclusive boundary evidence, source
effective dates, native description and raw receipt. Do not infer end-date
sentinel semantics until current payloads demonstrate them.

**Series definition** grain: `(seriesbreak, keyid, metadata_version)`.
Required admitted attributes are `description`, `native_unit`, `scale`,
`frequency`, `measurement_basis`, `reporting_population`, `asset_scope`,
and any provider-supported tenor/counterparty/collateral dimensions.
These are **proposed normalized names**, not asserted API columns.
If an attribute cannot be established, the affected feature stays unadmitted.

Allowed measurement-basis vocabulary is proposed as
`POINT_IN_TIME|DAILY_AVERAGE|WEEKLY_CUMULATIVE|SETTLEMENT_CYCLE_ASOF|OTHER`.
Each mapping cites official documentation and is versioned. It must not be
guessed from a familiar-looking series abbreviation.

## Observation grain and types

Logical grain: `(provider, seriesbreak, keyid, as_of_date)`.
Observed version identity additionally includes normalized content and the
admitted metadata version. A source response without its era can be normalized
only after the captured date/break metadata resolves one valid era.

| Proposed field | Type/behavior |
| --- | --- |
| `source` / `dataset` | Stable source/capability identity. |
| `seriesbreak` / `keyid` | Exact strings; opaque, no semantic parsing as a primary key. |
| `as_of_date` | Strict ISO source date, preserved without shifting to publication day. |
| `value` | Finite `Decimal` or null with an evidence-based state/reason. |
| `native_unit` / `scale` | From admitted metadata/form evidence; no automatic dollars/millions conversion. |
| `measurement_basis` | Versioned source mapping, carried with every value. |
| `value_state` | `OBSERVED|SUPPRESSED|NOT_REPORTED|UNAVAILABLE|INVALID`, only when supported by evidence. |
| `metadata_version` | Immutable reference to source definition/era mappings. |
| `version_id` / `content_hash` | Stable observation/version identity. |
| `raw_run_id` / `raw_pointer` | Receipt and row/envelope pointer, source-owned. |
| `retrieved_at` / `first_seen_at` | Actual UTC capture/knowledge clocks. |
| `provider_published_at` | Nullable reliable provider publication evidence only. |

Use decimal text in SQLite and declared decimal Parquet types. Preserve native
scale and display unit. Negative positions or changes may be valid; validate
sign restrictions per admitted series, not with a global nonnegative rule.

## Missingness, suppression and removal

The provider describes category suppression when reporting counts are too low;
the payload's suppression sentinel remains to be characterized. A blank, dash,
null, absent row, zero and unavailable response must not collapse into one state.
An observed zero is a number; a suppressed observation is unknown.

If a complete refreshed response omits a previously present row, first establish
whether the endpoint is an authoritative replacement snapshot. Only then may a
versioned removal/tombstone event mark it withdrawn from that selection.
Otherwise report a coverage discrepancy and withhold dependent publication.
The original observation and raw response remain inspectable.

## Revision and clock semantics

The provider warns that historical values may be revised. Overlap refreshes
must re-read dates even after a previous success. Identical normalized content
does not create another Silver version; corrections and reversions do.
Metadata changes also create versions and revalidate the affected observations.

Common C06 requires a root-locked monotonic capture sequence persisted with
each raw receipt. Preserve it when timestamps tie and during replay. Reject
inconsistent clock regression or ambiguous imported ordering under the recorded
policy rather than guessing order from UUIDs or provider row order.

`as_of_date`, nullable `provider_published_at`, HTTP retrieval and pipeline
first-seen are distinct. A Thursday schedule cannot fabricate actual historical
release timestamps. Backfilled 1998 data becomes known at today's capture.
Observed-as-of selects records and metadata first seen by the cutoff before
calculating a prior-period change. Unknown original vintages remain unknown.

## Features and comparability

Level changes require equal series era, unit, population and measurement basis,
and two observed values. Preserve `previous_as_of_date`, `gap_days`, input
version tokens and method version. With missing/suppressed inputs, return null
plus a reason; do not skip to a convenient older observation invisibly.

Cross-era stitching requires a separately reviewed mapping backed by the
provider definitions. Default views break at era boundaries even if labels or
IDs look similar. Ratio denominators must match scope/basis and be nonzero.
Do not sum overlapping financing totals/subcategories or add receive/deliver
fails into a claimed unique market balance.

## Publication, rights and operational metadata

Each response receipt includes URL/parameters, status, safe HTTP headers,
bytes/SHA-256, timeout/retry outcome and UTC clock. Metadata captures are
versioned inputs with their own receipts. Multi-response selections carry the
expected/received date and series sets plus a coverage result.
Incomplete response sets remain auditable but cannot advance a complete
watermark or committed snapshot. Source failure is distinct from suppression.
Local source inputs receive the same immutable raw/receipt archive before fact
persistence or replay is introduced; network transport reuses that writer.

Cross-store commit follows common C07: stage/fsync immutable files, commit DB
facts plus a pending snapshot, finalize and verify files, atomically replace
the current pointer, then mark the journal published. Readers pin one shared
generation. Restart reconciles pending work under the root lock, including a
pointer already published before its final journal mark. Boundary-by-boundary
failure injection must prove coherent reads and no false success watermark.

The [terms of use](https://www.newyorkfed.org/privacy/termsofuse) permit defined
uses subject to attribution and other conditions. The final output must retain
the applicable copyright/source notices, source URL and terms reference;
transforms are visibly attributed to the pipeline's methodology. Do not apply
reference-rate-specific notices automatically to primary-dealer statistics.

The proposed local artifact includes schema/method, native values, unit/scale,
era/basis, coverage, source/knowledge clocks, generated UTC, input tokens,
attribution and limitations. Raw provider responses, configuration and secrets
stay in the source-owned root. A future QuantMeridian module reads the artifact
under its own independently reviewed contract.

## Fixture priorities

Invented examples must cover two eras meeting at a boundary; an opaque ID reused
with changed meaning; missing/suppressed/observed-zero/negative values; decimal
precision; a revision then reversion; metadata-only correction; repeated dates;
unknown series; a partial response set; transient HTTP failures; invalid JSON;
oversized response; a report holiday/gap; and a proven source withdrawal.

An independent expected-output table checks levels, null-safe changes and
unit/basis guards. A fixture with cumulative weekly fails must not be silently
converted into unique daily fails. Replay proves that the source website's
current catalog is unnecessary to reconstruct an archived selection.
