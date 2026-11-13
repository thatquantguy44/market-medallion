# FINRA Reg SHO Query API profile

Selected 2026-11-10 for personal noncommercial internal research. This explicitly
amends the original TXT-first plan; it does not enable website downloads.
Traces: SSV-T01–SSV-T12 / SSV-REQ01–SSV-REQ11 / SSV-NFR01. Runtime implementation now includes
approved-use admission, the source schema/facility registry and a pure receipt-bound
JSON parser plus pure quantity validation/text conversion and per-observation
ratios, plus [immutable local Bronze and sanitized outcomes](bronze_contract.md).
T06–T08 add [audited local SQLite and observed membership](local_storage_contract.md),
with strict per-pipeline configuration and a tested CLI/script. T09 research and
[T10 typed local publication](parquet_contract.md) and [T11 terminal-ledger replay](replay_contract.md)
are implemented. T12 adds [bounded admitted acquisition](acquisition_contract.md)
and verified page collections. Downstream exchange and jobs are not implemented.

## Approved route and evidence

The user confirmed a **Public API credential** and acceptance of the FINRA API
terms. The nonsecret evidence reference is
`USER_ATTESTED_PUBLIC_API_TERMS_2026_11_10`. This is an operator attestation,
not independent license documentation or an inferred redistribution grant.
[API terms](https://developer.finra.org/finra-api-terms-service) and
[Equity-specific terms](https://developer.finra.org/specific-terms-equity-data)
remain the governing documentary references.

A bounded verification on November 10 New York time obtained HTTP 200 from the
FINRA identity service and HTTP 200 for the production metadata endpoint:

- Dataset: `otcMarket/regShoDaily`; returned identity `OTCMARKET/REGSHODAILY`.
- Metadata: `https://api.finra.org/metadata/group/otcMarket/name/regShoDaily`.
- Check ID: `dc9f572037dd430c80783cb9315edd48`.
- Check start timestamp (receipt `captured_at`):
  `2026-11-11T02:48:40.663706+00:00`; this is not an observation clock.
- Metadata: 1,066 bytes; SHA-256
  `0444c8491e61c5160f4a94e302efc7f4aa0fecc721db5d7e87cf09ac5b244d4f`.
- Exact metadata and nonsecret HTTP evidence remain ignored under
  `data/finra_short_sale_volume/access/<check_id>/`.

That initial access check made no observation query. Authentication proves that the saved client
pair can obtain a token; metadata retrieval does not prove data-query entitlement,
partition completeness, facility coverage or historical observations. Public
credential type and terms acceptance remain user-attested.

## Pinned schema and source version

Internal schema profile: `finra-reg-sho-api/1`. The bounded SSV-T02 check separately
verified active source `dataVersion: 1` using `/datasets` and explicit
`Data-Version: 1` requests/response headers. `Data-API-Version` is a separate
transport header and is not accepted as dataset-version evidence. Other source
versions, mock datasets and TXT layouts are rejected by the checked manifest.

| Literal JSON field | Metadata type | Implemented normalization |
| --- | --- | --- |
| `tradeReportDate` | Date, `yyyy-MM-dd` | Unshifted `trade_date` |
| `securitiesInformationProcessorSymbolIdentifier` | String | Exact `symbol_raw` |
| `shortParQuantity` | Number | Exact Decimal shares `short_volume`; includes exempt |
| `shortExemptParQuantity` | Number | Exact Decimal shares `short_exempt_volume` |
| `totalParQuantity` | Number | Exact Decimal shares `total_volume` |
| `marketCode` | String | Exact `market_raw` |
| `reportingFacilityCode` | String | Exact raw facility identifier; explicit registry mapping required |

The returned partition field is `tradeReportDate`. The
[dataset documentation](https://developer.finra.org/docs#reg-sho-daily-short-sale-volume)
describes rolling twelve-month availability. The November 10 partition inventory
contained **252 dates, 2025-11-10 through 2026-11-10**. This is an availability
snapshot, not verified observation coverage for all those dates.
`validate_acquisition_window` requires explicit ISO bounds and a caller-supplied
New York cutoff date; it rejects older-than-twelve-calendar-month, future and
reversed windows. It does not infer holidays or current partition availability,
and must not apply today's window to archived local imports or replay.

## Verified facility registry and exclusions

The production metadata description lists N/Q/B/D. The observed production rows
use the longer reporting-facility identifiers below. Six bounded November 9 rows
confirm these literal pairs; the description is not treated as an alias map.

| Scope ID | `reportingFacilityCode` | `marketCode` |
| --- | --- | --- |
| `nasdaq_carteret_trf` | `NQTRF` | `Q` |
| `nasdaq_chicago_trf` | `NCTRF` | `B` |
| `nyse_trf` | `NYTRF` | `N` |

The `listed_trfs` selection expands to exactly these three disjoint TRFs.
Individual scopes may be selected instead. Selecting an aggregate alongside a
member, repeating a member, using a raw code as a scope alias, or specifying
an unknown/excluded scope fails before any provider I/O. A row must match both
an admitted facility code and market code within the selected scopes.

The November 9 market D query returned **HTTP 204, zero bytes, Record-Total 0**.
It provides no ADF facility identifier. ADF is excluded pending its identifier
and empty-partition contract; consolidated TRF/ADF, ORF, TXT and mock profiles
also remain excluded. The checked `manifests/finra-short-sale-volume.yml` is a
schema/scope contract, not an access grant or activation flag.

Sanitized [source evidence](evidence/t02_source_contract.json) records all seven
response digests, the dataset descriptor, the partition bounds and aggregate
sample outcomes. Exact provider observations remain ignored under
`data/finra_short_sale_volume/t02/79c14c9f27104a94b77dc9a4c1494625/`.
The six rows are partial samples, not complete date/facility partitions. The
initial checker rejected the unexpected 204; its failed report is preserved.
Offline review reconciled the retained response and validated the three supported
TRF mappings without refetching or changing the original bytes.

These verification receipts label request-start time as `captured_at`; that
clock is not a Silver first-seen timestamp. No observations were promoted.
Future ingestion must stamp actual retrieval completion and keep provider
publication time unknown unless source-owned evidence establishes it.

## Implemented parser and next bounded scope

SSV-T01–T12 are implemented for the selected source profile: strict schema/version,
explicit scope exclusions, disjoint selection, acquisition-date guards and a pure
receipt-bound parser are implemented. It rejects truncated JSON, unknown/duplicate
fields, duplicate logical keys, malformed dates, invalid symbols and impossible
or nonfinite/negative quantities. Whole and fractional quantities are exact Decimal
shares; binary floating point and integer truncation are forbidden. A complete
empty response is distinct from missing, pending, denied or failed extraction.
T05 now archives admitted exact bytes before parsing, with safe request evidence,
immutable sequence/clocks and sanitized rejection outcomes. [Acceptance](t05_acceptance.md).
T06–T08 subsequently deliver the [local loop](t06_t08_acceptance.md), revisions,
complete replacement/removal and local UTC/sequence cutoffs. Partial pages never
publish rows or infer removals. T09 adds [scope-safe research Gold](research_gold_contract.md),
with exact matched sums, prior complete dates and local cutoff evidence.
T10 adds [typed publication and C07 recovery](t10_acceptance.md). T11 adds [offline replay](t11_acceptance.md). T12 adds [bounded acquisition](t12_acceptance.md). **Next: SSV-T13 health/retention.**
T04's pure ratios and quantity codec do not constitute published Gold or persistence.

Future local input is an approved archived JSON response and its request/scope
receipt, not a TXT file silently relabeled as API data. The TXT schema, trailer
and older regimes stay separate and unadmitted until their own use decision.
Existing date/first-seen/revision and denominator controls continue unchanged.

SSV-T12 will use FINRA's
[OAuth client-credentials flow](https://developer.finra.org/docs#authorization),
then Bearer authorization for the selected production endpoint. The user's
`FINRA_API_KEY` currently contains the API Client ID; `FINRA_PASSWORD_SECRET`
contains its API Client Secret. Runtime config must name these variables and
expose pipeline-owned `network` settings. Never store the values, Basic header,
token or token response in Bronze, receipts, logs, Git or run YAML.

The verification reused one `shared.http` client after a credential-free HEAD
probe of the official Developer Center. It used direct routing with recorded
startup fallback and verified TLS. Future reusable API transport must characterize
its chosen probe, token and data hosts, redirects, limits and retry behavior with
offline tests before acquisition. No process-global opener is allowed.

API completeness has no TXT trailer: reconcile requested date/facility scope,
all pages, unique keys and consistent provider record counts/offsets/limits.
Partial, unstable or mismatched results cannot replace a complete partition or
remove earlier rows. Unknown publication timestamps remain null; response Date
and retrieval time are not provider publication vintages. API release timing,
empty-facility meaning and correction visibility still need source conformance.

## T03 verification checkpoint

**111 SSV-AC03 parser tests plus 84 admission/registry tests pass.** Full offline
regression: **677 passed, 10 optional integration tests skipped**. Ruff lint/format
and source distribution/wheel builds pass. The [offline T03 reconciliation](evidence/t03_parser_conformance.json)
proves all quantities in the six retained rows are exact, all three pages remain
partial, raw digests are unchanged and excluded market D still rejects. No new
provider-data request occurred. G0 and parser Checkpoint A are ready for this
selected profile; G1/G2 remain open. No HTTP adapter, warehouse, export or job exists.

## SSV-T03 fractional-quantity amendment

The first offline parse rejected the retained Carteret page because the original
integer-only assumption was wrong: **all six retained rows have a fractional
short or total quantity**, with ten fractional values across the three pages.
The API metadata says Number, not integer. FINRA's [January 14, 2026 notice](https://www.finra.org/rules-guidance/notices/trade-reporting-notice-20260114)
sets February 23, 2026 as the effective date for NMS fractional-share trade
reporting, up to six decimal places. The notice explains the reporting change;
the retained responses establish this API's actual fractional quantities.
The linked 2011/2021 TXT layout still says no decimals and cannot override those
API observations. No API historical cutover or maximum decimal scale is inferred.

This explicitly amends the earlier integer-only API normalization and the T03/T04
acceptance wording. Source Data-Version 1 and the seven Number-typed fields did
not change. Return Decimal for all three quantities, including whole shares;
preserve every supplied digit and scale without float conversion, rounding or
truncation. Future SQLite/Parquet/export contracts must support fractional shares.

## SSV-T03 implemented parser boundary

`data.api.parse_page(payload, manifest, receipt)` accepts immutable raw bytes
and an in-memory receipt with `url`, `method`, `request_payload`,
`request_data_version`, `http_status`, `headers`, `bytes` and `sha256`.
Additional envelope/diagnostic fields are ignored and are never used as clocks
or coverage flags. The caller owns credential redaction before archival.

The supported request is the exact production POST endpoint, a positive explicit
limit and nonnegative offset, and exactly two EQUAL filters: `tradeReportDate`
and one selected TRF's `marketCode`. Optional projections must name all seven
fields once; asynchronous requests and other filter shapes are unsupported.
Request/response version, offset and effective limit must agree. The response
must provide Data-Version, Record-Total, Record-Offset and Record-Limit;
Record-Max-Limit and Total-Records-On-Page are checked when present. A receipt's
byte count and SHA-256 must match the original payload.

A successful JSON response is strictly UTF-8, a top-level array and exactly the
seven reviewed fields in each object. Duplicate JSON keys, duplicate logical
rows, wrong dates/facilities, nonfinite or negative quantities, and
impossible exempt/short/total relationships reject the page. Whole, fractional
and exponent JSON numeric forms are decoded exactly with Decimal, without floats.
Symbols remain nonempty printable strings with no surrounding whitespace;
case, punctuation, leading zeros and printable internal spaces are preserved.
No undocumented API symbol-width rule is inferred from the TXT layout.

Default operating budgets are 20 MiB per page and 5,000 rows, configurable by the
caller as positive integer limits. Decimal quantities have at most 4,096
coefficient digits, adjusted exponent below 4,096, and stored exponent between
-4,096 and 4,096. Numeric tokens have a 4,160-character budget; integer response
headers have a 4,096-digit budget. These are operating limits, not provider widths
or an API scale cap of six. No global integer/decimal settings are changed.

HTTP 200 requires JSON; an empty body is invalid. HTTP 204 requires zero bytes and
Record-Total 0 at offset 0. A known selected TRF may return a verified empty page;
this never admits ADF or manufactures a zero-valued observation. Nonempty pages
cannot exceed the limit or remaining total. Short positive pages stay partial;
an empty page before the declared total is inconsistent and fails. A page is
`partition_complete` only when offset 0 and its parsed row count equals the total.
Cross-page stability and complete replacement promotion remain later tasks.

The parser returns immutable typed rows and page metadata, including original
payload digest, declared date/facility scope, offset, effective limit and total.
It performs no network, file, environment or clock access and creates no Bronze,
Silver, Gold or publication artifact. Archived local parsing does not apply a
current rolling acquisition window. T04 adds pure ratio arithmetic and exact
quantity text readback; DB/Parquet round trips and publication remain later
slices. No new provider-data request is part of T03/T04.

## T04 verification checkpoint

71 SSV-AC04 arithmetic/codec checks pass, bringing the focused FINRA total to
266. Full offline regression: **748 passed, 10 optional integrations skipped**;
Ruff lint/format and source distribution/wheel builds pass. The parser now reuses
`data.quantities.validate_quantities` without changing its acceptance budgets.
`data.gold.share_ratios` uses method `finra-short-sale-volume-ratios/1`, unit 1,
34 significant digits and half-even rounding only for derived ratios. The
[numeric contract](data_contract.md#ssv-t04-arithmetic-and-quantity-text-boundary)
defines source quantity exactness, text representation and caller-context isolation.
[Offline evidence](evidence/t04_semantics_conformance.json) reconciles all 18
quantity tuples and 12 ratios in the retained six-row sample; bytes are unchanged,
pages remain partial and no observation was promoted or clock assigned. G1/G2
remain open. T05 subsequently added [Bronze receipts](bronze_contract.md); next
is T13 health/retention after accepted T12 bounded acquisition. Scheduling and
interchange remain later tasks.

## SSV-T12 request/collection amendment

[Acquisition contract](acquisition_contract.md) adds the sole accepted `sortFields`
shape: `["securitiesInformationProcessorSymbolIdentifier"]`, alongside the existing
exact trade-date and market filters. Offsets advance by actual returned rows.
Request/page/row/byte bounds remain finite; synchronous API responses cap at 5000
rows/3MB. A complete single response can promote directly. Multi-page promotion
requires contiguous full coverage with stable counts, unique symbols and an exact
second normalized pass. This is observed agreement, not a provider snapshot token.

Original raw pages/receipts remain separate. `finra-ssv-acquisition-page/1` marks
verification/subsequent pages as evidence-only even if a changed response becomes
short/empty. `finra-ssv-page-collection/1` is a derived manifest of both page-ID
lists; its payload hash is the manifest's hash, not a fabricated HTTP body.
Its first original API receipt remains unmodified. Collection parsing verifies
all page identities/scopes/counts/hashes before the unchanged partition transaction.
T11 replay preserves these explicit formats. Provider publication stays null;
collection first-seen is the new capture clock after all inputs are observed.

[Real conformance](evidence/t12_acquisition_conformance.json) reconciles one Chicago
TRF date, 2026-11-09, with 5,849 rows over two two-page passes, replay and typed
publication. No all-facility/history or release-calendar claim is made.
