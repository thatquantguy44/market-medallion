# FINRA Reg SHO Query API profile

Selected 2026-11-10 for personal noncommercial internal research. This explicitly
amends the original TXT-first plan; it does not enable website downloads.
Traces: SSV-T01–SSV-T03 / SSV-REQ01–SSV-REQ03. Runtime implementation now includes
approved-use admission and the source schema/facility registry. The parser,
data acquisition, warehouse and jobs are not implemented.

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

| Literal JSON field | Metadata type | Planned normalization |
| --- | --- | --- |
| `tradeReportDate` | Date, `yyyy-MM-dd` | Unshifted `trade_date` |
| `securitiesInformationProcessorSymbolIdentifier` | String | Exact `symbol_raw` |
| `shortParQuantity` | Number | Exact integer `short_volume`; includes exempt |
| `shortExemptParQuantity` | Number | Exact integer `short_exempt_volume` |
| `totalParQuantity` | Number | Exact integer `total_volume` |
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

## Next bounded implementation contract

SSV-T02 is complete for the selected source profile: strict schema/version,
explicit scope exclusions, disjoint selection and acquisition-date guards are
implemented. **Next: SSV-T03**, a pure
JSON-response parser with invented fixtures. It must reject truncated JSON,
unknown/duplicate fields, duplicate logical keys, malformed dates, invalid symbols
and impossible or nonintegral counts. Decode numeric values losslessly; metadata's
Number label does not justify binary floating-point share counts. A complete
empty response is distinct from missing, pending, denied or failed extraction.

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

## Verification checkpoint

**63 SSV-AC02 registry tests plus 21 admission tests pass.** Full offline
regression: **566 passed, 10 optional integration tests skipped**. Ruff lint and
format checks pass. The pure validation helpers also reconcile all six retained
source rows, the metadata and version headers. No reusable HTTP adapter, parser,
warehouse, export or job is implied. G0 is ready only for the explicitly selected
three-TRF profile; full extraction/recovery and G2 source coverage remain open.
Source distribution and wheel builds pass.
