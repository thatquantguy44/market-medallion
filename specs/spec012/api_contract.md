# FINRA Reg SHO Query API profile

Selected 2026-11-10 for personal noncommercial internal research. This explicitly
amends the original TXT-first plan; it does not enable website downloads.
Traces: SSV-T01–SSV-T03 / SSV-REQ01–SSV-REQ03. Runtime implementation remains
approved-use metadata only. The parser, data acquisition, warehouse and jobs are
not implemented.

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

No observation query was made. Authentication proves that the saved client
pair can obtain a token; metadata retrieval does not prove data-query entitlement,
partition completeness, facility coverage or historical observations. Public
credential type and terms acceptance remain user-attested.

## Metadata-backed schema and unresolved facility mapping

Internal schema profile name: `finra-reg-sho-api/1`. This is a project schema
identifier, not a verified FINRA `Data-Version` value. Pin source version behavior
in SSV-T02 before accepting observations.

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
describes rolling twelve-month availability; actual first/last available dates
remain unverified. Do not infer a historical archive outside that window.

**Mapping discrepancy to resolve in SSV-T02:** production metadata's description
of `reportingFacilityCode` enumerates N/Q/B/D, while the documented JSON examples
use values such as NYTRF/NQTRF/NCTRF. Preserve this disagreement and raw values;
do not equate a market code to an API facility identifier. ADF identifiers and
consolidated/ORF availability are not established by this metadata check.

## Next bounded implementation contract

SSV-T02 pins the supported facility/date/schema profiles, selected source version,
explicit exclusions and rejection of overlapping scope. SSV-T03 then adds a pure
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
