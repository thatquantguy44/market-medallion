# FINRA short-volume approved-use review

Reviewed 2026-11-10, America/New_York. Traces: **SSV-T01 / SSV-REQ01 / SSV-AC01**.
This is an evidence and admission-policy checkpoint. The user has provisioned
Public API credentials; bounded authentication and production metadata retrieval
succeeded. The later SSV-T02 bounded sample verified six TRF records, source
version 1 and partition inventory. No warehouse, full extraction or job exists.

## Selected use and current decision

The user confirmed **personal noncommercial internal research** in this session.
No commercial, public-distribution or model-training scope is admitted by that
statement. It describes the intended use, not an attestation of provider rights.

**Current operational decision: Public Query API selected for the user's
personal noncommercial internal research.** The user confirmed a Public credential
and accepted API terms. Record this as
`USER_ATTESTED_PUBLIC_API_TERMS_2026_11_10`, not independently verified license
documentation. The user created and accepted their own access; the agent did not
create an account or accept an agreement for them. The [API profile](api_contract.md)
records the explicit schema/partition amendment to the original TXT-first design.
Website TXT automation/database permission remains unresolved and unadmitted.

Authentication and production schema-metadata retrieval both returned HTTP 200
in check `dc9f572037dd430c80783cb9315edd48`. No observations were requested.
The `.env` FINRA assignments were normalized without displaying values; the file
remains ignored with owner-only permissions. Client values and token responses
were not retained in verification output or tracked evidence.

The source-owned admission gate records the intended purpose, route,
authorization kind, a nonsecret evidence reference, permitted operations and,
for the API, the Public credential type. A reference records operator evidence;
it is not independent verification or a substitute for authenticating credentials.
Missing, pending, mismatched or disallowed profiles must fail before provider
I/O. No connector or command consumes this gate until its own implementation.

## Official evidence

| Source checked | Relevant evidence | Decision or remaining gap |
| --- | --- | --- |
| [Dataset overview](https://www.finra.org/finra-data/browse-catalog/short-sale-volume) | Noncommercial use is described as free. | Cost alone does not establish automated loading rights. |
| [Website terms](https://www.finra.org/terms-of-use), Restrictions (d), (e), (m) | Database creation needs an express exception; bulk extraction and predictive/AI use are restricted. | Website automation and durable TXT-derived loading are not admitted without applicable permission. |
| [API terms](https://developer.finra.org/finra-api-terms-service), updated March 19, 2026, sections2 and3 | Specific terms control; access requires FINRA-issued credentials and an applicable license. API bulk access is distinguished from other collection methods. | Read with the Equity terms; a website download is not covered merely because an API exists. |
| [Equity API specific terms](https://developer.finra.org/specific-terms-equity-data), updated December 20, 2022, sections1,2 and6 | The otcMarket group accepts Public/Firm/Organization credentials, permits noncommercial internal use and derived data, and adds no dataset-specific retention condition. | Candidate fit for the selected purpose; all general terms still apply. Public redistribution is outside this selected project scope. |
| [Getting started](https://developer.finra.org/node/1146) | Individual users can provision Public and Mock credentials; individual use is noncommercial. | The user must obtain and accept their own access. Mock credentials are not production entitlement. |
| [Fees](https://developer.finra.org/fees) and [Public credentials](https://developer.finra.org/fees/public) | Public credentials have no connection fee; Firm/Organization credentials are paid alternatives. | Paid routes are not selected. |
| [API FAQ](https://developer.finra.org/support?field_faq_category=471) | Public credentials have a 10GB monthly cap and become inactive on exhaustion. | Future acquisition must budget usage; no upgrade or paid overage is authorized. |
| [Reg SHO API documentation](https://developer.finra.org/docs#reg-sho-daily-short-sale-volume) | Production dataset otcMarket/regShoDaily is distinct from regShoDailyMock and has rolling twelve-month availability. JSON fields differ from TXT layout. | API selection needs its own schema, completeness/pagination and history review. It cannot silently replace the file contract. |
| [API explorer](https://developer.finra.org/docs/api-explorer/query_api-equity-reg_sho_daily_short_sale_volume) | The displayed example URL targets regShoDailyMock. | Explorer examples are development shapes, not actual observations or live conformance. |
| [Daily file landing page](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files) | Facility files are separate from consolidated listed-security TRF/ADF scope. Updates may arrive later; an ADF file may be empty. | Preserve the existing separate ORF and consolidated/constituent contracts. Exact payload/header/archive conformance remains unverified. |
| [File layout PDF](https://www.finra.org/sites/default/files/2021-07/DailyShortSaleVolumeFileLayout.pdf) | Modern files have six pipe-separated columns, a record-count trailer, and exempt shares included within short shares. An earlier layout omits exempt volume. | Documentation supports invented fixtures; it does not pin actual bytes, encoding or consolidated market-field formatting. |

These are documentary findings and a user-attested project admission decision,
not a claim that FINRA has approved this implementation. Reviewed pages can
change; recheck current terms before activation. The initial review made no API
requests; the T01 check requested only a token and production metadata. The
separate T02 sample and its limits are recorded in the [API contract](api_contract.md)
and [aggregate evidence](evidence/t02_source_contract.json). Raw observations stay
ignored; no full extraction or redistribution scope was added.

## Concrete admission contract

`approved_use` is a mapping validated by
`pipelines/finra_short_sale_volume/manifest.py`. Its fields are:

- `purpose`: exactly `personal_noncommercial_internal_research`.
- `route`: `invented_fixture`, `approved_local_file`, `finra_daily_files`, or
  `finra_query_api`.
- `authorization`: `pending`, `provider_permission`, or `accepted_api_terms`.
- `permission_reference`: a nonempty nonsecret reference when provider use is
  granted; evidence is kept outside the credential value itself.
- `operations`: an explicit list selected from `fixture`, `local_import`,
  `acquire`, `transform`, and `internal_export`.
- `credential_type`: `public` for the API; otherwise null.

An invented-fixture profile admits only `fixture` and needs no provider grant.
Local files and daily-file downloads require `provider_permission` plus a
reference and an explicit operation grant. A local-file profile never admits
network acquisition. Query API profiles require `accepted_api_terms`, a
reference, `credential_type: public` and an operation grant. Acknowledgements
must match the route; a noncommercial label or credential type alone is
insufficient. Other audiences, routes and unknown fields are rejected.

An internally exported artifact stays within the same authorized user's
research scope. Its future writer must preserve FINRA attribution and source
scope. No redistribution grant is inferred here. The pure T03 parser performs
no I/O; future acquisition, storage, transform and export entry points must invoke
this gate before their own I/O. None exist yet, so there is no end-to-end
acquisition-refusal claim.

## Next bounded action

**SSV-T01 is complete for the selected API route:** purpose, user-attested terms
reference, pure admission metadata and negative fixtures are recorded. Its
[API contract](api_contract.md) explicitly amends SSV-T02/T03 without enabling TXT
acquisition. SSV-T02 is now complete: source version 1, seven fields, exact three-TRF
facility/market pairs and rolling acquisition-date bounds are pinned and tested.
G0 is ready for that explicit profile; partial samples do not establish G2.

SSV-T03 is complete: 111 invented-fixture checks and [offline sample reconciliation](evidence/t03_parser_conformance.json)
validate the pure receipt-bound parser. Actual fractional quantities corrected
the earlier integer-only assumption; the [API amendment](api_contract.md#ssv-t03-fractional-quantity-amendment)
requires exact Decimal shares. No new provider-data request was made.
Next is **SSV-T04**, decimal arithmetic and lossless serialization.
ADF, consolidated, ORF, TXT and other source versions remain excluded. There
is no reusable authentication or data acquisition adapter yet. NAAIM stays
paused and all schedules remain stopped.

## Historical T01 verification checkpoint

The focused SSV-AC01 suite has **21 passing offline tests**. It first failed
collection because the FINRA module did not exist, then passed with the pure
gate. Fixtures use invented permission references, never actual provider grants.
Tests cover missing profiles without I/O, pending rights, operation-specific
permissions, local-only acquisition refusal, API-specific terms/credential
metadata, scope expansion and unknown fields. This verifies the helper only;
future entry-point integration and real observation conformance remain unverified.
The later bounded authentication/metadata check is separate live evidence and
is not included in those unit-test counts.

Full offline regression: **503 passed, 10 skipped** (optional integration
coverage was not enabled). Repository Ruff lint and formatting checks pass;
source distribution and wheel builds pass. These results do not establish
FINRA live access, actual provider-schema conformance, PostgreSQL/Delta parity
for this new pipeline, or integrated transport/export admission enforcement.

SSV-T02 checkpoint: 63 registry tests plus the original 21 admission tests pass;
full offline regression now records **566 passed, 10 skipped**. The source sample
confirms the selected three facility pairs while preserving the initial checker
failure on market D's HTTP 204. No complete partition or warehouse was published.

## SSV-T03 verification checkpoint

195 focused admission/registry/parser tests pass, including 111 parser tests.
Full offline regression: **677 passed, 10 optional integrations skipped**; Ruff
lint/format and source distribution/wheel builds pass. All six retained source
rows reconcile exactly, including ten fractional quantities; hashes are unchanged
and every sampled page is partial. Market D remains excluded despite HTTP 204.
The earlier integer-only offline rejection and its correction are recorded in
[T03 aggregate evidence](evidence/t03_parser_conformance.json). No observations
were promoted, no clocks assigned and no provider-data calls or schedule changes
occurred during T03. G1/G2 and complete-partition/history conformance remain open.
