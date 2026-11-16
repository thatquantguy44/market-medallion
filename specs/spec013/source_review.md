# OTC-T01 — route and use review

Reviewed **2026-11-15 America/New_York** against the official sources below.
This review implements OTC-REQ01 / OTC-AC01; it records a project admission
policy, not actual account acceptance, credential entitlement or live conformance.
No source observations were acquired. Spec012 permission is not reused.

## Route decision

**Candidate for future bounded automation: FINRA Query API, `otcMarket` /
`weeklySummary`, Public credential, noncommercial internal research.** The
current documentation lists that production dataset separately from
`weeklySummaryMock` and `weeklySummaryHistoric`. Neither alternative is admitted
by this slice. Current metadata, response schema, summary selection, stable
extraction and one real profile still require T12 conformance. The explorer's
mock URL is never production evidence.

A linked guide's automation instructions are technical instructions; this
project does not interpret them as blanket website retention/derivation rights.
Website download automation and local real-file use require separately recorded
provider permission. The older file shape is used only for invented fixtures.
There is no file-to-API fallback, account creation or paid-access selection.

## Evidence and bounded interpretation

| Official source | Reviewed evidence | Project consequence |
| --- | --- | --- |
| [Website terms](https://www.finra.org/terms-of-use), modified 2023-11-09 | Noncommercial use is subject to restrictions on databases, copying in bulk and derivatives, with specific exceptions requiring separate evidence. | Do not admit website persistence/automation/export from the existence of a Download button. |
| [API terms](https://developer.finra.org/terms-and-conditions), updated 2026-03-19, §§2–3, 10 | Equity-specific terms take precedence where applicable; use needs granted API access and appropriate credentials. Restrictions and termination remain applicable. | Require the operator's accepted-terms reference, Public metadata and the selected dataset before future provider I/O. No agreement is accepted by code. |
| [Equity-specific terms](https://developer.finra.org/specific-terms-equity-data), updated 2022-12-20, §§1–2, 6 | Covers `otcMarket`; permits noncommercial personal/professional use and derived data. Conditional redistribution requires FINRA attribution, no incremental charge, no further redistribution and compliance measures. No additional dataset retention period is specified. | Admit only explicitly declared internal operations with FINRA attribution. Public/commercial exports are outside this slice. No inferred 365-day expiry or perpetual license. |
| [Current API documentation](https://developer.finra.org/docs#query_api-equity-weekly_summary) and [fees](https://developer.finra.org/fees) | Public credentials are listed for Weekly Summary; production has a rolling twelve-month window. A separate historic dataset has different query constraints. The fee table lists Public at zero monthly cost with limited access. | Record route availability as documentary evidence only. Entitlement, current limits and complete bounded extraction must be checked before a future run. History is not enabled here. |
| [OTC landing page](https://www.finra.org/filing-reporting/otc-transparency) | Security-specific small non-ATS activity is aggregated without individual attribution; reported activity is delayed. | Keep product/tier populations and grouped labels distinct; never infer short-sale intent. |
| [Linked v6 user guide](https://www.finra.org/sites/default/files/2019-11/OTC-transparency-website-user-guide-v6.pdf), 2019-10-31, pp.1–2,38 | Describes two-/four-week tier delays and points automation to Developer Center while discussing older file support. | Delay is descriptive metadata, not an actual publication clock or current route test. |
| [File API v0.4](https://www.finra.org/sites/default/files/OTC-Transparency-Data-File-Download-API-v04.pdf), 2019-01-16, pp.3–4,10 | Documents weekly security/participant detail and different summary grains, plus a pipe-delimited ATS sample whose week comes from selection context. | Pin that literal header in the invented ATS/T1 parser; do not call it a conformed current API schema. |

## Admission contract

`pipelines/finra_otc_transparency/manifest.py::require_approved_use` is a pure
pre-I/O gate for future adapters. Its exact fields are `dataset`, `purpose`,
`route`, `authorization`, `permission_reference`, `credential_type`,
`provider_dataset`, `attribution`, and `operations`. Unknown/missing fields fail.

- Dataset: `finra-otc-transparency`; purpose:
  `personal_noncommercial_internal_research`.
- `invented_fixture`: only `fixture`; authorization `pending`; reference,
  credential, provider dataset and attribution null. This grants no provider use.
- `finra_query_api`: authorization `accepted_api_terms`, nonempty nonsecret
  operator evidence reference, credential `public`, provider dataset
  `weeklySummary`, attribution `FINRA`. Only explicitly listed operations pass.
- `approved_local_file` / `finra_download_files`: authorization
  `provider_permission` with independent evidence, attribution `FINRA`, no API
  credential/dataset metadata. A local profile cannot declare `acquire`.
- Real-data operations: `acquire`, `local_import`, `retain`, `transform`,
  `internal_export`. Missing/unlisted use is denied. Retention and transforms
  require their own declared operations. Public redistribution, paid service,
  commercial use and model-training authority are not implemented operations.

References record operator admission; strings are not independent license
verification. No active approved-use YAML or credential is committed. Future
adapters must apply the gate before network probes, data reads/writes and export,
verify current provider access/schema/limits, and use run-scoped `shared/http.py`.
Local validation, planning, parsing and replay must remain network free.
Retention while authorized preserves required lineage; no deletion/apply policy
is added. Changed access or termination requires a separate rights/retention
review rather than an automated deletion or permanent-use claim.

## Slice boundary and next evidence

T01 has admitted/denied offline profile tests. T02 catalogs six weekly detail
populations; T03 implements only the invented ATS/T1 pipe profile. Header-only
syntax yields `PARSED_EMPTY` with **unverified completeness**, not proof that
FINRA published a complete empty population. Actual route access, payload
conformance, complete extraction, historical coverage, export and schedules
remain future work. The first implementation after Checkpoint A is T04 identity.

## T06 fixture expansion review — 2026-11-16

Rechecked the [official landing page](https://www.finra.org/filing-reporting/otc-transparency),
[Equity Data terms](https://developer.finra.org/specific-terms-equity-data) and
[v0.4 historical file specification](https://www.finra.org/sites/default/files/OTC-Transparency-Data-File-Download-API-v04.pdf),
including visual review of printed p.10. The historical non-ATS sample has a
different header from ATS, no MPID column and an explicit reporting-week field.
It does not establish a current six-profile payload or stable group classification.

T06 therefore reuses only the invented ATS grammar across explicit tier fixtures
and names its non-ATS grammar `finra-otc-non-ats-weekly-invented-pipe/1`. The latter
deliberately adds optional supplied IDs and an explicit invented attribution
column to exercise T04 identity. It is not a transcription of the historical
non-ATS header or current Query API. No schema inference, new permission, provider
data request or agreement acceptance occurs. Real route/schema/grouping and
extraction conformance remain T12, separately for every selected actual profile.
