# FINRA OTC transparency data contract

**Version:** v1 fixture checkpoint. **Reviewed:** 2026-11-15.
**Runtime status:** T01–T03 implemented offline; acquisition/storage/jobs absent.
Read [Spec013](README.md) and the
[program contract](../../docs/securities_finance/contract.md). Normalized names
and broader schema below remain design decisions; current endpoint conformance is pending.

## Implemented T01–T03 contract

[Current route/use review](source_review.md) reconciles website, API and equity
terms independently of Spec012. The pure approved-use gate denies absent,
wrong-dataset, mock, unaccepted, unlisted and commercial/public operations.
Public `weeklySummary` is the future candidate, not an enabled source. Local
real files require independent provider permission. No active use grant ships.

The versioned [manifest](../../manifests/finra-otc-transparency.yml) enumerates six
disjoint populations: `ATS_WEEKLY_DETAIL` / `NON_ATS_WEEKLY_DETAIL` × `T1`, `T2`,
`OTCE`. Scope IDs include product and tier. Documentary expected delays are two
weeks for T1 and four for the other tiers; these are not release timestamps.
Each profile retains its review date and source-evidence reference.
Only ATS/T1 has an implemented invented parser. The other five entries are
candidates with no parser. All six have `conformance: NOT_CHECKED`.

`select_partitions` accepts a nonempty explicit list of dictionaries containing
exactly `reporting_week_start`, `product`, `tier`. Weeks are strict ISO Monday
labels; no holiday/Tuesdays are rewritten. Up to 312 distinct partitions may be
selected under a smaller caller budget. This is a local planning ceiling, not a
provider limit or admitted historical range. Duplicates, tier aliases, summaries,
monthly and block products are rejected.

### ATS/T1 invented pipe profile

`finra-otc-ats-weekly-pipe-fixture/1` uses the literal v0.4 ATS file sample header,
not the differently shaped Query API response. Fields, in order:

```text
tierIdentifier|issueSymbolIdentifier|issueName|marketParticipantName|MPID|totalWeeklyShareQuantity|lastUpdateDate|totalWeeklyTradeCount
```

- Strict UTF-8 without BOM, `|` delimiter, CSV double-quote quoting/escaping;
  LF/CRLF syntax produces the same rows. Blank records and wrong column counts
  are errors, including unexpected trailing columns.
- Product and reporting Monday come from explicit selection context. Every row's
  tier must be `T1`. Context alone does not verify a real file's provenance.
- Symbol/name/participant/MPID values are preserved; empty/control-character text
  is rejected. Symbol/MPID tokens cannot contain whitespace. This does not infer
  firm identity, CRD or a cross-period identity key; T04 owns that work.
- Counts are exact nonnegative integers plus original token text, never float.
  Missing/decimal/signed/exponent/grouped values are rejected. At most 38 ASCII
  digits is a local fixture-profile bound, not a verified provider maximum.
- `lastUpdateDate` is a strict ISO date. Initial publication date and CRD remain
  null because this file supplies neither. No midnight/publication/first-seen
  timestamp is invented.
- Duplicate `(symbol_raw, MPID)` within the selected partition rejects the whole
  parse, including conflicting names/quantities. New bytes require revalidation.
- Defaults bound payload to 2,000,000 bytes and 10,000 rows; overrides must be
  positive finite integers. No process-global CSV or HTTP settings are modified.
- Output retains schema, partition, SHA-256 and immutable typed rows. Header-only
  syntax produces `PARSED_EMPTY`; every result has `completeness: UNVERIFIED`.
  Neither parsing nor a 200/empty response can establish complete membership.

[Checkpoint A](checkpoint_a.md) records fixture provenance, tests and exclusions.
T04+ add identity, Bronze, persistence, clocks and extraction completeness before
this can become a real weekly panel. The remaining sections describe that full
contract; they do not claim those later components are implemented.

## Source inventory and evidence tiers

| Source | Verified documentation | Conformance still required |
| --- | --- | --- |
| [OTC landing](https://www.finra.org/filing-reporting/otc-transparency) | Delayed ATS/non-ATS activity, reporting-firm scope and un-attributed small activity. | Current downloadable archive population and permitted transfer mechanism. |
| [Current linked v6 guide](https://www.finra.org/sites/default/files/2019-11/OTC-transparency-website-user-guide-v6.pdf) | Weekly tier families, detail/download structure, updates and automation guidance. | Literal current headers, routes, missing sentinels and stable participant IDs. |
| [Historical API file specification](https://www.finra.org/sites/default/files/OTC-Transparency-Data-File-Download-API-v01.pdf) | Historical field families and older machine-download design. | Whether those endpoints still work and terms admit this project's exact use. |
| [Developer documentation](https://developer.finra.org/docs) | Metadata/query partitioning and paging concepts. | Current real weekly dataset schema, allowed credential and resource limits. |

The older guides support discovery, not an immutable current API guarantee.
Store source-review date and evidence pointer per manifest profile. Where a
current response disagrees, reject that profile and update the contract/fixtures
before admitting the changed data. Never select an endpoint because it returns
a familiar-looking JSON body; mock data must be explicitly excluded.

## Schema and types

The v6 ATS weekly download example identifies tier, issue symbol/name, participant
name, MPID, weekly share/trade quantities and last update date. Non-ATS and API
views differ; some headers omit identifiers. The discovery/selection context
supplies reporting week and product when the file does not contain them.

| Proposed field | Type | Rule |
| --- | --- | --- |
| `reporting_week_start` | ISO date | Preserve provider label and its documented period basis. |
| `product` | enum | `ATS_WEEKLY_DETAIL` or `NON_ATS_WEEKLY_DETAIL`; totals/monthly/blocks use other datasets. |
| `tier` | enum | Verified Tier1/Tier2/OTCE map, retained alongside raw provider code. |
| `symbol_raw`, `issue_name_raw` | string | Preserve spelling/suffix/case; name is descriptive. |
| `participant_name_raw` | string | Original attribution label, including grouped/un-attributed labels. |
| `mpid` | string or null | Supplied identifier only, never inferred from name. |
| `firm_crd` | string or null | Preserve source identifier if a verified route supplies it. |
| `participant_key` | string | Route/profile-scoped stable token with explicit evidence basis. |
| `identity_basis` | enum | `PROVIDER_ID`, `SOURCE_ROW_LABEL`, `AGGREGATED`, `UNKNOWN`. |
| `weekly_shares`, `weekly_trades` | integer | Nonnegative lossless counts; missing is not zero. |
| `provider_update_date` | date or null | Date-only provider evidence, not a timestamp. |
| `provider_initial_publication_date` | date or null | Present only on a verified route providing it. |
| `summary_code_raw` | string or null | Reject incompatible summary/product codes. |
| `scope_id`, `schema_profile` | string | Identify the exact product/tier/route semantics. |

Parser mapping is pinned to each acquired representation. A route that supplies
only names cannot claim stable firm identity across renamed reports. If rows
cannot be uniquely keyed with available evidence, quarantine the ambiguous
partition instead of inventing an MPID or merging on display name.

## Logical grain and scope

Proposed observation grain is `(provider, dataset/product, tier,
reporting_week_start, symbol_raw, participant_key)` within one schema profile.
An API summary by symbol has a different grain and must never be added to its
participant-detail rows. The manifest identifies disjoint detail partitions and
any separate reconciliation-only totals. ORF-style OTC securities are a tier
population, not an interchangeable substitute for NMS listed securities.

A file membership ledger records complete selected populations and changed
membership. Normalized row hashes exclude transport-only metadata, but include
all content defining volume, attribution, identity basis, product and tier.
Renaming or regrouping source participants creates observed changes with lineage.

## Clocks, delays and refresh

Retain reporting-week start/end basis, nullable publication date/time, nullable
provider update date, retrieval time, first-seen time, run/capture IDs, schema
and manifest hashes. Never synthesize a publication timestamp from a date-only
value, the reporting Monday or the expected delay. Preserve date precision.

The guide describes two-week delayed Tier1 and four-week delayed Tier2/OTCE
weekly releases; verify the current calendar and holiday handling at admission.
Lag measures data availability, not pipeline failure. A recent retrieval can
legitimately contain weeks-old observations. Required partitions have statuses
such as pending release, complete, complete-empty, missing, rejected or failed.

Repeated captures must revisit a configured overlapping range to detect
corrections. Do not freeze a week forever after the first success. A separate
bounded archive reconciliation operation checks older revisions within approved
resources/rights. Backfilled rows first become system-known at actual capture.

## Replacement and absence semantics

Each complete week/tier/product snapshot has an expected/observed file or page
ledger. Missing and truncated responses cannot update membership. If a complete
replacement removes a participant/security row, append a removal observation;
as-of reads before it still show the prior row. An empty complete response needs
provider-supported evidence, not only `[]` or a 200 HTTP status.

Do not manufacture a zero volume when a participant disappears. It may reflect
regrouping, a source correction or reporting thresholds. Preserve the provider's
aggregation flag. When a combined population lacks a tier or product, return
partial/unavailable derived fields with explicit coverage, not a smaller
denominator that silently changes the measure.

## Validation and safe Gold

Validate integer counts, required context, strict dates, expected summary/profile,
uniqueness, finite partition bounds and supplied source IDs. Reconcile provider
detail to separately documented totals only when definitions and populations
match. Tolerances for published rounded percentages must be separately recorded;
integer volume sums use exact arithmetic. Never repair mismatches by scaling.

Proposed safe features: participant reported shares/trades, average trade size
`shares / trades` when trades > 0, within-product share of a fully observed
security/tier/week population, prior available matching period and date gap.
Show attribution coverage and grouped-bucket share next to such measures.

An ATS/non-ATS ratio requires both complete same-period/same-tier populations
and an explicitly reviewed denominator definition. It is not share of all-market
volume. HHI/effective venue counts and historical concentration scores remain
deferred until treatment of grouped/unidentified participants is approved.
Do not combine weekly rows with monthly totals or overlapping block-size buckets.

## Storage and resource contract

Archive exact downloads/discovery pages and metadata under one source-owned root.
Use SQLite transactions for versions/coverage and typed Parquet for committed
snapshots. Preserve large counts without float conversion. Stream files and
bound total bytes, rows, partitions and pages. For an admitted API route, pin
sort/filter/limit behavior and prove no page overlap or omission before promotion.
Changing data during extraction requires a consistency check or explicit retry.

Each publication records its complete partition set and atomic current pointer.
No mutation may replace another pipeline's warehouse tables or register a fake
job before runtime implementation exists. Retention is provider/use specific;
dry runs enumerate references and blocked deletions. Preserve replayability
for every retained published snapshot.

## Proposed research artifact

`finra-otc-transparency-research/1` contains source, admitted scope and use label,
method/schema version, reporting week, tier/product, provider delay metadata,
capture/first-seen clocks, observation versions and exact population coverage.
Participant data includes identity basis and grouped/un-attributed status.
Unknown fields remain null; missing rows are distinguished from no trading.
Credentials, raw payloads, operational paths and internal permission documents
are excluded. QuantMeridian admission and display require its separate consumer
specification; no sibling repository changes are part of this pipeline delivery.
