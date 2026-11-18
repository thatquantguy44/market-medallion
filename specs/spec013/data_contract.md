# FINRA OTC transparency data contract

**Version:** v3 local fixture revisions. **Reviewed:** 2026-11-17.
**Runtime status:** T01–T07 offline, including Bronze, immutable SQLite versions,
complete replacement membership, explicit migration and bounded history/cutoffs.
T08 clock refinement, real acquisition, derived Gold/Parquet and jobs remain pending.
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
Catalog v2 implements invented fixture parsers for all six entries. All six retain
`conformance: NOT_CHECKED`; no actual provider schema is admitted. Historical
catalog v1 descriptors remain byte-verifiable; its ATS/T1 parser stays admitted
and its five candidate schemas remain unsupported.

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
  tier must match the explicit selection. Context alone does not verify a real file's provenance.
- Symbol/name/participant/MPID values are preserved; empty/control-character text
  is rejected. Symbol/MPID tokens cannot contain whitespace. This does not infer
  a legal firm, CRD or a cross-source entity match. T04 defines source-evidence
  keys separately below.
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
T04 identity, T05 Bronze, T06 initial fixture panel and T07 revision/membership
history are implemented below. T08+ refine availability clocks and add features
and real extraction before this becomes a real weekly research pipeline.

## OTC-T04 identity contract

Implemented offline; [acceptance and limits](t04_identity_acceptance.md).

Participant keys identify source evidence, not a reconciled legal firm. The
identity version includes route, schema and product/tier scope in a deterministic
SHA-256 key. A supplied MPID is the primary source identifier; otherwise a
supplied CRD is used. ID kind and raw identifier are part of the key, preserving
leading zeros. A changed display name under the same supplied identifier keeps
that source key while retaining the new label and any supplied CRD separately.
This does not prove a permanent cross-period firm/ownership match.

Without an ID, a nonempty reported label uses `SOURCE_ROW_LABEL`, scoped to its
reporting week as well as route/schema/population. Changed labels create distinct
keys; repeated labels across weeks do not silently establish firm continuity.
`AGGREGATED` requires an explicit source classification and a nonempty bucket
label, not a guess from an English name; bucket keys are week scoped and never
stand for individual firms. Contradictory firm IDs on grouped/unknown attribution
are rejected. `UNKNOWN` has no participant key; attempts to admit unknown or
ambiguous duplicate `(symbol, participant_key)` rows fail closed. Mixed partition,
route or schema identity rows also cannot pass one population check.

All supplied IDs/names and the evidence basis remain inspectable. No name lookup,
fuzzy match, crosswalk, grouping inference, ID coercion or external call occurs.
The ATS fixtures apply this contract to supplied MPIDs. The T06 non-ATS
invented grammar additionally exercises CRD, name-only and explicit group evidence.
No real coverage or longitudinal firm reconciliation is established.

## OTC-T05 Bronze contract

Implemented offline; [T05 acceptance](t05_bronze_acceptance.md).
The helpers are bounded to archive/read/fixture validation.
Each immutable capture directory owns exact received part bytes, optional exact
discovery bytes and a canonical, hashed receipt. One explicit partition and
finite expected-part ledger accompany each capture. Missing, unexpected and
truncated parts remain visible; `RECEIVED_ALL` describes the submitted ledger,
not independently established source completeness. Every capture and parse
outcome retains `membership: UNVERIFIED`. Header-only parsing never proves a
complete empty provider population.

The receipt retains canonical manifest and schema snapshots/hashes, run/capture
UUIDs, an aware UTC archive clock, a durable root-local capture sequence, route
and admission hash, safe public response metadata, byte lengths and SHA-256s.
Publication time remains null. Source HTTP dates are retained as header text;
they never become capture/first-seen or publication timestamps. Identical bytes
in separate captures remain separate events. Retrying one ID is idempotent only
when its content/lineage is unchanged; conflicts cannot overwrite it.

A shared `.writer.lock` serializes this pipeline root. Sequence reservations are
immutable and allocated before capture publication; failed writes may leave
unused reservations. Equal capture clocks remain ordered by sequence; clock
regressions are rejected. Only invented fixtures can override the archive clock;
real retention always takes the worker's actual UTC time. A future replay import
must verify original receipts instead of accepting a bare historical timestamp.
Captures publish by fsync plus atomic directory rename.
Incomplete staging directories are not readable captures. Readback verifies
bytes, receipt hash, ledger, reservation and storage identity without creating a
root or touching the network. Root/archive-owned symlinks, nonregular files and
traversal components are rejected; operator-owned parent paths are not a sandbox.

Admission precedes filesystem access. Invented operations require the fixture
profile. Real retention/read needs explicitly admitted `retain`; validation also
needs `transform`. No active real grant is shipped. Raw receipt metadata excludes
cookies/auth headers and query/credential-bearing URLs; only public FINRA HTTPS
resource identities and bounded allowlisted response headers are retained.

Validation happens after durable raw capture. Missing/partial, HTTP failure,
unsupported contract/route and parser rejection persist as immutable linked
outcomes with no invented zero row count. Current v2 invented ATS and non-ATS single-file profiles are parsed; historical
v1 ATS/T1 remains supported. Multipart and historical candidate/unknown schemas
retain raw evidence without a guessed parser. Successful results remain
`PARSED` / `PARSED_EMPTY`, with typed rows and unverified membership for T06 to
handle under its separate complete local-population contract.

Readback verifies captured descriptor types and hashes independently of current
parser admission, preserving inspectable bytes for unknown archived versions.
Current validation still refuses unsupported manifests, schemas and routes; it
does not silently upgrade a receipt. Part metadata and requested ledgers are
snapshotted before hashing or waiting for the writer lock. Reservations are bounded to
100,000 entries, including ignored temporary artifacts.

Defaults bound one capture to 20 MiB, 64 received/expected parts, 10,000 parsed
rows and 64 KiB of canonical metadata. Byte/row overrides are positive integers
within hard local ceilings of 64 MiB / 1,000,000 rows. These are local resource
policies, not provider limits. No HTTP adapter, warehouse, CLI, replay service,
Gold publication, retention deletion or Dagster definition is introduced by T05.

## OTC-T06 local panel contract

Implemented offline; [acceptance](t06_local_panel_acceptance.md) records all six
fixture populations and transaction/readback evidence. This closes fixture
OTC-AC06; selected actual-source conformance is still T12.

ATS fixtures use the existing eight-column grammar across explicit T1/T2/OTCE
selections. Non-ATS fixtures have a separate, deliberately invented grammar:
`finra-otc-non-ats-weekly-invented-pipe/1`. Its columns are the eight ATS columns
followed by `firmCRDNumber|attribution`. MPID/CRD may be empty; attribution is
explicit REPORTED/AGGREGATED/UNKNOWN. A reported label without IDs forms only a
week-scoped name key; a group cannot assert firm IDs. Unknown/unkeyable or duplicate
rows reject the whole partition. These fields are synthetic test inputs, not a
current or historical FINRA non-ATS file/API schema. All counts/dates/text retain
the strict T03 grammar. No group is inferred from its display name.

`run_fixture` requires the fixture-only use profile. `--invented-fixture` is the
CLI acknowledgement; actual data/grants cannot authorize fixture operations.
A complete local publication additionally requires exact archived canonical
`finra-otc-fixture-population/1` discovery bytes binding origin, partition,
schema, expected `file` part, payload SHA and explicit expected row count.
This synthetic assertion establishes COMPLETE_FIXTURE/COMPLETE_EMPTY_FIXTURE only.
Bronze membership remains UNVERIFIED and no provider completeness claim is made.
Missing assertion, mismatched count/hash/scope, partial parts, unsupported captures
and malformed rows cannot publish. Even header-only needs an explicit empty
fixture assertion. Quantities are normalized decimal text in SQLite/JSON, beside
original token text, avoiding int64/float loss. Supplied MPID/CRD stay strings.

`<root>/otc.sqlite3` is source-owned, marked by application/schema/contract IDs.
`otc_attempts` retains capture/run/clock/sequence/context; `otc_rows` owns exact
initial panel records; `otc_publications` owns complete partition coverage and the
selected capture. A separate durable PENDING attempt precedes one transaction
that inserts rows, coverage/selection and terminal PUBLISHED status. A rollback
cannot publish any of them; a later transaction records sanitized FAILED status.
If failure recording also fails, PENDING remains, and `ingest-capture` can resume
it. Terminal FAILED attempts remain immutable and need a new capture for retry.
A commit acknowledgement interruption may leave a fully coherent committed
publication; inspect it before retrying. No external file/current pointer is
part of T06; the DB/file publication journal remains T10 work.

The existing root writer lock serializes writers; readers use a SQLite read
transaction and a single consistent selection. Publication seals bind coverage,
row hashes and immutable capture context. Reads verify these seals/counts without
parsing Bronze, creating stores, repairing files or probing a network. They expose
attribution row counts for PROVIDER_ID/SOURCE_ROW_LABEL/AGGREGATED/UNKNOWN and the
expected delay separately for every requested population. They do not combine
products/tiers into a ratio or a market-wide total. Missing requested populations
stay explicit. Current published coverage and latest warehouse attempt are separate;
a trailing failed/deferred attempt produces a degraded panel when all requested
populations have publications. The panel is not T13's full raw-capture health
inventory: a storage failure before PENDING is saved is observable in retained
Bronze and the nonzero command result, without an invented terminal DB attempt.

T06 admits initial membership only. A different complete capture for an already
published partition records REPLACEMENT_DEFERRED/T07_REQUIRED without updating
rows or membership. Identical capture retry verifies the existing committed selection, is idempotent
and preserves file hashes/mtimes. T07 must implement observed revision/reversion/removal history
before any later capture can supersede that selection. No as-of query is offered.
First-seen is actual capture time; reporting/update dates stay date labels and
unknown publication timestamp stays null. Fixture overrides in the low-level
archive API remain explicitly synthetic; real retention cannot backdate its clock.

Local limits: up to 10,000 panel rows, 64 MiB of normalized publication/read JSON,
312 explicitly selected partitions, 256 MiB SQLite file and a five-second DB busy
timeout. SQLite max-page count prevents growth beyond that file budget. Bronze
byte/part/metadata limits continue to apply. Database/archive-owned symlinks,
nonregular files, foreign storage IDs and WAL sidecars are refused. Operator-owned
parent paths remain trusted. Read-only open never performs hot-journal recovery;
an interrupted store needing recovery must be handled by an authorized writer.

## OTC-T07 immutable revisions and membership contract

Implemented offline; [T07 acceptance](t07_revisions_acceptance.md) closes fixture
OTC-REQ07/OTC-AC07 for all six profiles. The T06 behavior above describes the
preserved v1 protocol; current new stores use `finra-otc-sqlite/2` and transform
`finra-otc-membership/2`, independently of catalog/schema versions.

Semantic content excludes capture/run/transport metadata and original integer
formatting. Exact count tokens remain in each complete membership snapshot.
Repeated values retain the same occurrence and first-seen; corrections, reversions
and revival after removal append immutable occurrences with the original capture
clock/sequence. Source labels, identity evidence and dates remain semantic content.
Only a complete fixture replacement can remove membership; removal appends null
content with state REMOVED and never means zero volume. Partial/unverified/
rejected attempts cannot advance complete selection. A complete capture behind
newer accepted membership is OUT_OF_ORDER, without retroactive replacement.

`otc2_attempts`, `otc2_versions`, `otc2_snapshots`, `otc2_members` and
`otc2_migrations` hold the v2 state. Occurrences, complete membership, coverage and
terminal publication commit together after durable PENDING. Raw receipts,
legacy tables and terminal attempts remain immutable. Read-only v1 inspection
never upgrades; writes require explicit fixture-only `migrate-warehouse`.
Migration revalidates selected legacy rows against Bronze, seeds only original
selected publications and retains hashes of all old table contents. Deferred
captures are reconsidered only by explicit v2 ingestion, preserving the old result.

Current/as-of readers select observed captures using aware UTC and optional
original sequence for equal-clock ties. They expose value and membership lineage
separately, validate seals and use one consistent read transaction. These cutoffs
are capture knowledge under the current transform, not provider vintages or
processing-time history. Reporting/update dates remain dates and publication time
stays null. T08 retains broader release/date/holiday work. Retained/read history
is capped at 100,000 occurrences (default read 10,000); legacy migration caps at
1,000 attempts. Existing T06 row/byte/database limits remain in force.

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
| `participant_name_raw` | string or null | Original attribution label, including grouped/un-attributed labels. |
| `mpid` | string or null | Supplied identifier only, never inferred from name. |
| `firm_crd` | string or null | Preserve source identifier if a verified route supplies it. |
| `participant_key` | string or null | Versioned source-evidence token; unknown is unkeyable and quarantined, never a shared fake firm. |
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
