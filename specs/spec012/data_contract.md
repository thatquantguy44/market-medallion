# FINRA short-sale-volume data contract

**Version:** design v1 with the 2026-11-10 SSV-T01 admission checkpoint.
**Implementation status:** pure approved-use gate only; parser, connector and
storage are absent. Provider facts below are documented; normalized names and
storage remain proposals.
See [Spec012](README.md) for requirement IDs and the
[program contract](../../docs/securities_finance/contract.md) for common controls.

## Approved-use admission checkpoint

The [2026-11-10 source review](source_review.md) is the current rights/route
evidence and `approved_use` mapping contract. The user selected personal
noncommercial internal research. `manifest.require_approved_use` validates that
purpose, route, authorization kind, nonsecret permission reference, explicit
operation grants and optional Public credential metadata without I/O.

Current operational admission is **invented fixtures only**. An actual
file-permission or accepted API-terms reference is still required. The Public
Query API has documented noncommercial internal/derived-data rights, but a
separate JSON schema/completeness plan and account are needed before selecting
that route. API evidence does not grant website TXT collection permission.
Future I/O entry points must call the gate; none are implemented yet.

## Evidence register and remaining conformance

| Evidence | Documented fact | Still to verify before implementation |
| --- | --- | --- |
| [Current daily files](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files) | Individual facility downloads plus listed-security consolidated TRF/ADF; updated files and publication guidance. | Current link patterns, archive bounds, correction labels and each payload's market encoding. |
| [Layout PDF](https://www.finra.org/sites/default/files/2021-07/DailyShortSaleVolumeFileLayout.pdf) | Pipe-delimited fields, header/trailer and schema change in 2011. | Literal header spellings/whitespace, newline/encoding, trailer representation and consolidated market field. |
| [Source overview](https://www.finra.org/finra-data/browse-catalog/short-sale-volume) | Publicly disseminated off-exchange trades, non-commercial free use. | Dataset-specific retention, automation, transformations and consumer-display terms. |
| [API documentation](https://developer.finra.org/docs) | Credential types, metadata, filtering and pagination. | Whether a free permitted API route covers the requested real history; mock is inadmissible. |

The generic source overview inconsistently describes consolidated scope compared
with the dedicated daily-file page. Treat the latter's TRF/ADF description as
the discovery starting point and require actual scope reconciliation. Do not
silently add ORF or treat a comma-separated market list as one individual market.

## Schema and types

| Provider field/concept | Proposed normalized field | Type and semantics |
| --- | --- | --- |
| Date | `trade_date` | Strict `YYYYMMDD` converted to an unshifted ISO date label. |
| Symbol | `symbol_raw` | String; preserve case/suffix/punctuation as supplied. |
| ShortVolume | `short_volume` | Nonnegative arbitrary-precision integer shares, including exempt shorts. |
| Short Exempt Volume | `short_exempt_volume` | Nonnegative integer subset; null only for a separately supported earlier schema. |
| TotalVolume | `total_volume` | Nonnegative integer shares in exactly the same reported scope. |
| Market | `market_raw` | Preserve literal provider field; map only through verified schema profile. |
| Download context | `file_scope`, `facility_id` | Distinguish named facility, consolidated TRF/ADF and ORF. |
| Discovery context | `schema_version`, `requested_partition` | Prevent old/new layouts or scopes from mixing. |

The exact current literal header must be pinned from a permitted source-shaped
sample; the PDF's display labels alone do not approve a tolerant alias parser.
Use explicit aliases only when verified and regression-tested. A non-integer,
negative, malformed date or impossible count relationship invalidates promotion.
SQLite columns must preserve counts beyond signed 64-bit where necessary; choose
decimal text or explicitly bounded integer encoding and document it. Parquet
must use an equally lossless integer/decimal type with a schema fingerprint.

## Grain and identity

Proposed logical observation key:

```text
(provider=finra, dataset, file_scope, facility_id, trade_date, symbol_raw)
```

The manifest defines whether a dataset permits multiple rows for a symbol.
Exact duplicates within one complete input fail uniqueness rather than being
silently summed. The source symbol is an observation-time identifier, not a
permanent security ID. Crosswalking to CUSIP/ISIN requires separately dated
evidence with unresolved/ambiguous outcomes; names cannot force a match.

Rows from individual facilities and the consolidated representation are
alternate views of overlapping activity. No rollup can contain both. An
off-exchange aggregate uses an explicit admitted set of disjoint facilities and
reports its members, completeness and requested universe. ORF is never used as
a missing listed-market constituent without an approved contract change.

## Clocks and observed versions

Retain `trade_date`, nullable `provider_published_at`, nullable source update
label/date, `retrieved_at`, `first_seen_at`, `capture_id`, `run_id`, content hash,
manifest hash and parser/transform version. The 6 p.m. target is scheduling
guidance, not an actual publication timestamp. HTTP Last-Modified is retained
as HTTP evidence and not promoted to a provider publication clock automatically.

An identical normalized row creates no new value version, even if a later fetch
has a new run ID. A correction and later reversion both create new immutable
versions. The original first-seen time belongs to that observed version; each
capture still receives a distinct receipt. As-of reads select versions observed
by the cutoff, then derive ratios. Backfill first-seen is actual import time.

## Partition completeness, absence and replacement

A partition records expected files, discovered files, response outcomes,
trailer count, parsed count, accepted count and the selected source revision.
Publication requires the selected file to be complete. A valid zero trailer
means a verified empty file, distinct from missing file, holiday, not-yet-posted,
zero-byte response, access error, truncation and rejected schema.

Absence of a symbol from a complete short-sale file is `NO_REPORTED_ROW`; it
does not manufacture a denominator or assert zero trading. If a corrected full
replacement removes a previously reported row, append a removal version tied
to that complete replacement. A partial response cannot remove prior rows.
Observed-as-of selection must reproduce membership as well as changed values.

## Validation and Gold formulas

Required relationships: `0 <= short_exempt_volume <= short_volume <= total_volume`.
For the current regime all counts are required. The trailer must equal parsed
records under the verified file format; conflicting duplicates block publication.
Reject requested-date/file-date mismatches, unknown scopes and unverified market
codes. A schema drift outcome preserves Bronze and the old committed snapshot.

Proposed Gold fields include `short_share`, `short_exempt_share`, prior available
trade date, day gap and ratio change. Compute `short_volume / total_volume` and
`short_exempt_volume / total_volume` separately; denominator zero yields null.
There is no extra exempt addition. An aggregate ratio is ratio of summed counts
over admitted disjoint files, never the unweighted mean of facility ratios.

Rolling features are deferred until the window calendar, minimum coverage and
stable facility population are explicit. Do not produce a hard-to-borrow score,
days-to-cover, naked-short count or borrow-cost estimate from these values.

## Memory, files and atomic publication

Stream text records under configured byte/row bounds. Reject embedded archives,
redirects outside admitted official hosts, HTML/login bodies and incomplete
downloads. Store immutable inputs before parsing. Stage normalized results and
publish only after transaction and typed snapshots pass. A committed pointer
selects one snapshot; readers must not glob all output partitions into history.
Dry-run retention must explain every referenced capture/version before deletion.
Provider retention requirements are unresolved and never inherit NAAIM's 365 days.

## Proposed downstream artifact

`finra-short-sale-volume-research/1` is a proposed file contract, not implemented.
It contains dataset/scope/facility, requested/observed coverage, trade date, integer
counts, ratio unit, nullable ratio, version token, capture clocks, method version
and approved-use/source labels. It excludes credentials, local filesystem paths
and raw source payloads. Export eligibility is checked against the rights profile.
No QuantMeridian code, DB grant or cross-repository config is changed here.
