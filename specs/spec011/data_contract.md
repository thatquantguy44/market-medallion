# Settlement facts and threshold membership contract

**State:** normalized types and SEC SQLite slice implemented; later adapters planned.
**Date:** 2026-11-04. Inherits [C01–C11](../../docs/securities_finance/contract.md).
T18's normalized type contracts and serialization are implemented for both source
packages. The parser implements the six-column/trailer local profile version2,
reconciled against two approved September2026 archives after an unsuccessful
HTTP request. See the [source checkpoint](source_review.md) for sample hashes,
explicit bounds, trailer checks and coverage limitations. T04 implements SEC
SQLite persistence; PostgreSQL/Delta and threshold persistence remain planned.

## 1. Provider structures and grains

SEC documentation names these six fields; exact byte headers/ASCII encoding are
pinned for the two local samples: `SETTLEMENT DATE`, `CUSIP`, `SYMBOL`,
`QUANTITY (FAILS)`, `DESCRIPTION`, `PRICE`. Raw rows remain available in Bronze.

The samples have two terminal ASCII records, in order: `Trailer record count N`
and `Trailer total quantity of shares Q`. Both must match the parsed rows;
missing, malformed, duplicated or nonterminal trailers block acceptance. They
are integrity metadata, excluded from facts and fact-row budgets.

Proposed FTD fact key: `(dataset, reporting_regime, settlement_date, cusip)`.
Both September samples prove uniqueness within their observed scope. A conflicting
duplicate is a blocked
partition, not an instruction to sum quantities. An identical duplicate can be
diagnosed and rejected unless an explicit schema contract permits deduplication.
Source symbol/description stay attributes with revision history.

Nasdaq's linked current text header was observed as:

```text
Symbol|Security Name|Market Category|Reg SHO Threshold Flag|Rule 3210|Filler
```

A final 14-digit creation token was observed; preserve and validate it separately.
The historical pilot-list field documentation is not a substitute for the
threshold file's current schema. NYSE's exact download format remains open;
reserve no fabricated provider column names. Provider adapters normalize into a
common membership contract only after their independent schema/sample checks.

Membership fact key: `(provider, market, list_date, raw_symbol, rule_family)`;
where the source publishes a stable issue ID, retain it as an additional
identifier and evaluate a versioned key change. Provider/market/rule scope must
not collapse into a single exchange-independent symbol boolean.

## 2. Proposed normalized FTD fields

| Field | Type | Meaning / invariant |
| --- | --- | --- |
| source, dataset, reporting_regime | string | SEC identity and applicable reporting/format regime |
| settlement_date | ISO date | Source settlement label; never shifted to trade date |
| cusip | string | Exact nine-character identifier incl. leading zeroes; preserve unusual source token for diagnostics |
| symbol_raw, description_raw | string/null | Supplied source attributes; no identity inference |
| quantity_fails | nonnegative integer | Reported outstanding shares; no imputation |
| prior_close_price | decimal/null | Source price; `.` becomes null with `SOURCE_PRICE_UNAVAILABLE` |
| price_basis | enum | `PROVIDER_PREVIOUS_CLOSE`; price-date identity remains unknown unless independently verified |
| row_state | enum | `REPORTED`, `WITHDRAWN`; absence lookup has separate coverage state |
| partition_id, raw_member, raw_row_number | string/int | Provenance within the selected archive |
| content_hash, version_id | hash | Normalized content identity and observed version identity |
| provider_published_at, source_updated_at | timestamp/null | Only when source evidence supplies the clock and semantics |
| retrieved_at, first_seen_at | UTC timestamp | Capture clock and first observation of this normalized version |
| capture_sequence, run_id, raw_sha256 | int/string/hash | Stable replay ordering and immutable raw lineage |

Dates from a semi-monthly file must lie within its declared period. Weekend or
holiday labels are diagnostic pending the admitted settlement calendar; do not
silently drop or shift them. Integer overflow and nonfinite/negative prices are
blocking validation errors under the selected format contract.

Bounds are explicit configuration, never inferred from filename suffix alone.
The reconciled September files contain Sep1–14 and Sep15–30 respectively.
The manifest accepts starts1/15/16, with end14/15 for start1 and month-end for
start15/16, within the same month. This bounded validation does not establish
which split a different month uses or certify an expected settlement calendar.

## 3. Proposed normalized list and coverage fields

One coverage record per `(provider, market, list_date, capture)`:

- `requested_date`, `source_date`, `source_created_at_raw`, parsed source clock
  when its time zone is known, `calendar_version` and `source_schema_version`;
- `coverage_state`: COMPLETE, COMPLETE_EMPTY, NOT_PUBLISHED, UNAVAILABLE, PARTIAL
  or FAILED; expected/received row counts and validation evidence;
- receipt/capture sequence, first-observed knowledge time and current version.

Membership includes provider/market/list date/raw symbol/rule-family, source
name/market category/flags as available, source row pointer and common lineage
clocks. Preserve unknown flag tokens and block promotion until a reviewed
schema version defines them. A NYSE market selector is not inferred from an
issuer's current listing or from the symbol alone.

## 4. Missingness and removal decision table

| Situation | Required behavior |
| --- | --- |
| Partition never captured or failed/truncated | UNKNOWN/UNAVAILABLE; no absence event, zero or exit |
| Complete modern SEC partition, eligible date, absent CUSIP | NOT_REPORTED_IN_COMPLETE_PARTITION with coverage evidence; optional explicitly derived zero only under an admitted universe/date contract |
| Pre-2008 SEC absence | BELOW_REPORTING_THRESHOLD_OR_UNKNOWN, not zero |
| New complete exchange list excludes a previously present symbol | Observed non-membership at that list date; confirmed exit only with admitted adjacent-date/calendar continuity |
| Complete replacement of the same partition omits an older reported row | Versioned withdrawal at replacement capture time; earlier as-of result preserved |
| Complete-empty list validates schema and date | Explicit empty membership population; different from no file |
| Symbol moves between markets | Separate source facts plus dated mapping evidence; no invented seamless episode |

Partition coverage is historical metadata too. A later file repair may establish
coverage only from its capture time; earlier cutoffs cannot inherit that repair.

## 5. Clocks and calendars

The SEC settlement date is an economic label. Expected publication occurs after
the reporting period, but the release schedule does not backdate first-seen.
Nasdaq file date, created time, publication and retrieval remain distinct.
Date validation must account for the announced December 2026 change rather
than assuming creation-day equals trading-day across all eras.

Use UTC for knowledge cutoffs; use the admitted settlement/list calendar for
adjacency. Store calendar source/version and holiday exceptions. At an equal
capture timestamp, a persisted monotonic `capture_sequence` per root orders
events; replay reuses it. Ambiguous imported ordering fails rather than sorting
random run IDs into an invented history.

## 6. Identity and the optional research projection

Crosswalk grain: `(namespace, raw_identifier, effective_from, effective_to,
mapping_version)`, with target security ID, evidence, first_seen_at and match
status. Preserve many-to-many candidates; admit only the explicitly validated
single security mapping for a requested date and knowledge cutoff.

The FTD-owned optional projection consumes versioned native artifacts and an
explicit mapping artifact. It must declare whether it aligns list_date with
settlement_date for descriptive comparison or selects the latest available
observation at a knowledge cutoff. These are separate modes. No carry-forward
is implicit; any as-of carry must include original date and age/maximum age.

Projection metadata includes input snapshot IDs, mapping/calendar versions,
join mode, cutoff, raw/joined/unmatched/ambiguous counts and row-count/cardinality
checks. A failed mapping may suppress joined metrics while preserving the two
native timelines. Counts from SEC/threshold populations are never denominators
for one another without a separately documented population contract.

## 7. Episode and feature definitions

An observed episode starts with a present list row. It can be called an observed
entry only when the preceding expected complete list excludes that issue under
the same rule/market definition. Its last-observed date, observed present-list
count, calendar span and censoring flags are separate fields. Missing expected
dates split certainty; publish a gap flag rather than bridge the missing data.

FTD previous-observation change uses current selected balance minus the prior
available selected balance and includes both dates plus gap. It remains a
balance change, not newly failed shares. A rolling maximum or positive-report
count requires explicit selected settlement dates and coverage; never use a
calendar-day forward fill to manufacture a daily history.

## 8. Export and failure contract

Native artifacts contain schema/method versions, source identity, generated
time, knowledge mode/cutoff, complete partition inventory, reported history,
missing/withdrawn states, units and revision tokens, with attribution and source
links. Consumer limits are explicit: proposed default 104 observations per
selected security and a required selected-security list; partition inventories
can be summarized with bounded pagination. Maximum file bytes are validated.

Reject duplicate dates/keys, malformed source identity, ambiguous unit/mapping
or changed required schema. Do not leave a prior artifact looking freshly
generated after failure. Retain its actual old generation time plus health
failure evidence; consumers decide whether dated prior results are usable.

## 9. Storage and platform compatibility

**Owner requirement, 2026-11-03:** PostgreSQL and Databricks compatibility are
required for Spec011, for both SEC FTD and exchange-threshold packages. SQLite
remains the offline development backend. PostgreSQL is an alternative operational
warehouse; Databricks consumes a validated committed snapshot into Delta. This
does not require Spark in the parser or make Databricks a local ingestion backend.
SEC SQLite/PostgreSQL and coherent Parquet are implemented; threshold PostgreSQL
and both Delta adapters remain absent. R12/A14/A15 still gate full Spec011 parity.

| Logical value | SQLite | PostgreSQL | Parquet / Databricks Delta |
| --- | --- | --- | --- |
| CUSIP, symbol, source/partition/version IDs, hashes and raw tokens | TEXT | TEXT | STRING; no numeric identifier inference |
| Settlement/list date | Validated ISO date text | DATE | Explicit DATE |
| Known knowledge/publication clock | Canonical UTC timestamp text | TIMESTAMPTZ | Explicit UTC timestamp schema and UTC Spark session; unknown remains null |
| Capture sequence, row positions and bounded metadata counts | INTEGER with explicit range checks | BIGINT with explicit range checks | Explicit signed 64-bit type with matching checks |
| Exact FTD quantity / price | Lossless integer/decimal text, decoded to Python int/Decimal | Unconstrained NUMERIC plus exact source text | Exact text plus declared DECIMAL projections only when lossless and representable |
| Missing-price/coverage/withdrawal state | Explicit state/reason text | Explicit state/reason text | STRING; SQL null alone cannot encode all states |

FTD quantities are not restricted to int64 by the source contract. Do not use
SQLite REAL, PostgreSQL floating point, Spark DOUBLE or implicit casts for
balances/prices. Preserve exact source numeric tokens alongside normalized
values, including price scale. PostgreSQL NUMERIC and Databricks DECIMAL have
different representable limits; Databricks precision is at most 38. See official
[PostgreSQL numeric types](https://www.postgresql.org/docs/current/datatype-numeric.html)
and [Databricks decimal types](https://docs.databricks.com/aws/en/sql/language-manual/data-types/decimal-type),
reviewed 2026-11-03.

T18 must pin versioned numeric precision/scale and explicit Arrow/Spark schemas
before storage code. Canonical exact-text fields remain available in every
backend. A value outside a required numeric projection's range/scale blocks
that publication with a sanitized diagnostic and preserved Bronze; it must
never become a rounded number or an unexplained null. Integer values above
int64 but within the selected decimal range must round-trip. Tests include
decimal38 boundaries, a 39-digit overflow and a price needing excess scale.
No schema inference or automatic Delta schema evolution is permitted.

Warehouse configuration selects `sqlite` or `postgres`; each root is bound to
one backend/warehouse identity. PostgreSQL schema/role and Databricks
catalog/schema/staging location are explicit source-owned settings. Environment
variable references supply credentials; checked-in examples contain no secrets.
Do not reuse AAII tables, data roots, advisory-lock identities or runtime
credentials by importing its adapters. Migration between roots uses a validated
replay/export with original capture lineage, never changing a bound root in place.

Both warehouse adapters implement the same normalized transaction/query boundary:
fact revisions, partition coverage, audit, pending publication and capture sequence.
PostgreSQL commits related state in one transaction and serializes writers for
the same pipeline identity. All values use parameterized SQL; configured SQL
identifiers are separately validated. Backend errors cannot promote facts or
claim a successful generation. Test concurrent/retried captures and rollback.

Delta publication uses committed, hashed, explicitly typed Parquet snapshots in
a configured Unity Catalog staging volume. Validate source/regime/schema,
row counts, unique version keys and exact numeric representability before writes.
Use idempotent revision-key publication with source-owned table names; see
[Delta MERGE](https://docs.databricks.com/aws/en/delta/merge). Keep original
capture_sequence for equal-clock ordering. Recompute current/as-of selection
from preserved revisions and coverage, including withdrawals.

Individual Delta table commits do not establish a coherent multi-table delivery.
Pin immutable table versions in a generation manifest and expose a generation
only after facts, coverage and audit all verify. A failed attempt keeps the
prior generation current; a retry is idempotent. Consumer views/readers must
honor that generation rather than selecting each table's latest independently.

SQLite/PostgreSQL parity requires an actual dedicated PostgreSQL integration
run. Delta compatibility requires an actual Spark/Delta or Databricks round trip,
in addition to offline schema/SQL and failure-injection tests. Record engine
versions and whether a Databricks workspace smoke ran; skipped tests or mocked
Spark calls alone do not establish compatibility. Deploying a cloud workspace
or enabling schedules remains separate from implementation acceptance.

## 10. Pipeline-specific runtime configuration

The SEC FTD runtime settings live in
[`config/sec_fails_to_deliver/`](../../config/sec_fails_to_deliver/README.md).
The checked-in `pipeline.example.yml` is copied to ignored `pipeline.local.yml`;
an optional ignored `.env` holds the variable named by `postgres.password_env`.
The user confirmed **PostgreSQL as default** and **Databricks disabled until
its target is configured**. PostgreSQL17.6 was provisioned only as a disposable
integration target; no retained production database or Delta target is claimed.

Runtime config version1 has explicit pipeline identity, root/backend, PostgreSQL
settings, Delta target settings, storage profile and named manifest/input pairs.
Relative paths resolve from the YAML's directory. Config selection is an explicit
`--config` argument, with no inherited AAII runtime or automatic environment-file
execution. Secrets are absent from YAML and validation output; validation reads
neither secret values nor input/manifest files and opens no service connection.

Configurable numeric projections start at quantity precision38/scale0 and price
precision38/scale12. Precision may be1–38; price scale may be0–precision.
These drive the implemented serialization codecs, not projections performed
by `inspect`. UTC, exact numeric-text retention and rejected lossy
conversion are enforced contract rules. A SHA-256 `storage_contract_id` includes
the pipeline and storage settings, excludes connection/path settings and must
be bound to the durable root by T04. Changing precision/scale requires a distinct
storage identity or an explicit migration; the loader alone does not enforce
persisted root binding. SQLite and SEC PostgreSQL enforce that binding. T18
Arrow/Spark schemas and serialization round trips are complete; threshold
PostgreSQL and Delta adapters remain absent.

Implemented commands are `validate-config --config ...` and
`inspect --config ... --partition ...`. Config-mode inspection selects exactly
one configured manifest/input pair, with no conflicting input override. Existing
`--manifest ... --input ...` inspection remains available. Both paths preserve
provider bytes/semantics and make no request. Configuration validation performs
no warehouse/Delta I/O or source-manifest checks; its backend capability flag
describes implemented code, not a tested connection.
T04 additionally supplies configured `run` and bounded `gold` commands for
explicitly selected SQLite roots; `validate-config` reports the selected backend's
implementation capability without opening it.

## 11. Implemented T18 record schemas and serialization

Source-owned definitions live in
[`sec_fails_to_deliver/storage.py`](../../src/market_data_pipeline/pipelines/sec_fails_to_deliver/storage.py)
and [`reg_sho_thresholds/storage.py`](../../src/market_data_pipeline/pipelines/reg_sho_thresholds/storage.py).
The threshold package contains normalized type contracts only, with no physical
exchange parser, source admission or runtime configuration borrowed from SEC.
Both use the provider-independent field codecs in
[`shared/storage_types.py`](../../src/market_data_pipeline/shared/storage_types.py).
No core/type-contract import requires Arrow or Spark; Arrow loads on request.

| Source | Versioned record schemas | Identity and key fields |
| --- | --- | --- |
| SEC FTD | `sec-ftd-facts/1`, `sec-ftd-coverage/2`, `sec-ftd-audit/1`, `sec-ftd-publications/1` | Source/dataset/reporting regime; settlement date/CUSIP for facts; partition/version/run/capture sequence for lineage |
| Thresholds | `reg-sho-membership/1`, `reg-sho-coverage/1`, `reg-sho-audit/1`, `reg-sho-publications/1` | Dataset; independent provider/market/list date/raw symbol/rule family for membership; partition/version/run/capture sequence for lineage |

`storage_schemas(settings)` for FTD consumes the pipeline config. Threshold
`storage_schemas(schema_version="reg-sho-storage/1")` pins its independent
normalized version; its future source config must select an admitted version.
Schema metadata contains the schema version and storage contract ID. Changes to
field meaning/types require a version change; changing numeric settings changes
the config contract ID. Neither automatically migrates an existing root.

Each RecordSchema provides `normalize`, `sql_types`, `encode`/`decode`,
`arrow_schema`, `arrow_table`/`read_arrow` and `spark_schema_json`. The last
returns explicit Spark StructType JSON with field nullability; constructing a
Spark DataFrame and enforcing a UTC session belong to the later Delta adapter.
SQL mappings are field types/bindings, not a table installer or full DB adapter.
PostgreSQL uses native date, UTC datetime and Decimal values with NUMERIC;
SQLite encodes dates/clocks/decimals as lossless text. Both preserve exact
quantity/price token columns. Arrow uses DATE32, microsecond UTC timestamps,
int64 metadata and configured decimal128 projections; no inference or floats.

SEC REPORTED facts require raw numeric tokens matching their exact projections.
The source `.` price requires null plus `SOURCE_PRICE_UNAVAILABLE`. WITHDRAWN
facts retain keys/version/capture lineage with null observation values and no
invented source-row pointer. Quantity and price are therefore nullable storage
columns, with conditional semantic checks. Source symbols and description stay
nullable strings; original quantity/price tokens are now retained by the parser.
Excess nonzero fractional digits/overflow fail before Arrow/SQL encoding.
Trailing decimal zeroes can fit a projection without losing the separate exact
token. Projection behavior is independent of the caller's Decimal precision.

Threshold raw flags are an opaque JSON object of unique string tokens, not a
guessed NYSE physical schema. Native identifiers stay strings. Coverage records
distinguish complete-empty, incomplete, missing and failed lists; complete
coverage requires consistent dates, count evidence when supplied and a validated
envelope assertion. Typed assertions do not themselves prove source completeness
or authorize collection. That remains the exchange adapters' responsibility.

Both warehouse Protocols pin transactional capture-sequence allocation, run
recording, atomic facts/membership + coverage + audit promotion, cutoff selection
and publication-journal operations. Future adapters must preserve equal-clock
capture ordering and validate all records before commit. The Protocols supply
no persistence, revision generation, query engine or crash recovery themselves.
T04's source-owned SQLite adapter implements the local capture/current-selection
subset; T05 adds observed-as-of, T06 replay, T07 Parquet and T19 SEC PostgreSQL.

T18 evidence is real SQLite/Parquet serialization plus native PostgreSQL binding
and explicit Spark schema tests. T19 adds actual SEC PostgreSQL server parity;
threshold PostgreSQL and Spark/Delta remain T20–T22. Historical first-seen clocks, new source
coverage and warehouse readiness are not inferred from these tests.

## 12. T04 bounded persistence decisions

The first concrete adapter is SQLite, used through the T18 source-owned boundary.
PostgreSQL remains the configured operational default and is implemented by T19,
with no fallback to SQLite. Databricks stays disabled.

Each attempt reserves an ordered capture/run, writes immutable original manifest
and storage settings plus a capture receipt, and retains bounded input bytes
before parsing. Complete payloads have their original SHA-256. Oversized inputs
retain at most archive-limit+1 bytes as an explicitly incomplete capture, with a
captured-prefix hash and unknown original hash. Missing files have no fabricated
payload/hash. Coverage `raw_sha256` therefore becomes nullable for unavailable
or failed captures in `sec-ftd-coverage/2`; COMPLETE states require a real hash.
Version1 was type-only; no persisted version1 warehouse was migrated.

The root and database bind one dataset/regime, backend, warehouse ID, storage
contract and record-schema versions. Reject incompatible or unbound nonempty
roots before input capture. A single-root writer lock and SQLite transaction
serialize run allocation and atomic facts/coverage/audit promotion. Bronze is
durable before promotion; initial Gold is database-backed. T07 adds the separate
Parquet/current-pointer publication journal. Interrupted STARTED attempts
remain visible for recovery; they do not imply fact promotion or success.

A valid local file imports reported facts with MATCHED validation and PARTIAL
population coverage: trailers do not certify the settlement calendar/universe.
Missing files produce UNAVAILABLE coverage; rejected inputs produce FAILED
coverage and no fact promotion. Header-only/zero-row archives remain unsupported
and are rejected distinctly. An unchanged payload may be recaptured without
new facts. At the T04 checkpoint, changed replacements and overlapping accepted
partitions were retained/audited but rejected. T05 below supersedes the changed-row
restriction; overlapping partition scopes remain rejected.

T04 passes its focused cases and the full suite of 316 tests (2 existing opt-in
PostgreSQL skips), plus actual September ingestion/reopen and unchanged recapture.
The store has 117,025 unique facts, 663 missing prices and 21 observed settlement dates; original trailer
quantity sums reconcile. First-seen is actual November3 acquisition completion,
with separate acquisition-start clocks in Bronze. No PostgreSQL/Delta engine
was touched. See the [runbook](../../docs/deployment/sec_ftd_runbook.md) and
ignored `data/sec_ftd/t04-verification.json`. Later sections supersede this checkpoint.

## 13. T05 observed-revision decisions

Accept changed rows from validated partial captures without treating omissions
as withdrawals. Compare each incoming observation against the latest version
of its native key, not every previously seen content hash. A correction and a
reversion each create a fresh version with current capture lineage; unchanged
facts retain their original version/first-seen. Keep the T04 content-hash recipe
(including exact numeric source tokens) so existing roots do not acquire false
versions solely from a code upgrade. Formatting changes to retained source
tokens remain observable attribute revisions.

Timestamp cutoffs are inclusive, timezone aware and applied before ranking
versions, to both facts and coverage. An optional sequence cutoff refines captures
at the exact timestamp; omitting it includes all captures at that timestamp.
Rank withdrawals before excluding them from ordinary Gold so old facts cannot
resurrect. Expose withdrawn states explicitly when requested.

Withdrawal requires a reviewed completeness sidecar for the exact archive,
member and partition, with a validated settlement-date inventory and source,
calendar and review references. The configured sidecar is archived unchanged
and hashed in receipt version2; legacy version1 captures remain readable.
No sidecar means PARTIAL and no withdrawals. A partial/failed capture cannot
authorize removal; contradictory supplied evidence rejects promotion. These
are scoped, operator-reviewed assertions, not automatic external verification
of the references or a certification of the entire security/history universe.
The approved real September inputs retain their current uncertified scope.

## 14. T06 deterministic offline replay contract

Replay reads only original Bronze artifacts and original terminal outcomes.
New captures seal `outcome.json` after database promotion, binding a checksum,
the exact capture-receipt digest, encoded audit/coverage records, and a count/hash
of that run's inserted fact versions. This is integrity evidence, not a signature
or provider authentication. Legacy receipt1/2 captures without an outcome file
require their intact source SQLite audit/coverage/fact ledger for reconciliation;
replay never writes reconciliation files into the source. An existing outcome
file must agree with that ledger when it is available. A completed replay root
seals a checksummed ordered capture inventory in `replay.json`; it pins every
run/sequence/outcome hash and total version count. That complete sealed archive
can replay without its database. A missing/stale inventory or missing capture,
including the final capture, blocks database-free replay. Missing/STARTED/PARTIAL outcomes,
ambiguous sequences, unsupported versions and any contradiction fail closed.

Read the source under a shared root lock and read-only database snapshot where
available. Validate the complete bounded inventory before building a new root.
Order strictly by persisted positive capture sequence, never UUID or directory
order. Preserve IDs, capture/acquisition/completion clocks, original outcomes,
coverage versions and all exact numeric tokens. Reparse only originally accepted
captures, use archived completeness evidence, and regenerate versions through
the normal warehouse boundary. Never re-admit a historical rejection under a
newer rule. Verify each reconstructed run's fact count/hash against its outcome.

`replay --source-root ... --config <destination.yml> --max-captures 1000` requires
an explicitly selected SQLite or PostgreSQL destination with the same storage
contract and Delta disabled. The destination must be empty, separate and nonoverlapping with
the source. Build in a private sibling staging root, verify artifacts and logical
state, then rename the completed root into place. Failure removes only this
invocation's staging root; no partial destination is published. A new warehouse
identity is minted while capture/version identities are retained. Source input,
manifest and evidence paths in live YAML are never opened; no transport or
network preflight is invoked. T07 publication runs before destination exposure.

Acceptance A09: invented corrections/reversions, equal-clock cutoffs, unchanged
captures, complete withdrawals/reappearance, rejection/failure and receipt1/2
replay yield equal facts, coverage, audit and Gold. Exercise missing/damaged
artifacts/outcomes, unsupported parser/schema versions, sequence ambiguity,
nonempty/overlapping targets and injected promotion failure. Reconcile the real
September root into an isolated destination without changing original knowledge.

## 15. T07 coherent Parquet publication decisions

Publish four exact-schema files per immutable generation: all fact versions,
current non-withdrawn Gold, all coverage versions and audit rows. Each manifest
binds snapshot/run/capture, warehouse/storage/schema identities, row counts and
file SHA-256 values. The database publication journal advances STAGED →
COMMITTED → PUBLISHED; only atomic `parquet/current.json` selects consumers.

Recovery validates checksums, Arrow metadata and row counts. A complete pending
generation is finalized idempotently; an incomplete one is ABANDONED without
moving the prior pointer. Recovery precedes a new capture, and the new capture
then publishes its own state. Partial writes, rename, pointer and post-pointer
watermark failures therefore expose either the prior coherent generation or a
fully verified newer one, never a mixed table set or false current watermark.

## 16. T19 SEC PostgreSQL decisions

The database/schema must exist; a new pipeline root requires no preexisting
SEC-owned tables or view. Credentials are read only from the configured
environment-variable name at connection time. The adapter binds root and
database identities plus backend/database/schema/storage contracts, uses native
DATE/TIMESTAMPTZ/NUMERIC(precision,scale), and serializes writers with a
PostgreSQL session advisory lock in addition to the root lock. Promotion and
publication transitions are transactional; no SQLite fallback exists.

Actual PostgreSQL17.6 integration tests cover corrections/reversions,
equal-clock cutoffs, exact values above int64, evidence-gated withdrawal,
failed-promotion rollback, concurrent retry, wrong-root refusal, Parquet and
SQLite→PostgreSQL replay. Failed replay cleanup is allowed only for tables newly
created by that invocation after the stored database identity matches its root;
preexisting or changed stores are never dropped. This evidence closes SEC T19,
not threshold T20 or Delta T21/T22.
