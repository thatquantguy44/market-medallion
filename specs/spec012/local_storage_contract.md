# SSV-T06–T08 — audited local partitions and observed changes

Selected 2026-11-13 on `feat/finra-local-revisions`, from merged T05 PR #20.
**Status:** implemented and accepted; [acceptance](t06_t08_acceptance.md) and
[nine-capture evidence](evidence/t06_t08_local_conformance.json).
Traces SSV-REQ06–08 / SSV-AC06–08, with C04–C07 and existing admission/parser
contracts. Implement in three tested increments: T06 SQLite plus local CLI,
T07 numeric revision/reversion semantics, T08 complete replacement/removal and
absence inspection. Acquisition, cross-page assembly, aggregate/temporal Gold,
Parquet, replay, exports and Dagster remain later slices.

## State, types and publication

Keep T05 raw bytes/receipts/outcomes unchanged. Bind a private source-owned
`warehouse/` directory and SQLite identity to its root, manifest, transform
version and fixture/provider input kind. Initialization is atomic. A missing,
foreign or damaged bound store must not be silently replaced. No other source's
configuration, tables or services participate. Use stdlib SQLite, explicit
transactions, parameterized queries, foreign keys, full synchronous writes and
bounded local queries. Keep unknown provider publication and retrieval clocks null.

The logical key is `(trade_date, facility_id, symbol_raw)` under the fixed
FINRA dataset/scope/schema identity. Store exact source Decimal text using the
existing codec, including values beyond int64 and fractional scale. Derive only
per-observation short/total and exempt/total using the fixed T04 method. No float,
implicit quantization, short-interest interpretation or calendar inference.

A complete API page means offset zero, received rows equal Record-Total and all
existing receipt/row checks passed. This single-page boundary admits complete
empty arrays and the validated empty 204 response. Every complete capture owns a
full partition membership referencing immutable Silver versions; a transactional
partition pointer selects that membership for local Gold. Failed transactions
leave prior membership and values intact. T10 will introduce typed files and the
DB/files journal; T06–T08 publish inside SQLite only.

STARTED attempts are durable before parsing/transform. Their terminal status and
capture result commit with facts/membership/pointer. Fixed failure codes exclude
raw exception text. Failed execution is retryable before a later terminal capture
is processed; an interrupted STARTED attempt must be recovered before another
capture. A fault after a successful DB commit cannot relabel it failed. Exact
terminal capture retries return original evidence and do not duplicate attempts
or facts. Capture identity, receipt and parser/transform profile must agree.

## T07 — observed revisions

Order terminal captures by `(archived_at, original_capture_sequence)`; refuse
clock regression or processing an older capture after a later terminal one.
Equal-time captures retain sequence order. First-seen is the original local T05
capture time, never trade date, request-start time or inferred publication.
Local current-vintage imports cannot supply pre-capture historical knowledge.

A normalized content hash includes the stable key, raw facility/market identifiers
and numeric values. Numeric equality ignores display scale and signed zero without
using ambient Decimal arithmetic. The first version retains its original exact
text; every capture still retains its own bytes and membership/source-row lineage.
An unchanged row adds no version. A correction or reversion appends a new version,
even if that value appeared earlier. Version IDs bind content to original capture
lineage. As-of reads select the latest complete membership at/before the UTC cutoff;
an optional original sequence resolves an equal-time cutoff. Never sort UUIDs.

## T08 — replacement and absence

Only an authoritative complete replacement can append removal versions and
replace membership. A partial page publishes no rows and removes none. Invalid,
incomplete-body, HTML, source HTTP failure and parse rejection retain Bronze and
sanitized audit evidence. A valid empty replacement retracts prior members;
another empty capture does not duplicate removals. A removed row returning later
creates a new version. Different facilities/dates remain independent.

Coverage inspection reports both latest local attempt and last committed complete
partition. Distinguish local MISSING (no capture/attempt), PENDING (unresolved local
STARTED attempt), FAILED, PARTIAL, COMPLETE and VALID_EMPTY. These are local evidence
states, not provider-release/calendar claims; source scheduling remains T13.
A symbol is NO_REPORTED_ROW only under a committed complete membership excluding
it. Before complete evidence it is UNKNOWN. Report no synthetic zero quantity or
denominator. Preserve prior committed membership after a failed/partial refresh,
while exposing the newer attempt independently. Include capture lineage and
CURRENT_VINTAGE / LOCAL_CAPTURE knowledge labels in inspection.

## Verification and usability

Invented fixtures precede changes. T06 proves raw-to-SQLite-to-Gold readback,
reopen, audit, admission-before-I/O, root/schema/kind binding and failure rollback.
T07 proves identical/scale-only repeats, corrections/reversions, exact quantities,
UTC/sequence cutoffs, old-clock refusal and idempotent/concurrent retries.
T08 proves removal/return, valid empty versus partial/rejected/missing/pending,
independent partitions, failed commit recovery and no invented zeroes. Exercise
real CLI processes via a source-owned strict fixture-default YAML and script.
Run all FINRA regressions, full offline suite, lint/format, build/source readback
and independent temporary-root conformance before closing tasks. Provider-data
requests and existing operational stores are outside this acceptance.

Official references checked before coding:
[FINRA API documentation](https://developer.finra.org/docs),
[Equity use terms](https://developer.finra.org/specific-terms-equity-data), and
[Python 3.12 SQLite transactions](https://docs.python.org/3.12/library/sqlite3.html#transaction-control).
The existing admission and verified three-TRF schema remain unchanged.

## Implemented storage details

Silver values are canonical JSON documents with exact decimal text, explicit
SQLite key/time columns and capture-bound content/version hashes. Membership
hashes bind symbol, version and source-row order to a terminal result. The Gold
view joins the committed partition pointer to that complete membership. Reads
verify identity, actual schema, foreign keys, version/membership checksums and
partition scope. A separate root binding refuses a lost warehouse directory;
missing/foreign bindings and symlinked files/SQLite sidecars refuse repair.
Initialization failures may require explicit investigation; automatic warehouse
recreation is deliberately absent. No service or existing store was provisioned.

Bound versions at 250,000, audit/history output at 1,000 rows, metadata at 64 KiB
and default payload/parser limits at 20 MiB / 5,000 rows. Identical captures at
the version budget still append capture evidence without allocating a version.
Attempts retain parser/transform settings even on failure. `completed_at` is the
local precommit completion observation after transformation, not an asserted
provider clock or exact fsync completion. A postcommit response failure leaves
APPLIED evidence; retry verifies and returns that original result.
