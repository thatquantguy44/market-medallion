# SSV-T05 — immutable API input and rejected-capture evidence

Selected 2026-11-13 on `feat/finra-daily-short-sale-volume`.
Traces **SSV-REQ05 / SSV-AC05**, with admission SSV-AC01 and unchanged T03/T04
parser/quantity semantics. This slice implements local filesystem storage and
parse-outcome evidence. Acquisition, SQLite, publication, export and jobs remain
later tasks; no provider-data request is part of T05.

## Admission and privacy boundary

Every filesystem entrypoint checks the current approved-use profile first.
Archiving admits only `fixture`, `local_import` or `acquire` purpose; parsing
requires `fixture` or `transform`. A read accepts the declared admitted operation.
Fixture-only admission never grants provider use, and fixture/provider artifacts
cannot be relabeled at readback. The selected API and approved local API-response
routes are supported; the website daily-file route remains excluded.

Validate the reviewed manifest and exact production POST/version/date/one-TRF
request scope before writing. An auth endpoint, unknown filter/body field,
unsupported facility, credential-bearing URL or other unadmitted scope is refused
without creating the archive. This is not a live acquisition/date-window gate;
valid old archived trade dates retain their original scope.

Retain only the parser's validated public request fields and allowlisted,
syntax-checked response headers. Drop all opaque diagnostics, request headers,
authorization/cookie/proxy material and unrecognized response headers. Invalid
known response evidence is explicitly rejected; dropping it cannot convert a
bad response into a successful parse. Original byte/hash claim mismatches remain
rejections even though the archive computes its own true payload digest/length.
An explicit `body_complete: false` also remains a rejection. Persist an admission
fingerprint, not arbitrary permission-reference text.

Security screening precedes storage: recognizable credential JSON keys, including
escaped keys, and Basic/Bearer credential material are refused without persisting
the body. Do not rewrite raw bytes to conceal credentials. This is a narrowly
defined data-response boundary, not a universal secret detector; callers must
never supply token responses or credentials as market-data input. Within the
admitted, bounded, noncredential data-response scope, malformed JSON, schema/row
rejections and source HTTP failures retain exact original bytes before parsing.

## Files and capture identity

Pipeline-owned implementation: `data/capture.py` and `io/bronze.py`, with the
package initializer and `tests/test_ssv_bronze.py`. Reuse only provider-independent
file locking/time helpers. No schema or dependency change.

Under an explicit caller-owned root, `bronze/format.json` identifies this storage
contract. `bronze/captures/<capture_uuid>/` contains complete `payload.bin` and
canonical `receipt.json`. Receipt identity binds source/schema/manifest, actual
byte count/hash, run/capture UUIDs, admission fingerprint, finite capture settings,
sanitary API receipt and fixed issue codes. No arbitrary caller diagnostic or
exception text is retained. Canonical UUIDs prevent path traversal; owned archive
components/files reject symlinks and corruption.

Reserve a monotonically increasing capture sequence under the root writer lock
using immutable `bronze/sequences/<ordinal>.json` entries. Publish capture folders
atomically after all files/directories are flushed. A failure may leave a reserved
sequence gap, never a partially published capture or reused ordinal. The sequence
supports later deterministic equal-time replay; it does not create Silver facts.
Bound the reservation inventory at 100,000 entries and fail explicitly at that
operating limit. Keep private directories/files at creation and verify originals,
receipt identities and sequence lineage at every read.

A supplied capture UUID is the idempotency key: an exact retry reuses its original
sequence and archive clock; conflicting bytes, request/run/profile or explicit
clock evidence refuses overwrite. Identical bytes under a new UUID remain a new
capture. Same input is not a permanent partition-skip policy.

`archived_at` is local archive observation time under the writer lock before
atomic publication (injectable for tests), not a claim of exact fsync completion.
`retrieved_at` is nullable explicit retrieval-completion evidence, never copied
from T02's ambiguous request-start `captured_at`. Keep provider publication time
null and assign no Silver first-seen clock in T05. An explicitly provided retrieval
completion cannot follow archive time. Omitted retry clocks preserve existing
capture clocks rather than stamping a new time on an old capture.

## Parse outcomes and recovery

`archive_input` writes the capture before `parse_capture` invokes the existing
pure parser. A parser rejection leaves the original intact and appends a sanitized
rejected outcome; unexpected execution failure leaves the original available for
retry without inventing a validation result. Outcomes are immutable and keyed by
capture UUID plus a versioned parser/settings profile under `bronze/outcomes/`.
An exact retry must agree with existing outcome bytes. Never persist raw exception
messages, accepted normalized rows, a current pointer or publication/coverage state.
A successful page's completeness flag describes that page only.

Byte budgets default to 20 MiB and parser rows to 5,000; require explicit positive
integer bounds. Reject an oversized input before writing, without truncating it.
Read receipts under a finite metadata bound and payloads under an explicit byte
bound. Staged initialization/capture/outcome failures must not expose partial
final files; tests cover refusal, retry, sequence gaps and concurrent writers.

## Acceptance evidence

[T05 acceptance](t05_acceptance.md) and [sanitized conformance](evidence/t05_bronze_conformance.json)
record the passing implementation. Use invented valid/fractional, malformed and hostile metadata/body fixtures.
Prove exact readback, Decimal parser compatibility, rejected-byte retention,
credential exclusion/redaction, admission-before-I/O, immutable conflicts, original
clock/sequence reuse, corruption refusal, interruption recovery and offline-only
behavior. Run the focused suite first, full regressions, lint/format and wheel/
sdist checks. Record sanitized evidence and update Spec012, its program tracker
and master pickup. Retained real samples and operational stores are not test
fixtures or permission evidence. Next remains **SSV-T06**, one local SQLite slice.

Official references rechecked 2026-11-13:
[API docs](https://developer.finra.org/docs#response-headers),
[Reg SHO dataset](https://developer.finra.org/docs#reg-sho-daily-short-sale-volume),
[API terms](https://developer.finra.org/finra-api-terms-service) and
[equity terms](https://developer.finra.org/specific-terms-equity-data).
The existing user-attested admission is retained; this code adds no new use grant.
