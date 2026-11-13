# SSV-T12 — bounded admitted synchronous acquisition

Selected 2026-11-13 on the existing `feat/finra-parquet-publication` branch.
Traces SSV-NFR01 / SSV-AC12 and common C05–C07. Reuses the accepted API/parser,
Bronze, SQLite, publication and replay contracts. Provider/SQLite schemas remain
compatible; explicit evidence-page and collection formats extend capture provenance.

## Selected first acquisition boundary

An explicit date range and disjoint facility selection expand to calendar-date/
facility requests, including a configured backward correction overlap. Re-fetch
all planned partitions, including previously complete ones. Do not infer holidays
or availability from an empty local store. Validate the expanded window against
the rolling twelve-calendar-month guard using the current New York date.

Synchronous responses cap at 5000 records/3MB. A complete single response may
replace membership directly. Larger partitions use the bounded two-pass collection
below; exhausted/incomplete collections retain evidence without changing prior
membership. Asynchronous jobs remain unsupported. No synthetic provider response
or false completeness claim is made.

Finite limits cover expanded days, partitions, request attempts (including startup
probe allowance, auth, metadata and retries), per-response/total bytes, page rows,
retry count, cumulative sleep and run duration. Conservative request preflight
reserves two possible route probes plus all planned attempts. Request and byte
budgets are charged before/while reading, including auth/metadata/error bodies.
A Retry-After beyond the remaining wait/duration budget stops rather than retrying
early. Requests are serial; no source-wide default or background schedule exists.

## Admission, authentication and HTTP

Use a separate strict acquisition YAML with `live: false` by default, the selected
Public API approved-use profile with acquire/transform operations, pipeline-owned
`network`, credential environment-variable references and finite limits. Both
configuration and CLI live opt-ins are required. Local configuration/commands stay
compatible and do not load credentials or probe. Fixture execution requires an
explicit injected transport; it cannot reach the network.

After gates and bounded plan validation, select/probe a single run-scoped client
through `shared/http.py`, with redirects disabled. The startup HEAD uses the fixed
FIP authentication endpoint, with Basic credentials sent only to that trusted URL.
The initial metadata HEAD returned404 and the documentation HEAD returned403;
those attempts made no data request. The run still authenticates with POST and
verifies metadata with an authenticated GET. Reuse the client for fixed FINRA FIP,
metadata and data URLs. No process-global opener, cross-host authorization redirect,
proxy-credential logging, automatic `.env` loading or paid credential scope.
Credentials/tokens remain in memory and never enter receipts or journals. Obtain
and expire a bounded FIP bearer token; verify production metadata before querying.

Retry only transient connection/timeout failures and429/500/502/503/504 under the
same route and budgets. Do not retry denied/auth/schema/redirect errors as successes.
Record bounded response bytes before parser/warehouse promotion. Preserve partial
read prefixes with explicit incomplete-body evidence; no-response transport errors
have no fabricated raw capture. Reject recognizable credential-bearing bodies
rather than storing secrets; retain only their fixed-code/hash evidence.

## Outcomes and recovery

A root-bound source-owned acquisition journal records each run's plan, immutable
request events and per-partition outcomes. Transport/probe/auth/metadata failures,
budget exhaustion and NOT_ATTEMPTED partitions remain distinct. Only APPLIED
terminal partition results count complete; the command returns incomplete/failure
when any planned partition is incomplete. HTTP error bodies can become REJECTED
Bronze/ingestion evidence; a later valid retry is a separate capture. Actual
retrieval completion is distinct from original trade date; provider publication
stays unknown. Success is not a freshness/calendar certification.

Each existing warehouse mutation retains its original transaction/recovery rules.
A crash can leave a RUNNING acquisition report or archived unprocessed capture;
inspect the journal and resume the original capture before new ingestion if needed.
A new run re-fetches explicit bounds/overlap; no hidden cursor skips corrections.
T10 Parquet publication remains explicit. T13 source-aware health will combine
transport journals with local ingestion coverage; the old coverage interface alone
does not describe a newer failed network refresh.

## Verification and acceptance

Inject transport for auth/metadata, complete/empty/partial data, corrections,
429 Retry-After (seconds/date), timeout/TLS/redirect, denied access, HTML, truncation,
version/schema drift, duplicate headers, oversized/credential-bearing bodies,
budgets, interruption and no-network gates. Verify raw lineage and prior-membership
preservation, offline replay and explicit typed publication after acquisition.
Separate real-source conformance is bounded to one selected facility/date using
already admitted Public access, with exact raw bytes kept ignored and aggregate
hash/count evidence tracked. It does not certify all facilities/history or G1/G2.

Primary references checked before coding:
[FINRA API authentication, headers, limits and Reg SHO dataset](https://developer.finra.org/docs),
[API terms](https://developer.finra.org/finra-api-terms-service) and
[Equity terms](https://developer.finra.org/specific-terms-equity-data).
The existing user-attested noncommercial scope remains unchanged.

## T12b — verified page collection amendment

The first live Chicago query returned 5000 of 5849 rows. Complete the synchronous
path with bounded pagination rather than treating primary TRF partitions as
permanently partial. Add `max_pages` (default 3, maximum 20) and a stable raw-symbol
sort, with exact date/market equality filters. Advance offsets by actual returned
rows, honor effective limits/payload caps, and require an unchanged total,
contiguous offsets, unique symbols and complete coverage within each pass.

For a multi-page partition, fetch a second complete pass under the same query
and require exact normalized row equality (including scale and global source-row
positions) before promotion. This is observed two-pass agreement, not a provider
snapshot token or a guarantee of historical publication time. Any change, missing
page or budget failure leaves prior membership intact. Single-response complete
partitions need no assembly; their atomic HTTP response remains the original path.

Retain every original page as an independent raw capture. Verification/subsequent
pages use an explicit evidence-only Bronze format so even a changed, now-short
verification response cannot promote early. A distinct page-collection capture
contains only a canonical manifest of the two ordered page-ID lists and limits;
it is a derived collection, never a fabricated provider response. Its receipt
binds the first original request, all original page hashes through capture IDs,
and a new collection clock/sequence after all inputs. Its payload hash describes
the manifest. Parsing validates each original page and both passes, then supplies
one complete logical partition to the unchanged version/membership transaction.
Source rows use offset plus original page row; the original pages retain local
row positions. Provider publication remains null. Collection/schema tags preserve
these distinctions through T11 replay. No existing capture or schema is relabelled.

Request preflight includes both passes and every allowed page/retry. Configurable
partition row capacity is `page_rows * max_pages`; count/version, byte, request,
wait and duration limits remain enforced. Full partition replay requires destination
row/byte limits covering its original collection parser profile.
