# SSV-T12 — bounded acquisition acceptance

Implemented and accepted for bounded conformance 2026-11-13 on the existing
`feat/finra-parquet-publication` branch / [PR #25](https://github.com/thatquantguy44/market-medallion/pull/25),
following the user's request to extend the branch. Traces SSV-NFR01 / SSV-AC12
and common C05–C07. [Contract](acquisition_contract.md),
[evidence](evidence/t12_acquisition_conformance.json),
[runbook](../../docs/deployment/ssv_local_runbook.md#t12-bounded-admitted-acquisition).

## Result

`acquisition-plan` validates an explicit date/facility range and backward correction
overlap without credentials, network or store creation. `acquire` requires separate
strict YAML, applicable Public API acquire/transform grants and dual live opt-ins.
Finite day/partition/page/row/request/byte/retry/wait/duration budgets apply before
and during work. Complete partitions are re-fetched under overlap, not skipped.
Pipeline-owned `network` selects one shared run-scoped verified client; no global
opener, route change after selection or authorization redirect is introduced.

Fixed trusted FIP HEAD/POST, authenticated metadata GET and version-pinned data
POST requests remain within the selected provider profile. Tokens stay in memory;
credentials use environment references, with no automatic `.env` loading. Unknown
cookie headers are ignored; duplicate source completeness claims reject. Probe,
auth, metadata, data/retry and per-partition evidence live in a bound journal.
No-response failures, partial reads, rejection and NOT_ATTEMPTED remain explicit.
Raw error/partial bytes are retained when safe; credential-bearing bodies are refused.

Synchronous pages retain original raw payload/receipt/clock/sequence. Full single
responses can promote directly. Larger partitions collect bounded contiguous pages
under the reviewed sort/date/market filters, then require a second agreeing complete
normalized pass. Missing/changed/duplicate evidence or exhausted limits cannot
replace prior membership. Verification pages are explicitly evidence-only, even
if a changed response becomes complete/empty. A separately tagged derived manifest
binds both passes; its hash describes that manifest, not a synthetic provider
response. The existing atomic version/membership transaction promotes it once.
Source rows use global offset plus original page row; provider publication stays
null. Two-pass agreement is observed consistency, not a provider snapshot token.

Provider/SQLite schemas and existing local interfaces remain compatible. T11 replay
preserves page/collection formats and original clocks/IDs/versions/membership/audit;
its input limits must cover the original collection parser profile. T10 publication
remains explicit and reconciles independently. Transport journals remain original
run evidence rather than fabricated replay network attempts.

## Deterministic verification

- **75 new / 626 FINRA tests** cover planning, strict config, HTTP budgets,
  credential-safe representations, journals, auth/metadata, gates and actual CLI
  planning/refusal. Retry-After seconds/date, timeout, redirects/denial, HTML,
  truncation/oversize, duplicate headers and source drift retain honest outcomes.
- Advancing clocks preserve retrieval-before-archive; late responses cannot promote.
  Empty replacement/removal remains distinct from partial/failure. Verified page
  collections, changed/shrunk second passes, missing/repeated/nested/tampered pages,
  original offset lineage and offline replay pass. Existing626-check regression
  includes the accepted pre-T12 parser/storage/publication/replay suites.
- **Full offline suite: 1,614 passed /92 optional skips.** Ruff lint/format and
  wheel/sdist build/readback pass. All 31 FINRA modules, portable script, both config
  examples and conformance evidence are packaged; no `.env`, local YAML, runtime
  database or raw provider capture is included.
- Pinned agent-skills 188 entries verify. All 61 pinned QuantSmith Git guidance
  objects match; its previously recorded external checkout drift remains unchanged.
  No library refresh, dependency change or sibling-repository configuration copy.

## Separate real-source conformance

Existing Public credentials and user-attested personal noncommercial scope were
used for **Chicago TRF,2026-11-09 only** in a fresh ignored root. The final run
used 6 post-probe HTTP attempts (auth, metadata and four data pages),8 charged
request-budget units, and the configured fixed endpoints. Its four original
pages represent two complete two-page passes over **5849 rows**. Equality was
verified before one derived collection promoted 5849 immutable versions/Gold rows.

Nine actual local portable-script commands replayed all five captures into an empty
root, compared exact Gold/audit and independently published both roots. All seven
published tables agree, including 5849 Gold rows. All 40 original source files stay
unchanged. Raw licensed rows remain ignored; tracked evidence contains hashes,
counts, nonsecret IDs, scope and knowledge labels. Original first-seen clocks remain
actual capture/collection time, not the historical trade date.

Preliminary attempts are retained: metadata HEAD404, documentation HEAD403 and
FIP duplicate non-source cookie headers initially stopped before data queries.
The fixed identity probe and cookie whitelist are regression-tested. An initial
5000/5849 partial page did not promote; this finding drove verified collection
support. No failed evidence was relabelled as complete.

This proves one date/facility and the bounded mechanism, not all facilities/history,
release-calendar correctness or G1/G2. No other operational store, Slack destination,
service or schedule was modified. Live defaults remain disabled. NAAIM stays paused.
**Next: SSV-T13 release-aware health and lineage-safe retention dry run**, combining
transport journals, terminal source coverage and published generation state.
