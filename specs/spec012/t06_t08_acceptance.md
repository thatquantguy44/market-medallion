# SSV-T06–T08 acceptance — audited local partitions and observed membership

Accepted 2026-11-13 on `feat/finra-local-revisions`, from merged T05 PR #20
(main `9f211ce`), delivered in [PR #23](https://github.com/thatquantguy44/market-medallion/pull/23).
Current main `6790882` was integrated, preserving the presidential-research release. Traces **SSV-REQ06–08 / SSV-AC06–08**, with C04–C07 and
unchanged T01–T05 admission, schema, parser, numeric and Bronze behavior.
[Storage contract](local_storage_contract.md), [tasks](todo.md),
[sanitized conformance](evidence/t06_t08_local_conformance.json),
[local runbook](../../docs/deployment/ssv_local_runbook.md) and
[per-pipeline config](../../config/finra_short_sale_volume/README.md).

## Delivered behavior

T06 imports one complete invented date/facility response through immutable Bronze
into audited SQLite Silver/Gold. Source quantities remain exact decimal text in
canonical version documents with explicit natural-key/time columns. Captures,
version links, complete membership and the partition pointer commit together.
Durable attempts retain parser/transform settings before parsing; failure leaves
prior committed membership intact. Strict fixture-default YAML, source-owned CLI
and `scripts/run_finra_short_sale_volume.sh` provide a runnable local loop.

T07 appends corrections and reversions without duplicating unchanged numeric
values. Hashing ignores numeric display scale and signed zero without ambient
Decimal rounding; original version text and all raw capture representations
remain intact. As-of membership follows original local archive clock plus capture
sequence. Equal-time cutoffs are explicit; clock/order regression refuses.
Terminal retries verify profile/evidence and do not duplicate facts or attempts.

T08 appends absence versions only from complete authoritative replacements.
Partial/rejected/failed refreshes cannot replace values or membership. Valid empty
replacements retract prior members; repeat empties do not duplicate removals;
returning rows get new versions. Local missing/pending/failed/partial/complete/
valid-empty states are distinct, and symbol absence is unknown without complete
evidence. These are local evidence states, not provider calendar/release claims.

Identity binds root, schema, manifest, transform and fixture/provider kind.
Readers verify actual SQLite schema, foreign keys, terminal/version/membership
hashes and scope. Missing/damaged bound stores and symlinked components refuse
recreation. This is local POSIX/SQLite operation, not tamper-proof storage or an
off-machine recovery service. `completed_at` is a local precommit observation
after transformation; it does not claim exact fsync completion. Unknown provider
clocks stay null; first-seen never borrows trade date or T02 request start.

## Evidence actually produced

- **64 new checks:** 17 pipeline, eight CLI/config, 17 revision/cutoff/context and
  22 replacement/absence/integrity/failure tests. All **379 FINRA tests** pass.
- Full offline suite: **1,367 passed, 92 optional integration skips**. External
  database variables were unset. Existing AAII/NAAIM/FOMC/SEC code and tests were
  unchanged. CI separately checks the repository's isolated PostgreSQL paths.
- Ruff lint/format, wheel/sdist builds and source/config/script readback pass.
  The original seven FINRA core/Bronze modules remain byte-identical to merged T05.
- Independent real CLI/script processes in a temporary root ran **nine invented
  captures**: initial, repeat, correction, reversion, partial, removal, empty,
  return and rejection. Nine audits/captures, **eight versions**, eleven complete
  capture-membership rows, one current partition and two final Gold rows reconcile.
  Cutoff row counts are `[2,2,2,2,2,1,0,2,2]`. SQLite integrity/foreign keys pass;
  exact raw readback, terminal retries and a metadata privacy sentinel reconcile.
- Failure injection covers durable STARTED evidence, version writes, membership,
  precommit and postcommit response failure. An interrupted STARTED attempt
  blocks another capture until resumed. Prior coherent publication or fully
  committed replacement remains selected; no failure silently deletes history.
- Repository agent-skills verifies at its existing pin (188 entries). QuantSmith
  checkout revision/tree drift persists; all **61 pinned guidance objects** were
  checksum-verified and read without modifying/refreshing the external checkout.

Tests first failed against absent local modules and revision helpers. Further
failing cases exposed ambient-context ratio text, membership/scope integrity,
missing-store recreation and absent attempt settings; these are now guarded.
No provider-data requests, credential loading, existing operational-store writes,
Slack messages, new service or schedule activation were involved.

## Next boundary

**SSV-T09:** scope-safe research Gold, prior available trade date and explicit
gap/population rules. Checkpoint B is accepted for invented local input; G1/G2,
real complete-partition/history coverage, cross-page assembly, Parquet, replay,
acquisition, source-aware health, research exchange and jobs remain open. Keep
NAAIM paused and all acquisition schedules stopped. This acceptance does not
claim the whole FINRA pipeline is production-ready.
