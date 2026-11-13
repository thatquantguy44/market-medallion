# SSV-T11 — offline replay acceptance

Accepted locally 2026-11-13 on `feat/finra-parquet-publication`, added to
[PR #25](https://github.com/thatquantguy44/market-medallion/pull/25) at the user's
request to extend the same branch. T10 PR #24 merged while T11 was in progress;
merged main `ac4d520` was integrated without changing the validated tree. Traces SSV-REQ11 / SSV-AC11 and common C05–C07.
[Contract](replay_contract.md), [conformance](evidence/t11_replay_conformance.json),
[runbook](../../docs/deployment/ssv_local_runbook.md#t11-offline-replay).
T11 is complete for invented local input; G1/G2 and real complete-source coverage remain open.

## Result and evidence boundary

`replay --source-root <original> --config <destination-yaml>` rebuilds a separate
absent/empty destination from original Bronze plus a verified terminal operational
ledger. This is not a database-free backup. It checks admission before I/O and
requires supported manifest, parser and transform identities. Unresolved STARTED,
failed-only/unprocessed captures, missing outcomes, corrupt lineage and clock
regression refuse before destination creation. Historical failures/interrupted
attempts followed by a terminal retry retain their exact audit interpretation.

Source archive and ingestion locks pin one inventory/ledger. Original payloads,
receipts, parse outcomes and sequence reservations are copied exactly; capture
allocation and wall-time re-archiving are bypassed. Every original parser profile
is reparsed, and normal ingestion recomputes versions/removals/membership/pointers.
Terminal results, including original completion clocks, must match exactly.
Original validated attempt IDs, order, profiles, errors and clocks are retained.
All six logical operational projections, including current Gold, compare exactly.

First-seen clocks, original capture/run/version UUIDs, equal-time sequence cutoffs,
retrieval/publication nulls and numeric precision survive. Original unused sequence
slots remain allocator-only evidence, so later ingestion cannot reuse them. Fresh
warehouse identity and separate replay completion metadata do not replace source
knowledge. T10 publication remains explicit; replay does not copy old catalog,
files or event IDs, and new published table values reconcile independently.

## Atomic destination and limits

A private sibling rebuild must pass complete parity before its fresh identity is
sealed to the final destination path and the directory is atomically installed.
This sealing applies only to newly reconstructed private state; no existing store
can be moved through the public API. Populated/overlapping/direct-symlink roots
refuse, including concurrent destination content created just before installation.
Failures during copying, ingestion, verification, sealing or pre-installation
remove only owned staging and retain source/destination contents. An installed
response failure leaves a complete report/store that ordinary readers can verify;
retrying into that populated target refuses rather than overwriting it.

Default limits are 1000 captures, 250000 state rows and 128MiB combined archive/state
bytes. Each input/profile is also bounded by destination YAML limits. Capture count
and SQL row/byte estimates are checked before state materialization; all archive
file sizes precede payload loading. Up to 16 profiles per capture and 100000 source
sequence reservations are supported. Explicit overrides remain finite. No automatic
resume/adoption of hard-crash private staging is implemented.

## Checks performed

- **41 new / 551 FINRA tests.** Corrections/reversions/removals, unchanged/equal-time
  repeats, valid empty, partial/rejected input, original cutoffs, failed retries,
  fractional and 4096-digit quantities, multiple dates/facilities, research and new
  Parquet-table parity pass. Allocator-only slots, extra profiles and post-replay
  ingestion retain original lineage semantics.
- Failure cleanup and installed-response failure, concurrent target population,
  populated/overlapping/symlink roots, fixture/provider relabel refusal, corrupt or
  self-hashed malformed provenance, clock regression and early budgets pass.
- **Full offline suite:1,539 passed, 92 optional skips.** Ruff check/format,
  wheel/sdist build and byte-for-byte source readback pass; all 25 FINRA modules,
  portable script/config and evidence are packaged without local runtime files.
- **24 actual portable-script commands / eight invented captures** reconstruct
  prior/current, repeat, correction, reversion, empty/removal, partial and rejected
  cases. Original audit/Gold/coverage/research, a correction cutoff and new published
  tables match. A populated retry returns exit2. All 38 original source files stay
  unchanged; source and destination warehouse UUIDs differ. The retained report
  binds capture inventory and exact logical-state hashes without absolute paths.
- agent-skills verifies 188 entries. All 61 pinned QuantSmith Git guidance hashes
  verify; its previously recorded checkout drift remains unchanged. No refresh,
  external working-tree edit or dependency change was made.

Reproduce with Python3.12 and pinned development dependencies:

```bash
.venv/bin/python -m pytest -q tests/test_ssv*
env -u MARKET_DATA_TEST_POSTGRES_DSN -u FOMC_TEST_POSTGRES_CONFIG .venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
python3 scripts/verify_agent_skills.py
```

No provider-data request, credential loading, operational-store modification,
Slack message or service/schedule activation occurred. **Next: SSV-T12 bounded
admitted acquisition**, with explicit complete-partition design, finite budgets,
correction overlap and source-owned retry/HTTP settings. Source-aware health,
research interchange, retention and stopped orchestration remain later tasks.
