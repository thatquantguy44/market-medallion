# OTC-T07 — immutable revisions and membership acceptance

**Verified:** 2026-11-17, America/New_York. Implemented on
`feat/finra-otc-contract-parser` in
`7935bd15d8e83dd06dce4b2e13b08a7c38130708`, following T06
`c9263d5eddbe556dfaa27b7840edd524e5d569ad`.
The user authorized this bounded acceptance closeout after implementation.

This closes the **invented-fixture portion of OTC-REQ07 / OTC-AC07** for all
six ATS/non-ATS × T1/T2/OTCE populations. [Evidence](evidence/t07_closeout.json)
records the actual results. No provider schema conformance, real-data history,
publication vintage or final source-delivery acceptance is established.

## Delivered behavior and evidence

| Behavior | Deterministic evidence |
| --- | --- |
| Identical semantic values in new captures keep the original value occurrence and first-seen. | `tests/test_otc_versions.py`: all-six repeat cases; row-order, CRLF and count-token formatting changes create no spurious values. |
| Corrections, reversions and reappearances append distinct immutable occurrences. | All-six correction/reversion/removal/empty/revival cases; content hash can repeat while occurrence ID changes. |
| Missing rows become REMOVED only after complete replacement. | All-six partial refusal, complete removal and explicit-empty cases; tombstones have null content and never invent zero volume. |
| Source evidence remains exact and scoped. | Supplied-ID renames retain keys and revise labels; name/group renames create separate membership without fabricated firm continuity; quantity tokens and leading-zero IDs remain strings. |
| Failed replacements retain the prior coherent population. | Thirty profile/boundary combinations inject failures at versions, members, snapshot, terminal status and commit; no history or selection escapes rollback. |
| Capture cutoffs retain durable ordering. | All-six equal-clock cases use original capture sequence; date-only/naive/invalid cutoffs reject; older complete captures cannot replace newer membership. |
| Historical readback fails closed on corruption. | `tests/test_otc_history_integrity.py`: missing removals, nonpublished origins, clock/index mismatch, invalid content/state, broken revision chains and changed legacy tables. |
| Legacy evidence survives an explicit atomic migration. | `tests/test_otc_migration.py`: unchanged v1 inspection, v1 write refusal, successful migration, three rollback boundaries, corrupt-source/budget refusal and admission before filesystem access. |

`data/versions.py` separates semantic content from run/capture/transport lineage.
Original count tokens belong to each membership observation. They can change
without making a new normalized value. Changes to source labels, identifiers,
update dates or schema context remain semantic changes.

Warehouse contract `finra-otc-sqlite/2` and transform
`finra-otc-membership/2` are independent of the existing catalog/schema versions.
New `otc2_attempts`, `otc2_versions`, `otc2_snapshots`, `otc2_members` and
`otc2_migrations` tables preserve the original v1 tables. A durable PENDING
attempt precedes one transaction for occurrences, membership, coverage and terminal
success. Terminal failed attempts remain immutable; retry needs a new capture.

A new complete capture can select existing value occurrences while recording its
own complete membership and exact raw tokens. First-seen is the value occurrence's
actual capture clock; membership has its own capture/sequence/receipt lineage.
Reversions and revival after removal have new first-seen clocks. Provider update
and reporting dates stay dates; provider publication timestamps remain null.

## Migration and bounded readers

Read-only v1 inspection remains available and does not upgrade a store. Writing
requires explicit `migrate-warehouse --invented-fixture`. Migration verifies old
selected rows and context against retained Bronze, then seeds only the selected
v1 publications. It preserves all raw files, deferred/rejected attempts and legacy
table contents, with logical hashes checked on later v2 opens. It does not invent
intermediate history or automatically process deferred captures. An explicit
`ingest-capture` can create a new v2 result for such a capture while retaining the
old v1 terminal attempt.

`inspect` and `history` use an aware `--as-of` timestamp with optional
`--as-of-sequence`. Without sequence, all captures at that clock are included.
These are **observed-capture knowledge cutoffs under the current transform**,
not reconstructed historical provider vintages or a time-travel record of when
an operator later migrated/processed the capture. Late processing behind already
accepted membership is OUT_OF_ORDER; full ordered replay remains T11.

Readers use consistent SQLite transactions, verify immutable evidence and never
repair/migrate stores or contact providers. Limits remain 10,000 selected rows,
312 explicit partitions, 64 MiB read JSON and a 256 MiB database. T07 adds a
100,000 retained/read-version ceiling, with a default 10,000-version read budget,
and a maximum 1,000 legacy-attempt migration budget.

## Verification and native/package equivalence

**424 OTC tests pass**, up from T06's 330. Full offline regression passes:
**2,251 passed / 92 existing optional skips**. The skips require separate
PostgreSQL/Spark integration environments; this fixture-only SQLite slice does
not claim those integrations were exercised. Repository Ruff lint/format and the
canonical Dagster inventory (8 jobs, 8 schedules, 12 sensors) pass.

Source distribution and wheel builds pass. Exact byte/inventory readback matches
all **204 runtime Python modules** across source, sdist and wheel. No runtime
behavior change was required for this closeout.

The reproducible [CLI checker](../../scripts/verify_spec013_t07.py) creates fresh
temporary fixture roots and denies socket connections in every CLI child. For
each of six profiles it executes initial/repeat/correction/reversion/unverified/
removal/empty/revival: **8 captures, 7 complete snapshots, 11 value/removal
occurrences, 3 tombstones and 3 final selected rows**. It checks intermediate
capture cutoffs and exact large integer values. Unverified replacement leaves
selection unchanged. The frozen v1 fixture migrates, explicitly processes its
old deferred capture and preserves every legacy table plus raw hash/mtime.

An isolated wheel interpreter, confirmed to import outside the source checkout,
returns exactly equal JSON for **159** historical/current/history/retry/migration
checks against those same stores. All **270 retained fixture-store files** keep
their hashes and modification times across reads and immutable retries.

Reproduce the regression and CLI checks from the repository root:

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python scripts/verify_dagster_docs.py

t07_tmp="$(mktemp -d)"
.venv/bin/python -m build --no-isolation --outdir "$t07_tmp/dist"
.venv/bin/python -m venv --system-site-packages "$t07_tmp/wheel-venv"
"$t07_tmp/wheel-venv/bin/python" -m pip install --no-deps --no-index "$t07_tmp"/dist/*.whl
.venv/bin/python scripts/verify_spec013_t07.py \
  --wheel-python "$t07_tmp/wheel-venv/bin/python" \
  --output "$t07_tmp/t07_cli.json"
```

The wheel uses dependencies already installed in the repository environment;
`-I` child processes ignore shell Python path settings. No provider request or
private configuration is part of the checker. Operator-supplied manifests and
fixtures remain explicit source-distribution inputs.

The 188-entry agent-skills pin verifies. The configured external QuantSmith
checkout still differs from its revision/tree pin; its normal verifier fails.
All **61 pinned guidance Git objects** were independently hash-verified and the
relevant pinned instructions read without modifying that checkout or repinning.

## Exact pickup and exclusions

**Next: OTC-T08 / OTC-REQ08 / OTC-AC08**, reporting versus availability clocks.
Preserve date/timestamp precision, documentary tier delay and capture first-seen;
characterize holidays, null publication times and date-only updates before changes.
T08 implementation has not started. A later user instruction prioritizes all-pipeline
PostgreSQL consolidation before beginning it; see the current master handoff.
[Task list](todo.md) owns the remaining OTC execution order.
Checkpoint C also requires T09's complete-population features and remains open.

Real source conformance/acquisition stays T12. Venue ratios, Parquet, replay,
health/retention, admitted research export and Dagster are later tasks. No real
observation, permission grant, provider request, service activation, Slack message,
external warehouse change or NAAIM resume is included in T07 closeout.
