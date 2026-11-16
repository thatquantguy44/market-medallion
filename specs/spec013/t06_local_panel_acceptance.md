# OTC-T06 — six-profile local panel acceptance

Implemented offline on `feat/finra-otc-contract-parser`, continuing the user-selected
branch after T05 `6cb0376a964aaff5dc618db28c00644c2c637054`. The parser/archive
increment is committed as `270914d`; resolve the final panel commit with
`git log -1 -- specs/spec013/t06_local_panel_acceptance.md`.

**Verified:** 2026-11-16, America/New_York. This closes the **fixture portion of
OTC-REQ06 / OTC-AC06 and Checkpoint B** for all six selected populations.
Actual source conformance remains OTC-T12; source G1/G2 and historical research
acceptance remain open. No selected profile was excluded to close T06.

## Delivered behavior

| Evidence | Result |
| --- | --- |
| ATS × T1/T2/OTCE | Existing historical-header invented grammar, explicit tier context, exact quantities and supplied MPIDs. |
| Non-ATS × T1/T2/OTCE | Separately named invented grammar with optional supplied MPID/CRD and explicit REPORTED/AGGREGATED/UNKNOWN attribution. Names without IDs and groups stay week scoped; unknown/unkeyable rows reject. |
| Exact canonical synthetic discovery assertion + full parsed file | COMPLETE_FIXTURE, or COMPLETE_EMPTY_FIXTURE when the explicit asserted count is zero. |
| Parse/received-all/header-only without assertion | UNVERIFIED; no membership/publication. |
| Wrong assertion, parser error, missing/truncated evidence | Explicit negative attempt, immutable raw retained, no false zero or replacement. |
| New complete capture for an already published partition | REPLACEMENT_DEFERRED/T07_REQUIRED; previous membership remains selected. |
| Identical capture retry | Same attempt/publication; no duplicate rows or file modification. |

The non-ATS grammar is deliberately synthetic. Its header is the eight ATS
fixture columns plus `firmCRDNumber|attribution`; it is not the different
historical non-ATS header or a current Query API response. See the dated
[source review](source_review.md#t06-fixture-expansion-review--2026-11-16).
Catalog v2 retains the v1 descriptors. Old ATS/T1 outcomes revalidate unchanged;
the old five candidate schemas remain unsupported rather than silently upgraded.
All six actual profiles retain `conformance: NOT_CHECKED`.

`data/panel.py` binds synthetic population evidence and normalizes exact records.
`pipeline.py` admits fixture operations before I/O, captures/validates Bronze,
and passes one explicit population to `io/warehouse.py`. `<root>/otc.sqlite3`
contains source-owned metadata, attempts, rows and selected publications.
The grain is `(week, product, tier, symbol_raw, participant_key)` in an initial
complete fixture population. Quantities are normalized decimal text with the
original tokens retained; above-int64 values and leading-zero IDs survive.
Reporting/update dates remain dates, publication timestamp stays null, and
first-seen is capture time. Per-population attribution counts and documentary
expected delay accompany every selection. No combined denominator is calculated.

## Transaction and recovery evidence

A durable PENDING attempt precedes the publication transaction. Rows, complete
coverage, selected capture and terminal PUBLISHED status commit together under
the existing root writer lock. Injected failures at row persistence, coverage
insertion, terminal status update and DB commit are parameterized across **all
six** profiles: no rows, selection or success escape rollback, and FAILED remains
visible beside retained Bronze. Failures in recording failure may leave PENDING.
An interrupted PENDING attempt can resume through `ingest-capture`; a terminal
FAILED attempt requires a new capture. T06 does not implement replacement history.

A separate reader during the uncommitted write sees only the prior snapshot and
PENDING attempt. Simulated worker termination rolls back publication; resumption
publishes one coherent population. Four concurrent writers produce one initial
publication and three explicit deferred replacements. Publication seals verify
coverage, rows and capture context; corrupted metadata/rows/status fail readback
without repair. Foreign SQLite identity, symlinks and unsafe sidecars are refused.
Read-only inspection creates no files, parses no Bronze and makes no HTTP probe.
It is a bounded panel read, not T13's full raw-capture health inventory.

Resource ceilings are 10,000 panel rows, 64 MiB normalized publication/read JSON,
312 explicit partitions, a 256 MiB SQLite file and five-second DB busy timeout.
Bronze byte/row/part budgets still apply. Source-owned database/archive components
are checked; operator-owned parent paths are trusted. No external DB/file journal
or Parquet pointer is introduced; those boundaries remain T10.

## Verification

**88 added tests / 330 total OTC tests pass.** The additions cover six-profile
parsing/archive compatibility, local persistence, 24 per-profile SQL/commit fault
cases, separate empty evidence, exact integers/tokens, missing populations,
read-only bounds, denial, immutable retry, concurrency, crash/resume, corruption
and the actual module CLI. Tests first failed for missing profile, panel and CLI
behavior. Review reproduced four completeness/coverage/limit gaps plus malformed
attempt-context handling; the fixes have passing regression cases. Two final
retry tests reproduced false success after loss of committed rows/selection;
idempotent successful retries now verify the selected publication before returning.

Full offline suite: **2,157 passed, 92 existing optional skips**. Repository Ruff
lint/format and canonical Dagster parity (8 jobs, 8 schedules, 12 sensors) pass.
The 188-entry agent-skills snapshot and 61 pinned QuantSmith Git objects verify;
known external checkout revision/tree drift is unchanged. No library repin occurs.

A separate native module-CLI smoke publishes all six populations: **18 rows**,
exact `12345678901234567890`, MPID `0007` and CRD `00123`, with actual capture-date
first-seen. All **33 files** retain hashes/mtimes across six immutable ingestion
retries and inspection. A seventh rejected capture leaves all 18 selected rows
unchanged and reports DEGRADED_FIXTURE; 38 files remain retained afterward.
The four actual retained T05 synthetic captures also revalidate their original
PARSED/PARTIAL/REJECTED/PARSED_EMPTY outcomes without changing any of **19 files**.

Source/wheel builds and byte readback pass for all **199 runtime modules**, with
exact wheel module inventory. An isolated wheel reads the same six-population /
18-row panel, retries all six published captures and preserves every one of the
**38 retained file hashes/mtimes**. All **229 local links/anchors** in root
navigation, current handoff, register/roadmap and OTC specs verify. The source-only
manifest and invented fixtures remain inputs supplied by the operator; the wheel's
runtime does not silently discover repository files.

```bash
.venv/bin/python -m pytest -q tests/test_otc_*.py
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python scripts/verify_dagster_docs.py
.venv/bin/python -m build --no-isolation
```

[Runnable local commands](README.md#verification-commands-and-proposed-local-usage)
require `--invented-fixture`, an explicit root and population, and the current
manifest for import. `validate`, `run`, `inspect` and `ingest-capture` are implemented.
Counts are JSON strings. Exit 0 indicates valid/published/complete fixture state;
exit 2 indicates nonpublished/partial/degraded state, and exit 1 a sanitized local
operation error. No real-data config or grant is loaded by these commands.

## Exact next slice

**OTC-T07 / OTC-REQ07 / OTC-AC07:** append immutable normalized observation
versions and complete replacement membership, with repeated/corrected/reverted/
removed fixtures, partial-replacement refusal and durable equal-clock ordering.
Plan an explicit warehouse contract migration and transformation-version boundary;
existing terminal attempts, deferred captures and initial publications are evidence
and must not be overwritten. Exclude run/capture metadata from content identity
without losing lineage. Preserve exact quantity text, attribution scope, original
first-seen and old Bronze descriptors. Real extraction remains T12.

No actual source observations, permission acceptance, HTTP adapter, model/features,
Parquet, replay service, Slack notification, job/schedule activation, external
warehouse or NAAIM resume is included in T06.
