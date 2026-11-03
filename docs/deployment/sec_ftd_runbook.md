# SEC FTD local ingestion runbook

T04 implements authorized local ZIP capture, exact typed SQLite facts, audit,
coverage and database-backed Gold. This is a manual research slice. PostgreSQL
remains the selected operational default, but its adapter is T19; Delta is T21.
Neither is silently substituted or activated. No SEC HTTP adapter, job, schedule,
Parquet publisher, revision replacement or offline replay is implemented here.
NAAIM remains paused.

## Configuration and first local run

Read the [pipeline-specific config guide](../../config/sec_fails_to_deliver/README.md).
Copy its example to ignored `config/sec_fails_to_deliver/pipeline.local.yml`.
For this delivered SQLite slice, explicitly set `warehouse: sqlite` and choose
a new empty pipeline-owned `root`. Keep Databricks disabled. The checked-in
example intentionally stays `warehouse: postgres` and returns a T19 error for
`run` until that adapter exists. No password or `.env` is needed for SQLite.

```bash
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver validate-config \
  --config config/sec_fails_to_deliver/pipeline.local.yml
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver run \
  --config config/sec_fails_to_deliver/pipeline.local.yml \
  --partition september_2026_a
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver run \
  --config config/sec_fails_to_deliver/pipeline.local.yml \
  --partition september_2026_b
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver gold \
  --config config/sec_fails_to_deliver/pipeline.local.yml --limit 20
```

These are implemented commands. The partition names select configured local
input/manifest pairs. `gold` requires an explicit row limit1–1000 and refuses
to create a nonexistent warehouse. Decimal JSON values are strings, preserving
precision; exact source tokens are separate fields. `validate-config` reports
whether the selected backend is implemented, not whether a server is reachable.

## Pipeline-owned root

```text
<root>/
  .writer.lock
  .warehouse.json
  warehouse.sqlite3
  bronze/<run_id>/
    manifest.yml
    storage.json
    payload.bin        # absent when the source is unavailable
    receipt.json
```

The root marker and DB bind the dataset/regime, SQLite backend, warehouse ID,
storage contract ID and record-schema versions. Reuse only the matching config;
do not change numeric types/backend on a bound root or move a different DB under
its marker. Unbound nonempty roots, missing artifacts, symlinked DB/marker and
identity disagreements fail before input capture. No existing store is reset.

Bronze files are created exclusively and never overwritten by an import.
Original manifest/storage settings and bounded input are synced before the
immutable receipt. Hashes verify every artifact before fact promotion. Receipt
acquisition start and completion clocks are separate; first-seen uses acquisition
completion, never the historical settlement date. Original provider publication
and original download time remain unknown. All persisted knowledge clocks use UTC.

Each run reserves a sequence and STARTED audit under the root lock. Accepted
facts, coverage and terminal audit commit in one SQLite transaction. Gold is the
`sec_ftd_gold` database view, not a filesystem export. Reopening selects the same
exact observations. An unchanged payload creates a new capture/audit and coverage
observation, with zero new fact versions and unchanged original first-seen.

## Outcomes and limits

| Outcome | Persisted meaning |
| --- | --- |
| SUCCEEDED with PARTIAL coverage | Every selected row passed file/type validation; expected date/universe coverage remains uncertified. All admitted rows are available in Gold. |
| SOURCE_UNAVAILABLE | FAILED audit, UNAVAILABLE coverage, no payload and no invented original hash. |
| INVALID_ARCHIVE | REJECTED audit and FAILED coverage; original bounded challenge/corrupt/unsafe ZIP bytes remain in Bronze. |
| EMPTY_PARTITION_UNSUPPORTED | Zero-row/header-only input is retained and rejected, not certified as a complete zero-fails population. |
| INPUT_TOO_LARGE | At most archive-limit+1 bytes are retained, explicitly incomplete; captured-prefix hash is distinct from unknown original hash. |
| STORAGE_VALIDATION_FAILED | Exact numeric/type constraints failed; no fact promotion. |
| WAREHOUSE_WRITE_FAILED | Transaction rolled back; failed audit/coverage are recorded when the warehouse is available. |
| REVISION_REQUIRES_T05 | Changed replacement retained and audited; previous facts remain current until revision handling is delivered. |
| OVERLAPPING_PARTITION | An accepted partition's date scope would overlap; the new capture cannot silently replace or combine it. |

Rejected/failed captures never erase prior Gold. A failed latest capture is
visible in coverage/audit alongside older retained facts. Success does not
certify absence/zero values, fail age, new daily failed shares or historical
point-in-time availability. Gold is labeled CURRENT_VINTAGE and NOT_CERTIFIED
for population coverage; observed-as-of queries are not delivered until T05.

Clock regression fails allocation before a new capture. A process interruption
can leave a STARTED audit and partial/unparsed Bronze directory; this never means
successful fact promotion. Preserve those artifacts for investigation. Automated
resume/replay and broader recovery are later tasks. A missing DB/marker is an
integrity failure, not permission to recreate an empty store. Preserve the root
and use a separate empty destination for future validated replay/restore work.

## Verified September checkpoint

The ignored `config/sec_fails_to_deliver/t04-verification.local.yml` explicitly
selects SQLite and `data/sec_fails_to_deliver/t04-verification`. Two approved
September archives imported **50,332 + 66,693 = 117,025** unique facts, spanning
21 observed settlement labels from September1–30, 2026. Both source trailer sums
reconcile; 663 prices are null with their source reason. A third unchanged capture
inserted zero facts. All three immutable captures verify after reopening.

First-seen is November3, 2026 UTC, not September. Source population coverage
remains PARTIAL/NOT_CERTIFIED. Aggregate evidence is ignored
`data/sec_ftd/t04-verification.json`; no actual security rows or runtime config
are committed. Full offline evidence is **316 passed, 2 existing opt-in
PostgreSQL skips**, plus Ruff, changed-file formatting and package build.

Next: [S11-T05](../../specs/spec011/todo.md#s11-t05--preserve-ftd-revisions-and-withdrawals)
observed corrections/reversions and coverage-qualified withdrawals. PostgreSQL
and Delta engine parity, SEC acquisition and research exports remain separate
unchecked deliveries; see the [source checkpoint](../../specs/spec011/source_review.md).
