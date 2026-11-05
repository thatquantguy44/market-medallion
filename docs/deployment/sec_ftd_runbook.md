# SEC FTD local ingestion runbook

T04 implements authorized local ZIP capture, exact typed facts, audit, coverage
and database Gold. T05 adds revisions/cutoffs/withdrawals, T06 replay, T07
coherent Parquet and T19 the SEC PostgreSQL adapter. Bounded exact-URL
acquisition, no-request health, a native QuantMeridian export and stopped
Dagster definitions are also implemented. SQLite and PostgreSQL are explicit
selections with no fallback. T21 Delta publication is implemented and locally
verified; this is a manual research slice;
live acquisition and its schedule are disabled by default. NAAIM remains paused.

## Configuration and first local run

Read the [pipeline-specific config guide](../../config/sec_fails_to_deliver/README.md).
Copy its example to ignored `config/sec_fails_to_deliver/pipeline.local.yml`.
Choose `warehouse: sqlite` or `postgres`, a new empty pipeline-owned `root`, and
keep Databricks disabled. PostgreSQL also requires an existing writable schema
and the password environment variable named in YAML. No password is needed for
SQLite; neither backend silently falls back to the other.

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
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver publish \
  --config config/sec_fails_to_deliver/pipeline.local.yml
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver health \
  --config config/sec_fails_to_deliver/pipeline.local.yml
```

These are implemented commands. The partition names select configured local
input/manifest pairs. `gold` requires an explicit row limit1–1000 and refuses
to create a nonexistent warehouse. Decimal JSON values are strings, preserving
precision; exact source tokens are separate fields. `validate-config` reports
whether the selected backend is implemented, not whether a server is reachable.

For an admitted exact SEC ZIP URL, set `acquisition.enabled: true` in the local
YAML and set `SEC_FTD_USER_AGENT` to an operator identity with contact. Then use
`acquire-run --partition ...`. Startup chooses direct/proxy once; the response
is byte-bounded and staged atomically before the standard immutable capture. No
filename/date discovery or bulk crawl occurs.

Publish an independent research exchange for QuantMeridian with
`export-research --destination ...`. Consumers read only its `current.json`;
see [the integration contract](../integration/QuantMeridian.md).

## Pipeline-owned root

```text
<root>/
  .writer.lock
  .warehouse.json
  warehouse.sqlite3   # SQLite only; PostgreSQL state is in the configured schema
  bronze/<run_id>/
    manifest.yml
    storage.json
    coverage-evidence.json # optional reviewed proof, retained exactly
    payload.bin        # absent when the source is unavailable
    receipt.json
    outcome.json       # new T06 terminal admission/fact fingerprint receipt
  replay.json          # completion plus pinned full capture inventory
  parquet/
    current.json       # sole consumer selector
    snapshots/<snapshot_id>/{facts,gold,coverage,audit}.parquet
    snapshots/<snapshot_id>/manifest.json
```

The root marker and DB bind dataset/regime, backend, warehouse ID, storage
contract and record-schema versions; PostgreSQL also binds database/schema.
Reuse only the matching config;
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
facts, coverage and terminal audit commit in one database transaction. Gold is the
`sec_ftd_gold` database view, not a filesystem export. Reopening selects the same
exact observations. An unchanged payload creates a new capture/audit and coverage
observation, with zero new fact versions and unchanged original first-seen.

## Parquet publication and recovery

Every terminal capture stages exact Arrow-schema `facts`, current `gold`, all
coverage versions and audit rows. File checksums and row counts are sealed in
`manifest.json`; the database advances STAGED → COMMITTED → PUBLISHED. Only the
atomic `parquet/current.json` pointer makes a generation visible. Never glob all
snapshot folders.

On the next `run` or explicit `publish`, recovery validates pending checksums,
schemas and counts. A complete generation is finalized and selected; an
incomplete one is ABANDONED without moving the prior pointer. Recovery occurs
before a new capture, and that new capture then publishes its own generation.
This prevents a recovered old generation from becoming a false watermark for
newer committed data.

## PostgreSQL operation

Provision the database, login/optional role and schema outside this pipeline;
do not place passwords in YAML. Load the trusted `.env` into the process, then
use the same `run`, `gold`, `publish` and replay commands. A new root requires
no existing `sec_ftd_*` tables/view in its configured schema. Once bound, the
root marker and database identity must agree. Native DATE, TIMESTAMPTZ and
NUMERIC(precision,scale) preserve the portable contract. A session advisory
lock serializes database writers in addition to the root filesystem lock.

Use a dedicated empty schema for integration tests. The opt-in suite reads
`MARKET_DATA_TEST_POSTGRES_DSN`; it creates and drops only random test schemas.
Do not point it at a schema containing retained state.

## Delta publication

T21 publishes only the committed snapshot selected by `parquet/current.json`.
It validates the manifest, checksums, row counts, explicit Spark types, required
fields and exact numeric representability before any Delta write. Automatic
schema evolution must remain disabled and the Spark session timezone must be
UTC. Facts, coverage and audit use independent insert-only revision-key merges.

Because three Delta commits are not one transaction, the publisher writes a
checksummed generation manifest containing the exact version of each table and
then atomically advances `current.json`. Consumers must use
`read_delta_generation`; they must not independently select each table's latest
version. A failed or interrupted attempt leaves the prior pointer selected and
can be retried without duplicate revisions.

The target is permanently bound to the first source warehouse before its first
table write. A different warehouse with the same storage schema is rejected,
as is any attempt to publish a lower capture sequence. This prevents independent
pipeline roots from contaminating one Delta target.

The checked-in integration proof uses OpenJDK17, Spark4.0.1 and Delta Lake4.0.1
with a pipeline-owned local path target:

```python
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.delta import (
    DeltaPathTarget,
    publish_delta_snapshot,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import current_snapshot

config_path = Path("config/sec_fails_to_deliver/pipeline.local.yml")
config = load_config(config_path)
target = DeltaPathTarget(
    Path("data/sec_fails_to_deliver/delta"),
    config.databricks.table_prefix,
    config.storage.contract_id,
).bind()
publish_delta_snapshot(spark, config_path, current_snapshot(config.root), target)
```

For Databricks, copy the complete SEC root containing the selected pointer and
snapshot under the configured Unity Catalog volume, set that staged root in a
Databricks-specific config copy, enable its catalog/schema/staging volume, install
the repository wheel, and run
[`publish_sec_ftd_delta.py`](../../notebooks/publish_sec_ftd_delta.py). The
publisher creates source-owned `{prefix}_facts_versions`,
`{prefix}_coverage_versions` and `{prefix}_pipeline_runs` tables. The generation
metadata lives under `{staging_volume}/delta/{prefix}`. No workspace, job,
credentials or schedule is provisioned by this repository, and no Databricks
workspace smoke has been claimed.

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
| COVERAGE_EVIDENCE_INVALID | Reviewed proof does not match the captured archive/date inventory; no fact promotion. |
| OVERLAPPING_PARTITION | An accepted partition's date scope would overlap; the new capture cannot silently replace or combine it. |

Rejected/failed captures never erase prior Gold. A failed latest capture is
visible in coverage/audit alongside older retained facts. Success does not
certify absence/zero values, fail age, new daily failed shares or historical
point-in-time availability. Gold is labeled CURRENT_VINTAGE by default and
NOT_CERTIFIED for overall population coverage. Historical T04 captures rejected
with REVISION_REQUIRES_T05 remain rejected historical outcomes.

## Observed revisions and knowledge cutoffs

Changes are compared with the latest version of each settlement-date/CUSIP key.
A correction, reversion or reappearance creates a new version; an unchanged
repeat does not. First-seen is the actual capture completion clock. Missing
rows in PARTIAL captures leave earlier facts selected.

```bash
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver gold \
  --config config/sec_fails_to_deliver/pipeline.local.yml --limit 20 \
  --as-of 2026-11-03T09:00:00Z --as-of-sequence 1
```

`--as-of` requires a timezone-aware timestamp and is inclusive. Optional
`--as-of-sequence` refines only captures at that exact timestamp; earlier
timestamps remain included. Facts and coverage use the same cutoff and report
OBSERVED_AS_OF. Dates before capture do not reconstruct historical availability.
`--include-withdrawn` exposes explicit withdrawal versions; normal Gold ranks
versions before excluding withdrawals so older values cannot reappear.

## Optional reviewed completeness evidence

Each configured partition optionally accepts `coverage_evidence`, a JSON path
relative to its YAML file; omit it or use null for PARTIAL coverage. The bounded
64 KiB document has exactly these fields:

```json
{
  "schema_version": "sec-ftd-completeness/1",
  "source": "sec",
  "dataset": "sec-fails-to-deliver",
  "reporting_regime": "semi-monthly-2009+",
  "period_start": "2026-09-01",
  "period_end": "2026-09-14",
  "member_name": "cnsfails202609a.txt",
  "raw_sha256": "<exact lowercase 64-character ZIP SHA-256>",
  "expected_settlement_dates": ["<each reviewed ISO settlement date>"],
  "source_reference": "<reviewed complete-file evidence>",
  "calendar_reference": "<reviewed expected-date inventory evidence>",
  "review_reference": "<operator review decision>"
}
```

Copy source/dataset/regime and scope values exactly from the selected manifest;
the placeholders above are not runnable evidence. Dates must be sorted, unique
and match the archive exactly. References record operator-reviewed assertions;
the pipeline does not fetch or authenticate them. Successful validation permits
COMPLETE coverage for that file scope and explicit WITHDRAWN versions for
previously reported keys absent from the replacement. Withdrawals carry null
observation values, never invented zero balances. Facts, coverage and audit
commit together. Complete-empty certification remains unsupported.

Receipt version2 retains the exact proof bytes and digest; version1 receipts
remain readable without evidence. Real September captures have no reviewed
completeness proof and remain PARTIAL. Overall source coverage remains
NOT_CERTIFIED even when one file scope is COMPLETE.

Clock regression fails allocation before a new capture. A process interruption
can leave a STARTED audit and partial/unparsed Bronze directory; this never means
successful fact promotion. Preserve those artifacts for investigation. Automated
resume and broader recovery are later tasks. A missing DB/marker is an
integrity failure, not permission to recreate an empty store. Preserve the root
and use the separate-empty-destination replay below; never recreate the source.

## Deterministic offline replay

Copy the pipeline example to an ignored destination config, explicitly selecting
`warehouse: sqlite`, the original storage profile, disabled Databricks and a new
empty root. Use that destination configuration, not the source config unchanged:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver replay \
  --source-root data/sec_fails_to_deliver/t04-verification \
  --config config/sec_fails_to_deliver/replay.local.yml --max-captures 1000
```

`max-captures` defaults to1000 and accepts1–10000. Replay never opens configured
live input/manifest/evidence paths, resolves proxy/DB secrets or invokes HTTP.
It uses archived receipt1/2, exact manifest/storage/payload/proof bytes and
supported parser/record versions, ordered by original capture sequence. It
retains original IDs and knowledge clocks, not replay time as first-seen.

New runs seal `outcome.json` after transactional promotion. This checksummed
receipt binds the original capture receipt, audit/coverage and inserted-version
count/hash. Older captures without it require the intact source SQLite ledger;
replay reads it under a shared lock/read-only snapshot and writes reconciled
outcomes only into the new root. A completed replay root also has a checksummed
ordered inventory in `replay.json` that pins every capture/outcome and the total
version count. With that matching full inventory and sealed outcomes, the source
database is unnecessary. Missing inventory or any missing capture blocks replay.
After appending new captures, use the intact ledger to produce a fresh portable
root; an older replay inventory cannot certify the expanded history.
Hashes detect corruption, not authenticate
provider or operator assertions. Existing outcomes must match an available ledger.

Only originally accepted captures are reparsed. Rejected/failed captures keep
their original outcomes, including T04's REVISION_REQUIRES_T05. A missing,
unsupported or STARTED outcome requires investigation; replay never guesses.
An interrupted capture without valid original artifacts is also blocking.
If promotion committed but outcome sealing failed, the original ledger can
reconcile it; preserve that ledger rather than deleting the missing outcome.

Replay verifies each regenerated run's versions and builds a new warehouse ID in
a private sibling staging directory. It renames the completed root into place
only after verification; failures remove only that invocation's staging root.
Nonempty/overlapping destinations are rejected. A concurrent writer populating
the destination prevents the rename. `replay.json` records rebuild completion;
its generated time does not change observation knowledge. CLI exit0 means a
faithful rebuild, even when the reported history includes rejected/failed runs.
Replay publishes a coherent Parquet generation before exposing its destination.
The destination may be SQLite or an empty PostgreSQL target. PostgreSQL replay
failure removes only tables created by that invocation after verifying their
new warehouse identity; a preexisting/mismatched store is never cleaned.

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

T05 verification: **346 passed, 2 skipped**, plus Ruff and package builds. A
fourth real-store capture adds zero versions, leaving 117,025 fact versions and
four audits (three receipt1, one receipt2). Ignored aggregate evidence is
`data/sec_ftd/t05-verification.json`. No real withdrawals were manufactured.

T06 verification: **419 passed, 2 skipped**. The isolated
`data/sec_fails_to_deliver/t06-verification` root reproduces every encoded fact,
coverage and audit row from all four source captures. All four legacy outcomes
are reconciled; first-capture/before-capture/current counts are50,332/0/117,025.
All source files remain byte-for-byte unchanged. The ignored destination config
is `config/sec_fails_to_deliver/t06-verification.local.yml`; aggregate evidence
is `data/sec_ftd/t06-verification.json`. Configured live paths/CA are deliberately
unavailable in that verification to prove the offline boundary.

T07 real-data verification published 117,025 typed fact/Gold rows and four
coverage/audit rows from that isolated T06 root. Quantity is decimal128(38,0),
price decimal128(38,12), and knowledge clocks are UTC timestamps. T19 was tested
against disposable PostgreSQL17.6 for corrections/reversions, equal-clock
cutoffs, evidence-gated withdrawal, exact values, rollback/failed promotion,
concurrency, root identity, replay cleanup/retry and Parquet.
The final full suite with PostgreSQL integrations enabled is **430 passed**;
Ruff, formatting, documentation validation and sdist/wheel builds also pass.

T21 local Delta verification uses the same correction/retry boundary described
above, including exact decimals, a quantity above int64 and a failed post-facts
publication that leaves the prior pinned generation readable. The next storage
delivery is [S11-T22](../../specs/spec011/todo.md#s11-t22--publish-threshold-snapshots-to-databricks-delta);
see the [source checkpoint](../../specs/spec011/source_review.md).
