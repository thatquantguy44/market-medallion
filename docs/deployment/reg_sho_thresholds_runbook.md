# Nasdaq and NYSE Reg SHO threshold-list runbook

The package owns independent Nasdaq and NYSE physical parsers, one normalized
versioned membership store, coverage-qualified observed episodes and a native
research exchange. It does not join symbols to SEC CUSIPs or infer short
interest, fail age, close-out duration, or a zero from a missing list.

## Safe local sequence

1. Copy the [example config](../../config/reg_sho_thresholds/pipeline.example.yml)
   to ignored `pipeline.local.yml` and select an empty durable root.
2. Run `validate-config`, `health`, and `plan`. None makes a provider request.
3. For an approved local file, use `run --partition ...`. For an admitted exact
   official URL, enable acquisition locally, set `REG_SHO_USER_AGENT`, and use
   `acquire-run --partition ...`.
4. Inspect `gold`; then publish a consumer artifact with `export-research`.

Use `replay --source-root <retained-root> --config <empty-destination.yml>` to
rebuild a separate SQLite or configured PostgreSQL root without provider access. Replay preserves original
capture IDs, sequences, knowledge clocks, exact membership/coverage/audit
versions and Bronze bytes, verifies normalized equality, then atomically exposes
the destination with a checksummed inventory.

Original payloads, provider manifest and receipt are immutable under
`<root>/bronze/<run_id>/`. SQLite or PostgreSQL stores membership versions,
coverage and audit under a root/storage identity. PostgreSQL also binds the
configured database/schema and uses a session advisory writer lock. A complete replacement can correct a row or
withdraw an omitted row; the older version remains selectable at its capture
cutoff. The provider's 14-digit creation token is retained as raw text because
the source does not establish its timezone.

## Coverage outcomes

| Outcome | Meaning |
| --- | --- |
| `COMPLETE` | Header, selected date/market, rows, flags and creation trailer validate. |
| `COMPLETE_EMPTY` | The official envelope validates and contains zero membership rows. |
| `UNAVAILABLE` | No local/provider response was captured; prior membership remains selected. |
| `FAILED` | Bytes were captured but the provider-specific envelope was rejected. |

Observed entries/exits require complete expected dates. Missing/failed dates
split certainty and produce gap/censoring flags. The current export calendar is
explicitly labeled `configured-and-observed-list-dates/1`; it is a bounded
research calendar, not a complete exchange holiday service.

## Scheduling boundary

`reg_sho_threshold_list_job` and `reg_sho_threshold_lists_daily_live` are
source-owned Dagster definitions. The schedule is stopped by default. It skips
unless `REG_SHO_SCHEDULED_LIVE_ACCESS=true`, `REG_SHO_CONFIG`, and
`REG_SHO_PARTITION` are supplied. These settings do not resolve Nasdaq/NYSE
retention or redistribution rights; review them before activation. NAAIM remains
paused and is not affected.

## Delta snapshot and generation publication

T22 provides an independent threshold publisher under `pipelines/reg_sho_thresholds/`.
It freezes the complete terminal membership, coverage and audit history, plus
current coverage-qualified episodes, into typed immutable Parquet under
`<root>/delta-staging/<snapshot_id>/`. It reads either the configured SQLite or
PostgreSQL warehouse under the existing source writer lock. No HTTP or Spark is
needed for staging:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.reg_sho_thresholds stage-delta \
  --config config/reg_sho_thresholds/pipeline.local.yml
```

Staging refuses unfinished `STARTED` captures and enforces a per-table row budget
(default 1,000,000). Snapshot IDs derive from the warehouse identity, capture
sequence, calendar and file checksums. Equal state reuses the same snapshot,
independent of database row order. Original UTC microseconds, revision keys,
withdrawals, raw lineage and independent provider/market identity are retained.
`delta-staging/current.json` selects the staged source snapshot; it does not mean
Delta publication succeeded. Configured dates and observed coverage define the
explicit `configured-and-observed-list-dates/1` episode calendar. A calendar
change requires a new terminal capture before publishing at a later sequence.

For a local engine proof, install the existing optional `.[delta]` dependencies
and OpenJDK 17. This creates an isolated path target and makes no provider request:

```python
from pathlib import Path
from market_data_pipeline.pipelines.reg_sho_thresholds.delta import (
    DeltaPathTarget, publish_delta_snapshot, read_delta_generation,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.delta_snapshot import stage_delta_snapshot
from market_data_pipeline.pipelines.reg_sho_thresholds.storage import storage_schemas

config = Path("config/reg_sho_thresholds/pipeline.local.yml")
snapshot = stage_delta_snapshot(config)
target = DeltaPathTarget(
    Path("data/reg_sho_thresholds/delta"), "reg_sho",
    storage_schemas()["membership"].contract_id,
).bind()
# spark must be Delta-enabled, with session timeZone=UTC and schema autoMerge=false.
publish_delta_snapshot(spark, config, snapshot, target)
frames = read_delta_generation(spark, target)
```

Tables are `reg_sho_membership_versions`, `reg_sho_coverage_versions`,
`reg_sho_episodes` and `reg_sho_pipeline_runs`. Membership/coverage/audit keys
and complete normalized values are immutable. Episodes are a complete derived
replacement because corrections and gaps can split or remove an episode.
The generation manifest pins all four exact Delta log versions and row counts,
the source snapshot checksum, warehouse/storage identity and capture sequence.
The target permanently binds to one source warehouse; sequence rollback and
same-sequence changed content fail. Read with `read_delta_generation`, which
verifies `current.json` and its checksummed generation; unpinned table heads can
contain an incomplete attempt. For observed cutoffs, filter membership/coverage
by `first_seen_at` and the optional equal-time `capture_sequence` tie-break,
then rank their native keys by those two fields descending. Apply the
`REPORTED` filter after ranking; withdrawals must not resurrect older rows.
Episodes in each generation describe that snapshot's current state.

Each table write is independently committed. A partial table failure leaves the
previous current generation usable; retry the same immutable source snapshot.
If a generation was sealed before the pointer write failed, retry validates and
reuses its pinned versions without changing the sealed metadata. Retain the
Delta log/data versions referenced by every retained generation. Do not vacuum
or delete them while a reader or recovery path still needs them.

Unity Catalog publication uses the source-named
[`publish_reg_sho_thresholds_delta.py`](../../notebooks/publish_reg_sho_thresholds_delta.py)
notebook and the optional `databricks` config. Keep `enabled: false` until the
owner supplies a catalog, schema and `/Volumes/<catalog>/<schema>/<volume>/...`
staging directory. Copy a verified `delta-staging` tree and its `.warehouse.json`
source identity marker into a source root under that volume, retain their relative
layout, and point a workspace-local config at that root. The publisher reads the
staged files and identity; it does not connect to the original PostgreSQL service.
Install the built wheel, configure the Spark session as above, and pass the
config path and optional snapshot path to the notebook. It writes only the
configured threshold table prefix and generation metadata under
`<staging_volume>/delta/<table_prefix>/`. Source acquisition stays separately gated.
When a managed runtime supplies Delta without Python package metadata, the
manifest records `delta_spark_version: null`; record the cluster's Runtime version
with the workspace smoke evidence. This does not change table schema validation.
Validate the workspace's volume locking/atomic-replace behavior and permissions
before operational use; no Unity Catalog workspace run is claimed by local tests.

Official contracts checked for this implementation:
[Delta merge](https://docs.delta.io/delta-update/),
[version-pinned reads](https://docs.delta.io/delta-batch/), and
[explicit Spark read schemas](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.schema.html).

## Research-delivery acceptance and later gates

T17/G2 is complete for the bounded native-source internal-research delivery;
see [acceptance and source-use attestation](../../specs/spec011/acceptance.md).
The ten authentic October26–30 NASDAQ/NYSE files and their original capture
receipts are retained under ignored `data/spec011-t17/thresholds/`. The local
ZIP bundles unchanged payloads, their hash/capture manifest and the user's
verbal-permission attestation. It contains no runtime config or secrets.
The source population outside those dates/markets is not certified.

NYSE startup uses the canonical published source page rather than the
GET-only Download route for HEAD, and `Accept: */*` for the exact validated GET.
The same run-scoped selected client is reused; receipts record its probe URL.
Startup HTTP rejection still prevents data requests. No global HTTP policy changed.

PostgreSQL17.6 and actual local Spark4.0.1/Delta4.0.1 parity/recovery are verified
in the full 492-test run with no skips. Databricks workspace publication has not
been exercised. Broader source use, recurring operation, public redistribution,
consumer integration and the optional joined projection remain separate scopes.
All acquisition/schedule defaults and the local sample acquisition config remain
disabled/stopped; NAAIM remains paused.
