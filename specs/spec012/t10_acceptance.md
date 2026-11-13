# SSV-T10 — typed publication acceptance

Accepted locally 2026-11-13 on `feat/finra-parquet-publication`, created from latest
merged main `55dcd5d` / PR #23, delivered in
[PR #24](https://github.com/thatquantguy44/market-medallion/pull/24).
Traces SSV-REQ10 / SSV-AC10 and common C04–C07.
[Contract](parquet_contract.md), [conformance](evidence/t10_parquet_conformance.json),
[runbook](../../docs/deployment/ssv_local_runbook.md#t10-typed-publication-and-recovery).
This closes T10 for invented local input; G1/G2 and real-source coverage stay open.

## Result and storage boundary

`publish` captures one bounded, verified operational SQLite read transaction for
an explicit date, disjoint facility selection, lookback and optional local cutoff.
It writes Silver history/removals, current per-facility Gold, research Gold,
observed-date coverage, terminal captures, membership and run attempts. Current
partitions must be complete; valid empty is supported. A global STARTED source
attempt must be resumed before publication. No missing denominator is imputed.

The separate publication catalog owns immutable generation facts and the
COMMITTED/PUBLISHED journal. It is the database side of the C07 protocol;
operational ingestion remains independent and its accepted schema is unchanged.
Published readers select only `parquet/current.json`, verify frozen facts/files,
and never treat newer operational Gold as part of an older publication.
`run` does not implicitly publish. The new `publish`, `published` and
`recover-publication` commands reuse existing configuration and portable script.

Schemas have date32 dates, microsecond UTC clocks, int64 counters, booleans,
identifiers and nested component lineage. Exact decimal structs encode sign,
coefficient digits and exponent; quantities wider than decimal256, tiny exponents,
scale and signed zero survive without float conversion or quantization. Shares,
unit-1 ratios and the fixed method participate in schema identity. Canonical
`record_json` retains each full row, and every typed column is compared to its
reconstructed record during readback. Empty tables retain fixed schemas.

## Failure and integrity evidence

The root ingestion lock serializes source writers, publishers, readers and
recovery. Staged files are read back, hashed and fsynced before an atomic catalog
transaction commits frozen rows plus COMMITTED metadata. Directory finalization
and pointer replacement are fsynced before the final PUBLISHED mark.

| Injected boundary | Reader after failure | Recovery |
| --- | --- | --- |
| Completed staging | Prior coherent pointer, or no publication | Discard only owned uncommitted stage; no success mark |
| Inside catalog transaction before commit | Prior pointer | Roll back all facts/journal rows |
| After durable catalog commit | Prior pointer; recovery required | Finish exact staged rows/files |
| After finalization | Prior pointer; recovery required | Verify final files and select new generation |
| After pointer replacement | Verified new COMMITTED generation; no published timestamp | Verify and finish journal |
| Before final journal mark | Same verified COMMITTED generation | Mark only after complete verification |
| After final mark, before response | New PUBLISHED generation | Idempotent verified result |

Tests advance ingestion to a different value after each injected failure and
prove recovery uses the previously frozen value. A commit-success/response-failure
case proves cleanup queries durable state and preserves committed staging.
First-publication failures cannot fabricate a prior generation or success mark.

Corrupt Parquet/manifest/catalog membership, wrong schemas/counts/hashes, missing
or regressed pointers, unsafe symlinks, foreign staging owners, bound catalog loss
and altered DDL refuse. A corrupt pending stage leaves the old coherent reader
available but blocks recovery/new publication. Concurrent unchanged publications
serialize and reuse the current generation. Returning to an older projection
creates a new event; explicit old UUIDs cannot roll back the pointer.

Finite row/byte preflight runs before document loading; logical documents,
Arrow buffers, compressed files and Parquet decompression are also bounded.
Defaults are250000 total rows and128MiB. Documents cap at262144 bytes, publication
history at10000 generations. No automatic retention or backup/restore is added.

## Verification performed

- **79 new / 510 FINRA tests** across typed codecs, source projection, catalog,
  publication/recovery and actual CLI processes. Existing T09 query tests stay green.
- **Full offline suite: 1,498 passed, 92 optional skips.** Optional PostgreSQL/Delta
  checks require their dedicated configured environments; no local service was started.
- Ruff check and format, wheel/sdist build and byte-for-byte source readback pass.
  Distributions include source, local config/script and acceptance evidence;
  no `.env`, local configuration, operational database or runtime data is packaged.
- Eight actual portable-script commands in a new temporary invented-data store
  verify publish, unchanged retry, inspection and pending-pointer recovery. Catalog
  facts equal every decoded file, the pending journal clears, and all ten original
  source files remain byte-identical. Evidence contains no absolute runtime paths.
- Pinned agent-skills verifies188 entries. QuantSmith's existing checkout drift
  remains reported; all61 pinned Git guidance objects were independently checked
  against the manifest without updating either library or external working tree.

Reproduce the regression checks with Python3.12 and pinned dev dependencies:

```bash
.venv/bin/python -m pytest -q tests/test_ssv*
env -u MARKET_DATA_TEST_POSTGRES_DSN -u FOMC_TEST_POSTGRES_CONFIG .venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
python3 scripts/verify_agent_skills.py
```

No provider-data requests, credential loading, source-schema/access change,
operational data modification, Slack message or service/schedule activation.
This is local POSIX publication, not T14 interchange or source-aware freshness.
**Next: SSV-T11 replay**, preserving ordering, original first-seen clocks,
versions/removals and membership into an explicitly empty destination. Acquisition,
release-aware health, retention, research exchange and orchestration remain later.
