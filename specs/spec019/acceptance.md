# PostgreSQL primary consolidation acceptance

Accepted locally on 2026-11-18, America/New_York, branch
`feat/postgres-pipeline-consolidation`. All eight implemented pipelines use the
existing `127.0.0.1:55438/additional_market_data` database as their configured
primary warehouse. [The operating register](../../docs/deployment/postgres_warehouses.md)
owns the schema/writer/config/root mapping. No push, PR or merge was performed.

## Retained data and operating boundaries

- AAII/FOMC retain all 34 original table counts. FOMC now uses its dedicated writer;
  existing AAII reader grants remain unchanged.
- SEC FTD: 117,025 facts, four audit/coverage records; 30 source files unchanged.
- Reg SHO: 408 membership records, ten audit/coverage records; 39 files unchanged.
- FINRA short volume: 25,230 versions/memberships, 17 captures/attempts and three
  partitions; original frozen publication and all 115 original files unchanged.
- OTC: native primary initialized with zero observations; invented fixtures exist
  only in temporary test schemas. Real profile conformance remains NOT_CHECKED.
- Presidential: 3,903 facts/memberships, three captures, two publication-journal
  records and frozen publication unchanged; all 65 original files unchanged.
- NAAIM: 131 delayed versions, one capture/event and latest match; all eight
  original files unchanged. Raw capture/snapshot are retained; no missing raw
  reference was invented. Acquisition remains paused.

Original files preserve SHA-256 and modification times. PostgreSQL-bound roots
fail closed without their database; SQLite/JSON recovery stores stay untouched.
Source-owned native adapters own schemas and transactions. Provider-independent
settings, binding and connections remain in shared helpers. Raw files, frozen
publications, Parquet and research exports retain their existing local roles.
NAAIM Gold is derivative; its health/export read primary data. Retention is
transactional and was tested only in fixtures; no operative deletion occurred.

FINRA health remains DEGRADED and NAAIM capture_stale. Migration is not a source
refresh. No provider request, Slack notification, job launch, schedule activation
or NAAIM resume occurred. Current local Dagster UI was unavailable on 2026-11-18;
stored activation states were not inferred from the older checkpoint. No worker
was restarted. Private `.env` now selects the accepted PG configs/roots for future
launches; activation flags are unchanged. Eight jobs/eight schedules/twelve
sensors remain the canonical code inventory in [DAGSTER.md](../../DAGSTER.md).

## Verification

The complete offline suite has **2,263 passes / 206 optional skips**. All
**201 PostgreSQL integration cases** pass in disposable validation storage:
119 consolidation/SEC/threshold cases and 82 existing AAII/FOMC cases. Twelve
additional shared settings/binding checks pass in the 131-case consolidation
suite. Six native CLI cases also pass while importing exclusively from the built
wheel. Ruff lint/format, Dagster docs and focused Dagster checks pass.

Source/sdist/wheel match every byte of all **209 runtime Python modules**;
private configs/environment files are excluded. The final 225,443,684-byte custom
backup was actually restored: all **78 tables** match row-multiset hashes,
counts and owners. All eight writer roles retain isolated own-schema access and
no elevated/login privileges. Temporary validation and final-restore databases
were removed; operative databases, original stores and both backups remain.
Ignored backups: `data/backups/postgres-consolidation-2026-11-17/` and
`data/backups/postgres-consolidation-2026-11-18-final/`. Role dumps omit passwords.

## Evidence and pickup

- [FINRA cutover](evidence/pg_t05_finra.json), [OTC empty primary](evidence/pg_t07_otc.json).
- [Presidential cutover](evidence/pg_t09_presidential.json), [NAAIM cutover](evidence/pg_t10_naaim.json).
- [All operative selectors](evidence/pg_t11_operating.json), [package parity](evidence/pg_t11_package.json),
  [restored backup](evidence/pg_t11_backup.json).
- Native implementation checkpoints: FINRA `c8f7152`, OTC `b1702dc`, presidential
  `11700e7`, NAAIM `55ac940`; foundation `0900c7d`. Resolve the final acceptance
  SHA with `git log -1 -- specs/spec019/acceptance.md`.

`origin/main` was independently checked through `ls-remote` at
`1c256bf75fdf4fe1863346fee1b6f0bde6a8f85a`, already an ancestor of HEAD. A repeated
fetch encountered unrelated invalid local ref `refs/heads/docs/finra-merged-handoff 2`;
no local ref was deleted or repaired. This did not prevent verification/commits.
Spec019 is complete. The next bounded planned implementation is Spec013 OTC-T08
(reporting/availability clocks); it was not started during consolidation. Preserve
all source gates, the NAAIM pause, exact integer tokens and capture-time semantics.
