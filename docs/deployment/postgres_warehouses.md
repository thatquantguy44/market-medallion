# PostgreSQL primary warehouse consolidation

**Verified checkpoint:** 2026-11-18, America/New_York.
[Spec019](../../specs/spec019/README.md) owns the user-authorized migration of all
existing pipelines to PostgreSQL primary storage. The target is the existing
local `additional_market_data` database at `127.0.0.1:55438`.

| Pipeline | Schema | Effective writer role | Current primary status |
| --- | --- | --- | --- |
| AAII | `aaii` | `aaii_writer` | Existing PostgreSQL; unchanged. |
| FOMC minutes | `fomc_meeting_minutes` | `fomc_writer` | Existing PostgreSQL; schema/objects and local config now use dedicated writer. |
| SEC FTD | `sec_ftd` | `sec_ftd_writer` | Migrated: 117,025 facts, four coverage/audit records. |
| Reg SHO thresholds | `reg_sho_thresholds` | `reg_sho_writer` | Migrated: 408 membership records, ten coverage/audit records. |
| FINRA short volume | `finra_short_sale_volume` | `finra_ssv_writer` | Migrated: 25,230 versions, three selected TRF partitions and original frozen publication. |
| FINRA OTC | `finra_otc_transparency` | `finra_otc_writer` | Native empty primary initialized; 50 PG checks; no real or invented operative observations. |
| Presidential research | `presidential_research` | `presidential_writer` | Migrated: 3,903 facts and memberships, three captures and two publication-journal records. |
| NAAIM delayed | `naaim_exposure_public_delayed` | `naaim_writer` | Migrated: 131 delayed versions, retained capture/event/latest; acquisition remains paused. |

The shared login is the existing **administrator** `market_data`. It can assume
all writer roles; an operator session using that login is not least-privilege.
Each writer is NOLOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE, NOREPLICATION and
NOBYPASSRLS. Actual SET ROLE checks confirm a writer has USAGE/CREATE only in its
own pipeline schema among these eight namespaces. Existing
`quantmeridian_aaii_reader` grants in `aaii` are unchanged. No broader reader or
PUBLIC grants were added. Passwords remain in this project's private environment.

## Current local configuration

- AAII: ignored `config/config.yaml` → schema `aaii`.
- FOMC: ignored `config/fomc_meeting_minutes/pipeline.local.yml` → dedicated writer.
- SEC FTD: ignored `config/sec_fails_to_deliver/pipeline.local.yml` → new
  `data/postgres-primary/sec_fails_to_deliver` root and schema `sec_ftd`.
- Thresholds: ignored `config/reg_sho_thresholds/pipeline.local.yml` → new
  `data/postgres-primary/reg_sho_thresholds` root and matching schema.
- FINRA short volume: ignored `config/finra_short_sale_volume/pipeline.local.yml`
  selects the accepted T16 source root, now explicitly bound to PostgreSQL.
  `config/finra_short_sale_volume/postgres.local.yml` names its writer/schema and
  password environment reference. The old SQLite files remain recovery evidence.

SEC/threshold local configs explicitly select `warehouse: postgres`, reference the
shared password environment variable and keep acquisition disabled. There is no
SQLite fallback. The retained SQLite roots stay unchanged for recovery. Their
original captures, exact values, audit/coverage and first-seen/sequence match the
new primary stores through the existing source-owned native replay adapters.
Thirty SEC and thirty-nine threshold source files preserve hashes/mtimes.
New replay publication identities are destination-owned and do not replace old
publication evidence. Schedules remain stopped; no provider request was made.

## Backup and provisioning evidence

The ignored backup folder is `data/backups/postgres-consolidation-2026-11-17/`.
It contains a custom-format database dump, roles without password hashes,
checksums, archive inventory, provisioning record and migration parity reports.
The database dump was actually restored into a disposable validation database;
all **34** existing AAII/FOMC table counts match the live database. That disposable
database is reserved for isolated adapter tests and is not an operational target.

[Provisioning utility](../../scripts/provision_postgres_warehouses.py) defaults to
read-only inventory; `--apply` provisions role/schema boundaries transactionally.
It preserves existing table counts and AAII reader ACLs, and checks writer
cross-schema isolation before commit. Existing FOMC table-owned sequences move
with their tables. A failure rolls back the whole change. Run only with the
configured password environment variable already exported and a current backup:

```bash
.venv/bin/python scripts/provision_postgres_warehouses.py --config config/config.yaml
```

The [master handoff](../handoffs/project_bootstrap.md#agent-start-here) and owning
Spec019 tasks identify the next unfinished adapter. [DAGSTER.md](../../DAGSTER.md)
remains the only job/schedule/sensor inventory; PostgreSQL availability does not
activate automation or resume NAAIM.

## FINRA native primary cutover

[PG-T05 evidence](../../specs/spec019/evidence/pg_t05_finra.json) records exact
current and frozen publication hashes before/after migration. All **115** original
files retain hashes/mtimes; only `.postgres-primary.json` was added. Native tables
preserve 25,230 value versions/membership rows, 17 capture/attempt records, three
partitions and the original frozen publication catalog. Native audit insertion
order is explicit, so retries preserve the original durable sequence.

The CLI migration, new writes/cutoffs, no-SQLite-fallback behavior, native health/
retention and PG-source recovery replay are verified. All 24 native FINRA tests
and 14 shared primary tests pass; 92 existing CLI/replay/operations tests pass.
Health partition results are exactly unchanged and remain **DEGRADED**; this
migration is not a new successful source refresh. Retention remains PRESERVE_ALL
with deletion disabled. No acquisition, Slack or schedule activation occurred.

## OTC native primary

[PG-T07 evidence](../../specs/spec019/evidence/pg_t07_otc.json) verifies the empty
operative root `data/postgres-primary/finra_otc_transparency`. Its ignored
`config/finra_otc_transparency/postgres.local.yml` selects `finra_otc_writer`.
Native migration/empty initialization and six-profile fixture transactions pass
50 actual PG checks. No real profile conformance or source access is implied.

## Presidential native primary

[PG-T09 evidence](../../specs/spec019/evidence/pg_t09_presidential.json) verifies
all 3,903 facts and the unchanged frozen publication/observed-as-of results. All
65 original files preserve hashes/mtimes; the original SQLite is recovery only.
The same `data/presidential_research` root is bound to `presidential_writer`;
ignored `config/presidential_research/postgres.local.yml` documents its target.
Sixteen actual PG tests verify revisions, transaction/publication failure recovery,
CLI and backup/restore with no SQLite present. No live refresh occurred.

## NAAIM delayed native primary

[PG-T10 evidence](../../specs/spec019/evidence/pg_t10_naaim.json) reconciles all
131 versions, exact ordered/current values, capture-time first-seen and delayed
latest metadata. One retained raw capture/snapshot and event are present; no
missing raw reference is invented. Eight original files preserve hashes/mtimes.
The same `data/naaim-exposure-public-delayed` root is bound to `naaim_writer`;
ignored `config/naaim_exposure_public_delayed/postgres.local.yml` documents the
target/environment reference. All 22 native tests and 18 existing NAAIM tests
pass. Health remains **capture_stale**; the user pause remains. Migration performs
no retention deletion, provider acquisition or schedule activation.
