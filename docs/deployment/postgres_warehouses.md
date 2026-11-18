# PostgreSQL primary warehouse consolidation

**Verified checkpoint:** 2026-11-17, America/New_York.
[Spec019](../../specs/spec019/README.md) owns the user-authorized migration of all
existing pipelines to PostgreSQL primary storage. The target is the existing
local `additional_market_data` database at `127.0.0.1:55438`.

| Pipeline | Schema | Effective writer role | Current primary status |
| --- | --- | --- | --- |
| AAII | `aaii` | `aaii_writer` | Existing PostgreSQL; unchanged. |
| FOMC minutes | `fomc_meeting_minutes` | `fomc_writer` | Existing PostgreSQL; schema/objects and local config now use dedicated writer. |
| SEC FTD | `sec_ftd` | `sec_ftd_writer` | Migrated: 117,025 facts, four coverage/audit records. |
| Reg SHO thresholds | `reg_sho_thresholds` | `reg_sho_writer` | Migrated: 408 membership records, ten coverage/audit records. |
| FINRA short volume | `finra_short_sale_volume` | `finra_ssv_writer` | Namespace provisioned; native primary adapter/migration pending. |
| FINRA OTC | `finra_otc_transparency` | `finra_otc_writer` | Empty namespace provisioned; native adapter pending; no real source observations. |
| Presidential research | `presidential_research` | `presidential_writer` | Namespace provisioned; native adapter/migration pending. |
| NAAIM delayed | `naaim_exposure_public_delayed` | `naaim_writer` | Namespace provisioned; offline migration pending; acquisition remains paused. |

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
