# Spec019 — PostgreSQL primary warehouses for every existing pipeline

**Status:** foundation and native SEC/threshold primary migrations complete; FINRA short-volume primary cutover complete; OTC native empty primary accepted; presidential primary migrated; NAAIM remains in progress.
**User decision:** 2026-11-17, America/New_York. Include every existing pipeline;
PostgreSQL is the primary warehouse, not an additional reporting copy.
**Branch:** `feat/postgres-pipeline-consolidation`, based on T07 closeout
`73780722c7aaa133408776bbf5a904913ed5929e`.

## Objective and boundaries

Use the existing local PostgreSQL database `additional_market_data` at
`127.0.0.1:55438` for all eight implemented source packages. Keep source-owned
schemas and effective writer roles, preserving raw local archives, identifiers,
exact values, observed revisions, capture ordering and first-seen clocks.
A PostgreSQL-bound root must fail closed when its database is unavailable;
it must never silently fall back to an old SQLite/file warehouse.

This user priority precedes OTC-T08 and the remaining source-build queue.
NAAIM acquisition remains paused. No provider request, schedule activation,
consumer access expansion or sibling-repository integration is authorized.
Only this repository's retained stores and existing PostgreSQL service are used.

## Capability map and ownership

| Module / pipeline | Current primary | Target schema | Effective writer | Dependency |
| --- | --- | --- | --- | --- |
| `aaii` | PostgreSQL | `aaii` | `aaii_writer` | Existing; preserve reader grants |
| `fomc_meeting_minutes` | PostgreSQL | `fomc_meeting_minutes` | `fomc_writer` | Provision role/config without changing data |
| `sec_fails_to_deliver` | PostgreSQL primary migrated | `sec_ftd` | `sec_ftd_writer` | Roles/config; retained-history parity |
| `reg_sho_thresholds` | PostgreSQL primary migrated | `reg_sho_thresholds` | `reg_sho_writer` | Roles/config; retained-history parity |
| `finra_short_sale_volume` | PostgreSQL primary migrated | `finra_short_sale_volume` | `finra_ssv_writer` | New provider-owned PG adapter |
| `finra_otc_transparency` | PostgreSQL empty operative primary | `finra_otc_transparency` | `finra_otc_writer` | New provider-owned PG adapter; no invented production observations |
| `presidential_research` | PostgreSQL primary migrated | `presidential_research` | `presidential_writer` | New provider-owned PG adapter; preserve publication contracts |
| `naaim_exposure_public_delayed` | JSON files | `naaim_exposure_public_delayed` | `naaim_writer` | New provider-owned PG adapter; offline retained-state migration only |

Build order: backup/access foundation → existing SEC/threshold adapters → FINRA
warehouse/catalog → OTC → presidential → NAAIM → end-to-end cutover verification.
Provider DDL, migrations and semantic code stay in each pipeline package;
`shared/` may contain only provider-independent connection/config helpers.
The common target login remains the existing `market_data`; it is an administrator
login, not a restricted service credential. Explicit SET ROLE applies each writer's
schema boundary during pipeline operations. Existing passwords remain environment-only.

## Requirements and acceptance

| Requirement | Acceptance / evidence |
| --- | --- |
| PG-REQ01: Preserve existing data and access. | PG-AC01: Verified database/role backup; AAII/FOMC counts and existing reader grants unchanged after provisioning. |
| PG-REQ02: Isolate pipeline ownership. | PG-AC02: Eight schemas/writer roles; writer can operate its own tables and cannot read/write other pipeline schemas after SET ROLE. No public grants added. |
| PG-REQ03: Migrate exact retained history. | PG-AC03: Every accepted real source table/ledger reconciles before/after, including counts, exact content hashes and capture/first-seen/sequence. Source stores remain unchanged. |
| PG-REQ04: Make PostgreSQL authoritative. | PG-AC04: Explicit bound-root configuration; reads and new transactions use PG; unavailable/wrong target refuses without SQLite fallback; unchanged local stores are recovery evidence. |
| PG-REQ05: Preserve transactions and publication. | PG-AC05: Injected failures cannot expose partial membership, false success or mixed file/database generations; retries are idempotent. |
| PG-REQ06: Preserve source holds and truth. | PG-AC06: No new source requests, NAAIM resume or automation activation; empty OTC scope stays explicit; current docs/configs identify actual PG destinations. |

## Implementation and verification

Python 3.12, pinned psycopg 3 and PostgreSQL; retain pure Python transforms and
source-owned transaction models. Use parameterized values and psycopg SQL
Identifiers for all configurable SQL names. Do not store passwords/DSNs in
tracked configuration, root bindings, evidence or logs.

Characterize current behavior with offline fixtures first. For each adapter,
exercise actual isolated PostgreSQL schemas for initial writes, identical retries,
revisions, removals, cutoffs, rollback and backend/root identity refusal. Migrate
retained data only after fixture parity passes. Run the full offline suite,
focused PG integration tests, Ruff and package verification at checkpoints.
The [task list](todo.md) owns bounded increments and [plan](plan.md) owns migration
and recovery details. Commands are recorded as each interface is implemented;
planned commands must not be presented as available runtime.

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python scripts/verify_dagster_docs.py
python3 scripts/verify_handoff.py --staged
```
