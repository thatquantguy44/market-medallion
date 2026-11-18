# Tasks — PostgreSQL primary consolidation

## PG-T01 — Inventory, backup and isolate schemas

- [x] Inventory accepted source roots and exact live schemas/roles/counts.
- [x] Create and verify existing database/role backups without passwords.
- [x] Provision owned schemas/writers and verify isolation; preserve readers.
**Verify:** read-only catalog assertions and backup readback.
**Files:** Spec019, source-owned config examples, provisioning utility, handoff.

## PG-T02 — Migrate SEC FTD through its native PostgreSQL adapter

- [x] Configure a separate PG root and replay original accepted history.
- [x] Reconcile facts, coverage, audit, values and capture clocks exactly.
**Verify:** existing SEC PG tests plus actual source/PG logical parity.

## PG-T03 — Migrate Reg SHO through its native PostgreSQL adapter

- [x] Configure a separate PG root and restore original accepted history.
- [x] Reconcile audit, coverage, membership/removal and capture clocks exactly.
**Verify:** existing threshold PG tests plus actual source/PG logical parity.

## PG-T04 — Characterize FINRA short-volume primary boundaries

- [ ] Pin warehouse/catalog/identity/publication invariants with offline fixtures.
- [ ] Define provider-owned PG schema/config and atomic migration boundaries.
**Verify:** fixture parity and failure expectations before implementation.

## PG-T05 — Implement and cut over FINRA short-volume PostgreSQL

- [ ] Add native warehouse/catalog transactions and explicit backend selection.
- [ ] Verify actual PG revisions, reads, cutoffs and publication failure recovery.
- [ ] Migrate accepted three-TRF history and bind PG after exact parity.
**Verify:** focused PG tests and retained-source equivalence; split implementation into bounded increments.

## PG-T06 — Characterize OTC PostgreSQL primary boundaries

- [ ] Pin T07 versions/membership/migration/history behavior on fixtures.
- [ ] Define native PG configuration, identity and explicit migration.
**Verify:** all-six-profile PostgreSQL acceptance expectations.

## PG-T07 — Implement OTC PostgreSQL primary

- [ ] Add native PG transactions, consistent cutoffs and no-fallback binding.
- [ ] Verify all-six fixture parity and rollback; initialize empty operational namespace.
**Verify:** actual isolated PG tests; operational schema contains no invented observations.

## PG-T08 — Characterize presidential PostgreSQL boundaries

- [ ] Pin observation/version/membership and publication/export/backup behavior.
- [ ] Define PG primary and derivative-file boundary without weakening publication seals.
**Verify:** frozen reconstruction expectations and migration parity design.

## PG-T09 — Implement and migrate presidential PostgreSQL primary

- [ ] Add native adapter and explicit PG configuration without live-source changes.
- [ ] Preserve observed-as-of, exact values, revision lineage and publication recovery.
- [ ] Migrate retained frozen reconstruction with exact readback equivalence.
**Verify:** actual isolated PG tests and retained-store parity; split into bounded increments.

## PG-T10 — Implement and migrate retained NAAIM PostgreSQL primary

- [ ] Add typed source-owned retained-capture/version/latest storage and config.
- [ ] Migrate only retained source evidence with unchanged first-seen and delay labels.
- [ ] Verify read/write/revision/rollback/no-fallback behavior; keep capture/schedule paused.
**Verify:** actual isolated PG tests and offline retained-file parity.

## PG-T11 — Final primary cutover and operating acceptance

- [ ] Verify every pipeline's future CLI/Dagster writes select its PG primary.
- [ ] Preserve all existing activation controls; update canonical DAGSTER.md as needed.
- [ ] Record eight target schemas/roles, backups, parity and recovery evidence.
- [ ] Full offline/PG checks, Ruff, package and handoff validation pass.
**Verify:** per-pipeline operating checks, role isolation and source-preservation manifest.
