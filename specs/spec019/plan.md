# Spec019 implementation and migration plan

1. Inventory accepted roots and live PG schemas/roles. Back up the existing
   database and role definitions without password hashes; verify backup readability.
2. Provision missing non-login/non-superuser writer roles and owned schemas in
   `additional_market_data`. Preserve AAII's consumer reader and all existing data.
   Apply an effective FOMC writer role only after ownership/grant parity checks.
3. Use existing SEC/threshold PG adapters for empty destination roots, replaying
   retained original history and checking exact logical parity before selecting
   PG configs. Never reinterpret original capture clocks as migration time.
4. Implement native provider-owned PostgreSQL adapters for remaining pipelines.
   Reuse semantic transforms, not another provider's table model. Add explicit
   configuration/binding that refuses fallback and mismatched root/database/schema.
   Keep writer locking and use PG transactions/advisory locks where required.
5. Migrate source state into initially empty PG namespaces transactionally. Verify
   logical counts/content, originals and revision/coverage/audit lineage. Do not
   mix accepted real inputs with replay/test/demo roots. OTC has no accepted real
   observations, so create its empty primary capability without loading fixtures
   into the operational schema. NAAIM uses retained files only and stays paused.
6. Verify native CLI and existing Dagster launch paths use the selected PG primary.
   Preserve job names, stopped schedules and existing activation holds. Update
   DAGSTER.md if config/launch behavior changes, plus owning specs/registers.
7. Record exact schema/role/config/root mappings and successful checks. Existing
   local source stores and backups remain available; no deletion is part of cutover.

Each new backend is split into contract/fixtures, adapter, migration/cutover and
verification increments in todo.md. A failed migration leaves the source and old
selection intact. Binding advances only after full destination parity; the binding
and database identity must be checked on every operational open. Cross-resource
publication failures remain visible and recoverable under each provider's journal.
No generic SQLite SQL emulation layer or opaque SQLite blob in PostgreSQL counts
as native relational primary storage.
