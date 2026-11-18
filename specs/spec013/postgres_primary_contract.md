# OTC native PostgreSQL primary contract

**Status:** implemented and verified under [Spec019 PG-T06/T07](../spec019/todo.md).
All existing T07 fixture semantics remain the acceptance baseline. No real OTC
observation/profile conformance or provider access is admitted by this storage work.

The native adapter preserves logical warehouse v2 and use a distinct
PostgreSQL primary identity. It owns native `otc_*`/`otc2_*` relational tables,
original JSON text, context seals, exact tokens and durable capture sequences.
A v1 SQLite store must first take the existing explicit T07 v1-to-v2 migration;
the PostgreSQL migration must not reinterpret or upgrade old data implicitly.
All legacy tables/migration records remain unchanged and separately fingerprinted.

Explicit migration holds the source writer lock, verifies current storage identity,
copies/compares every logical table inside one PG transaction, then durably binds
root/pipeline/database/schema/role/store identity. A committed but unbound copy can
reconcile/retry. Once bound, all existing reads/writes/history/cutoffs use native PG
and fail closed without SQLite fallback; no migration/repair happens on reads.
Native schema fingerprints and immutable legacy-table fingerprints are verified
at every open. Existing row/JSON/version bounds stay in force; native relation/
index sizes retain the 256 MiB local warehouse ceiling.

Administrative empty initialization may create storage/identity and empty archive
directories, but must create no capture, value, membership, publication or invented
provider completeness. The operative OTC schema uses this empty initialization.
Only temporary test schemas receive invented fixtures. Data methods continue to
require their existing fixture admission; storage initialization grants no real
source access and changes no schedules.

Acceptance exercises all six profiles, unchanged readback/first-seen, new
corrections/reversions/removals, incomplete refusal, equal-clock sequence, explicit
legacy migration, rollback/retry, metadata/schema corruption and no-fallback reads
with the recovery SQLite file absent. The existing CLI receives explicit native
migration and empty-initialization commands. All **50 actual PG tests** pass, including 30 write/commit rollback
boundaries across six profiles, legacy migration, corruption, outage and CLI.
The unchanged OTC offline suite has **424 passes**.
[Operative evidence](../spec019/evidence/pg_t07_otc.json) verifies zero captures,
versions, membership or invented data; real source conformance stays NOT_CHECKED.
