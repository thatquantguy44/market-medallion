# FINRA short-volume PostgreSQL primary contract

**Status:** native warehouse/catalog primary implemented and retained-history
cutover accepted under [Spec019 PG-T04/T05](../spec019/todo.md), verified
2026-11-17. [Evidence](../spec019/evidence/pg_t05_finra.json) records the actual
three-TRF migration and 115 unchanged source files.

The source-owned `io/postgres.py` adapter uses PostgreSQL relational tables for
identity, attempts, captures, versions, capture membership and current partitions.
Publication identity, generations and frozen rows use separate `publication_`
table names in the same source-owned schema. It retains original JSON text,
identifiers, exact quantities and immutable record/receipt semantics. No opaque
SQLite database is stored in PostgreSQL.

An explicit verified migration holds the original ingestion lock, validates
SQLite identities and copies all warehouse/catalog records transactionally into
an initially empty native namespace. It compares canonical row hashes/counts
and original attempt insertion order before commit. After commit, a durable
`.postgres-primary.json` binds the exact root, pipeline, database/schema/role and
store UUID. This file contains only a password environment reference. Binding
advances only after complete parity; interrupted commit-to-binding can safely
reconcile/retry, with no source deletion or implicit target overwrite.

Once bound, reads/writes use PostgreSQL even when the recovery SQLite files are
absent. A missing/unavailable/wrong primary or changed schema refuses; SQLite is
never a fallback. Original root/store/manifest/use identity checks still precede
access. The native primary identity seals DDL, relation/column/constraint/index/
view/sequence/trigger definitions, logical store identity and original migration
fingerprints. No migration or repair occurs on reads.

Native `attempt_ordinal` preserves SQLite's original implicit rowid order for
capture retries, then uses a PostgreSQL identity sequence for later attempts.
Count-byte queries use PostgreSQL octet lengths. These provider-owned dialect
adaptations preserve the existing logical row contract; they do not emulate a
SQLite schema or run a temporary SQLite primary. Sessions explicitly SET ROLE and
search_path, serialize writers with a schema advisory lock and use consistent
read transactions. Source warehouse and publication catalog retain their separate
transaction/journal boundaries and every existing file-pointer recovery phase.

The native fixture suite checks lossless migration, new revisions/cutoffs,
SQLite-file independence, unavailable-primary refusal, original retry ordering,
metadata/schema corruption, migration rollback/retry, four warehouse rollback
boundaries and all seven publication failure/recovery stages. The source-owned CLI `migrate-postgres --config ... --postgres-config ...`
selects only a fully verified primary and sanitizes connection failures. Health
and retention remain read-only, classify the binding as store identity, and
PG-source replay projects original logical audit fields without its internal
ordinal. Explicit recovery replay may create a separate SQLite recovery artifact;
it never repoints or changes the bound operational PG primary. Provider API rights, limits, clocks and activation
gates are unchanged; all acquisition schedules stay stopped.
