# Presidential native PostgreSQL primary

Spec019 PG-T08/PG-T09 preserves the existing offline fixtures and frozen study.
The source-owned adapter stores native metadata, captures, facts, capture_facts
and publication_journal tables. Exact JSON/source tokens, missingness, version
identities, capture sequence and actual admission first-seen remain unchanged.
PostgreSQL is authoritative after an explicit locked, transactional migration;
original SQLite/raw/frozen files stay unchanged recovery evidence. A root-bound
identity and native schema fingerprint reject wrong targets/corruption. Missing
PostgreSQL refuses without fallback. Publication seals and atomic file generation
selection remain unchanged; journal updates go to the primary even with the old
SQLite file absent.

Acceptance requires actual PG revisions/reversions/equal-clock cutoffs, retries,
rollback after capture/fact/membership/commit boundaries, no-fallback and migration
retry, coherent publication failure recovery, typed Parquet and portable backup/
restore. Backup reconstructs an explicit separate SQLite recovery artifact from
verified admitted captures (it does not copy the stale original recovery DB).
The retained frozen study must retain all 3,903 facts, three captures and two
publication journal records, exact observed-as-of results, current bundle and
original source hashes/mtimes. No source requests, consumer/sibling changes or
schedule activation are part of this migration.

**Acceptance:** 16 actual PG tests pass, including native database-trigger rejection,
four write/commit failures, four publication failure/recovery boundaries, CLI,
no-file reads and verified backup/restore. 43 existing warehouse/publication/
backup/replay/health/CLI tests pass. [Operative evidence](../spec019/evidence/pg_t09_presidential.json)
reconciles all 3,903 facts/memberships, three captures and two journal records;
65 source files preserve hashes/mtimes and frozen publication/observed reads match.
