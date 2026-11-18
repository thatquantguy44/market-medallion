# NAAIM delayed native PostgreSQL primary

Spec019 PG-T10 is an offline storage migration explicitly authorized by the user.
NAAIM acquisition and schedule remain paused. The three-month provider delay,
internal research boundary, attribution and capture-time first-seen stay intact.
No new source schema or access policy is introduced.

Native relational tables own capture receipts/events, exact observed version
values and current delayed latest metadata. Original decimal text remains in
canonical values JSON plus unrounded native NUMERIC columns; dates/timestamps
have typed projections and an explicit ordinal preserves source list ordering.
Repeated values retain first-seen; corrections/reversions append occurrences.
Migration copies only retained local evidence, records missing raw references
explicitly, reconciles exact ordered versions/latest, then binds the root. The
old versions JSON is unchanged recovery evidence. Bound reads, export, health,
ingestion and retention use PG; missing PG refuses without file fallback.

Capture/archive evidence is retained before a database write. Captures, new
versions and latest are committed together; a failure leaves no partial logical
success. The local Gold latest file is a derivative, updated after commit. It
may lag after a file failure, but PG is authoritative and a retry can republish.
Dry-run retention remains read-only. Applied retention commits a replacement of
retained events/history/latest before removing expired files, so interruptions
may leave extra held files, never references to already-deleted evidence. No
retention deletion is run on the operative root as part of migration.

Acceptance requires actual PG exact migration/first-seen/delay parity, new
writes/revisions/reversions, all write/commit failure boundaries, no-file-fallback,
export and health, retention dry run/apply/rollback, CLI and explicit migration
failure/retry. The retained 131-version store and source file SHA/mtimes must
remain unchanged except for the new binding marker.

**Acceptance:** 22 actual PG tests pass: exact high-precision decimals, revisions/
reversions, four database rejections, four write boundaries, commit rejection,
derived-file recovery, no-legacy-file reads/export/health/retention, retention
rollback/apply, CLI, missing-raw reporting and committed-unbound migration retry.
The 18 existing offline NAAIM checks pass. [Operative evidence](../spec019/evidence/pg_t10_naaim.json)
reconciles 131 versions, one retained raw capture/snapshot, one event and latest;
all eight original files retain hashes/mtimes. Health still reports capture_stale;
no provider request or retention deletion occurred and NAAIM remains paused.
