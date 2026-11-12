# FOMC-T03 — content-addressed originals and interrupted capture recovery

Current status: accepted under [T03 closeout](t03_acceptance.md); local object
migrations are applied and all 72 canonical captures pass readback/restore.
Earlier sample counts below remain checkpoint evidence.

Traces FOMC-AC02/05/12. All helpers are pipeline-owned and use the standard
library. No migration or recovery operation makes a provider request.

## Original-byte storage and explicit migration

New local captures retain their existing `captures/<UUID>/receipt.json`,
`payload.bin` and `committed.json` paths. `payload.bin` is a hard link to the
verified original at `objects/sha256/<digest>.bin`; repeated/bundled originals
share one persistent byte object while every capture URL, receipt and association
remains separate. `objects/format.json` pins `fomc-local-objects/1`. Object
publication is exclusive and atomic; conflicting bytes or a conflicting marker
refuse. The root must support hard links on one filesystem.

`migrate_local_objects(root, max_captures=10000)` explicitly verifies a bounded
legacy batch before replacing independent payload files with identical hard
links. Each replacement is atomic and retryable. Original receipt bytes, UUIDs,
clocks, URLs and commit markers remain unchanged. Interrupted/corrupt captures
must be reviewed/recovered before migration; no automatic deletion is performed.

After initializing owned Bronze storage, explicitly call
`PostgresObjectStore(bronze).migrate(max_captures=10000)`. The separately marked
`fomc-bronze-objects-postgres/1` schema adds `bronze_object_state`, `bronze_objects`
and `bronze_capture_objects`. The transactional migration validates every legacy
payload's hash/size and source/capture metadata, stores one original per digest,
and links every capture. Only then does it make the legacy payload column
nullable and clear its redundant copies. Receipts, IDs, clocks, inventory links
and the inventory pointer are unchanged. Any failure rolls back DDL and data.

New writes and reads detect the explicit object marker; unmigrated schemas keep
the earlier layout. Incomplete/conflicting object schemas refuse. Capture writes
and migration share the schema advisory lock; capture reads use the shared lock.
Concurrent migration/capture and dependency-check readback are integration-tested.
Existing objects are compared before reuse; corruption is never overwritten.
Stop older writers before migration. Rollback requires a reviewed, verified
rehydration of per-capture payloads before removing the new marker/tables; no
such destructive rollback has been run. The migrations are now applied to the
existing local FOMC schema, with pre/post restore verification.

## Durable intents and recovery boundaries

`AcquisitionRun.start(root, manifest=..., store=..., source_url=..., settings=...,
limits=...)` is the operator entrypoint for live runs. It durably records a
`probe` intent before selecting the run-scoped route. Startup failures retain a
sanitized rejected receipt with null HTTP status and no invented response body;
exception details, proxy URLs and credentials are not stored. Successful startup
records only route/fallback metadata and returns one reusable client. GET request
budgets remain separate from the bounded startup probe.

The lower-level `prepare_fomc_http` is a route selector without archival context.
Use `AcquisitionRun.start` for recorded startup outcomes. Injected clients can
still construct `AcquisitionRun` directly; acquisition records each GET intent
before calling that client. A failed intent write prevents network access.

`attempts/<UUID>.json` retains official URL, kind, phase, request-start time,
manifest/hash and byte bound before acquisition. Local imports use their actual
intent time and retain their supplied original capture time separately. Each
response receipt reuses its attempt UUID and records the phase. Incomplete reads
retain the received prefix; close failures remain incomplete. All metadata files
are published from fsynced staging files with exclusive hard links and directory
fsyncs. Partial final receipts/markers are not exposed. Empty/failed responses
remain rejected evidence.

The durable sequence is intent → receipt → content object → payload link →
PostgreSQL capture/object association → filesystem commit marker → terminal
attempt outcome. Failure at any stage leaves either retryable original bytes or
an explicit incomplete attempt. Staging files and orphan objects are retained;
there is no automatic garbage collection or silent refetch.

After the owning writer stops, `recover_attempt(root, capture_id, store)` can:

- Relink a missing payload from the exact hash/size-verified local object.
- Retry a database write or finish a missing marker/outcome using the same UUID,
  bytes, clocks and receipt, without creating another observation.
- Report `interrupted_before_receipt` or `missing_original_bytes` when durable
  bytes are unavailable; it cannot manufacture a successful receipt.
- Refuse corrupt objects, receipts, markers, intent conflicts or unknown attempts.

`committed` means archived; the returned `accepted` still distinguishes a rejected
source/startup response. Recovery moves no inventory pointer. Legacy captures
remain readable/retryable without fabricated acquisition intents.

## Evidence and remaining acceptance

[Sanitized source evidence](evidence/storage_recovery_sample.json) verifies a
28-capture local-review migration without changing original receipt bytes or
capture hashes/clocks. Offline imports, two explicit repeated local imports and
one interrupted/recovered local import retain 31 PostgreSQL receipts linked to
28 originals. All original bytes round-trip. Recovery after a real database
commit finishes the missing marker under the same identity. The temporary FOMC
schema was removed; the production schema and its five original captures remain
unchanged. This verification made zero provider requests.

Fixtures additionally exercise object corruption, legacy migration rollback,
concurrent capture, startup rejection, partial reads, failed intent/receipt/object/
payload-link/database/marker/outcome writes, and repeated offline recovery.
These are injected-failure tests, not a physical power-loss certification.

T03 is accepted for the selected 1990-onward foundation; the reviewed source
exceptions and complete inventory census are recorded in `t03_acceptance.md`. Container screening detects wrong HTML/PDF framing and
PNG signatures; full PDF structure/page checks and image/text quality belong to
T04. No full backfill, document/text catalog, job or schedule is enabled.

Final increment verification: **900 offline tests passed, 37 optional skips**;
all **179 focused FOMC checks passed**, including **27 real PostgreSQL tests** in
isolated schemas. Ruff lint/format, source/wheel builds and artifact inclusion/
runtime-input exclusion checks pass. No production schema migration or inventory
publication was performed.
