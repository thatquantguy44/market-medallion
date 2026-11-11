# FOMC implementation tasks and pickup

Updated: 2026-11-11. The user selected implementation, reuse of this project's
PostgreSQL location and a pipeline-owned configuration folder. No recurring
schedule or embedding service was selected. Work on `feat/spec017-fomc-inventory`.

## FOMC-T01 — normalized inventory contract (complete)

Traces FOMC-AC01. Strict manifest, reviewed URL boundary, frozen meeting/document
references and source-page association evidence are implemented. **61 offline
tests pass** with invented normalized current/historical candidates. This is
not a Federal Reserve JSON API and does not parse native HTML. Same-day sessions,
non-contiguous joint dates, bundled-document references, format variants,
overlapping inventories and conflicting source claims retain distinct evidence.
See [inventory contract](inventory_contract.md).

## FOMC-T02 — configurable PostgreSQL inventory catalog (complete)

Traces FOMC-AC11. Own config loader, example/ignored local config and PostgreSQL
adapter are implemented. **14 offline configuration tests and five real
PostgreSQL integration tests pass**. Verification covers exact metadata readback,
old/current snapshots, idempotent/concurrent retries, failed-transaction rollback,
corruption refusal, empty inventories and reinitialization. Tests use isolated
FOMC schemas; the production schema is initialized with zero inventory imports.
Configuration and operating commands: [config folder](../../config/fomc_meeting_minutes/README.md).
The [sanitized backend evidence](evidence/postgres_inventory.json) records actual
location/schema and verification scope. Complete documents/text are not stored yet.

Full offline regression: **823 passed, 15 optional skips**. The five FOMC
PostgreSQL tests additionally passed against the existing service in isolated
schemas, for **80 focused checks** total. Ruff lint/format and build/artifact
checks pass. Local config remains ignored and owner-only.

## FOMC-T03 — native inventory adapter and immutable Bronze (next)

Traces native-source integration of FOMC-AC01 plus FOMC-AC02/FOMC-AC12.
First split native page discovery and raw-document archival into separately
reviewable tasks (at most five substantive files each). Then:

- [ ] Characterize current/historical native HTML from the official source
  shapes linked in [README](README.md#official-sources-and-access). Parse only
  reviewed minutes/predecessor labels into T01 candidates, preserving exclusions,
  pending/not-listed evidence and dates/session qualifiers. Flag ambiguous labels
  such as the 1937 Executive Committee link under the 1936 page; do not guess.
- [ ] Capture original inventories/documents/addenda before extraction, with
  exact hash/bytes, safe HTTP evidence and actual capture clocks. Add explicit
  finite acquisition controls using the run-scoped shared HTTP client and
  pipeline-owned network config. Local import paths remain network-free.
- [ ] Add PostgreSQL original-byte storage and lineage in the same FOMC schema,
  alongside filesystem originals. Introduce a reviewed catalog/schema migration;
  do not silently repurpose the T02 inventory contract or marker.
- [ ] Prove rejection evidence, raw-byte readback and database/file failure
  behavior with invented fixtures, then record bounded actual-source conformance
  separately. No corpus-wide backfill is implicit in the first live sample.

## Later slices (not started)

- FOMC-T04: complete native/OCR text, PostgreSQL text/document-version storage,
  source/capture clocks, correction/reversion and coherent catalog/replay
  (FOMC-AC03–05/FOMC-AC12). Keep archive and text completeness distinct.
- FOMC-T05: deterministic chunks and PostgreSQL chunk metadata/text (FOMC-AC07/12).
- FOMC-T06: bounded historical/current acquisition, health and stopped orchestration
  (FOMC-AC06), after local archival/replay evidence passes.
- FOMC-T07: independently selected optional embeddings (FOMC-AC08–10). No model,
  paid service, vector extension or deployment activation is selected.

Before continuing read root AGENTS.md, README.md, docs/pipelines.md,
specs/spec001/README.md, docs/handoffs/project_bootstrap.md and this spec/contract.
Verify the local workflow library. QuantSmith's external checkout still differs
from its pin; adopted guidance was read from hash-verified pinned Git objects.
Keep NAAIM paused. FINRA remains at SSV-T05 for its next continuation.
