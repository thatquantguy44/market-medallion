# SSV-T11 — deterministic empty-target replay

Selected 2026-11-13 as an extension to `feat/finra-parquet-publication` / PR #24.
Traces SSV-REQ11 / SSV-AC11 and common C05–C07. No source/provider schema or
access-policy change: replay uses only this pipeline's existing local artifacts.

## Inputs, identity and scope

Require a verified source operational warehouse, its Bronze archive and terminal
capture ledger. This slice is not a database-free recovery bundle. Every archived
capture must have an original terminal APPLIED, PARTIAL or REJECTED result, the
supported parser/transform, selected parse outcome and consistent audit evidence.
Refuse unresolved STARTED attempts, failed-only/unprocessed captures, missing
captures/outcomes, ambiguous identity, corruption or clock regression before
creating the destination. Historical FAILED/INTERRUPTED attempts followed by a
terminal retry are retained as audit evidence, not executed as new successes.
Unused sequence reservations may represent abandoned archive attempts; retain
these allocator-only records without manufacturing observations. Reject unsafe
or unexpected archive entries. Fixture/provider kind and approved operation gates
apply before filesystem access; no permission is inferred from source data alone.

The destination is explicitly absent or empty, disjoint from the source and
selected by the destination YAML. Both logical paths and resolved paths must be
safe; aliases, overlapping roots and populated destinations refuse. Source
archive and ingestion locks pin one capture/ledger inventory during replay.
Resource limits bound capture count, each input, total archive bytes and database
row/byte projection before loading documents. Defaults:1000 captures,128MiB total
archive/state bytes and250000 state rows; CLI overrides remain finite.

## Rebuild and validation

Reconstruct from the original payload/receipt and original parser profile, in
capture-sequence order with nondecreasing archived_at clocks. Preserve all capture
and run UUIDs, original clocks, retrieval/publication nullability, sequence slots,
manifest hashes and source version IDs. Do not re-archive through `run`, allocate
new capture sequences, or replace first-seen with replay wall time. Parse outcomes
must match their original evidence. Recompute versions, removals, membership and
partition pointers through the existing ingestion path. Terminal outcomes must
match exactly, including original completion time. Preserve historical attempt
IDs, ordering, statuses, clocks and errors after validating their terminal links.
Compare every logical operational table and current Gold to the verified source.
Schema/algorithm mismatches or a nondeterministic transform refuse publication.

Build in a private sibling staging root. Its fresh warehouse UUID is independent
of the source. After complete parity, seal that newly rebuilt store's root identity
to the intended destination while it is still private, then atomically rename it
into place and fsync its parent. This is a replay-only initialization step, not a
supported way to relocate an existing bound warehouse. Failures remove only the
owned private staging root and leave the source and destination contents intact.
Concurrent destination population must never be overwritten. A successful replay
writes `replay.json` with source/destination warehouse IDs, inventory and logical
hashes, counts, original knowledge labels and a separate replay completion clock.
An existing replay destination is never an implicit resume or overwrite target.

T10 publication remains explicit: rebuilding operational state does not copy old
publication catalog/files, generations or pointer. After replay, `publish` creates
a new independently verified publication event for an explicit request. Logical
facts and cutoff results must match; warehouse/publication UUIDs need not match.
No HTTP, environment loading, source probing, service, schedule or Slack activity.

## Verification

Invented tests cover unchanged repeats, corrections/reversions/removals, valid
empty, partial and rejected captures, fractional/wide counts, multiple facilities
and dates, equal-time sequence cutoffs, historical failed retries and post-replay
ingestion. Compare exact logical tables, all capture-bound cutoffs, research and
T10 published tables. Exercise admission, corrupt/missing/provenance conflicts,
ordering gaps/regression, finite budgets before loading, populated/overlapping/
symlink roots, concurrent destination writers and staging/install failure cleanup.
Actual portable-script replay into an empty root records acceptance evidence.
