# Common engineering and research contract

**Status:** proposed implementation requirements shared by Specs011–015.
**Updated:** 2026-11-03. Source specs refine these requirements and never erase
provider-specific grain, interpretation or access limits.

## C01 — Ownership and package boundaries

Each implemented pipeline owns a snake_case package under
`src/market_data_pipeline/pipelines/`. It owns manifests/schema validation,
transport, parsers, typed records, storage/adapters, DQ, replay, CLI, operations,
export and Dagster definitions. Create packages only with working slices.
`shared/` contains provider-independent helpers supported by actual reuse;
there is no prerequisite generic-ingestion-framework rewrite.

Tests live in `tests/test_<source>*.py`; invented fixtures in
`tests/fixtures/<source>/`; source manifests in `manifests/`; deployment
examples/resources stay source named. Preserve existing AAII and NAAIM
interfaces and data. NAAIM's 365-day policy is specific to NAAIM and is not a
default license or retention policy for the new sources.

## C02 — Provider evidence and access

Before the first provider-dependent implementation, record official URLs,
review timestamp, schema/version, cost, authentication, automated-download
route, rate policy, terms, retention, transformations, attribution and audience.
Separate permission to view/download from permission to persist, automate,
redistribute or publish derived output. A mock endpoint is test documentation,
not an admitted data feed. General terms and dataset-specific allowances must
be reconciled where they differ; mark the remaining capability blocked rather
than substituting a paid source or bypassing a restriction.

Use public documented APIs or linked files first. Keep an approved local-file
ingest path available. User-agent/contact information belongs in configuration;
never invent a contact identity. No credentials, cookies or tokens enter Git,
raw request metadata, exported artifacts or logs. Resolve material access gaps
with the owner before dependent live operations; offline fixture work can
continue. Buying accounts or accepting provider agreements remains separate.

## C03 — Bronze and extraction completeness

Archive exact response bytes and a sanitized receipt before normalization. A
receipt includes provider/dataset, public request identity, selected bounds,
retrieved UTC timestamp, HTTP status, safe headers, SHA-256, byte count, run ID,
parser/schema/manifest versions and partition key. Archive landing-page/index
evidence that selected a particular file when relevant. Original compressed
files stay original; extracted members have hashes and provenance to the ZIP.

Downloads have byte/time limits, finite retry budgets and paced requests.
Respect Retry-After for eligible transient responses. Do not retry a schema
error as if it were a network failure. Verify content type and recognizable
file structure; a status-200 HTML challenge is not an empty dataset. ZIP readers
reject path traversal, duplicate/conflicting member names, unexpected members,
unbounded expansion and truncated archives. Never execute provider content.

Define completeness at a source partition: file/date/facility, market/list date,
weekly report/tier, filing or series/regime/date-range. Persist expected and
received partitions, count/trailer checks and explicit completeness evidence.
Missing, failed, suppressed, unavailable, valid-empty and not-yet-published are
separate states. Source failure may leave independent completed partitions
usable, but a dependent aggregate cannot claim complete coverage.

## C04 — Grain, identifiers and values

Declare the natural key and unit before the table. Keep source identifiers as
strings, including CUSIP/CIK/accession/series/participant codes and leading
zeros. Preserve raw symbols and punctuation. Symbol matching without dated
identifier evidence produces an unresolved candidate, never an automatic
issuer/security merge. Prefer explicit crosswalk records with evidence,
effective dates, knowledge dates, confidence and ambiguity/cardinality status.

Integers remain integers; amounts and decimal values preserve source precision.
Publish decimal text or typed decimal fields when appropriate. Null, missing,
suppressed and reported zero remain distinguishable. No implicit currency,
share-count, business-calendar, split-adjustment or percent conversion occurs.
Keep both the original token and normalized value/reason where needed for audit.

## C05 — Observation, publication and knowledge clocks

Store source observation/settlement/trade/report/portfolio date separately from
provider publication time (nullable), provider file update timestamp (nullable),
retrieved_at, first_seen_at, run completion and export generation time. Keep
timestamps timezone aware and UTC for comparisons, preserving supplied source
precision. Date-only labels remain date-only labels; do not invent midnight
publication. A documented expected release window is scheduling metadata,
not proof of historical availability.

Historical import first-seen is actual capture time. Current-vintage history
must carry `CURRENT_VINTAGE`; local observed-as-of queries use only versions
first seen by the cutoff. As-of joins apply cutoffs to facts, mappings,
calendars, source eligibility and classifications. Neither a filed date nor an
HTTP Last-Modified value alone establishes the exact historical public clock.
If clock precision is insufficient for a requested intraday claim, withhold the
claim and expose that limitation.

## C06 — Revisions, removals and replay

Use deterministic normalized content hashes under stable natural keys. An
unchanged observation does not create another Silver version; every new
observed change, including a return to a previous value, creates a new version
with the actual capture-time first-seen clock and Bronze lineage. Recheck
overlapping published partitions; completed partitions can be corrected.

A later authoritative complete replacement that removes a previously reported
row creates a versioned absence/retraction record within that partition. It
must not delete old knowledge or continue publishing the old row as current.
Failure, truncation or partial coverage never establishes removal. Genuine
new-date absence obeys the provider's separate missing-row contract. Do not
infer row deletion from a delta-only response.

Replay consumes archived bytes, receipts, approved schemas and transformation
versions in deterministic capture order, without network. It recreates source
state in an empty destination while preserving original first-seen clocks.
Verify schema/capture provenance and test unchanged, revised, reverted and
removed rows at cutoffs on both sides of each capture. Preserve source-version
IDs and deterministic report values; incidental new replay-run IDs may differ.

Persist a monotonic capture sequence within each pipeline root to disambiguate
equal timestamps. Order by actual capture timestamp and that original sequence;
reject inconsistent clock regression/ambiguous imported ordering under a
documented policy. Replay preserves that sequence rather than sorting UUIDs.

## C07 — Atomic local storage and publication

Design portable exact types and a normalized warehouse boundary before storage;
prove the first local loop with SQLite and typed Parquet before adding a
warehouse service. Spec011 additionally requires PostgreSQL adapters and
Databricks Delta publication with actual backend parity/integration evidence
before G2; its R12/A14/A15 and data contract define that scope. Other source
specs retain their own target requirements. Staging failures must not advance
Gold/current pointers or
success watermarks. Single-root locking and transactional publication prevent
mixed old/new exports. A current pointer identifies one committed snapshot;
consumers never glob all historical files together. Capture/audit failures are
visible even when the previous committed snapshot remains available.

The selected warehouse and filesystem do not share a transaction. Implement an
explicit publication journal: stage/fsync immutable files; commit DB facts and a pending
snapshot record; finalize/fsync the named files and verify their manifest;
atomically publish the current pointer; mark the journal published. Snapshot
readers pin the same generation in DB and files. A restart under the root lock
reconciles pending records and verifies a pointer updated before the final
journal mark. Failure-injection tests at each boundary prove either the prior
coherent generation or the fully completed new one is selected, with no false
success watermark. Record actual protocol/adapted helper behavior per source.

Backfills require explicit partition/date bounds, a dry-run inventory, maximum
bytes/storage budget, checkpoint/resume semantics and an overlap reconciliation
plan. No source-wide download is the default. Retention is source-specific,
dry-run-first and lineage-safe. Reproducible backups/restores use an empty
destination; logs and local data remain ignored. Validate adapters against their
own source semantics rather than assuming AAII adapter tests cover them.

## C08 — Gold metrics and research exchange

Every metric names its numerator, denominator, unit, covered population,
observation date(s), minimum history, gap policy and version lineage. Missing
denominators, incomplete populations, unknown identifiers and mixed periods
produce null/unavailable with reasons. No renormalization silently removes
missing constituents. Counts of observed episodes or venue concentration must
carry their actual sampling/coverage basis.

Each delivered source supplies a bounded versioned file artifact for a future
QuantMeridian consumer: schema and method version, source/dataset identity,
generated-at, knowledge basis, reported dates, available clocks, raw-value
units, coverage/completeness, null reasons, revision tokens, attribution and
source links. The consumer does not receive provider secrets, database writer
credentials or local Bronze paths. Design the schema here; integration into
QuantMeridian is a separately scoped change in that repository.

This program does not infer borrow rates, utilization, lendable shares,
beneficial-owner supply, recall probability, legal violations or an executable
locate from proxy data. Report design may pose hypotheses; predictive claims
require a separate time-aware research evaluation.

## C09 — Configuration and Dagster

Use source-owned YAML plus ignored environment variables for local settings,
with checked-in examples and strict validation. Explicit flags/configuration
control source scope, dates, output root, network allowance, retry budget and
size bounds. Configuration loading performs no provider request.

Manual operation remains the initial runtime model. Add jobs and **stopped**
schedules only after local source tests pass. Jobs live in their source package;
the repository orchestration module composes implemented definitions. Schedule
timing uses the actual release calendar/time zone and publication delay. A
local no-request health command reports acquisition state, missing expected
partitions, latest successful capture, latest observation, trailing failures,
source lag and current publication state independently. No new hosted service
or NAAIM schedule activation is implied.

## C10 — Verification and delivery evidence

Each spec has invented happy-path and adversarial fixtures. Tests inject
transport and clocks. Offline CI never depends on provider access, local
credentials or an operator's existing data. Cover parser/schema drift,
duplicates, completeness, failed/empty responses, units, dates, identity joins,
atomic failures, observed revisions/reversions/removals, as-of selection, replay,
bounded exports and sanitized diagnostics.

Run focused tests during each slice; run the relevant complete suite, Ruff,
format and package checks at source completion. Inspect manual CLI outcomes
where delivered. Record actual test counts and evidence paths at completion;
proposed commands and unchecked tasks never imply passing evidence. A bounded
live smoke is separate from ordinary tests and follows the admitted source route.

## C11 — Coding conventions and existing verification commands

Use Python 3.12 type annotations, dataclasses where helpful, pure transforms,
injected I/O and parameterized SQL. Keep each focused task small enough for
roughly 1–5 files; split any task that spans unrelated subsystems. For example:

```python
from decimal import Decimal

def matched_share(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    return Decimal(numerator) / Decimal(denominator)
```

The example assumes validated matching populations; it is not a generic license
to divide unrelated source measures. Current repository verification commands:

```bash
python3 scripts/verify_agent_skills.py
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m build --no-isolation
git diff --check
```

Future source CLI examples in a numbered plan are interface proposals until
their owning task is delivered. Do not route nonexistent providers through the
existing AAII CLI. Record an implementation's tested command before closing it.
