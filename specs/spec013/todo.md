# Tasks — FINRA OTC transparency

All tasks are **not started**. Proposed code paths are relative to
`src/market_data_pipeline/pipelines/finra_otc_transparency/`. Each is S/M and
limited to five likely files; split growing tasks before implementation. Test
commands below identify future targets, not tests already delivered.

## OTC-T01 — Resolve source route and reuse contract

**Depends:** Spec012 delivery; **traces:** OTC-REQ01 / OTC-AC01.
- [ ] Reconcile documented download automation with general/dataset/API terms.
- [ ] Record retention, transforms, exports and non-commercial use boundaries.
- [ ] Deny unadmitted real-data acquisition/export in the future use profile.
**Verify:** documentary review and offline admitted/denied profile tests.
**Files:** `specs/spec013/data_contract.md`, `manifest.py`, `tests/test_otc_manifest.py`.

## OTC-T02 — Define products, tiers and requested partitions

**Depends:** OTC-T01; **traces:** OTC-REQ02 / OTC-AC02.
- [ ] Inventory weekly ATS/non-ATS detail with schema/coverage identifiers.
- [ ] Exclude overlapping summaries/monthly/blocks from the detail population.
**Verify:** finite partition tests reject duplicate/overlapping or unknown selections.
**Files:** `catalog.py`, `manifest.py`, `manifests/finra-otc-transparency.yml`, `tests/test_otc_catalog.py`.

## OTC-T03 — Parse an invented weekly profile

**Depends:** OTC-T02; **traces:** OTC-REQ03 / OTC-AC03.
- [ ] Declare literal headers, delimiter, encoding, dates/counts and context source.
- [ ] Cover valid, empty, malformed, duplicate and changed-schema examples.
**Verify:** `.venv/bin/python -m pytest -q tests/test_otc_parser.py`.
**Files:** `data/weekly.py`, `tests/test_otc_parser.py`, up to three invented fixtures.

### Checkpoint A — Current contract and source shape

- [ ] Current transfer/use evidence is distinct from old documentation examples.
- [ ] Pure fixture parser passes and no live extraction or schedule is active.

## OTC-T04 — Preserve attribution and identity basis

**Depends:** OTC-T03; **traces:** OTC-REQ04 / OTC-AC04.
- [ ] Model supplied MPID/CRD, name-only, grouped and unknown identities explicitly.
- [ ] Prevent a display-name rename from silently merging unrelated participants.
**Verify:** changed-name, missing-ID and ambiguous-label cases preserve uncertainty.
**Files:** `data/identity.py`, `data/weekly.py`, `tests/test_otc_identity.py`.

## OTC-T05 — Capture bytes and partition completeness

**Depends:** OTC-T03; **traces:** OTC-REQ05 / OTC-AC05.
- [ ] Preserve payload/discovery metadata, schema/manifest hashes and requested scope.
- [ ] File/page absence and parser rejection retain receipts with explicit outcomes.
**Verify:** byte/hash comparison and missing-page negative fixture.
**Files:** `io/bronze.py`, `tests/test_otc_bronze.py`.

## OTC-T06 — Deliver the local weekly detail slice

**Depends:** OTC-T04, OTC-T05; **traces:** OTC-REQ06 / OTC-AC06.
- [ ] Persist one invented complete ATS/non-ATS partition in source-owned SQLite.
- [ ] Couple data, coverage and run status in a safe transactional publication.
**Verify:** start with one profile, then parameterize end-to-end readback and
injected persistence failures over all six selected product/tier profiles
(ATS and non-ATS × Tier1/Tier2/OTCE). Unsupported profiles keep delivery open
unless the owner explicitly narrows the selected scope.
**Files:** `pipeline.py`, `io/warehouse.py`, `cli.py`, `__main__.py`, `tests/test_otc_pipeline.py`.

### Checkpoint B — Local inspectable panel

- [ ] Reporting week/tier/product and attribution coverage survive the complete flow.
- [ ] No source claim implies short direction, lending inventory or market-wide totals.

## OTC-T07 — Implement immutable revisions and replacement membership

**Depends:** OTC-T06; **traces:** OTC-REQ07 / OTC-AC07.
- [ ] Identical captures are idempotent; corrections/reversions remain distinct.
- [ ] Row removals require a complete replacement, and retain cutoff lineage.
**Verify:** same/changed/reverted/removed fixtures, partial-replacement refusal
and equal-clock captures ordered by the persisted common-C06 sequence.
**Files:** `io/warehouse.py`, `pipeline.py`, `tests/test_otc_versions.py`.

## OTC-T08 — Separate reporting and availability clocks

**Depends:** OTC-T07; **traces:** OTC-REQ08 / OTC-AC08.
- [ ] Preserve dates versus timestamps and tier-specific expected delay separately.
- [ ] Backfill actual capture time drives first-seen, never historical report Monday.
**Verify:** holidays, null publication time, date-only updates and cutoff ordering.
**Files:** `data/weekly.py`, `io/warehouse.py`, `tests/test_otc_clocks.py`.

## OTC-T09 — Calculate complete-population venue features

**Depends:** OTC-T08; **traces:** OTC-REQ09 / OTC-AC09.
- [ ] Compute exact share/trade-based features with zero-denominator nulls.
- [ ] Publish matching population and attribution coverage alongside every ratio.
- [ ] Withhold combined features when required products/tiers are incomplete.
**Verify:** independent expected arithmetic plus mixed-scope and partial-population tests.
**Files:** `data/gold.py`, `tests/test_otc_gold.py`, `io/warehouse.py`.

### Checkpoint C — Honest historical research values

- [ ] Current/as-of features select correct versions and complete population membership.
- [ ] Grouped/unknown participants remain visible; concentration methodology stays deferred.

## OTC-T10 — Publish atomic typed snapshots

**Depends:** OTC-T09; **traces:** OTC-REQ10 / OTC-AC10.
- [ ] Preserve counts, context and identity fields through typed Parquet round-trip.
- [ ] Advance one current pointer only after all requested partition writes succeed.
**Verify:** readback/schema fingerprint and common-C07 failure injection at
staging, DB commit, file finalization, pointer publication and final journal
marking; restart selects one coherent generation without a false success watermark.
**Files:** `io/parquet.py`, `pipeline.py`, `tests/test_otc_parquet.py`.

## OTC-T11 — Replay archived source history

**Depends:** OTC-T10; **traces:** OTC-REQ11 / OTC-AC11.
- [ ] Reproduce versions/removals/first-seen clocks from capture order in an empty target.
- [ ] Reject populated targets and all network paths in replay.
**Verify:** logical table/Gold equivalence across original and rebuilt roots.
**Files:** `replay.py`, `cli.py`, `tests/test_otc_replay.py`.

## OTC-T12 — Add bounded refresh through admitted transport

**Depends:** OTC-T01, OTC-T02, OTC-T11; **traces:** OTC-NFR01 / OTC-AC12.
- [ ] Enforce finite partition/page/byte/request budgets and overlapping correction refresh.
- [ ] Verify stable extraction ordering/completeness for the selected actual route.
- [ ] Handle 429/Retry-After, login/HTML, truncation and partial responses explicitly.
**Verify:** injected transport suite; permitted bounded live conformance or an
approved actual local source file with provenance and explicit owner acceptance
of local mode. Conformance is recorded by selected profile; a single sample
does not establish all six profiles or history. Fixtures alone are not real coverage.
**Files:** `source.py`, `cli.py`, `tests/test_otc_source.py`, `specs/spec013/data_contract.md`.

### Checkpoint D — Reproducible bounded operation

- [ ] Fresh source failures never masquerade as zero volume or a complete denominator.
- [ ] No paid fallback, bulk history or job activation occurred implicitly.

## OTC-T13 — Add coverage health and retention proposal

**Depends:** OTC-T12; **traces:** OTC-NFR02 / OTC-AC13.
- [ ] Show complete/pending/missing/rejected partitions under current release timing.
- [ ] Report last successful capture and trailing refresh failure independently.
- [ ] Retention dry run reconciles snapshot/capture references under approved policy.
**Verify:** staggered tiers, holiday, late release, partial refresh and reference tests.
**Files:** `operations.py`, `cli.py`, `tests/test_otc_operations.py`.

## OTC-T14 — Publish admitted research interchange

**Depends:** OTC-T10, OTC-T13; **traces:** OTC-REQ12 / OTC-AC14.
- [ ] Write versioned bounded file with scope/delay/identity/coverage/use labels.
- [ ] Provide invented consumer contract example and deny unapproved export uses.
**Verify:** independent contract readback, sensitive-field scan and wrong-version rejection.
**Files:** `research_export.py`, `cli.py`, `tests/test_otc_export.py`, `specs/spec013/data_contract.md`.

## OTC-T15 — Add stopped job definitions

**Depends:** OTC-T14; **traces:** OTC-NFR03 / OTC-AC15.
- [ ] Source-owned Dagster job/schedule default stopped with explicit live acknowledgement.
- [ ] Repository composition includes only implemented products/resources.
**Verify:** definition tests prove stopped defaults and explicit live admission.
**Files:** `dagster_defs.py`, `tests/test_otc_dagster.py`,
`src/market_data_pipeline/orchestration/dagster_defs.py`, `resources/dagster-otc.yml`.

## OTC-T16 — Record delivery evidence and operating handoff

**Depends:** OTC-T15; **traces:** OTC-NFR03 / OTC-AC15.
- [ ] Runbook records stop/restart/replay/recovery and verified product/coverage limits.
- [ ] Canonical pipeline/job registers and handoff identify Spec014 pickup.
- [ ] Every acceptance ID has deterministic evidence and open decisions are explicit.
**Verify:** full offline suite, Ruff, package build and AC evidence audit.
**Files:** `docs/deployment/otc_runbook.md`, `docs/deployment/dagster_jobs.md`,
`docs/pipelines.md`, `docs/handoffs/project_bootstrap.md`, `specs/spec013/todo.md`.

### Final delivery checkpoint

- [ ] OTC-AC01–OTC-AC15 have deterministic evidence and current rights/route decisions.
- [ ] Registry accurately states implemented profiles and any excluded historical products.
- [ ] Schedules stay stopped; Spec014 begins after acceptance on a fresh branch.
