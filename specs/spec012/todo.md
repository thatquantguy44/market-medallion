# Tasks — FINRA daily short-sale volume

SSV-T01 is **complete for the selected Public API route**, with source-review,
user-attested terms, pure admission tests and bounded authentication/metadata
evidence. SSV-T02–SSV-T16 are **not started**. Prefix paths below with
`src/market_data_pipeline/pipelines/finra_short_sale_volume/` unless a repository
path is shown. Each task is S/M (at most five likely files); split if it grows.
Tests below are future targets except the implemented SSV-T01 admission suite. [README.md](README.md) defines their acceptance IDs.

## SSV-T01 — Resolve rights and approved acquisition route

**Depends:** Spec011 delivery; **traces:** SSV-REQ01 / SSV-AC01.
- [x] Record the selected API purpose and accepted-terms acknowledgement:
  `USER_ATTESTED_PUBLIC_API_TERMS_2026_11_10`. [Official review](source_review.md)
  and [API profile](api_contract.md) preserve its scope and user-attested basis.
  Authentication/production metadata returned HTTP 200; no observations were queried.
  TXT permission remains separate and unresolved.
- [x] Distinguish free files, limited Public API, paid credentials and mock routes.
- [x] Implement admission metadata after recording the initial fixture-only decision;
  then record the selected user-attested API profile after terms confirmation.
  `manifest.require_approved_use` is pure; **21 offline tests pass** against
  missing/disallowed profiles, explicit operation grants and route-specific evidence.
  Future adapters must call it before I/O; no acquisition/export adapter exists.

**Checkpoint:** SSV-T01 is complete for this user-attested API route. G0 remains
open pending SSV-T02 facility/schema readiness. The user created the credential
and accepted terms; no account or agreement was created by the agent. The API
amendment below does not claim real observation coverage or integrated I/O guards.
**Verify:** documentary review plus an offline negative admission fixture.
**Files:** `specs/spec012/data_contract.md`, `manifest.py`, `tests/test_ssv_manifest.py`.

## SSV-T02 — Pin API facility and schema registry

**Depends:** SSV-T01; **traces:** SSV-REQ02 / SSV-AC02.
- [ ] Pin the seven-field production schema, `tradeReportDate` partition and
  selected source-version behavior from the [API contract](api_contract.md).
- [ ] Resolve metadata N/Q/B/D versus example NYTRF/NQTRF/NCTRF identifiers;
  record admitted facility pairs and explicit ADF/consolidated/ORF exclusions.
- [ ] Bound the documented rolling twelve-month regime; keep TXT headers,
  trailers and older regimes in a separate unadmitted profile.
- [ ] Reject unknown and overlapping scope selections before data acquisition.
**Verify:** table-driven schema/facility/version and overlapping-scope tests.
**Files:** `manifest.py`, `manifests/finra-short-sale-volume.yml`, `tests/test_ssv_manifest.py`.

## SSV-T03 — Characterize API parser with invented responses

**Depends:** SSV-T02; **traces:** SSV-REQ03 / SSV-AC03.
- [ ] Cover valid JSON arrays, suffix symbols, empty responses, malformed rows,
  nonintegral counts, duplicate keys/rows, unknown fields and truncation.
- [ ] Pin lossless numeric decoding, explicit JSON/date/schema rules and scope
  receipt validation; API page totals never become a fabricated TXT trailer.
**Verify:** `.venv/bin/python -m pytest -q tests/test_ssv_parser.py`.
**Files:** `data/api.py`, `tests/test_ssv_parser.py`, up to three invented response fixtures.

### Checkpoint A — Contract and parser

- [ ] Terms/route evidence is current; fixture tests prove accepted and rejected shapes.
- [ ] No live provider acquisition or scheduler activation occurred during fixtures.

## SSV-T04 — Enforce share-count arithmetic

**Depends:** SSV-T03; **traces:** SSV-REQ04 / SSV-AC04.
- [ ] Preserve integers; validate exempt <= short <= total with explicit zero cases.
- [ ] Prove exempt is a subset using 40/5/100 and large-count examples.
**Verify:** parameterized semantic tests and lossless count round-trip assertions.
**Files:** `data/api.py`, `data/gold.py`, `tests/test_ssv_semantics.py`.

## SSV-T05 — Archive input and rejected-capture evidence

**Depends:** SSV-T03; **traces:** SSV-REQ05 / SSV-AC05.
- [ ] Archive exact bytes/hash with manifest/schema/run receipts before parsing.
- [ ] Preserve rejected payloads and redact credential-bearing request material.
**Verify:** compare byte-for-byte input and rejected-capture receipts offline.
**Files:** `io/bronze.py`, `tests/test_ssv_bronze.py`.

## SSV-T06 — Deliver one local SQLite partition

**Depends:** SSV-T04, SSV-T05; **traces:** SSV-REQ06 / SSV-AC06.
- [ ] Import one invented file through Bronze/Silver/Gold and explicit run audit.
- [ ] A validation/write failure cannot advance the committed publication.
**Verify:** local end-to-end transaction/failure tests.
**Files:** `pipeline.py`, `io/warehouse.py`, `cli.py`, `__main__.py`, `tests/test_ssv_pipeline.py`.

### Checkpoint B — Local usable slice

- [ ] One file is inspectable with scoped counts and receipts; no source credentials needed.
- [ ] Existing AAII/NAAIM tests remain unchanged and pass relevant regressions.

## SSV-T07 — Capture revisions and reversions

**Depends:** SSV-T06; **traces:** SSV-REQ07 / SSV-AC07.
- [ ] Identical repeats retain run evidence without duplicate value versions.
- [ ] Changed/reverted rows preserve actual first-seen and observed-as-of selection.
**Verify:** three-capture fixture with cutoffs around each version, including
equal UTC timestamps ordered by the original common-C06 capture sequence.
**Files:** `io/warehouse.py`, `tests/test_ssv_versions.py`.

## SSV-T08 — Model complete replacements and absent rows

**Depends:** SSV-T07; **traces:** SSV-REQ08 / SSV-AC08.
- [ ] Removal versions require a complete authoritative replacement.
- [ ] Distinguish missing, failed, pending, empty and no-reported-row states.
**Verify:** incomplete replacement cannot delete; valid empty can revise membership.
**Files:** `io/warehouse.py`, `pipeline.py`, `tests/test_ssv_completeness.py`.

## SSV-T09 — Add scope-safe research Gold

**Depends:** SSV-T08; **traces:** SSV-REQ09 / SSV-AC09.
- [ ] Calculate denominator-matched ratios and explicit prior-date/gap fields.
- [ ] Reject constituent/consolidated double counting and return null on zero total.
**Verify:** independent expected values for individual and disjoint aggregate fixtures.
**Files:** `data/gold.py`, `tests/test_ssv_gold.py`, `io/warehouse.py`.

### Checkpoint C — Correct changing data

- [ ] Current/as-of results reconcile to capture lineage and replacement membership.
- [ ] No short-interest, borrow-rate or naked-short label is present in output.

## SSV-T10 — Publish typed Parquet snapshots

**Depends:** SSV-T09; **traces:** SSV-REQ10 / SSV-AC10.
- [ ] Stage typed files and atomically advance a single committed pointer.
- [ ] Preserve counts/ratios and schema identity through readback.
**Verify:** precision plus common-C07 failure injection at staging, DB commit,
file finalization, pointer publication and final journal marking; restart
selects one coherent generation and no false success watermark.
**Files:** `io/parquet.py`, `pipeline.py`, `tests/test_ssv_parquet.py`.

## SSV-T11 — Replay archived captures

**Depends:** SSV-T10; **traces:** SSV-REQ11 / SSV-AC11.
- [ ] Empty-target replay preserves ordering, versions and original first-seen clocks.
- [ ] Replay makes no network request and refuses a populated destination.
**Verify:** compare logical versions/Gold before and after clean replay.
**Files:** `replay.py`, `cli.py`, `tests/test_ssv_replay.py`.

## SSV-T12 — Implement bounded admitted acquisition

**Depends:** SSV-T01, SSV-T02, SSV-T11; **traces:** SSV-NFR01 / SSV-AC12.
- [ ] Enforce finite dates/files/bytes/requests with explicit overlap for corrections.
- [ ] Inject 429/Retry-After, timeouts, redirects, HTML and truncated responses.
- [ ] Record each file outcome; failures never claim complete extraction.
**Verify:** injected transport tests; separate permitted bounded live conformance
or an approved actual local file with traceable provenance and explicit owner
acceptance of local mode. Fixtures alone do not prove real-source coverage.
**Files:** `source.py`, `cli.py`, `tests/test_ssv_source.py`, `specs/spec012/data_contract.md`.

### Checkpoint D — Reproducible acquisition

- [ ] Offline replay works independently of transport; no archive-scale run is implicit.
- [ ] Live conformance, if performed, is identified separately from fixture evidence.

## SSV-T13 — Add release-aware health and retention dry run

**Depends:** SSV-T12; **traces:** SSV-NFR02 / SSV-AC13.
- [ ] Report requested/observed partitions, last success, trailing failure and pending releases.
- [ ] Retention dry run explains referenced bytes/versions under the approved policy.
**Verify:** holiday, late release, empty ADF, failed refresh and retention-reference tests.
**Files:** `operations.py`, `cli.py`, `tests/test_ssv_operations.py`.

## SSV-T14 — Define and write research interchange

**Depends:** SSV-T10, SSV-T13; **traces:** SSV-REQ12 / SSV-AC14.
- [ ] Export only admitted fields with version/scope/units/coverage/source labels.
- [ ] Provide an invented consumer contract fixture; reject disallowed output use.
**Verify:** contract/readback test and review for operational details/secrets.
**Files:** `research_export.py`, `cli.py`, `tests/test_ssv_export.py`, `specs/spec012/data_contract.md`.

## SSV-T15 — Add stopped orchestration

**Depends:** SSV-T14; **traces:** SSV-NFR03 / SSV-AC15.
- [ ] Source-owned job/schedule default stopped with explicit activation controls.
- [ ] Repository composition exposes only the implemented job and tested resources.
**Verify:** definition tests prove default-stopped state and missing-live-flag refusal.
**Files:** `dagster_defs.py`, `tests/test_ssv_dagster.py`,
`src/market_data_pipeline/orchestration/dagster_defs.py`, `resources/dagster-ssv.yml`.

## SSV-T16 — Close delivery evidence and operating handoff

**Depends:** SSV-T15; **traces:** SSV-NFR03 / SSV-AC15.
- [ ] Runbook records restart, stop, recovery and known source/rights limits.
- [ ] Pipeline/job registers and handoff record actual coverage and Spec013 pickup.
- [ ] Every acceptance ID is tied to recorded deterministic evidence.
**Verify:** full offline suite, Ruff, package build and AC evidence review.
**Files:** `docs/deployment/ssv_runbook.md`, `docs/deployment/dagster_jobs.md`,
`docs/pipelines.md`, `docs/handoffs/project_bootstrap.md`, `specs/spec012/todo.md`.

### Final delivery checkpoint

- [ ] All SSV-AC01–SSV-AC15 have deterministic evidence and source-policy decisions.
- [ ] Package/register status is accurate; remaining expansion decisions are explicit.
- [ ] Schedule remains stopped; Spec013 may begin after acceptance on a fresh branch.
