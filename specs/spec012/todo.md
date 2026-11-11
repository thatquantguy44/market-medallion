# Tasks — FINRA daily short-sale volume

SSV-T01 is **complete for the selected Public API route**, with source-review,
user-attested terms, pure admission tests and bounded authentication/metadata
evidence. SSV-T02 is **complete for the selected three-TRF API profile**;
SSV-T03/T04 are **complete**; SSV-T05–SSV-T16 are **not started**. Prefix paths below with
`src/market_data_pipeline/pipelines/finra_short_sale_volume/` unless a repository
path is shown. Each task is S/M (at most five likely files); split if it grows.
Tests below are future targets except the implemented SSV-T01–T04 suites.
[README.md](README.md) defines their acceptance IDs.

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

**Checkpoint:** SSV-T01 is complete for this user-attested API route. SSV-T02
below now makes G0 ready for the explicit three-TRF profile. The user created the credential
and accepted terms; no account or agreement was created by the agent. The API
amendment below does not claim real observation coverage or integrated I/O guards.
**Verify:** documentary review plus an offline negative admission fixture.
**Files:** `specs/spec012/data_contract.md`, `manifest.py`, `tests/test_ssv_manifest.py`.

## SSV-T02 — Pin API facility and schema registry

**Status:** complete for version 1 and the three verified TRFs. Documentary
items were checked off after the bounded source review; the registry and guard
items below are now verified by 63 offline tests plus 21 existing admission tests.
Raw samples remain ignored; ordinary tests use invented metadata and no provider
requests. At T02 closeout no parser, warehouse, schedule or full capture was implemented;
T03 below adds the pure parser.

**Depends:** SSV-T01; **traces:** SSV-REQ02 / SSV-AC02.
- [x] Pin the seven-field production schema, `tradeReportDate` partition and
  selected source-version behavior from the [API contract](api_contract.md).
- [x] Resolve metadata N/Q/B/D versus example NYTRF/NQTRF/NCTRF identifiers;
  record admitted facility pairs and explicit ADF/consolidated/ORF exclusions.
- [x] Bound the documented rolling twelve-month regime; keep TXT headers,
  trailers and older regimes in a separate unadmitted profile.
- [x] Reject unknown and overlapping scope selections before data acquisition;
  exact facility/market matching and explicit rolling acquisition-date guards
  also reject mixed pairs, future/out-of-window dates and unsupported versions.
**Source evidence:** [bounded source review](evidence/t02_source_contract.json)
pins version 1, seven fields, 252 partitions from 2025-11-10 through 2026-11-10,
and six November 9 rows confirming NYTRF/N, NQTRF/Q and NCTRF/B. Market D returned
HTTP 204 / Record-Total 0 and supplied no facility identifier. ADF, consolidated,
ORF and TXT remain excluded. The initial source-check script rejected HTTP 204;
its failed result is preserved and the retained empty response was reviewed offline.
**Verify:** 84 focused tests pass; full offline regression **566 passed, 10 skipped**.
Ruff lint/format and source distribution/wheel builds pass.
All six retained rows reconcile to the registry without changing their bytes.
G0 is ready for this bounded source profile; G1/G2 are not complete.
**Files:** `manifest.py`, `registry.py`, `manifests/finra-short-sale-volume.yml`,
`tests/test_ssv_registry.py`; sanitized evidence is under `specs/spec012/evidence/`.

## SSV-T03 — Characterize API parser with invented responses

**Status:** complete. The pure parser accepts bytes plus a manifest and receipt,
performs no I/O, and assigns no clocks. **111 invented-fixture tests pass**;
195 focused FINRA tests and full regression **677 passed, 10 skipped**. Ruff
lint/format and package builds pass. [Offline conformance](evidence/t03_parser_conformance.json)
reconciles all six unchanged retained rows, keeps all pages partial and rejects
excluded market D. No new provider-data request or observation promotion occurred.

**Evidence-driven amendment:** the initial integer-only parser rejected the
retained Carteret response. All six retained rows contain fractional quantities.
The [API contract](api_contract.md#ssv-t03-fractional-quantity-amendment) now requires
exact Decimal shares for whole/fractional numbers; the integer-only assumption
and rejection of every nonintegral value are superseded. The 40/5/100 example and
exempt-subset invariant are unchanged; decimal arithmetic/round trips remain T04.

**Depends:** SSV-T02; **traces:** SSV-REQ03 / SSV-AC03.
- [x] Cover valid JSON arrays, suffix symbols, empty responses, malformed rows,
  fractional/large quantities, duplicate keys/rows, unknown fields and truncation.
- [x] Pin lossless numeric decoding, explicit JSON/date/schema rules and scope
  receipt validation; API page totals never become a fabricated TXT trailer.
**Verify:** `.venv/bin/python -m pytest -q tests/test_ssv_parser.py`.
**Files:** `data/api.py`, `tests/test_ssv_parser.py`, up to three invented response fixtures.

### Checkpoint A — Contract and parser

- [x] Terms/route evidence is current; fixture tests prove accepted and rejected shapes.
- [x] No live provider-data acquisition or scheduler activation occurred during fixtures.

## SSV-T04 — Enforce share-count arithmetic

**Depends:** SSV-T03; **traces:** SSV-REQ04 / SSV-AC04.

**Status:** complete. **71 SSV-AC04 tests pass**, including 40/5/100, fractional
shares, large quantities, valid/invalid zero cases, exact text/JSON round trips,
independent rational rounding bounds, half-even ties and caller-context isolation.
The parser and ratio helper share one quantity validator with unchanged T03
budgets. Source quantities preserve their full Decimal tuple; only derived ratios
use 34 significant digits and ROUND_HALF_EVEN, method
`finra-short-sale-volume-ratios/1`, unit 1.

The [offline reconciliation](evidence/t04_semantics_conformance.json) checks all
18 quantities and 12 ratios in the six unchanged retained rows. Samples remain
partial; no provider-data request, capture clock or promotion was added.
Full Gold aggregation/history, warehouse and publication remain later tasks.
**Verification:** 266 focused FINRA tests; full regression **748 passed, 10 optional
skips**; Ruff lint/format and source distribution/wheel builds pass. Next: SSV-T05.

- [x] Preserve whole/fractional shares; validate exempt <= short <= total with explicit zero cases.
- [x] Prove exempt is a subset using 40/5/100, fractional and large-quantity examples.
**Verify:** parameterized semantic tests and lossless decimal quantity round-trip assertions.
**Files:** `data/api.py`, `data/quantities.py`, `data/gold.py`,
`tests/test_ssv_semantics.py`; sanitized offline evidence is under `specs/spec012/evidence/`.

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
