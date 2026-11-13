# FOMC quality slice tasks

Status: **Q01 accepted; Q02 in progress; Q03 planned**.
[Q01 acceptance](quality_report_acceptance.md) records the first working slice.
[Contract and capability map](quality_contract.md) · [Plan](quality_plan.md)
Branch: `feat/fomc-text-quality`, from merged main `1bd4ab5`.

## FOMC-Q01 — deterministic offline quality report

- [x] **Q01.1 — characterize report grain and counts** (QAC01–03, QAC12).
  Add fixtures/tests for shared/multiyear representations, overlapping issue
  memberships/chunks, all-clean inputs, blank units without chunks, extraction
  failures and unknown issue codes. Acceptance: exact unique/grouped counts,
  preserved citations/clocks and stable identities under shuffled input.
  Proposed files: `tests/test_fomc_quality.py`, reduced `tests/fixtures/fomc_meeting_minutes/`
  inputs, `data/quality.py` under the FOMC package. Verify pre-implementation
  failures, then focused tests for the pure builder.
- [x] **Q01.2 — verified read-only reporting** (QAC01, QAC04, QAC11–12).
  Pin coherent generations, verify originals/mirrors, build the report and refuse
  unsafe/conflicting output. Acceptance: no HTTP, extraction, source write or
  pointer change; corrupted/stale evidence fails. Proposed files: `io/quality.py`,
  `tests/test_fomc_quality_postgres.py`; reuse existing backend fixtures. Verify
  isolated PostgreSQL success/corruption/concurrency cases and unchanged source
  hashes/pointers.
- [x] **Q01.3 — CLI/script and first acceptance** (QAC03–04, QAC12).
  Add/report the supported command through the existing CLI/script; document
  other-user setup and exact options. Acceptance: accepted local archive report
  reconciles with its pinned catalogs and has a sanitized acceptance summary.
  Proposed files: FOMC `cli.py`, CLI tests, runbook, Q01 acceptance/evidence.
  Verify CLI exit/output/error behavior, offline script smoke and focused tests.

**Q01 checkpoint:** all report ACs have passing evidence; refresh master register
and current pickup. Preserve the dated backfill acceptance. Proceed to Q02 using
this exact report ID rather than an unpinned later catalog.

## FOMC-Q02 — source-page review and dispositions

- [ ] **Q02.1 — deterministic case selection and review records** (QAC05, QAC07,
  QAC11–12). Select at most 12 cases per batch with issue/format/profile strata,
  early/late coverage and explicit uncovered strata. Validate hash-bound review
  records and reject stale/unknown records. Proposed files: FOMC
  `data/quality_review.py`, `tests/test_fomc_quality_review.py` and minimal review
  fixtures. Verify shuffled input, empty strata, cap/truncation disclosure,
  duplicate cases, source/profile mismatch and unknown dispositions.
- [ ] **Q02.2 — inspect the selected original pages** (QAC05–07).
  Verify original hashes, render the named archived PDF pages and inspect HTML
  with archived assets/network disabled. Record tool versions, render hashes,
  observed text/citation differences, disposition and actual review time.
  Acceptance: every selected case has evidence; unresolved/uncovered scope stays
  explicit and no eligibility/status is silently changed. Proposed artifacts:
  ignored local review ledger/renders plus sanitized Q02 review evidence.
- [ ] **Q02.3 — publish the bounded review findings** (QAC06–07, QAC12).
  Record confirmed native defects separately from charts, OCR candidates and
  unresolved cases; add reduced reproduction fixtures for Q03 candidates.
  Acceptance: review summaries link to exact report/source/profile/page IDs and
  make no whole-corpus accuracy claim. Verify evidence hashes, local links and
  review-record tests; update Q02 acceptance, runbook and current pickup.

**Q02 checkpoint:** close the selected review batch with its limitations; select
only demonstrated native defects for Q03. More review batches and OCR-only cases
remain explicit backlog, not fabricated extraction work.

## FOMC-Q03 — confirmed extraction defects and versioned derivatives

- [ ] **Q03.1 — pre-fix regression evidence** (QAC08–09, QAC12).
  For each selected confirmed defect, bind a reduced fixture to the Q02 evidence
  and capture the baseline failure. Acceptance: the assertion detects the observed
  missing/corrupted text or citation behavior and passes only after a real fix.
  Proposed files: focused FOMC extraction regression tests/fixtures. If Q02 found
  no confirmed native defect, record that bounded no-fix result and its evidence;
  leave unresolved/OCR work visible rather than inventing a change.
- [ ] **Q03.2 — narrow transform/profile changes** (QAC08–09, QAC11–12).
  Fix only confirmed behavior; use new immutable profiles/derivatives and compare
  the complete selected unchanged-source input set. Acceptance: every changed
  extraction is explained, historical artifacts/IDs remain readable and exact
  source spans/citations survive. Likely files: FOMC `data/text.py` and the narrow
  dependency/parser helper actually implicated by Q02, plus regression tests.
  Verify focused red/green cases, compatibility diffs and full regression suite.
- [ ] **Q03.3 — publication and recovery qualification** (QAC09–10, QAC12).
  First prove coherent candidate publication and prior-pointer preservation on
  failure using isolated backend fixtures, including old-generation rollback.
  Then restore-test a fresh operational checkpoint, publish verified derivatives
  under the source lock, and verify originals/clocks/history, health, chunks,
  post-publication restore and empty-target offline replay. Acceptance: all
  checks match and disposable targets are removed. If the no-native-fix path was
  taken, record publication/recovery as not required with its reason, not passed.
- [ ] **Q03.4 — closeout and PR tracking** (QAC08–12).
  Record actual before/after counts, changed profiles, remaining flags, tests,
  source/render evidence and recovery results. Update acceptance/evidence,
  master register, runbook and handoff. Verify lint/format, required regression/
  isolated-backend tests, builds and documentation links; commit focused slices,
  push and track current-head PR checks/reviews. No merge or schedule activation.

**Final checkpoint:** Q01/Q02/Q03 status reflects executed evidence; original
backfill counts stay historical. Unresolved extraction/OCR/source-availability
work remains named. Both FOMC status sensors retain their existing state; NAAIM
remains paused and acquisition schedules remain stopped.
