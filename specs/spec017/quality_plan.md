# FOMC quality implementation plan

Status: **Q01 accepted; Q02/Q03 implementation in progress** on `feat/fomc-text-quality`.
[Contract](quality_contract.md) · [Tasks](quality_tasks.md)

## Architecture and interfaces

Q01 adds a pure `data/quality.py` report builder and a pipeline-owned
`io/quality.py` adapter. These Q01 paths and the `quality-report` command are implemented; see
[acceptance](quality_report_acceptance.md).
The adapter pins the existing inventory/text/chunk generations using verified
catalog readers and a stable source-operation lock/read transaction, then hands
plain verified records to the pure builder. It must not call extraction,
chunking, HTTP preparation or acquisition. Do not directly query arbitrary
latest extraction rows, infer version order from filenames, or bypass original
byte/mirror validation. Fail on an incoherent snapshot.

A report has `schema_id`, `report_id`, profile, inventory/text/chunk catalog IDs,
scope, unique totals, grouped memberships and ordered finding rows. IDs hash
canonical JSON without execution wall clocks. Sort rows by extraction ID, unit
ordinal and issue code. Validate schema/profile/IDs on read, and reconcile totals
with the pinned source catalogs. Preserve date-only/unknown release precision and
actual capture clocks. Source-year membership uses explicit meeting associations;
a document associated with multiple years appears in each year's membership but
only once in the overall unique total. Issue groups likewise overlap explicitly.

The implemented `quality-report` command uses the existing FOMC CLI and
`scripts/run_fomc.sh`. Follow existing config/error conventions, with explicit
`--output` and bounded scope. Supported invocation and options are in [Q01 acceptance](quality_report_acceptance.md). The output root
must be distinct from canonical `captures`, `objects`, `text`, `operations`,
`attempts` and `attempt_outcomes` locations. Refuse conflicting existing content.
Config loading and reporting never resolve a webhook secret or select HTTP.

Q02 adds a pure deterministic case selector and a validated review-record format.
Use a versioned JSON sidecar/ledger in ignored quality output, not new warehouse
tables. Bind every record to one Q01 report and exact source/extraction/unit IDs.
A stale/mismatched record cannot become evidence for a newer profile/source.
Review records are additive evidence and do not rewrite extractor status or chunk
eligibility. Rendered PDFs/HTML evidence remains local and hash-bound; sanitized
review summaries describe sampled scope, unresolved strata and defects.

Q03 changes only the demonstrated extractor behavior and its exact profile.
Reuse immutable text/chunk storage and catalog selection after verifying that the
current implementation can select new derivatives coherently. Characterize any
missing publication/rollback capability before extending it; do not claim it
exists because a low-level catalog API is present. Keep source revisions and
extractor revisions distinct and preserve old-generation reads/replay.

## Delivery checkpoints

| Checkpoint | Evidence required before proceeding |
| --- | --- |
| Q01 accepted | Deterministic report, matching catalog counts, no-network/no-source-write proof, usable CLI/script and fixture/isolated-backend tests |
| Q02 accepted for a bounded batch | Hash-bound rendered-source comparisons, all chosen case dispositions, uncovered strata/unresolved cases explicit |
| Q03 accepted | Pre-fix reproduction, explained old/new differences, unchanged historical artifacts, coherent publication, restore/replay/rollback and healthy final state; or evidence-backed no-native-fix outcome |

Each slice gets a focused commit and acceptance record on this branch. A combined
PR may collect the three slices while retaining their separate gates. Do not mark
implementation tasks complete when only contracts or plans exist.

## Test and verification plan

Use reduced invented fixtures for issue combinations, blank/image/text PDFs,
legacy and modern HTML, shared/bundled meeting associations and multiyear inputs.
Use the existing FOMC backend fixture with disposable schemas for I/O checks.
Monkeypatch HTTP/probe, extraction and write entrypoints to raise on Q01 paths;
prove source table/pointer/file hashes unchanged. Test corruption and pointer
races as refusal paths, not just happy-path reports. Case selection must be stable
under shuffled inputs and include empty units without chunks. Test stale review
records, unknown dispositions, missing render hashes and unsafe output paths.

For Q03, record each failing baseline assertion before the fix. Compare the
accepted source profile/results with the candidate for all selected unchanged
inputs, account for every intentional difference and verify historical IDs.
Run focused tests first, then the regression suite and isolated FOMC backend
suite after source changes. Tests requiring PostgreSQL must name their isolated
configuration; do not export the operational database as a generic test DSN.

Current validation commands (from the checkout root):

```bash
python3 scripts/verify_agent_skills.py
python3 scripts/verify_quantsmith.py
.venv/bin/python -m ruff check src tests scripts
.venv/bin/python -m ruff format --check src tests scripts
env -u MARKET_DATA_TEST_POSTGRES_DSN -u FOMC_TEST_POSTGRES_CONFIG \
  .venv/bin/python -m pytest -q
# Use your own FOMC backend fixture config; tests create disposable schemas.
FOMC_TEST_POSTGRES_CONFIG=/path/to/your/fomc-test-config.yml \
  env -u MARKET_DATA_TEST_POSTGRES_DSN .venv/bin/python -m pytest -q tests/test_fomc*.py
.venv/bin/python -m build --no-isolation
bash -n scripts/run_fomc.sh
./scripts/run_fomc.sh --help
```

Q01/Q02 focused test module names and executable report/review commands will be
added with implementation; this package does not document invented working CLI
flags. Documentation-only packaging is verified with local-link checks,
requirement/task traceability and `git diff --check`, without source/warehouse runs.

## Acceptance-to-task traceability

| Contract criteria | Implementation tasks | Planned verification |
| --- | --- | --- |
| QAC01–02 | Q01.1–Q01.2 | Catalog/source identity, unique vs membership counts, coherent read/refusal tests |
| QAC03 | Q01.1, Q01.3 | Canonical output/ID and pinned-catalog reconciliation |
| QAC04 | Q01.2–Q01.3 | No-network/no-source-write, output boundary and CLI/script checks |
| QAC05 | Q02.1–Q02.2 | Stable bounded selection, strata coverage and stale-source rejection |
| QAC06–07 | Q02.1–Q02.3 | Actual rendered-source evidence and validated dispositions |
| QAC08 | Q03.1–Q03.2 | Demonstrated pre-fix failure and source comparison, or bounded no-fix evidence |
| QAC09 | Q03.2–Q03.3 | Immutable profile compatibility, exact citations and atomic failure/rollback |
| QAC10 | Q03.3 | Actual before/after restores, offline replay and namespace cleanup |
| QAC11–12 | Q01.1–Q03.4 | Package boundaries, meaningful fixtures/backend checks, lint/build/docs and evidence |

## Risks and decisions

- **False quality improvement:** chart presence is evidence of nontext content,
  not a proven OCR failure. Keep flags until a reviewed policy says otherwise.
- **Inflated counts:** overlapping chunks, issue codes and meeting years need
  separate unique and membership counts; reports reconcile both explicitly.
- **Stale review:** pin report/catalog/source/profile hashes and reject mismatches.
- **Snapshot races:** pin/verify all generations under the source operation lock;
  retain completed concurrent work rather than deleting it.
- **Broad parser changes:** use a minimal failing fixture, new profile and explicit
  full-selection comparison; never overwrite an accepted derivative in place.
- **Operational publication:** fresh restore-tested checkpoints, coherent pointers,
  verified old-generation rollback and offline replay precede acceptance.
- **OCR/dependency uncertainty:** record candidate cases and the unresolved decision;
  no engine/service selection or invented text in this package.

## Library evidence

The repository-local agent-skills snapshot verifies at
`f63ec56a3cc936408d792956ae583c3c96a825bd` (188 files/symlinks). Its spec,
planning/task-breakdown, Git and documentation workflows inform this package.
QuantSmith's checkout verifier currently reports revision/tree drift; the existing
pin is not changed. All 61 guidance Git objects at pinned revision
`41c56458f476a6c32fa922f7b8c70bb92e4fd21e` were independently hash-verified;
engineering principles, spec workflow, catalog and data-quality role supplied
read-only guidance. No hooks, runtime or sibling configuration were copied.
