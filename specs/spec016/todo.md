# Spec016 cross-repository tasks and acceptance tracker

Status: **T01–T09 complete; remaining implementation tasks open**. Scope/requirements:
[README](README.md); design/gates: [plan](plan.md); interfaces:
[data contract](data_contract.md). Task IDs are stable. Each row is one focused
slice with no more than about five intended files; split a row before starting
if inspection requires more. File names marked new are proposed, not existing.

Owners: **MM** = MarketMedallion, **QM** = QuantMeridian, **WEB** = research website.
Every row's acceptance is its definition of done; attach requirement-tagged test
and command evidence before checking it. Gate reviews assess concrete evidence.

## G1 — Baseline and port

| Done / task | Owner / depends on | Requirement | Acceptance and verification | Intended files / scope |
| --- | --- | --- | --- | --- |
| [x] T01 Capture reproducible donor baseline | MM / none | R01,R02,N01 | Generate independent small numerical expectations and donor golden outputs with input digests. Original 29 tests pass; inventory source permissions and dirty trees. | `source_inventory.json`, new `tests/fixtures/presidential_research/baseline.json`, new baseline test, source admission notes (4). |
| [x] T02 Port parser/config layer | MM / T01 | R01,N01 | Five source parsers retain IDs, missingness and source dates. Run adapted parser tests without network or sibling imports. | new package `__init__.py`, `config.py`, `parsers.py`, `quality.py`, `tests/test_presidential_parsers.py` (5). |
| [x] T03 Port assembly methods | MM / T02 | R01,R03 | Baseline/robustness/inherited/cycle pure outputs match donor including flags, rounding and exclusions. Run adapted build tests and golden comparisons. | new `assemble.py`, `provenance.py`, source config, `tests/test_presidential_build.py`, golden output fixture (5). |

**G1 checkpoint:** no new provider call; donor behavior and package imports agree.

## G2 — One publishable local dataset

| Done / task | Owner / depends on | Requirement | Acceptance and verification | Intended files / scope |
| --- | --- | --- | --- | --- |
| [x] T04 Archive local inputs | MM / T03 | R02,N02 | Immutable bytes and config receipts survive malformed parse; raw digest mismatch rejected. Test injected bad input and no-network import. | new `pipeline.py`, `storage.py`, `tests/test_presidential_archive.py`, receipt fixture (4). |
| [x] T05 Preserve approval gate | MM / T04 | R03,R02 | Only evidenced APPROVED rows with eligible manual sources promote. Test overlap, absent evidence, candidate source and missing coverage failures. | new `review_workbook.py`, `tests/test_presidential_controls.py`, invented workbook fixture, controls fixture (4). |
| [x] T06 Publish one atomic baseline | MM / T05 | R05,R06,N02 | One strict v1 dataset+sidecar/catalog/pointer is coherent; crash at each file/pointer boundary retains previous valid state. | new `publication.py`, `tests/test_presidential_publication.py`, schema fixture, `storage.py`, `pipeline.py` (5). |

**G2 checkpoint:** installed-package fixture path produces a recoverable immutable
snapshot without the donor checkout or provider access.

## G3 — Explicit pipeline load in PRES

| Done / task | Owner / depends on | Requirement | Acceptance and verification | Intended files / scope |
| --- | --- | --- | --- | --- |
| [x] T07 Pin language-neutral boundary | MM then QM / T06 | R06,N02 | Valid and adversarial fixtures have identical Python/TS outcomes. Verify unknown fields, numeric strings, hash mismatch, canonical numbers and exact bytes. | MM contract schema and test-vector JSON; QM contract pin and contract test (4 total; coordinated commits). |
| [x] T08 Implement local catalog/snapshot API | QM / T07 | R07,N02 | Fixed-root bounded reads validate digests, trusted peer/origin and immutable ID selection; no outbound request or subprocess. Route tests cover 400/403/503, traversal/symlink/race and changing pointer. | new `server/presidential/reader.ts`, `reader.test.ts`, two research `route.ts` files, combined route test (5). |
| [x] T09 Add explicit dataset loading | QM / T08 | R08 | Empty start, selected load/refresh, manual/demo flows and safe asynchronous replacement work. Browser test proves failed load retains dataset/config/notes and no late response wins. | `ResearchInputs.tsx`, `presidential-impact/page.tsx`, new `bundle.ts`, bundle test, `test/presidential-impact.spec.ts` (5). |

**G3 checkpoint:** first fixture publication is usable through the actual server
and browser. Existing analysis/export behavior remains verified. Final rich
pipeline provenance export is completed in T15.

## G4 — Full data publication and comparisons

| Done / task | Owner / depends on | Requirement | Acceptance and verification | Intended files / scope |
| --- | --- | --- | --- | --- |
| [ ] T10 Add captured versions and Parquet | MM / T06 | R04,N01 | Identical/corrected/reverted facts preserve proper version history and cutoff ordering. SQLite reopen and exact typed Parquet roundtrip pass with equal-clock fixtures. | new `warehouse.py`, `storage.py`, `pipeline.py`, two storage/version tests (5). |
| [ ] T11 Publish complete frozen bundle | MM / T10,T07 | R05,R09,R10 | All five datasets and required supplementary evidence are atomic and profile-bound. Missing required member blocks publication. Verify golden counts and source/approval bindings. | `assemble.py`, `publication.py`, frozen profile, publication test, comparison fixture (5). |
| [ ] T12 Add paired/inherited views | QM / T09,T11 | R09 | Exact sample joins reconcile to two independent engine runs; inherited fields expose individual assumed dates. Mismatch/unequal samples fail; symmetric sensitivity exclusions verified. | new `comparison.ts`, comparison test, paired component, `page.tsx`, browser test (5). |

**G4 checkpoint:** generated fixtures fully reconcile. Actual 88/87/87/68/68
frozen-window evidence is recorded only after real inputs are admitted in T20.

## G5 — Cycles and provenance exports

| Done / task | Owner / depends on | Requirement | Acceptance and verification | Intended files / scope |
| --- | --- | --- | --- | --- |
| [ ] T13 Validate/recompute cycle evidence | QM / T11,T07 | R10 | Strict ledger adapter reproduces 1–4 assignments, first-year/succession flags, statistics and deterministic ties. Fixture tests include 1936 exclusion and n<2 dispersion. | new `cycle.ts`, cycle test, cycle fixture (3). |
| [ ] T14 Render cycle workspace | QM / T12,T13 | R10 | Both constructions and all-years/flagged-transition treatments show reconciled counts and year-end assignment. Desktop/mobile keyboard browser checks; no changed primary grouping. | new cycle component, `page.tsx`, `ResearchFigures.tsx`, browser test (4). |
| [ ] T15 Export pipeline provenance and all views | QM / T09,T12,T14 | R11 | v2 replay includes bound producer/input evidence and exact view state; v1 stays readable. Numeric IDs stable across export time/notes; detached replay reproduces results; CSV/SVG/PNG label sample and method. | `exports.ts`, `exports.test.ts`, `ResearchEvidence.tsx`, new provenance adapter, provenance test (5). |

**G5 checkpoint:** producer stopped/original paths absent replay passes; fixture
cycle results and exported views agree. Do not claim real 86-window parity yet.

## G6 — Operations and migration handoff

| Done / task | Owner / depends on | Requirement | Acceptance and verification | Intended files / scope |
| --- | --- | --- | --- | --- |
| [ ] T16 Implement deterministic replay/recovery | MM / T10,T11 | R04,R13 | Empty-root replay preserves IDs/clocks/outcomes and snapshot selection; corrupted/missing inventory fails; interrupted publication recovers coherently. | new `replay.py`, `operations.py`, replay test, recovery test, `publication.py` (5). |
| [ ] T17 Add health and backup/restore | MM / T16 | R13 | No-request health distinguishes stale capture, inadequate data/approval coverage and publication failure. Backup restores verified state into empty destination without secrets. | `operations.py`, `storage.py`, operations test, backup test (4). |
| [ ] T18 Complete CLI/package surface | MM / T05,T17 | R01,R12,N01 | Nine donor command capabilities plus import/publish/replay/check are installed and isolated; source selects/options validated. Help/smoke and wheel resource tests pass. | new `cli.py`, `__main__.py`, root `cli.py`, `MANIFEST.in`, CLI/package test (5). |
| [ ] T19 Specify and implement bounded fetch | MM / T18 | R12,N03 | First record current official access/schema/transport evidence; then use shared run-scoped client with gates before probe. Injected transient/HTTP/oversize/partial-batch tests preserve all successful raw captures. Block unresolved live sources explicitly. | new `source.py`, source manifest, network config, source test, source-review note (5). |
| [ ] T20 Admit real frozen inputs and verify parity | MM then QM / T15,T18 | R02,R05,R09,R10,N03 | Copy only selected authorized archives/workbook/evidence with source/destination digests; donor/target rebuilds reproduce five sample counts and 86 cycle rows. Browser outputs and detached replay reconcile to those exact inputs. | ignored transfer receipts/evidence; spec acceptance report, browser test if a regression is found (≤3 tracked files). |
| [ ] T21 Register stopped orchestration and runbook | MM / T17,T18,T19 | R12,R13 | Local rebuild job cannot fetch; live job obeys rights/settings and all new schedules are stopped. Dagster definition tests pass; registers/runbook describe actual status and rollback. | new `dagster_defs.py`, orchestration aggregator, Dagster test, runbook, job register (5). |

**G6 technical checkpoint:** tests/builds, failure recovery, real-input parity and
PRES end-to-end evidence pass. If live-source admission is unresolved, record it
as blocked separately; local rebuild and complete consumer acceptance can pass.

## Ownership cutover and documentation

| Done / task | Owner / depends on | Requirement | Acceptance and verification | Intended files / scope |
| --- | --- | --- | --- | --- |
| [ ] T22 Update receiving repository handoff | MM / T20,T21 | R14,R13 | Register source package/status, verified commands/gates, release SHAs, health ownership and actual schedule state. Check links and reconcile every completion claim with evidence. | README, pipeline register, master handoff, this task tracker, acceptance report (5). |
| [ ] T23 Update PRES documentation | QM / T15,T20 | R08,R11,R14 | Help/whitepaper explain explicit snapshot workflow, paired/cycle assignments, limitations and replay. Module spec links here and release evidence; no false live/verified badge. | PRES help, whitepaper, module spec, module handoff (4). |
| [ ] T24 Retire duplicate donor implementation | WEB / T22,T23 | R14 | Canonical install/export instructions replace duplicate ownership after parity is accepted; old research artifacts/protocols remain inspectable. Verify old run exports and new clean-checkout commands; retain rollback release. | pipeline TRANSFER/README, project README/MASTER_HANDOFF, migration note (5); remove executable duplicates only in a separately reviewed follow-up patch with explicit inventory. |

**Cutover checkpoint:** all three repositories name one canonical pipeline and
one coordinated contract. Existing scientific results, manual imports and old
manifests remain reproducible. No unchecked optional backend or live deployment
is reported as complete.

## Requirement coverage and optional follow-on

R01→T01–T03,T18; R02→T01,T04,T05,T20; R03→T03,T05;
R04→T10,T16; R05→T06,T11,T20; R06→T06–T07;
R07→T08; R08→T09,T23; R09→T11,T12,T20;
R10→T11,T13,T14,T20; R11→T15,T23; R12→T18,T19,T21;
R13→T16,T17,T21,T22; R14→T22–T24;
N01→T01,T02,T10,T18; N02→T04,T06–T08;
N03→T19,T20. Prefix each ID with `PR016-` in evidence.

Optional tasks require separately scoped acceptance before execution: actual
PostgreSQL adapter parity; Delta/Databricks publication; selected source refresh
cadence and retention; authenticated hosted reads and redistribution; licensed
S&P 500 replication; reviewed presidential-period reconstruction. None is a
substitute for completing the local PRES integration tasks above.
