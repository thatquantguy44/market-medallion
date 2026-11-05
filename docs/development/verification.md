# AAII slice verification

The latest package placement and checks are in the final **Pipelines namespace**
section. Earlier sections record historical layouts and verification runs.

Initial slice verified 2026-10-26 (America/New_York), Python 3.12 on macOS.
The Spec 003 completion results below supersede these initial counts/state.

- **54 tests passed** with `MARKET_DATA_TEST_POSTGRES_DSN` pointing to a dedicated
  disposable PostgreSQL 17.6 container; no live AAII request is part of the suite.
- Pure validation covers explicit units, rounding tolerance, missing/non-finite
  values, schema drift, duplicate/future dates, and malformed manifest fields.
- Local integration covers unchanged reruns, correction and reversion, as-of
  cutoff offsets, historical-feature recomputation, raw retention, replay,
  archive tampering, original rejection preservation, and root/backend binding.
- Snapshot tests cover typed dates/timestamps, row counts, partial publication,
  pointer stability after a mid-write failure, recovery export, and overlapping
  local writers without duplicate versions.
- PostgreSQL test verifies actual persistence, idempotency, revisions/reversion,
  failed-DQ isolation, Gold/as-of results, and transaction rollback.
- Dagster job materializes the fixture in process, exposes run metadata, and
  loads its stopped schedule. The CLI is tested via subprocess entrypoints.
- Ruff lint and formatting, `pip check`, and source/wheel package builds pass.
  Source distribution includes manifests and test fixtures.
- The README demo was executed and wrote a synthetic Bronze/Silver/Gold/audit
  dataset with committed Parquet files under ignored `data/aaii-demo`.

Review fixes included a root-anchored data ignore rule (source transformation
modules were previously hidden), strict chronological capture promotion,
promotion receipts so replay cannot accept previously rejected captures, and
warehouse identity checks. No vendored files or sibling configuration changed.

Not verified: recurring automatic download entitlement, hosted
GitHub CI execution, a deployed Dagster daemon/run launcher, or live Databricks
Delta/Unity Catalog behavior. Delta tests currently exercise input boundaries;
they do not establish live MERGE correctness. The runbook lists live acceptance
steps. No production services, schedules, accounts, or remote repository were created.

## Bounded live verification and configured PostgreSQL

An explicitly authorized live request to
https://www.aaii.com/files/surveys/sentiment.xls returned a real XLS workbook.
The inspected `SENTIMENT` sheet has a `Reported` label above the row-4
Date/Bullish/Neutral/Bearish header. Shares are fractions and the selected dates
are preserved exactly. Original response bytes, HTTP metadata, manifest and
selection bounds are retained in Bronze. Synthetic offline fixtures contain no
copied provider observations.

Live run ID: `2e02a48857a247918da76b6dd1a8c8cf`.

| Check | Verified result |
| --- | --- |
| Requested reported-date window | 2026-10-01 through 2026-10-26 |
| Persisted observation count | 4 Silver revisions and 4 Gold rows |
| Stored first/last reported dates | 2026-10-01 / 2026-10-22 |
| PostgreSQL endpoint | 127.0.0.1:55438 (container internal port 5432) |
| Database/schema | additional_market_data / aaii |
| Authenticated login / effective role | market_data / aaii_writer |
| Writer privileges | NOLOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE; owns aaii schema |
| Reimport of original bytes | 0 additional revisions |
| Parquet | Silver and Gold each contain 4 rows |
| Raw lineage | All four revisions match original byte SHA-256 |
| Publication timestamp | null; Reported Date is not an exact timestamp |

The local persistent Compose project `additional-market-data-live` is left
running. Its generated password is in ignored `.env` with mode 0600; runtime
settings are in ignored `config/config.yaml`. A machine-readable evidence record
is in `data/aaii-live/verification.json`. No credentials are present in this
record, tracked config, logs, or the package. The independent disposable test
database is removed after verification.

New offline checks cover config precedence, escaping of secret values, invalid
ports/roles/schema/SSL settings, XLS bounds and missing/error cells, schema drift,
reported-date isolation, and retention of HTTP failure responses. Routine
scheduled acquisition remains disabled pending entitlement review.

## Local workflow snapshot and stopping checkpoint

The original workflow submodule was replaced by 188 ordinary files/symlinks at
the identical revision `f63ec56a3cc936408d792956ae583c3c96a825bd` under `agents/`.
The original parent gitlink and checkout HEAD matched before export. Every copied
file/symlink was compared with that clean checkout. The offline checksum and
both library validators passed (24 skills, no errors). There is no remaining
gitlink or `.gitmodules`; CI now verifies the local snapshot without refreshing it.

The final read-only database check confirmed four Silver and four Gold rows,
reported dates 2026-10-01 through 2026-10-22, with the local live service healthy.
Snapshot verification, Ruff and staged/unstaged whitespace checks passed at
closeout. `docs/handoffs/aaii_checkpoint.md` records the next recommended scope
and the local resources left in place. No new AAII request was made for closeout.

## Hosted verification after push

The five implementation/checkpoint commits were pushed to `origin/main`.
GitHub Actions run `40434195208` for `c422c5d` was blocked before executing any
steps. Its check annotations report failed recent account payments or an Actions
spending-limit issue. This provides no hosted verdict on installation, tests or
build; the last local full suite passed all 54 tests. The account owner needs to
resolve the execution allowance before retrying CI. No billing settings, tests,
or verification gates were changed in response.

## Repository identity update — 2026-10-30

The repository and Python distribution are named `market-medallion`
(MarketMedallion). The request identifier and documentation use that identity.
The stable `market_data_pipeline` import path and `market-data` CLI are retained;
`market-medallion` is an additional CLI entrypoint. Local PostgreSQL, schema,
roles, Compose project/volume names, archived bytes and historical metadata are
preserved. The renamed checkout retains a compatibility symlink at the previous
path for existing absolute references.

Rename verification passed from the new checkout: GitHub reports
`thatquantguy44/market-medallion`, origin uses its canonical URL, installed
metadata reports `market-medallion` 0.1.0, and both CLI entrypoints validate the
manifest. The offline suite passed 53 tests with the isolated PostgreSQL test
skipped; lint, formatting, dependency checks, source/wheel builds, and workflow
snapshot integrity passed. Read-only inspection confirmed four live Silver and
four Gold rows, and all recorded Bronze/file/snapshot paths still resolve. No
provider data was fetched, and no database, role, port or persistent volume was
renamed.

Hosted verification also passed for rename commit `88e0354` on 2026-10-30:
[GitHub Actions run 41113974074](https://github.com/thatquantguy44/market-medallion/actions/runs/41113974074).
The hosted job passed all 54 tests, including isolated PostgreSQL integration,
snapshot verification, lint/formatting, and source/wheel builds. This supersedes
the original account-level execution block; no billing settings were changed.


## Spec 003 AAII completion — 2026-10-30 America/New_York

- Full suite: **68 passed**, including both real PostgreSQL tests against a
  dedicated disposable PostgreSQL 17.6 instance on localhost 55439. No test
  contacts AAII or writes to the live warehouse. Without the DSN: 66 passed,
  two PostgreSQL tests skipped.
- Synthetic binary XLS workbooks exercise Dagster's real XLS success/failure
  paths, idempotency, correction/reversion, as-of results, materializations and
  exact raw/HTTP/selection retention. Fixture generation uses optional isolated
  xlwt tooling; it adds no runtime dependency. The source package includes XLS.
- Operational tests verify New York overlap, stale/failed/partial state handling,
  connection-failure health, archived transient responses, Retry-After pacing,
  and stopped live schedule gating/dynamic bounds. Configuration defaults keep
  routine acquisition disabled.
- Historical tests verify strict rejection, explicit quarantine, partial status,
  CLI exits, original first-seen times, gaps and deterministic backfill replay.
- Recovery tests verify inventory/digest rejection, empty-target enforcement,
  Silver/Gold equality, physical audit pointers, distinct warehouse identity,
  PostgreSQL isolation and rollback, dry-run retention and current/Bronze protection.
- Ruff lint/formatting, source/wheel build, dependency checks and pinned library
  integrity pass. No dependency or vendored snapshot was changed.

The existing authorized workbook was imported offline into the unchanged live
PostgreSQL target after backing up and restoring the original four-date state to
an isolated PostgreSQL schema. Backfill run `28556d16d7ee4934bf80e8cd5a521120`
accepted 2,046 reported dates, 1987-07-24 through 2026-10-22, inserted 2,042
versions, and quarantined three invalid rows. The repeat import inserted zero.
The completed history was backed up, restored to SQLite and replayed from Bronze;
all Silver versions and Gold results matched. Publication timestamps remain null.
Machine-readable evidence is ignored `data/aaii-live/completion-verification.json`.
Only a dry-run retention was performed on the live root. No new AAII fetch,
schedule enablement, cloud deployment or data redistribution occurred.

The three exclusions are 1987-06-26, 1987-07-17 and 1996-01-11, all
missing/nonnumeric shares. Nine gaps over eight days remain visible. Both
backfill runs have partial audit status; the health readout intentionally reports
that trailing partial outcome until a later clean import. Historical observations
were first seen at the actual import, not reconstructed publication dates.

Recurring AAII acquisition rights, deployed daemon/alert routing, off-machine
backup policy and live Databricks remain external prerequisites. CFTC remains
unimplemented and starts on the next separate branch.

Hosted completion verification passed for `0a18288`:
[GitHub Actions run 41153586929](https://github.com/thatquantguy44/market-medallion/actions/runs/41153586929).
The clean Linux checkout passed all 68 tests, including PostgreSQL, pinned
snapshot integrity, Ruff lint/formatting and source/wheel builds. The completion
branch is pushed; no merge into main or recurring schedule enablement occurred.

## Pipeline-owned source organization — 2026-10-30

The user requested a folder per pipeline and a master pipeline document.
AAII's full implementation now lives under `src/market_data_pipeline/aaii/`.
Only provider-independent file/time/lock and YAML helpers live in `shared/`.
The existing repository CLI/console scripts and Dagster module remain supported;
`python -m market_data_pipeline.aaii` is also available. Internal imports and all
owned callers/notebook/runbook examples were migrated; see the layout guide.

- **69 tests passed**, including both integration tests on a fresh dedicated
  PostgreSQL 17.6 container on localhost 55439. The extra case runs the existing
  CLI import/as-of/replay/error flow through the pipeline-owned module too.
  Both root and pipeline-owned Dagster definitions resolve the same assets.
- Ruff lint/formatting, package builds, dependency checks and pinned snapshot
  integrity pass. No dependency or vendored file changed.
- A clean wheel inspection confirmed the canonical AAII/shared modules are
  included and the old internal modules are absent. Both module commands were
  exercised from the unpacked wheel, isolated from the source checkout.
  Both installed console entrypoints also validate the manifest.
- An AST comparison of 15 moved modules confirmed their behavior bodies are
  unchanged after excluding import nodes. Shared file/YAML helpers were moved
  from the existing implementation without changing their contracts.
- A read-only check of the existing live PostgreSQL target confirmed 2,046
  Silver/Gold rows, four audit runs and reported dates 1987-07-24 through
  2026-10-22. The committed Parquet Gold still has 2,046 rows. No live data,
  identity, archive, parser-version string, table or schedule was migrated.
- `docs/pipelines.md` contains 20 unique concrete pipeline/candidate IDs, retains
  every roadmap ID, distinguishes actual packages from absent/proposed packages,
  and links the current AAII contract/runbook/handoff. New organization-document
  links were checked locally. Separate uncommitted FX/web specs remain outside
  this refactor and are identified as local drafts in the register.

The disposable PostgreSQL test container was removed after verification; the
existing live service/volume remains available. No provider request, cloud
provisioning or schedule enablement occurred. CFTC implementation remains the
next separate branch after review/merge of AAII. The empty `docs/handoff.md` and
separate roadmap/spec draft edits remain user work.

## Configurable QuantSmith guidance reference — 2026-10-30

The user authorized a local QuantSmith library reference alongside the existing
agent-skills snapshot. `agents/README.md` now documents discovery, task routing,
path overrides, exact pin scope and compatibility with this project's contracts;
root `AGENTS.md` directs agents to use both libraries where relevant.

`agents/quantsmith.reference.json` records the supplied local checkout,
`MARKET_MEDALLION_QUANTSMITH_PATH`, commit
`5deea85438f6d46a8a14ddb85e3821a65fa62f4e`, tree
`db9b442f9ef97ad94c8fe05cae483d4d28344165`, and SHA-256 digests for 61 selected
guidance files. Each file was compared with its committed bytes when registered.
This is a local-reference pin of adopted guidance, not a vendored/runtime pin.
The external checkout's unrelated uncommitted drafts remain outside that pin.

- Five fixture-based Git tests pass: default/environment/explicit path selection,
  relocation with spaces, missing path handling, guidance drift at the same HEAD,
  changed commit rejection, unrelated-work preservation and path-boundary checks.
- Full offline suite: **72 passed, two PostgreSQL tests skipped**. The pipeline
  runtime is unchanged; hosted CI can run those two on its isolated service.
- Ruff lint/formatting and both real local library verifiers pass. agent-skills
  remains unchanged at its existing revision with all 188 files/symlinks verified.
- The QuantSmith resolver uses local Git and file reads only, with optional Git
  index writes disabled. It does not fetch, reset, copy SDK credentials/config,
  install hooks, import SDK runtimes or modify the external working tree.
- The verifier is included in the source distribution for its fixture tests;
  machine-specific QuantSmith availability is not required by hosted CI.

No dependencies, application code, data, schedules, external library files or
vendored snapshot contents changed. Separate roadmap/FX/web specification drafts
remain untouched.

## QuantSmith reference refresh — 2026-11-05

The reviewed local-reference pin advanced from
`5deea85438f6d46a8a14ddb85e3821a65fa62f4e` to
`41c56458f476a6c32fa922f7b8c70bb92e4fd21e`, tree
`9be0ada464c3a96268677d3b32e90c0be11beeb6`. The adopted inventory remains 61
guidance files. Only QuantSmith's root `README.md` and `agents/README.md` changed
within that inventory; their committed diffs update catalog counts, newer FX
workflow/harness status and the FX orchestrator listing. No routed engineering,
pipeline, provenance, quality, database or test-role instruction changed.

All selected guidance files match committed `HEAD` bytes. The external checkout's
unrelated local securities-finance drafts remain untouched and are reported as
`other_worktree_changes: true`; they are not hashed or adopted. The real
QuantSmith verifier, the unchanged 188-file agent-skills verifier and all five
fixture-based reference safety tests pass. The full offline suite is **461 passed,
8 opt-in skips**; Ruff, formatting and source/wheel builds pass. No external
checkout file, dependency, runtime configuration, data source, schedule or
application code changed.


## Pipelines namespace — 2026-10-30 America/New_York

AAII now lives under `src/market_data_pipeline/pipelines/aaii/`, imported as
`market_data_pipeline.pipelines.aaii`. This supersedes the direct `aaii/` layout
recorded above. Future implementations must use a separate snake_case subpackage
under `src/market_data_pipeline/pipelines/`; root agent guidance, the register,
layout guide, handoffs and planned FX/web-traffic paths all specify this rule.

- Offline suite: **72 passed, two PostgreSQL tests skipped**; no dedicated
  PostgreSQL test DSN was supplied for this package-only move. Both root and
  pipeline module CLI flows and the root Dagster forwarding remain covered.
- Repository-wide Ruff lint/formatting, dependency check and source/wheel builds
  passed. All 21 moved Python modules have identical behavior trees after
  excluding import statements.
- The clean wheel contains `pipelines/aaii/` and no former direct `aaii/` package.
  In isolated Python subprocesses outside the checkout, both module CLIs validate
  the manifest and import/publish the two-row synthetic fixture. Root and
  pipeline-owned Dagster definitions resolve to the same object from the wheel.

Only source placement, imports and documentation changed. Stored payloads,
identifiers, parser versions, tables and schedules retain their existing contracts.
No provider request, live warehouse write or recurring schedule was involved.


## Spec011 T22 threshold Delta — 2026-11-05

Branch `feat/spec011-threshold-delta`, aligned with merged `main` at `0169b4c`
(prerequisite checkpoint `46c5932`, PR #6).
The source-owned publisher and typed immutable staging snapshot satisfy T22/A15
without importing SEC schemas or requiring Spark for normal local commands.

- Full offline regression: **477 passed, 10 opt-in skips**. The skips are existing
  optional PostgreSQL/SEC Delta cases plus the two new threshold engine tests.
- Actual threshold engine suite: **2 passed** on Python3.12, OpenJDK17,
  Spark4.0.1 and Delta4.0.1. Full normalized table equality covers independent
  Nasdaq/NYSE identity, exact revision keys/UTC microseconds, corrections,
  reversions, withdrawals, equal-time capture cutoffs, complete-empty/unavailable/
  failed coverage, gap/censoring episodes and empty membership/episode tables.
- A failure after membership publication leaves the previous pinned generation
  readable. A failed pointer update after sealing all four versions also preserves
  the previous generation; retry reuses the exact sealed metadata and versions.
  Repeat publication is idempotent; source changes and sequence rollback fail.
- Offline staging tests exercise corruption, missing/unfinished warehouses,
  row budgets, no-request CLI staging, config enablement/identifier/volume gates,
  source identity and deterministic snapshot identity across database tie order.
- Repository Ruff lint/format, `pip check`, sdist/wheel builds and agent-skills
  integrity verification pass. No dependencies, database schema or schedules changed.
- QuantSmith verification fails because its current external revision/tree differs
  from the pin. The external checkout/reference was not changed or treated as
  verified guidance. This failure does not affect application checks.

Reproduce the engine run from this checkout with the installed optional Delta
runtime and existing Homebrew JDK (use your own JDK path on another machine):

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 MARKET_DATA_TEST_DELTA=true \
.venv/bin/python -m pytest -q tests/test_reg_sho_delta_engine.py
```

All fixtures contain invented provider-shaped values. No real provider request,
Databricks/Unity Catalog workspace write, cloud object, schedule activation or
NAAIM resume is part of this verification. The original provider payloads,
warehouses, configs and secrets remain unchanged. T17 source-evidence/provider-use
review and G2 acceptance remain outstanding before Spec012.


## Spec011 T17 bounded research delivery — 2026-11-05

Branch `chore/spec011-t17-acceptance`, from merged `main` `5b092af` (PR #7).
The [acceptance record](../../specs/spec011/acceptance.md) maps every A01–A15
criterion to concrete evidence, source-use attestation and selected exclusions.

- Final full repository suite with actual disposable PostgreSQL17.6,
  OpenJDK17, Spark4.0.1 and Delta4.0.1 enabled together: **492 passed, zero skips**.
  The [machine-readable record](../../specs/spec011/evidence/verification.json)
  binds counts/versions to implementation, tests and review-script hashes.
- The two approved SEC September archives reconcile every field and both
  trailers, all117,025 facts and raw lineage against retained facts and Gold.
  Original source/snapshot digests remain unchanged. No original retrieval or
  historical publication clock is invented; coverage remains PARTIAL.
- All ten authentic October26–30 Nasdaq/NASDAQ and NYSE/NYSE files reconcile
  source fields, row counts/trailers, original Bronze bytes, normalized values,
  coverage and capture clocks. Counts are404Nasdaq +4NYSE; NYSE October30 is
  valid complete-empty. The user attested verbal provider permission; independent
  documentary license verification is not claimed.
- Real ten-capture threshold replay and export pass with408membership rows,
  ten coverage rows and93observed episodes; typed Delta staging also verifies.
  The ignored local ZIP preserves all ten source payload hashes and provenance.
- Conformance discovered NYSE HEAD/content-negotiation behavior. Offline
  regressions first reproduced the failure, then verify the source-page probe,
  exact configured GET bytes/provenance, one route selection, startup rejection
  and manifest mismatch gating. Actual subsequent NYSE captures all succeed.
- Additional characterization covers overlapping/unknown identity mappings and
  an explicit holiday/weekend episode calendar. No provider data semantics or
  warehouse schemas changed.
- Repository Ruff lint/format, `pip check`, package builds and workflow-snapshot
  integrity pass. CI now supplies an explicit disposable database password in
  the service and DSN, so both source-specific PostgreSQL suites run rather than
  skip for a missing password field. QuantSmith's external revision/tree mismatch remains recorded;
  its checkout/reference were not changed or counted as verified guidance.

The controlled network work was the selected threshold capture and its bounded
method diagnosis; unit/backend tests made no provider requests. All source bytes
and raw security rows remain local/ignored; only sanitized aggregate evidence is
committed. The local acquisition config is disabled again. No production database,
Databricks workspace write, cloud deployment, schedule activation or NAAIM resume
occurred. G2 is complete for the selected internal-research native artifacts,
subject to review/merge; G3/G4 and optional T14 remain separate. Next: Spec012.
