# Spec 016 — Presidential data pipeline port and complete PRES integration

Status: **local implementation and frozen-history parity verified on feature branches**; live acquisition and merged-release cutover remain separate.
Created: **2026-11-04**, America/New_York.
Capability ID: `presidential-research-data`.
Target package: `src/market_data_pipeline/pipelines/presidential_research/`.

This is the authoritative multi-repository specification. Read the
[data contract](data_contract.md), [implementation plan](plan.md),
[task tracker](todo.md), and [inspection inventory](source_inventory.json).
For current acceptance evidence and remaining release boundaries, see the
[acceptance record](acceptance.md). NAAIM has not been resumed.

## Objective and definition of complete

Move ownership of the existing Presidential Impact acquisition, parsing,
political-control review and dataset assembly from the research website into
MarketMedallion. Integrate its validated, versioned outputs into QuantMeridian's
**PRES** module so a researcher can explicitly select a published dataset,
inspect its provenance, compare the two public return constructions, explore
inherited conditions and presidential cycles, and export a reproducible run.

Complete means the **local end-to-end research workflow** passes gates G1–G6 in
the plan. Copying the Python directory or importing one JSON file is only an
intermediate delivery. Hosted/public distribution and recurring acquisition
have separate source-rights and operational gates and are not prerequisites for
local completeness. No historical return series becomes independently verified
merely because this integration succeeds.

## Original repository inspection (2026-11-04)

Paths below are workspace discovery references, never runtime dependencies.

| Repository | Observed location / revision | Current responsibility |
| --- | --- | --- |
| `thatquantguy44/thatquantguy44.github.io` | `websites/thatquantguy44.github.io`, `37c9307e86711ffadc391b6cc49079812fb09c14` | Research protocols and a working standalone `docs/research/002_PresidentialImpact/pipeline/` package. |
| `thatquantguy44/market-medallion` | `pipelines/market-medallion`, `4006ff0bb5779398edb183838bb4db3143ef45a8` | Destination pipeline and publication owner. The legacy `pipelines/additional-market-data-pipelines` path is a compatibility symlink to this checkout. |
| `thatquantguy44/QuantMeridian` | `platforms/market_terminal`, `f0b00a383f14f8439bb7c5aa961a1e199edcdab6` | PRES UI, strict dataset parser, descriptive engine and browser exports. |

MarketMedallion and QuantMeridian contain unrelated uncommitted work at inspection.
The inventory hashes relevant working-tree files as well as recording HEAD;
these are inspection baselines, not a claim that all observed content is committed.
Implementation must establish isolated branches and record the actual merge SHAs.

At the original inspection, the donor package exposed nine CLI commands, generated-fixture tests,
raw fetch manifests, workbook approval export, five PRES-compatible datasets,
paired ledgers and cycle outputs. It has no MarketMedallion warehouse/revision
adapter, atomic multi-file publication protocol or source-owned Dagster integration.
At that inspection, PRES started empty, accepted a single local JSON up to 5 MiB, and used
`METHOD_VERSION = presidential-impact/0.2.0`. It did not yet have the local pipeline catalog reader or native paired/cycle
workflow. See [acceptance](acceptance.md) for their current implementation.

## Capability map and ownership

| Stable module ID | Owner | Responsibility | Depends on |
| --- | --- | --- | --- |
| `presidential-source-port` | MarketMedallion | Offline parsers, source catalog, reviewed controls, frozen assembly methods. | Donor inspection baseline only. |
| `presidential-publication` | MarketMedallion | Immutable captures, observed versions, warehouse/Parquet, committed research bundle, health/replay. | `presidential-source-port`. |
| `presidential-pres-consumer` | QuantMeridian | Local snapshot reader, explicit selection, current engine analysis, native comparisons/cycle, provenance exports. | Versioned `presidential-publication` contract. |
| `presidential-research-handoff` | Research website | Protocols, historical results, migration/deprecation pointer and author interpretation. | Verified publication and consumer deliveries. |

Build order: source parity → local publication → explicit PRES load → complete
research views → operational hardening → donor handoff. Runtime arrows go from
published artifacts into PRES; PRES never imports Python code or invokes a
fetch/build subprocess. MarketMedallion never imports application components.

## Scientific compatibility boundary

The frozen profile is `public-reconstruction-2026-11-03` with explicit
`decision_policy=window_start`. New data captures use a separately selected
profile; a later available year must not silently extend the frozen sample.

| Output | Frozen sample | Meaning |
| --- | --- | --- |
| `crsp-market-proxy-annual.json` | 1936–2023, 88 windows | Full CRSP-derived public market proxy. |
| `crsp-market-proxy-common-sample-annual.json` | 1936–2022, 87 windows | Baseline aligned to the Shiller reconstruction. |
| `shiller-sp-composite-annual.json` | 1936–2022, 87 windows | Separately labeled dividend-reinvestment-style approximation. |
| `crsp-market-proxy-inherited-conditions-annual.json` | 1955–2022, 68 windows | Baseline with frozen at-origin fundamentals. |
| `shiller-sp-composite-inherited-conditions-annual.json` | 1955–2022, 68 windows | Matching robustness sample with the same fundamentals. |
| `presidential-cycle-ledger.csv`, `presidential-cycle-summary.json` | 1937–2022, 86 paired windows | Supplementary end-of-year presidential assignment and transition flags. |

These counts are historical acceptance anchors from the donor handoff. Reproduce
them only from the corresponding frozen, checksummed inputs; invented fixtures
need their own expected counts. The current config's end year of 2023 and the
shorter Shiller sample are intentionally different.

Preserve these methods without “cleanup” changes during the initial port:

- Fama–French nominal market return is `(Mkt-RF + RF) / 100`. Compound monthly
  returns into index levels and annual endpoint returns; never add annual factors.
- Shiller monthly approximation is `(P[t] + D[t]/12) / P[t-1] - 1`, where `D` is
  an annual dividend measure. Preserve source-ID gates, missing-dividend failure,
  last-complete-December cutoff and the existing numeric formatting in parity.
- Annual windows request prior December 31 through current December 31. Political
  control is selected at the supplied decision date in half-open intervals.
  Mixed control remains a diagnostic, including succession changes.
- Frozen inherited inputs for return year Y use prior October CAPE (month-end,
  assumed available November month-end) and prior October FEDFUNDS (October 1,
  assumed available November 1). The composite record uses CAPE's later dates;
  individual dates remain in the audit. These are assumed availability dates,
  not recovered release vintages.
- Cycle year is `((Y - 1) % 4) + 1`, starting in 1937. Assign the return
  to the president in office on December 31; flag every first-cycle year as an
  inauguration transition and preserve a separate president-change flag.
  This never overwrites the primary window-start political assignment.

`PRESIDENTIAL_PERIOD` remains supported for independently supplied reviewed
windows. The donor's reserved empty `terms.csv` is not missing data to invent.
Exact seed-article S&P 500 replication remains unresolved and separately scoped.

## Requirements and acceptance

Each requirement ID is also its acceptance identifier; tests/evidence must cite it.

| ID | Required behavior and acceptance evidence |
| --- | --- |
| PR016-R01 | Port into one pipeline-owned snake_case package; preserve source IDs, semantics and all nine CLI capabilities. Original 29 offline tests plus normalized-output differential fixtures pass without sibling runtime imports. |
| PR016-R02 | Preserve immutable raw bytes, configs, review workbooks, approvals and manual evidence with hashes. Missing or mismatched inputs block reproducible publication; no absent hash is fabricated. |
| PR016-R03 | Review-only House/Wikipedia candidates cannot become controls. Only APPROVED rows with locators, evidence, reviewer, time and eligible manual source IDs promote; intervals must cover required decision dates. |
| PR016-R04 | Implement source-owned Silver revisions, deterministic ordering, SQLite and typed Parquet. Duplicate captures add audit receipts without duplicate facts; correction and reversion preserve both versions. Replay preserves original clocks and terminal outcomes. |
| PR016-R05 | Publish the five datasets and all associated comparison/fundamental/cycle evidence as one atomic, hash-verified generation. Interrupted writes cannot expose a mixed generation or advance success. |
| PR016-R06 | Keep `ResearchDataset.schemaVersion=1` strict and unchanged; use a separate versioned bundle/provenance contract. Python-produced valid/invalid fixtures pass the same acceptance/rejection checks in TypeScript. |
| PR016-R07 | Add explicitly enabled local catalog/snapshot reads with no provider HTTP, shell execution or arbitrary filesystem paths. Disabled/unavailable/invalid snapshots produce actionable states and never silently load a demo. |
| PR016-R08 | PRES begins empty, offers explicit dataset selection/load/refresh, retains manual JSON and illustrative actions, and keeps the last valid selection and notes on failed or superseded loads. The full baseline and common sample are visibly distinct. |
| PR016-R09 | Native paired-return and inherited-condition views use matched window IDs/dates, show each series and its sample/exclusions, and reconcile with each independent engine run. No pooled or unequal-sample party comparison. |
| PR016-R10 | Native cycle view has all-years and transition-flagged treatments, party × cycle summaries and both return constructions. Reconcile rows/statistics to the pipeline ledger; keep end-of-year assignment and succession flags visible. |
| PR016-R11 | Exported manifests link dataset, config, engine method, producer code/input provenance and snapshot identity. Numerical run identity remains based on normalized dataset/config/method; provenance has its own identity. CSV/SVG/PNG retain sample, construction, units and limitations. |
| PR016-R12 | Source-owned CLI/Dagster share gates and run-scoped HTTP configuration. Explicit fetch is separate from parse/build/load; local/replay/health paths cannot probe the network. New schedules are stopped. |
| PR016-R13 | Provide no-request health, coverage checks, backup/restore, interrupted-publication recovery and rollback evidence. Capture freshness, source observation coverage and approval coverage are distinct. |
| PR016-R14 | After parity and consumer acceptance, document canonical ownership and retire duplicate donor implementation through a tested transition; preserve frozen research results and protocols. |
| PR016-N01 | Python 3.12 target, pure transforms, pipeline-owned schema/storage/HTTP adapters; use existing pinned dependencies and shared helpers only where contracts fit. No sibling config, credentials or databases copied. |
| PR016-N02 | Reject oversized/malformed inputs and path escapes before use. Default dataset cap stays 5 MiB and each record array stays at 100,000; bundle limits are defined in the data contract. Failures retain the previous published/loaded valid state. |
| PR016-N03 | Source access/retention/redistribution decisions are recorded per source and output scope. Existing captures are not proof of recurring or public distribution rights. New live adapters require current official evidence first. |

## Sources and admission

Retain `FF_MKT_001`, `FEDFUNDS_001`, `SHILLER_001`, `HOUSE_GOV_001`,
`WIKI_CONGRESS_001`, `CTRL_074_A` and `CTRL_GOV_001`. The last two are disabled
manual evidence bundles, used only when approved controls cite them. The seed
article `S001` remains contextual and must not populate market observations.
FRED is the observed policy-rate acquisition route; reusing a separate FRED
warehouse is an optional future connector requiring its own contract.

The donor records an HTTP-only Yale exception. Importing its existing pinned
bytes is permissible within their documented scope; do not inherit that exception
as an unconditional new-fetch policy. At live-adapter implementation, recheck
official source routes, terms, attribution, schema and transport behavior. Require
a named reviewed exception if HTTP is still necessary; do not use an unreviewed
mirror or disable TLS verification. No new source-site verification was performed
while writing this spec.

## Structure, conventions and commands

Original planned MarketMedallion layout (the implementation also includes
`frozen.py`, `frozen_import.py`, `backup.py` and `bundle_assembly.py`):

```text
src/market_data_pipeline/pipelines/presidential_research/
  __init__.py, __main__.py, cli.py, config.py, source.py
  parsers.py, quality.py, review_workbook.py, assemble.py, provenance.py
  pipeline.py, storage.py, warehouse.py, publication.py
  replay.py, operations.py, dagster_defs.py
config/presidential_research/      # nonsecret examples and frozen profile
manifests/presidential_research/   # reviewed source/access declarations
tests/fixtures/presidential_research/  # invented or redistributable pinned bytes
tests/test_presidential_*.py
docs/deployment/presidential_research_runbook.md
```

Do not nest the donor's `pyproject.toml` inside the production package. Adapt its
MIT attribution/package metadata explicitly and add required non-code resources
to the distribution. Preserve the original package as a temporary reference only
during parity verification. Do not add placeholder dispatchers before code exists.

Style follows repository Ruff and typed pure functions; e.g.:

```python
def annual_return(start_level: float, end_level: float) -> float:
    if start_level <= 0 or end_level <= 0:
        raise ValueError("index levels must be positive")
    return end_level / start_level - 1.0
```

Current donor verification, executable today from the research repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s docs/research/002_PresidentialImpact/pipeline/tests -v
```

Target full verification after implementation, from MarketMedallion:

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
```

QuantMeridian verification commands are in the [plan](plan.md). All proposed
pipeline commands and API routes there are explicitly future interfaces.

## Scope boundaries and unresolved operational choices

Always preserve source meaning, frozen profiles, observed clocks, manual review,
strict contracts, offline replay and coherent generations. Preserve existing
AAII/SEC/NAAIM state and unrelated working-tree changes.

Decide before expanding beyond local delivery: hosted/public access, recurring
fetch cadence and source rights, real-input transfer scope, retention duration,
PostgreSQL/Databricks deployment and operator identity. These are follow-up
implementation decisions, not blockers to writing this specification.

Never infer political intervals from candidate counts, label CRSP/Shiller as
exact S&P 500 replication, backdate captured vintages, invent presidential-period
windows, fill missing observations with zeros, or present descriptive comparisons
as causal/predictive evidence.

## Specification preparation evidence

- Donor offline suite: **29 passed** on 2026-11-04; no provider request.
- MarketMedallion agent-skills snapshot verification passed at
  `f63ec56a3cc936408d792956ae583c3c96a825bd`.
- The current QuantSmith checkout fails its whole-reference revision/tree check.
  Relevant guidance was read from the pinned commit
  `5deea85438f6d46a8a14ddb85e3821a65fa62f4e` using `git show` and individually
  checked against the registered SHA-256 digests; no pin was changed.
- This historical evidence validates the original inspection and donor baseline;
  current implementation checks are recorded in [acceptance](acceptance.md).
