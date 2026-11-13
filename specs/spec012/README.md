# Spec 012 — FINRA daily short-sale volume

**Status:** SSV-T01–T05 complete for the selected three-TRF Public API profile:
approved-use admission, manifest/registry, pure Decimal parser, quantity/ratio
helpers and immutable Bronze with sanitized rejection evidence are implemented.
Acquisition, warehouse, published products and jobs remain absent.
Next: **SSV-T06**, one invented local SQLite partition.
**Reviewed:** 2026-11-13. **Delivery order:** 2 of 5.
**Pipeline ID:** `finra-short-sale-volume`.
**Package:** `src/market_data_pipeline/pipelines/finra_short_sale_volume/`
(source contract, registry, pure parser/numeric helpers and local Bronze). The [source review](source_review.md) records the user's
personal noncommercial scope and Public API terms acknowledgement. The selected
[API profile](api_contract.md) explicitly amends the file-first plan for JSON and
page completeness. G0 is ready for the explicit three-TRF profile; TXT, ADF,
consolidated and ORF scopes stay excluded. [T02 evidence](evidence/t02_source_contract.json)
records the six-row sample and its limits. [T03 evidence](evidence/t03_parser_conformance.json)
reconciles its fractional share quantities offline; the
[API amendment](api_contract.md#ssv-t03-fractional-quantity-amendment) supersedes integer-only normalization.

Read the [program roadmap](../../docs/securities_finance/README.md),
[common contract](../../docs/securities_finance/contract.md),
[source data contract](data_contract.md), [plan](plan.md) and [tasks](todo.md).
Spec011 delivery precedes this work by priority; its data is not a technical
dependency of this independent FINRA pipeline. Spec013 follows this delivery.

## Next agent handoff

**Start here:** [T05 acceptance](t05_acceptance.md), then
[SSV-T06 in the task list](todo.md#ssv-t06--deliver-one-local-sqlite-partition).
Continue on `feat/finra-daily-short-sale-volume`, based on merged main `b580689`.
T01–T05 are complete; T06–T16 have not started. G0/parser Checkpoint A are ready
for the three-TRF API profile; G1/G2 and local publication remain open.

Before coding, read root [AGENTS.md](../../AGENTS.md), the
[master handoff](../../docs/handoffs/project_bootstrap.md), [plan](plan.md),
[data contract](data_contract.md), [API contract](api_contract.md),
[source review](source_review.md) and [Bronze contract](bronze_contract.md).
Use pinned engineering workflows. QuantSmith checkout drift remains documented;
61 pinned Git objects were verified without refreshing either library.

**Next bounded slice — SSV-T06 / SSV-REQ06 / SSV-AC06:** import one invented,
complete API response through the existing Bronze and pure parser into audited
SQLite Silver/Gold. Implement source-owned `pipeline.py`, `io/warehouse.py`,
`cli.py`, `__main__.py` and `tests/test_ssv_pipeline.py`, splitting the task if
necessary. Transaction/validation failures must preserve the prior committed
publication. Prove approved-use checks before I/O and lossless quantity readback.
Do not count a partial API page as a complete partition or replace missing rows
without authoritative completeness. Cross-page assembly/replacements, revisions,
typed Parquet, replay, acquisition, export and Dagster remain later slices.

Reuse `archive_input`, `read_capture` and `parse_capture` from
[`io/bronze.py`](../../src/market_data_pipeline/pipelines/finra_short_sale_volume/io/bronze.py).
Keep the existing parser, quantity codec and fixed T04 ratio method unchanged.
T05 `archived_at` records local archive observation under the writer lock before
publication; it is not provider publication or retrieval completion. Use nullable
explicit `retrieved_at`; T02 `captured_at` was request start, never Silver first-seen.
Do not backdate knowledge or apply today's acquisition window to archived input.
Preserve original capture sequences/clocks for later replay. Raw provider samples,
credentials and runtime stores remain ignored. NAAIM stays paused; schedules stay stopped.

**Verified T05:** 49 Bronze tests / 315 focused FINRA tests; full offline suite
**1,169 passed, 92 optional skips**. Lint/format, wheel/sdist source readback and
invented-only conformance pass. [Evidence](evidence/t05_bronze_conformance.json)
records four exact-byte captures, including fractional, valid empty and rejected
input; no real observations were acquired or promoted. All five existing FINRA
core modules are byte-identical to merged main. See [acceptance](t05_acceptance.md)
for failure-boundary coverage and runnable fixture usage.

When T06 passes, update this section, [todo.md](todo.md), the
[master register](../../docs/pipelines.md), [program tracker](../../docs/securities_finance/roadmap.md)
and [master handoff](../../docs/handoffs/project_bootstrap.md) with actual evidence.

## Objective and research questions

Build a reproducible view of reported short-sale flow by trade date, security
symbol and FINRA facility. A researcher should be able to inspect the short
share of the corresponding reported volume, compare days with explicit gaps,
identify a source correction, and trace each displayed value to retained bytes.

Useful questions include whether a security's reported short-flow share has
changed within a fixed facility scope, whether the movement is broad across
facilities, and whether it coincides with independently dated settlement events.
Cross-source studies require the temporal and identity controls in the program
contract. They are future research, not a causal conclusion from this series.

## Official evidence and unresolved access

The [daily-file landing page](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files)
describes facility files and a consolidated TRF/ADF file for listed securities.
It gives an end-of-day publication target of 6 p.m. Eastern and identifies
subsequent corrected downloads as Updated. It explicitly permits zero-data ADF
files. The consolidated series begins August 2018; the Chicago facility series
begins September 2018. Exact archive availability must be inventoried.

The [file-layout guide](https://www.finra.org/sites/default/files/2021-07/DailyShortSaleVolumeFileLayout.pdf)
documents date, symbol, short volume, exempt short volume, total volume, market,
and a trailer count. Crucially, short volume already includes exempt trades.
Exempt volume must never be added a second time. The pre-February-2011 layout
has fewer columns and requires an explicit separate schema adapter.

The [source overview](https://www.finra.org/finra-data/browse-catalog/short-sale-volume)
states the data are free for non-commercial use and explains that the activity
is publicly disseminated off-exchange flow. General
[FINRA terms](https://www.finra.org/terms-of-use) restrict website-derived bulk
collection, databases and other uses unless separately permitted. SSV-T01 must
record an applicable dataset-specific permission or approved mechanism before
live automation, durable provider-data loading or distribution is enabled.
The existence of downloadable files is not blanket reuse permission.

The [API fee schedule](https://developer.finra.org/fees) distinguishes limited
Public credentials from paid Firm/Organization credentials. The
[Reg SHO API explorer](https://developer.finra.org/docs/api-explorer/query_api-equity-reg_sho_daily_short_sale_volume)
shows a mock endpoint. Mock responses are development data, never observations.
The current [API documentation](https://developer.finra.org/docs#reg-sho-daily-short-sale-volume)
also identifies the production `otcMarket/regShoDaily` dataset and a rolling
twelve-month window. The [Equity API terms](https://developer.finra.org/specific-terms-equity-data)
permit noncommercial internal use and derived data with valid access credentials.
The user subsequently confirmed a Public credential and accepted API terms.
Authentication and production metadata succeeded. SSV-T02 subsequently sampled
two November 9 records from each TRF, confirming source version 1 and the literal
facility/market pairs. These are partial contract samples, not complete extraction
or historical coverage. The market D empty 204 response supplied no ADF identifier.
The API profile is selected explicitly; no TXT permission is inferred.

## Scope and delivery boundary

The planned first usable slice will accept an approved archived API JSON response plus
its request/scope receipt into immutable Bronze, typed Silver versions, local
SQLite and an atomic Gold/Parquet snapshot. This selected API profile supersedes
the original TXT-first execution order; TXT files remain a separately gated path.
Support each verified current facility explicitly. A consolidated file has a
different scope and must be a distinct dataset, not extra rows to sum with its
constituent facilities. ORF remains separate from listed-security TRF/ADF scope.

After local semantics pass, add bounded acquisition, overlapping correction
refreshes, coverage health, replay, a rights-aware research export and a
stopped-by-default Dagster job. Initial history is one schema regime over an
explicit requested date range. Full archives and older schemas are bounded
extensions, not implicit work in the first run.

## Requirements and acceptance matrix

Every task/test must cite these stable IDs. SSV-AC01–05 have 315 focused tests:
84 admission/registry, 111 parser, 71 arithmetic/codec and 49 Bronze checks.
Storage/parse entrypoints enforce admission before I/O. Acquisition/export
entrypoints and delivery acceptance remain pending. Full offline regression:
1,169 passed, 92 optional skips. [T05 acceptance](t05_acceptance.md).

| Requirement | Acceptance criterion and planned evidence | Owning task |
| --- | --- | --- |
| SSV-REQ01: Record approved use and route. | SSV-AC01: Missing/disallowed rights profile refuses acquisition/export before I/O; offline fixtures remain usable. | SSV-T01 |
| SSV-REQ02: Register file/date/facility/schema scope. | SSV-AC02: Registry distinguishes individual, consolidated and ORF inputs; aliases and unavailable historical regimes are explicit. | SSV-T02 |
| SSV-REQ03: Parse documented file structure. | SSV-AC03: Invented API responses cover literal JSON fields, empty arrays, malformed rows/dates and schema drift; invalid responses cannot promote. TXT header/trailer coverage remains a separate profile. | SSV-T03 |
| SSV-REQ04: Preserve count semantics. | SSV-AC04: Fixture 40 short, 5 exempt, 100 total yields 0.40; whole/fractional shares remain exact decimals with exempt <= short <= total. | SSV-T04 |
| SSV-REQ05: Retain raw evidence. | SSV-AC05: Success and parse rejection preserve exact bytes/hash, manifest, run, schema and request metadata without secrets. | SSV-T05 |
| SSV-REQ06: Publish local transactional results. | SSV-AC06: One invented local API response with scope receipt produces audited SQLite Silver/Gold; failure leaves the prior committed snapshot intact. | SSV-T06 |
| SSV-REQ07: Preserve observed revisions. | SSV-AC07: Identical repeat adds no version; correction/reversion remain queryable by actual first-seen cutoff. | SSV-T07 |
| SSV-REQ08: Represent missing and removed rows. | SSV-AC08: Full replacement removal differs from no report, valid empty, failed fetch and partial file; missingness never invents zero. | SSV-T08 |
| SSV-REQ09: Produce scoped research ratios. | SSV-AC09: Matching numerator/denominator only; zero total returns null; mixed consolidated/constituent inputs are rejected. | SSV-T09 |
| SSV-REQ10: Publish typed snapshots. | SSV-AC10: Current pointer selects one complete DB/file generation after each C07 failure/restart boundary; counts and decimal ratios round-trip losslessly. | SSV-T10 |
| SSV-REQ11: Replay without network. | SSV-AC11: Empty-target replay reproduces versions, audit interpretation and cutoffs from archived captures. | SSV-T11 |
| SSV-NFR01: Bound acquisition and resources. | SSV-AC12: Date/file/byte/request limits and Retry-After apply; 429, truncation and login/HTML responses do not advance coverage. | SSV-T12 |
| SSV-NFR02: Expose honest health. | SSV-AC13: Calendar/release-aware status distinguishes pending, stale capture, failure and legitimate no-data facility. | SSV-T13 |
| SSV-REQ12: Export a narrow consumer contract. | SSV-AC14: Versioned file retains facility/scope, units, dates, lineage and rights label; consumer fixture rejects mismatched scope. | SSV-T14 |
| SSV-NFR03: Keep operation controllable. | SSV-AC15: Job/schedule are absent until implemented, then default stopped; manual kill switch and full evidence/runbook support handoff. | SSV-T15, SSV-T16 |

## Architecture and owned structure

Python 3.12, existing pinned YAML/Parquet libraries, standard-library parsing and
SQLite are the baseline. Keep HTTP, parsing, persistence and scheduling separate.
Do not import AAII warehouse internals or build a generic market-data framework
as a prerequisite. Proposed source-owned files are:

```text
pipelines/finra_short_sale_volume/
  manifest.py       # implemented approved-use and strict manifest/schema checks
  registry.py       # implemented API version, facility scopes and date-window guard
  source.py         # bounded API partitions and acquisition
  data/api.py       # implemented pure receipt-bound parser and Decimal records
  data/quantities.py # implemented exact quantity validation and text codec
  data/gold.py      # implemented per-observation ratios; aggregate/history later
  io/bronze.py     # exact input and request receipts
  io/warehouse.py  # SQLite observation versions and publication transactions
  io/parquet.py    # committed typed snapshot
  pipeline.py      # one requested partition from input to committed output
  replay.py        # archived-only deterministic reconstruction
  operations.py    # coverage, health and dry-run retention
  research_export.py
  cli.py / __main__.py / dagster_defs.py
tests/fixtures/finra_short_sale_volume/  # invented data only
```

The implemented pure helper uses exact Decimal shares and a fixed 34-significant-digit,
half-even ratio policy, independent of caller context. Exempt is a subset of short:

```python
from decimal import Decimal
from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import share_ratios

ratios = share_ratios(Decimal("40"), Decimal("5"), Decimal("100"))
assert ratios.short_share == Decimal("0.40")
assert ratios.short_exempt_share == Decimal("0.05")
```

The [T04 numeric contract](data_contract.md#ssv-t04-arithmetic-and-quantity-text-boundary)
and [offline evidence](evidence/t04_semantics_conformance.json) define the quantity
codec, zero/null behavior, method version and remaining publication boundary.

## Verification commands and CLI proposal

Existing repository checks remain executable:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m build --no-isolation
```

Future commands below are a proposed interface, absent until SSV tasks implement
and test them. Validation/check/replay never contact FINRA; local run does not
implicitly fetch. A later acquisition command must require bounded dates and
an admitted use profile, and print scope plus coverage, never a misleading
market-wide short-interest label.

```bash
.venv/bin/python -m market_data_pipeline.pipelines.finra_short_sale_volume validate --manifest manifests/finra-short-sale-volume.yml
.venv/bin/python -m market_data_pipeline.pipelines.finra_short_sale_volume run --input tests/fixtures/finra_short_sale_volume/current.json --root data/ssv-demo
.venv/bin/python -m market_data_pipeline.pipelines.finra_short_sale_volume check --root data/ssv-demo
.venv/bin/python -m market_data_pipeline.pipelines.finra_short_sale_volume replay --source-root data/ssv-demo --root data/ssv-replay
```

## Testing, rollout and boundaries

All ordinary tests use invented data and injected transport. Test both the raw
trailer-count contract and mathematical invariants, not just parser snapshots.
Include corrected removal, same-day second capture, pre-2011 rejection, symbol
suffixes, zero totals, zero-row ADF and every incomplete-partition outcome.
Live conformance is separate evidence and cannot be claimed from mock APIs.

Always preserve exact scope, raw bytes, timing, revisions, error outcomes and
schema/transform versions. Never infer short interest, borrow utilization,
locate availability, fee/rebate rate or market-wide demand from short flow.
Source-derived predictive/AI uses require resolving FINRA's specific terms.

Review changes to paid access, rights, historical regimes, derived measures,
cross-source matching, production warehouse or deployment before extending the
approved contract. Scheduling remains stopped until its own operational review.
NAAIM remains paused throughout this program. Spec013 begins after this
delivery's acceptance evidence and clean handoff, not merely parser completion.
