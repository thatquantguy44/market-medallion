# Spec 012 — FINRA daily short-sale volume

**Status:** SSV-T01–T03 complete for the selected three-TRF Public API profile:
approved-use admission, strict manifest/schema/version, disjoint facility
registry, acquisition-date guards and a pure receipt-bound JSON parser are
implemented. Connector, jobs and data products remain absent. Next: SSV-T04
decimal share arithmetic and lossless serialization.
**Reviewed:** 2026-11-10. **Delivery order:** 2 of 5.
**Pipeline ID:** `finra-short-sale-volume`.
**Package:** `src/market_data_pipeline/pipelines/finra_short_sale_volume/`
(source contract, registry and pure parser). The [source review](source_review.md) records the user's
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

Every task/test must cite these stable IDs. SSV-AC01–03 have 195 focused tests:
84 admission/registry and 111 parser checks. Acquisition/export entry points do
not exist, so their integrated guards and delivery acceptance remain pending.
Full offline regression: 677 passed, 10 optional skips.

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
  data/gold.py      # denominator-safe features
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

Use explicit types, stable provider strings and exact Decimal share quantities. A proposed pure
transform illustrates the style; this snippet is design guidance, not runtime:

```python
from decimal import Decimal

def short_share(short_volume: Decimal, total_volume: Decimal) -> Decimal | None:
    if total_volume == 0:
        return None
    return short_volume / total_volume
```

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
