# Spec 015 — NY Fed primary-dealer financing and settlement statistics

Status: **planned; connector, package, fixtures, warehouse and jobs absent**.
Created/reviewed: **2026-11-03**, America/New_York.
Capability ID: `nyfed-primary-dealers`.
Proposed package: `pipelines/nyfed_primary_dealers/`.
Program order: **5 of 5**, after Spec 014 research delivery.

Read the [program](../../docs/securities_finance/README.md),
[common contract](../../docs/securities_finance/contract.md),
[data contract](data_contract.md), [plan](plan.md) and [tasks](todo.md).
Every task is planned; publication and operational evidence have yet to exist.

## Objective

Provide reproducible, source-defined aggregate primary-dealer positions,
financing and settlement-fails observations for securities-finance research.
The first useful release should let a researcher inspect collateral-financing
conditions by admitted series, compare like-for-like observations and identify
changes in source definitions or disclosure coverage.

The source describes a dealer reporting population. It does not supply a
complete market census, dealer identities or security-level loan/borrow rates.
The release must make the reporting population, measurement basis and series
era visible beside every value and derived comparison.

The local research output may later feed a separately specified QuantMeridian
consumer through a versioned artifact. No sibling database, credential,
runtime configuration or deployment identifier is required for source delivery.

## Verified documentary contract

The [official statistics page](https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics)
offers exports and describes Thursday publication of the prior week's data,
revisions, rounding and suppression where too few dealers report. It segments
history into six definition regimes from 1998 through the current period.
The publication clock is a schedule expectation, not evidence of a particular
historical observation's actual release time.

The [Markets API documentation](https://markets.newyorkfed.org/static/docs/markets-api.html)
and its [official schema](https://markets.newyorkfed.org/static/docs/markets-api.yml)
document discovery of series breaks, timeseries and as-of dates, plus reads by
date or series. The reviewed JSON observation shape has `asofdate`, `keyid`
and decimal-text `value`; series definitions identify the series break and
description. The source's opaque IDs must be discovered and persisted.

The [fails guide](https://www.newyorkfed.org/markets/pridealers_failsprimer.html)
explains that weekly totals accumulate daily outstanding fails, including
unresolved older fails. This is distinct from unique failed trades or the age
of an individual fail. Source-defined as-of MBS settlement-cycle measures
must remain a separate measurement type.

The [New York Fed terms](https://www.newyorkfed.org/privacy/termsofuse) address
automated access, source attribution and modification labels. Admission must
retain applicable source notices and label any derived values explicitly.
Review the terms again at implementation and record the precise output notice.

This review fetched documentation/schema only. A current data payload, series
allowlist, units and missing-value sentinels remain to be characterized.

## Required source scope

Start with an explicit allowlist chosen from official metadata covering:

1. Securities-in/securities-out financing categories relevant to repo,
   reverse-repo and securities borrowing/lending, preserving provider directions.
2. Treasury/agency/MBS/corporate fails-to-receive and fails-to-deliver series
   only where definitions, units and reporting basis are verified.
3. Supporting positions or transaction series needed for a documented,
   comparable contextual view.

Each admitted series records units, scaling, frequency, aggregation basis,
asset universe, tenor/counterparty/collateral dimensions if provided, and its
effective era. Source families with different measures remain independently
selectable. The full catalog is inventoried; an allowlist controls publication.

The first release includes local fixture ingestion, SQLite versions, typed
Parquet publication, source replay, a local artifact, no-request health,
bounded historical refresh and stopped source-owned Dagster definitions.
Bulk history and schedule activation follow recorded run bounds and operator
direction after semantics pass. Current/reference rates, SOMA CUSIP holdings,
securities-lending auction results, tri-party/GCF market statistics and OFR
STFM are separate source scopes even when they share a website or topic.

## Functional requirements

| ID | Required behavior |
| --- | --- |
| NYF015-R01 | Record current official API/access/terms evidence and distinguish documented routes from unverified assumptions. |
| NYF015-R02 | Capture series-break, series-definition and as-of-date metadata; validate a named publication allowlist. |
| NYF015-R03 | Archive each bounded response before parsing with request, checksum, status, headers, clocks and metadata-version identity. |
| NYF015-R04 | Parse exact keys, dates, decimal values and source-supported missing/suppression states without coercion. |
| NYF015-R05 | Preserve definition eras, units and aggregation basis; block comparisons across incompatible metadata. |
| NYF015-R06 | Retain immutable observations, metadata revisions, removals and reversions with captured-first-seen semantics. |
| NYF015-R07 | Support deterministic local SQLite/Parquet publication and network-free replay into an empty target. |
| NYF015-R08 | Publish transparent level/change research views with exact source dimensions and explicit gap/missing behavior. |
| NYF015-R09 | Refresh overlapping report windows and approved older partitions without hiding revisions behind a watermark. |
| NYF015-R10 | Emit an attributed versioned local artifact and source-independent consumer contract. |
| NYF015-R11 | Provide no-request local health/audit and stopped-by-default callable Dagster operations. |
| NYF015-R12 | Record actual verification, limitations, recovery and the program completion handoff. |

## Non-functional requirements

| ID | Contract |
| --- | --- |
| NYF015-N01 | Python 3.12 pure transforms, source-owned modules and existing locked dependencies first. |
| NYF015-N02 | Explicit request/date/series/response bounds, timeouts and retry budgets; avoid an unbounded all-history call by default. |
| NYF015-N03 | Replay reproduces logical records, metadata versions, knowledge clocks and output hashes. |
| NYF015-N04 | Incomplete or inconsistent response sets cannot advance complete publication or watermarks. |
| NYF015-N05 | Source terms/attribution and derived-method labels travel with every consumer artifact. |
| NYF015-N06 | Local checks/tests have no provider dependency and never alter another pipeline's schedule. |

## Research features

- **Source levels:** each observation with series ID/description, era, source
  date, native units and aggregation basis.
- **Comparable changes:** level differences only between consecutive admitted
  observations of the same series/era/unit/basis; preserve prior date and gap.
- **Financing mix:** ratios or shares only after a methodology proves that
  categories are mutually exclusive and the denominator matches their universe.
- **Settlement context:** fails-to-receive and fails-to-deliver displayed
  separately. Summing them is not a measure of unique market settlement stress.
- **Coverage monitor:** metadata-era changes, suppressed/unavailable values,
  expected publication windows, observed revisions and retained-history coverage.

There is no initial composite stress score or implied security-level specialness.
Dividing a cumulative weekly fail number by five requires the actual business-
day count and a separately named derived daily-average method; the source level
must still be available. No such conversion is silently applied.

## Architecture and structure

The proposed source package is
`src/market_data_pipeline/pipelines/nyfed_primary_dealers/`, containing
`manifest.py`, `source.py`, `data/metadata.py`, `data/observations.py`,
`data/gold.py`, `io/bronze.py`, `io/warehouse.py`, `io/parquet.py`,
`pipeline.py`, `replay.py`, `research_export.py`, `operations.py`, `cli.py`
and `dagster_defs.py` as tasks require them.
Manifests/SQL/fixtures/runbooks retain source-specific directories.

Metadata capture → allowlist validation → bounded source responses → typed
observations → era/coverage checks → immutable local versions → committed
research snapshot → attributed artifact. Metadata is an input version, not
an untracked lookup to a provider's current description during replay.

Use immutable typed records, string series IDs and `Decimal` for source values.
Keep SQL parameterized and transport injected. Example style for a planned
feature helper:

```python
from decimal import Decimal

def level_change(current: Decimal | None, previous: Decimal | None) -> Decimal | None:
    if current is None or previous is None:
        return None
    return current - previous
```

Callers must first prove era, unit and aggregation compatibility. The helper
does not license arbitrary series subtraction.

## Acceptance and evidence

| Acceptance ID | Required deterministic evidence | Requirements |
| --- | --- | --- |
| NYF015-AC01 | Dated source/API/terms matrix lists admitted metadata/read routes and unresolved policy/payload facts. | R01 |
| NYF015-AC02 | Invented catalogs prove break boundaries, opaque series IDs, duplicate/unknown metadata and allowlist validation. | R02, R05 |
| NYF015-AC03 | Local input archiving precedes persistence/replay and retains exact bytes/receipts; bounded transport tests prove 400/429/5xx/timeout/oversize cannot produce a complete snapshot. | R03, N02, N04 |
| NYF015-AC04 | Decimal-text values, strict dates and actual missing/suppression codes survive round-trip without zero filling. | R04 |
| NYF015-AC05 | Compatible change calculations preserve gaps; era/unit/basis mismatch returns unavailable with a reason. | R05, R08 |
| NYF015-AC06 | Identical captures add no fact versions; corrections/reversions/metadata changes/removals remain queryable by cutoff; equal clocks preserve the original monotonic capture sequence in replay. | R06, R09 |
| NYF015-AC07 | Empty-root replay is network-free and reproduces versioned metadata, facts and original knowledge clocks. | R07, N03 |
| NYF015-AC08 | Failure injection at every C07 journal boundary selects one coherent SQLite/file generation after restart; partial sets cannot advance a complete watermark. | R07, N04 |
| NYF015-AC09 | Export carries attribution, source/derived labels, units, era, source/capture clocks, bounded coverage and input tokens. | R10, N05 |
| NYF015-AC10 | Freshness uses expected release/capture evidence; source-date age is a separate fact and suppression is not stale zero. | R11 |
| NYF015-AC11 | Offline Dagster inspection shows stopped defaults and no source request or NAAIM-state change at definition load. | R11, N06 |
| NYF015-AC12 | Runbook/evidence map names passed checks, unresolved coverage, recovery, manual controls and program follow-on choices. | R12, N01 |

## Commands and completion

Existing verification commands are executable today:

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
```

The planned CLI is `python -m
market_data_pipeline.pipelines.nyfed_primary_dealers`, with `validate`,
`ingest --input <local-response.json> --metadata <catalog.json> --root <root>`,
`fetch --dates <approved-date-list> --manifest <manifest> --root <root>`,
`replay --source-root <root> --root <empty-root>`, `check --root <root>` and
`export-research --root <root> --destination <artifact.json>`. These commands
are **not implemented**; verified exact syntax replaces this design when the
CLI task lands. Do not execute a guessed command from an unfinished spec.

Always preserve units, source population, metadata eras, revisions and lineage.
Resolve expansion into rates/auction/CUSIP datasets, new dependencies, large
history, deployment or schedule activation before performing those operations.
Never infer a suppressed value, splice eras on matching descriptions alone,
identify dealers from aggregate data or call overlapping categories additive
without an independently reviewed methodology.

Completion requires AC01–AC12 and a reproducible, attributed local research
delivery. Spec 014 is a delivery-order gate, not a runtime prerequisite.
Completion of Spec 015 closes the five-workstream research program; any later
combined score, terminal feature or production service receives its own scope.
