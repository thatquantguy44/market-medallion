# Spec 013 — FINRA ATS and non-ATS transparency

**Status:** Comprehensive implementation proposal; connector/package/jobs absent.
**Reviewed:** 2026-11-03. **Delivery order:** 3 of 5.
**Pipeline ID:** `finra-otc-transparency`.
**Proposed package:** `src/market_data_pipeline/pipelines/finra_otc_transparency/`.

Read the [program roadmap](../../docs/securities_finance/README.md),
[common contract](../../docs/securities_finance/contract.md),
[data contract](data_contract.md), [plan](plan.md) and [tasks](todo.md).
Spec012 must reach its delivery checkpoint before implementation here. This is
a priority dependency, not a requirement to read its daily short-sale records.
Spec014 SEC N-PORT follows this delivery.

## Objective and research use

Provide delayed weekly security-level trading volumes and counts by reporting
ATS/firm with transparent scope, identity and availability dates. Researchers
can inspect execution concentration, changes in venue composition and average
reported trade size, then compare those features with separately admitted
securities-finance evidence. The value is a reliable historical venue panel
whose population, revisions and gaps are inspectable.

The first research questions are: which reported venues account for a security's
observed off-exchange activity; whether that distribution changes through time;
and how much attribution is unresolved or aggregated by the provider. These
records do not identify long/short intent, securities loans or beneficial owners.

## Official evidence reviewed

The current [FINRA OTC transparency landing page](https://www.finra.org/filing-reporting/otc-transparency)
states that ATS/member-firm information is delayed and derived from trades
reported to FINRA. It explains that some small non-ATS activity is published
without firm attribution and links the official data site, technical notices,
equity ATS list and documentation. Its report families must remain separate.

The linked [Version 6 user guide](https://www.finra.org/sites/default/files/2019-11/OTC-transparency-website-user-guide-v6.pdf)
documents delayed Tier 1/Tier 2/OTCE weekly views, pipe-delimited downloads and
separate ATS/non-ATS products. It also describes automation via Developer Center
instructions and continued support for older download instructions. It remains
an older published guide; actual current payloads/routes require conformance
verification rather than copying historical endpoint examples into production.

The [Developer Center documentation](https://developer.finra.org/docs) describes
query metadata, partition filters and pagination; weekly summary partitions
include reporting week and tier. The [weekly-summary explorer](https://developer.finra.org/docs/api-explorer/query_api-equity-weekly_summary)
shows a mock route, which is excluded from real observations. Free Public API
access is limited and must be verified for this use; the
[fee schedule](https://developer.finra.org/fees) distinguishes it from paid
Firm/Organization access. No credentials are provisioned by this spec.

The [general website terms](https://www.finra.org/terms-of-use) and dataset/API
terms must be reconciled before any durable bulk collection or downstream reuse.
The guide's automation description does not resolve every retention, derived
measure, publication or commercial-use permission. OTC-T01 records the route-
specific decision. No provider contact, account registration, live bulk download
or source-derived predictive model is authorized by this documentation change.

## Scope and delivery layers

Initial scope is weekly ATS and weekly non-ATS security/participant detail for
the three independently partitioned equity tiers, within an explicit date range.
Build one tier/product fixture path first, then add the other product and tiers
through the same contract. Preserve a route's exact supported coverage. Monthly
products, blocks, aggregate ranking tables and fixed-income ATS data remain
distinct optional extensions and cannot be mixed into weekly detail totals.

Delivery includes an approved-use profile, source inventory, typed local parser,
immutable Bronze, versioned SQLite Silver, scoped Gold, atomic Parquet, archived
replay, bounded refresh, health and an admitted versioned research artifact.
Only then add stopped Dagster definitions. No schedule runs during specification.

## Requirements and acceptance matrix

| Requirement | Acceptance criterion and planned evidence | Owning task |
| --- | --- | --- |
| OTC-REQ01: Admit route and permitted use. | OTC-AC01: Use/profile review resolves automation/persistence/export; denied or missing profile blocks provider I/O. | OTC-T01 |
| OTC-REQ02: Catalog distinct products/tiers. | OTC-AC02: Weekly detail, totals, monthly and blocks cannot collide; all requested week/tier/product partitions are enumerated. | OTC-T02 |
| OTC-REQ03: Parse one weekly detail profile. | OTC-AC03: Invented pipe/CSV fixtures pin header, encoding, empty outcome, integer counts and unknown schema rejection. | OTC-T03 |
| OTC-REQ04: Preserve participant identity evidence. | OTC-AC04: MPID/CRD only when supplied; name-only and aggregated buckets stay explicit, with no fabricated entity match. | OTC-T04 |
| OTC-REQ05: Retain Bronze and coverage. | OTC-AC05: Exact bytes, discovery metadata, requested week and receipt survive rejection; missing page cannot appear complete. | OTC-T05 |
| OTC-REQ06: Deliver local weekly detail. | OTC-AC06: All six selected ATS/non-ATS × Tier1/Tier2/OTCE fixture profiles persist in source-owned SQLite with transactions and consistent Gold selection; selected real-profile conformance is recorded separately. | OTC-T06, OTC-T12 |
| OTC-REQ07: Preserve updates/removals. | OTC-AC07: Repeats, revisions, reversions and complete-replacement removals preserve actual observed cutoffs. | OTC-T07 |
| OTC-REQ08: Separate period/publication/read clocks. | OTC-AC08: Tier delays and date-only publication evidence never become invented timestamps; backfill is first known at capture. | OTC-T08 |
| OTC-REQ09: Produce denominator-safe venue features. | OTC-AC09: Shares/averages use one complete matching population; identity coverage and missing tiers remain visible. | OTC-T09 |
| OTC-REQ10: Publish typed snapshots. | OTC-AC10: Single pointer selects one complete DB/file generation after each C07 failure/restart boundary, with lossless counts and schema/version metadata. | OTC-T10 |
| OTC-REQ11: Rebuild archived history. | OTC-AC11: Network-free empty-target replay reproduces value and membership revisions. | OTC-T11 |
| OTC-NFR01: Bound refresh and extraction. | OTC-AC12: Finite weeks/tiers/products/pages/bytes, stable pagination and overlap; truncation or retry exhaustion prevents promotion. | OTC-T12 |
| OTC-NFR02: Expose coverage and health. | OTC-AC13: Pending delayed releases, source gaps, partial partitions, latest failed refresh and unknown identity differ in status. | OTC-T13 |
| OTC-REQ12: Export a scoped research file. | OTC-AC14: Artifact preserves delay, population, attribution coverage and rights; no secrets/raw operational paths appear. | OTC-T14 |
| OTC-NFR03: Keep manual control and truthful handoff. | OTC-AC15: Source-owned schedule defaults stopped; evidence/runbook/registers identify actual implemented coverage. | OTC-T15, OTC-T16 |

## Proposed architecture and package layout

Use the repository's Python 3.12 baseline, pure typed transforms, SQLite first
and existing Parquet support. File and optional admitted API acquisition feed
the same normalized partition contract but keep independent parser profiles and
route lineage. Do not infer compatible fields from a different summary table.

```text
pipelines/finra_otc_transparency/
  manifest.py / catalog.py      # rights, product/tier/schema/coverage
  source.py                    # bounded discovery and transfer
  data/weekly.py               # typed provider-profile parser
  data/identity.py             # scoped evidence, no name-based entity merge
  data/gold.py                 # complete-population research features
  io/bronze.py / warehouse.py / parquet.py
  pipeline.py / replay.py / operations.py / research_export.py
  cli.py / __main__.py / dagster_defs.py
tests/fixtures/finra_otc_transparency/  # invented data only
```

An illustrative pure transform preserves count semantics. This is a proposed
style example, not implemented runtime:

```python
from decimal import Decimal

def average_trade_size(shares: int, trades: int) -> Decimal | None:
    if trades == 0:
        return None
    return Decimal(shares) / Decimal(trades)
```

## Verification commands and proposed local usage

Existing verification commands remain available:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m build --no-isolation
```

These future commands are design targets, absent until tasks implement them:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.finra_otc_transparency validate --manifest manifests/finra-otc-transparency.yml
.venv/bin/python -m market_data_pipeline.pipelines.finra_otc_transparency run --input tests/fixtures/finra_otc_transparency/weekly.txt --root data/otc-demo
.venv/bin/python -m market_data_pipeline.pipelines.finra_otc_transparency check --root data/otc-demo
.venv/bin/python -m market_data_pipeline.pipelines.finra_otc_transparency replay --source-root data/otc-demo --root data/otc-replay
```

No current command should be extended to silently redirect to FINRA. The later
fetch command must require bounded reporting weeks, product/tier selections and
a use profile. Check/replay never fetch. Counts and coverage are reported with
their population and publication delay, including when some tiers are pending.

## Testing and operational boundaries

Use invented fixtures with at least two venues, an un-attributed group, a symbol
suffix, a zero-trade case, a changed participant name, a source correction and
a removed row. Test multiple products/tiers with staggered publication, a
holiday week, malformed counts, duplicate keys, unexpected summary codes,
truncated page sets, retry exhaustion and a conflicting complete replacement.

A live conformance run, only after route/use admission, verifies one bounded
week/tier pair against actual payload shape and discovery metadata. Do not
generalize success to all products or historical regimes. No public redistribution
or source-derived model usage is implied by offline tests or free downloads.

Always expose reporting period, source delay, capture/first-seen clocks,
participant-identity coverage and complete population. Never relabel ATS volume
as short selling, institution accumulation, borrow demand or a dealer's lending
inventory. Review paid APIs, broader retention/use, extra products, derived
concentration methodology, production deployment and cross-source joins before
expansion. Keep NAAIM paused and other pipeline runtime configuration independent.
