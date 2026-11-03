# Spec 001 — Positioning and market structure pipeline

Status: **draft for review**. Created 2026-10-26.

**Current implementation:** [Spec 002](../spec002/README.md) supersedes the
CFTC-first order, SQLite-only delivery, and scheduler deferral below. AAII local
CSV ingestion, Parquet, PostgreSQL, and Dagster are the first executable slice.
The CFTC/FINRA/OFR interfaces and acceptance criteria in this document remain
planned; the commands below are not implemented for those providers.

**Next-source priority update (2026-11-03):** the user-selected
[Specs011–015 securities-finance program](../../docs/securities_finance/README.md)
now governs next source builds. Spec011 owns SEC FTD + Reg SHO and Spec012
refines the FINRA short-volume proposal here. Preserve this original design as
context; use [Spec011's checkpoint](../spec011/source_review.md) for the current
pickup point and keep NAAIM paused.

## Objective

Build an independent, manifest-driven market-data pipeline using the sister
FRED repository's Bronze → Silver → Gold format. Serve quant research, SQL/BI
exploration, and model inputs with traceable raw data and explicit availability
timing. Research usefulness must be evaluated; ingestion does not establish
predictive value.

The user selected positioning and market structure as the main scope. Company
disclosures/insider activity and broader free alternative data remain later
phases, documented in `docs/roadmap.md`.

## Source scope

1. **First working slice: CFTC Commitments of Traders.** Support the Traders in
   Financial Futures futures-only report first. Add Disaggregated futures-only
   after the first report passes the same contract tests. Keep report families
   and futures-only/combined scopes separate. Identify contracts by provider
   codes rather than display names. Verify API dataset IDs and actual columns
   before implementation; none are assumed here.
2. **Next source: FINRA short-sale volume.** Preserve reporting-facility scope;
   produce short-volume shares only against the corresponding reported-volume
   denominator. Resolve current free download paths and usage terms first.
3. **Funding extension: OFR Short-term Funding Monitor.** Discover metadata,
   units, and underlying-provider restrictions per selected series. Prioritize
   funding/repo measures that complement the existing FRED project.

These are milestones within one ingestion pipeline. Each additional source must
preserve its own grain and source-specific Silver/Gold schemas; do not force
positions, trades, and rates into one scalar observation table.

Official source documentation and interpretation limits are in the
[catalog](../../docs/catalog/README.md). SEC settlement failures are a candidate
extension, not part of the first implementation.

## Architecture and stack

- Python 3.12 baseline; typed functions and immutable records for normalized data.
- Source-specific YAML manifests and schema validation, matching the sister repo.
- SQLite first for a complete local workflow. PostgreSQL and Databricks/Delta
  adapters are future targets, added only after local semantics are verified.
- Standard-library SQLite plus a small HTTP/YAML stack; select and pin dependency
  versions during implementation. No package dependencies are installed yet.
- Pure normalization, validation, and feature functions; injected transport for
  network tests; database I/O isolated under `io/`.
- No cloud accounts, paid APIs, email delivery, scheduler, or active trading.

```text
manifests/*.yml → source API/download → Bronze (raw payload + metadata)
  → source-specific normalization → DQ → Silver (versioned records)
  → Gold (latest research features + observed-as-of views)
  + audit (runs, dataset runs, DQ outcomes, lineage)
```

The original source layout below is superseded by the user-authorized
[pipeline-owned package layout](../../docs/development/pipeline_layout.md).

## Proposed project structure

Keep `config/`, `manifests/`, `sql/`, `resources/`, `notebooks/`, `tests/`,
`.github/workflows/`, numbered `specs/`, and the existing `docs/` categories.
Use `src/market_data_pipeline/` with top-level `config.py`, `pipeline.py`,
`cli.py`, `replay.py`, and `__main__.py`. Group implementation by purpose:

| Package | Responsibility |
| --- | --- |
| `sources/` | shared HTTP behavior, registry, provider-specific extraction |
| `catalogs/` | manifest models, validation, provider metadata |
| `data/` | raw archive, source-specific normalization and Silver models |
| `io/` | SQLite writes and future warehouse adapters |
| `validation/` | source-specific data quality |
| `governance/` | audit records, lineage, access/usage metadata |
| `writer/` | Gold features and observed-as-of queries |

Create package files when their slice is implemented. Reserved top-level
directories do not imply working functionality.

## Data contracts

Bronze retains the original response bytes, request URL and parameters, response
status and relevant headers, provider/dataset identifiers, SHA-256 digest,
retrieval timestamp in UTC, parser version, and run ID. Retain successful raw
responses even when normalization or DQ fails. Archive every paginated response
before promotion; incomplete extraction must not look like a complete dataset.

CFTC Silver logical grain:
`(source, report_family, report_scope, contract_code, position_date,
trader_category)`, with long/short/spreading positions where available and open
interest. Store contract codes as strings, including leading zeros. Missing
cells remain null, not zero. Reject non-finite values and fractional contract
counts; preserve a raw pointer for diagnostics.

For revisions, store content hashes and immutable versions under the logical
key. A repeat observation with identical normalized content creates no new
version. A changed observation creates a new version with `first_seen_at` and
raw lineage. Bronze and run audit may grow on every fetch; Silver/Gold must not
duplicate unchanged observations.

Keep `position_date`, `provider_published_at` (nullable), `retrieved_at`, and
`first_seen_at` distinct. Historical downloads are not historical publication
vintages. Observed-as-of queries may only use versions first seen by the cutoff;
never label reconstructed report-date history as leakage-free backtest data.

See [proposed dictionary](../../docs/dictionary/data_dictionary.md).

## CLI contract (planned; not implemented)

Run from the repository root after the package is built:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m market_data_pipeline validate --manifests manifests
.venv/bin/python -m market_data_pipeline run --source cftc --local \
  --db-path data/market_data.db --start 2024-01-01 --end 2024-12-31
.venv/bin/python -m market_data_pipeline replay --local \
  --db-path data/market_data.db --source cftc
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
```

Require bounded dates for initial network runs. Support an explicit full-history
operation and overlapping incremental windows later. Replay consumes archived
Bronze only and performs no network access. Validation performs no network or
warehouse writes. Errors and partial runs return a nonzero exit status.

## Code style

Use source and dataset names in identifiers and audit messages, stable string
keys, type annotations, and small pure transforms. Example planned transform:

```python
def net_position(long: int | None, short: int | None) -> int | None:
    if long is None or short is None:
        return None
    return long - short
```

Do not infer missing values, silently change report scope, or combine unrelated
provider categories. Keep SQL parameterized and secrets outside tracked files.

## Quality, failure behavior, and Gold outputs

Validate required columns, report family/scope, key uniqueness within an
extraction, parseable dates, nonnegative counts, and finite numeric values.
Quarantine invalid data with auditable reasons. Freshness is evaluated against
actual provider publication expectations and delays, not observation dates
alone. An empty result must be distinguished from transport or schema failure.

HTTP extraction uses timeouts, bounded retries for 429/transient failures,
Retry-After where supplied, and conservative request pacing. Verify pagination
and deterministic ordering against the chosen API. Fail a source independently;
record succeeded/partial/failed status. Do not advance a watermark or publish an
incomplete snapshot after extraction or blocking DQ failure.

First Gold outputs: latest positions, category net positions, net position as a
share of matching open interest (null for missing/zero denominator), and change
from the preceding available report with its date/gap visible. Rolling features
require explicit minimum history. Every feature retains source/report scope and
version lineage. Point-in-time features must choose available versions before
calculating temporal changes.

## Testing strategy and acceptance criteria

Use pytest with provider-shaped offline fixtures and injected HTTP responses.
No CI dependency on a live provider. Add tests alongside each working slice.

- [ ] Manifests reject unknown fields, invalid dates/report scopes, and duplicate
  dataset definitions before any network request.
- [ ] A bounded CFTC request handles multiple pages, throttling, empty responses,
  missing fields, and malformed payloads with explicit outcomes.
- [ ] Local end-to-end fixture run writes Bronze, Silver, Gold, and audit tables.
- [ ] Re-running identical data leaves Silver version and Gold row counts stable.
- [ ] Changed data retains both versions with correct observed-as-of selection.
- [ ] Normalization/DQ failures preserve raw payloads and cannot promote bad data.
- [ ] A failed dataset cannot invalidate another dataset's successful writes.
- [ ] Bronze replay reconstructs equivalent Silver/Gold in a clean database
  while retaining the original first-seen timestamps.
- [ ] Gold formulas handle nulls, zero denominators, category differences,
  and gaps without fabricated observations.
- [ ] Unit tests and local integration tests pass; lint passes; an explicitly
  requested, bounded live smoke run verifies actual provider behavior.
- [ ] README commands, dictionary, source terms, and runbook describe tested
  behavior and clearly distinguish historical observations from captured vintages.

## Boundaries and review decisions

Always: archive raw data, validate source schemas, preserve availability timing,
test idempotency/revisions, and keep this repository independent of its sibling.

Review before expanding: additional sources, production warehouse/deployment,
large historical loads, paid credentials, and data redistribution.

Never: copy sibling secrets/config/databases, edit vendored library files, claim
unobserved historical vintages, or interpret short-volume shares as short interest.

Defaults proposed for review: CFTC TFF futures-only first; SQLite local backend;
Python 3.12; future adapters deferred. The next workflow step after spec review is
an implementation plan and task breakdown, then verified vertical slices.
