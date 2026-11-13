# SSV-T09 acceptance — scoped research Gold and prior-date evidence

Accepted 2026-11-13 on `feat/finra-local-revisions`, added to existing
[PR #23](https://github.com/thatquantguy44/market-medallion/pull/23) at the user's
request. Traces **SSV-REQ09 / SSV-AC09**, completing local Checkpoint C alongside
T06–T08. [Contract](research_gold_contract.md), [tasks](todo.md),
[conformance](evidence/t09_research_gold_conformance.json) and
[local runbook](../../docs/deployment/ssv_local_runbook.md).

## Delivered behavior

The separate `research-gold` CLI/library reader selects complete committed
partitions for an explicit disjoint TRF population in one read-only SQLite
transaction. Each symbol requires a reported row in every selected facility.
Short/exempt/total quantities sum exactly; ratios use corresponding summed
numerators/denominators and the unchanged T04 34-digit half-even method.
An asymmetric 40/100 plus 9/10 fixture yields **49/110**, not a mean of ratios.
Zero reported totals retain quantities and null ratios with ZERO_TOTAL.

Missing partitions produce INCOMPLETE_POPULATION. A complete empty facility or
absent symbol produces NO_REPORTED_ROW and unavailable population quantities;
it does not supply zero or shrink the selected population. Overlapping,
consolidated, ADF/ORF/TXT and unreviewed scopes refuse before warehouse access.
Existing `gold`, capture, ingestion and SQLite schema behavior remain compatible.

The prior date is the nearest earlier population-complete date within explicit
lookback at the same local time/sequence cutoff. A missing symbol on that date
withholds prior ratios/change; an older symbol row is not substituted. Output
reports prior date, calendar-day gap, observed incomplete dates skipped,
component/capture/version/source-row lineage, ratio changes and local knowledge
labels. Calendar gaps include weekends without inferring trading sessions.
Current and prior ratios must both be defined for a change. Partial/failed
refreshes preserve the committed membership used by this reader; operational
freshness remains inspectable through coverage/audit.

## Verification actually performed

- **52 new tests:** 19 pure arithmetic, 32 query/temporal/concurrency and one real
  CLI test. **431 FINRA tests** pass, including all 71 existing T04 semantic cases.
- Full offline suite: **1,419 passed, 92 optional skips**, with external database
  variables unset. Ruff lint/format, wheel/sdist builds and packaged source,
  launcher, configuration, fixture and acceptance/evidence readback pass.
- Independent temporary-root CLI conformance used **eight invented captures**
  across two TRFs and two dates. It verified 49/110 and prior 29/110 against
  Fraction oracles, prior revision, prior/current empty evidence, partial-refresh
  preservation and original cutoff reproduction. Research reads left the full
  raw/warehouse file inventory byte-identical. No metadata privacy sentinel persisted.
- A disposable WAL concurrency test forces a writer to commit between two
  constituent reads: the report remains 49 shares in its original snapshot;
  the next report observes 108. No production/configuration setting was changed.
- Numeric tests cover 4096-digit values, extreme exponent alignment and derived
  ratios whose coefficient requires a wider exponent budget. Exact summation
  traps inexact/rounded operations. Caller Decimal context does not affect output.
  The extreme-ratio serialization test first failed, then passed after the bound
  included the fixed ratio coefficient width; source budgets are unchanged.
- Admission, overlapping/excluded scopes, invalid cutoffs, lookback and symbol
  limits refuse explicitly. Lookback defaults to 366 calendar days (1–3660);
  output defaults to 15,000 symbols (1–250,000), with no silent truncation. The same
  budget rejects oversized current/prior snapshots before loading row documents.
- Eight preexisting modules, including source parser/quantity codec, Bronze and
  SQLite schema/version bodies, remain byte-identical to T06–T08. The warehouse
  snapshot selection helper is shared with legacy Gold to avoid divergent rules.
- Agent-skills verified at its existing pin (188 entries). QuantSmith working
  checkout drift persists; all 61 pinned Git guidance objects were verified
  independently without modifying the checkout or refresh pins.

The source schema/use grant is unchanged. Official API, Equity terms and Python
Decimal references were checked before coding. No provider-data request,
credential loading, existing operational-store change, Slack message, service
startup or acquisition schedule activation was part of T09.

## Next boundary

**SSV-T10:** typed Parquet and coherent database/file publication with the C07
journal and failure/restart checks. T11 replay, acquisition, source-aware health,
research interchange/export and Dagster remain later work. Checkpoints B/C are
accepted for invented local input; G1/G2 and real complete-partition/history
coverage remain open. Keep NAAIM paused and schedules stopped.
