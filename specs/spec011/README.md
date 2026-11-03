# Spec011 — Settlement stress: SEC FTD and Reg SHO threshold history

**Capability:** `settlement-stress`. **Order:** 1 of 5 in the
[securities-finance program](../../docs/securities_finance/README.md).
**Updated:** 2026-11-03. **Status:** initial offline SEC manifest, ZIP/pipe parser
and local `validate`/`inspect` commands implemented; two approved September2026
archives reconcile under profile version2. Next: T18 portable storage/schema,
then T04 Bronze/SQLite/audit including rejected-input retention. PostgreSQL and
Databricks compatibility are required delivery work. Acquisition, persistence, research
exports, exchange adapters and Dagster remain unimplemented.

**Source IDs:** retain `sec-fails-to-deliver`; add `reg-sho-thresholds` for the
Nasdaq/NYSE list family. **Proposed source packages:**
`src/market_data_pipeline/pipelines/sec_fails_to_deliver/` and
`src/market_data_pipeline/pipelines/reg_sho_thresholds/`.

Read [data contract](data_contract.md), [implementation plan](plan.md),
[tasks](todo.md) and the [common program contract](../../docs/securities_finance/contract.md).
The [first source-review checkpoint](source_review.md) records current evidence,
selected partitions, implemented offline behavior and remaining admission gaps.
SEC FTD now has [pipeline-specific configuration](../../config/sec_fails_to_deliver/README.md)
and no-request config validation. PostgreSQL is the user-selected default;
Databricks stays disabled until configured. Storage adapters remain absent.

## Objective and research users

Give a securities-finance researcher an auditable timeline of reported equity
settlement failures and observed exchange threshold-list membership. The first
delivery must answer: which balance was reported for a settlement date, which
list included a security on a particular source date, what coverage supports an
apparent absence, and when did this pipeline actually learn each version?

This workstream combines research presentation while preserving independent
provider facts. SEC balances and exchange membership cannot be forced into one
daily scalar series. Neither feed identifies borrow fees, available shares,
failure cause or an individual trader. No fraud/naked-short classification,
buy-in forecast or automatic trade recommendation is part of this delivery.

## Verified evidence and unresolved source choices

Official sources were reviewed on 2026-11-03; this is a documentation review,
not a complete data acquisition or live parser verification.

| Evidence | Verified fact | Implementation uncertainty |
| --- | --- | --- |
| [SEC FTD page](https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data) | ZIP downloads contain pipe-delimited settlement balances and six published fields; two approved local September archives verify header/member/ASCII and count/quantity trailers | Broader historical format, expected calendars and population completeness remain unverified |
| Same SEC notes | Modern absence and pre-2008 reporting-threshold rules differ; price may be `.` | Partition/date coverage and identity universe must be proven before an absent row has meaning |
| [SEC developer guidance](https://www.sec.gov/about/developer-resources) | Automated access must be identified/paced; published ceiling is 10 requests/second across machines | Select a lower single-process budget and correct contact configuration before network implementation |
| [Nasdaq threshold page](https://www.nasdaqtrader.com/Trader.aspx?id=RegSHOThreshold) | Lists expose trade date, file creation time and a linked text file | Archive completeness, retention/use rights, and historical symbol identity remain to verify |
| [Linked Nasdaq text example](https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth20261102.txt) | Pipe header plus terminal creation token observed; no CUSIP field | Do not assume all eras have that format or parse the trailer as a security |
| [NYSE threshold page](https://www.nyse.com/regulation/regulation-sho) | Public date/market selectors and Download link exist | The unparameterized linked download failed during review; exact permitted date/market values, schema and historical coverage remain open |
| [Nasdaq copyright/use notice](https://www.nasdaqtrader.com/Trader.aspx?id=CopyDisclaimMain) | Storage/reuse rights are constrained and additional use may need consent | Reconcile intended recurring capture, durable history and report audience before dependent live use |
| [Nasdaq October 30 alert](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2026-64) | Announces December 6, 2026 expanded-hours/file-timing changes | Pin effective-date behavior; future notice is not evidence current files already use the new clock |

NYSE terms are linked from the official page. Resolve them and each market's
download allowance in S11-T01/T10; no hidden endpoint discovery, authenticated
access or circumvention is needed for the planned public-file scope. A blocked
exchange does not prevent SEC offline implementation, but cannot be marked as
complete threshold coverage.

## Selected scope and later extensions

The first candidate scope is one recent post-2008 SEC semi-monthly partition
and a small explicitly selected run of Nasdaq and NYSE list dates/markets.
S11-T01 records the exact selections, source sample and size limits. A broader
modern backfill follows only after replay and completeness tests. Older SEC
monthly/quarterly archives and the pre-2008 reporting regime are a separate
extension; do not pretend a recent-file parser covers all historical eras.

Capture both published list presence and full-list coverage. Derive observed
episode entry/exit only across adjacent complete list dates in a validated
market calendar; unobserved dates interrupt certainty. An episode already
present at the first collected date is left censored. A still-listed final
observation is right censored. Neither streak length nor exit time reconstructs
the age or settlement of a particular failed obligation.

## Requirements

| ID | Requirement |
| --- | --- |
| S11-R01 | Admit and record the exact source routes, usage scope, schema/sample, partitions, reporting regimes and release expectations independently for SEC/Nasdaq/NYSE. |
| S11-R02 | Archive bounded original ZIP/text payloads and source-selection receipts; validate archive safety and full partition structure before promotion. |
| S11-R03 | Preserve SEC settlement-date/CUSIP balances, raw symbols, exact decimal price/null token and historical reporting rule. |
| S11-R04 | Preserve per-provider/market/list-date membership, flags, list metadata and distinct complete-empty/missing/failed outcomes. |
| S11-R05 | Preserve natural identifiers and versioned dated mapping evidence; expose unmapped/ambiguous identity without name/symbol guesses. |
| S11-R06 | Retain unchanged idempotency plus corrections, reversions and authoritative complete-partition row removals under capture-time knowledge. |
| S11-R07 | Support current-vintage and observed-as-of queries, deterministic replay, SQLite/PostgreSQL, typed Parquet/Delta and recovery at publication boundaries. |
| S11-R08 | Produce scoped FTD history and threshold episodes with coverage/censoring/gap evidence, never synthetic fail age or zero-filled calendar history. |
| S11-R09 | Publish independent bounded research artifacts; optionally produce an identified joined view using only those artifact contracts and admitted mappings. |
| S11-R10 | Supply bounded manual acquisition/local-import commands, explicit backfill inventory, no-request health and stopped Dagster definitions after local semantics pass. |
| S11-R11 | Reconcile a bounded permitted real sample and document measured coverage, unresolved gaps, test evidence and operational handoff. |
| S11-R12 | Deliver SQLite and PostgreSQL warehouse adapters plus Databricks Delta publication for both source packages, with exact-value, revision, coverage and knowledge-clock parity. Backend implementation and verification are required before G2; cloud deployment is separate. |

NFRs: **S11-N01** deterministic offline tests and replay; **S11-N02** configurable
download/expansion/row/storage bounds with atomic failure; **S11-N03** sanitized
diagnostics and independent provider roots; **S11-N04** no existing-pipeline
regression or NAAIM activation. Common requirements C01–C11 apply in full.

## Acceptance criteria

| ID | Testable acceptance | Requirements |
| --- | --- | --- |
| S11-A01 | Invented ZIP fixture preserves a leading-zero CUSIP, settlement date, integer balance and `.` price as null; malformed date/count/header fails visibly. | R02,R03 |
| S11-A02 | Unsafe path, duplicate member, oversized expansion, HTML/error payload or incomplete file leaves the committed snapshot unchanged. | R02,N02 |
| S11-A03 | Incomplete/missing list never emits an exit event; valid empty list is distinguishable from a fetch failure. | R04,R08 |
| S11-A04 | Nasdaq creation trailer is excluded from membership; requested/source/filename dates and selected markets reconcile or fail. | R01,R04 |
| S11-A05 | Same payload gives no new fact version; correction, reversion and complete replacement row removal remain selectable at their true knowledge cutoffs. | R06,R07 |
| S11-A06 | Missing modern FTD row stays an explicit absence state unless complete partition/universe evidence permits an explicitly derived zero; pre-threshold absence is never normalized to zero. | R03,R08 |
| S11-A07 | Dated unambiguous mapping joins once; reused symbol, overlapping mapping or unknown CUSIP remains unresolved and does not multiply balances. | R05,R09 |
| S11-A08 | Episode fixture handles holidays, a missing list, left/right censoring, entry and confirmed exit without claiming legal fail/close-out duration. | R08 |
| S11-A09 | Network-disabled replay reproduces normalized versions/Gold and source first-seen clocks; equal capture timestamps use persisted sequence order. | R06,R07,N01 |
| S11-A10 | Failure injection before/after DB commit, file finalization and pointer publication recovers one coherent consumer snapshot. | R07,N02 |
| S11-A11 | Two source artifacts validate independently; the optional joined output lists input snapshot/mapping versions, cutoff and unmatched counts. | R09 |
| S11-A12 | Manual CLI requires bounds; no-request health makes no request; schedule definitions stay stopped and NAAIM remains paused. | R10,N03,N04 |
| S11-A13 | Bounded permitted sample reconciles selected source counts/values and explicitly records unavailable market/history coverage. | R01,R11 |
| S11-A14 | The same invented capture/revision/withdrawal/replay sequence produces equal normalized facts, coverage, audit outcomes and cutoff results in SQLite and a dedicated PostgreSQL test database; leading-zero IDs, counts above int64, exact prices and unknown clocks round-trip unchanged. | R03,R06,R07,R12 |
| S11-A15 | Explicit-schema Parquet reads and idempotent Delta publication preserve exact values, version IDs, capture ordering and coverage for both sources; an actual Spark/Delta or Databricks integration run proves repeat publication and recovery after an interrupted multi-table publication. Decimal overflow/rounding fails before advancing the consumer generation. | R03,R06,R07,R12 |

The A01/A02 parser components pass invented-fixture tests and the September SEC
samples reconcile every field and both trailers. Persistent-publication and
exchange evidence remain open; full acceptance is not claimed by this slice.

## Gold and consumer boundary

Proposed artifacts are `sec-ftd-research/1` and `reg-sho-thresholds-research/1`.
An optional `settlement-stress-research/1` projection is owned by
`pipelines/sec_fails_to_deliver/research.py`. It consumes the documented
threshold artifact as an input, not threshold internals or its private database.
The projection publishes source facts side by side and their identity/coverage
qualification; native source exports remain useful when no mapping is admitted.

FTD quantity times the supplied price may be exposed only as a separately named
**previous-close reference-value proxy**, with the price date/basis and null
behavior visible. It is not an outstanding contract liability or borrowing
cost. No shares-outstanding/float denominator is available from these feeds;
FTD/float and securities-lending utilization are deferred until another dated,
admitted source provides the correct denominator.

## Architecture, testing and boundaries

Python 3.12; source-owned manifest validation, pure transforms and injected
transport; SQLite for the first offline loop, PostgreSQL as a supported warehouse
and typed Parquet → Unity Catalog Delta as a required publishing route. Both
source packages own their adapters. The pure-Python parser/core must import
without Spark or Databricks installed. No new dependency is selected by this
document. Backend types and validation are specified in
[the data contract](data_contract.md#9-storage-and-platform-compatibility).
Core tests are proposed as
`tests/test_sec_fails_to_deliver.py`, `tests/test_reg_sho_thresholds.py` and
`tests/test_settlement_stress_research.py`. Fixtures are invented and provider
shaped; no real identifiers or provider payloads are committed as test data.

Follow the common contract's type/style/verification commands. Always preserve
source identifiers and clocks, immutable raw lineage, coverage and revisions.
Resolve source terms/schema gaps before dependent live use or publication.
Never count FTD daily balances as new daily failures, derive fail age from
consecutive balances, infer abusive shorting, or make a symbol-only join look
like a verified security identity. Paid services, unrestricted redistribution,
live trading and consumer-repository implementation are outside this spec.

## Next bounded task

Start **S11-T18**, then **S11-T04**, in [todo.md](todo.md): pin the portable
storage/schema boundary before immutable original-input/manifest receipts,
typed SQLite state, audit and initial Gold. Include rejected-input
retention to close the remaining T03 item. T02 is complete for the two approved
September2026 archives; broader history, exchange contracts and G2 remain open.
T19–T22 deliver the required PostgreSQL and Databricks adapters and parity tests;
they are not implemented by the current parser slice.
