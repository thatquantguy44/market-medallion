# Delivery roadmap — securities-finance research data

**Updated:** 2026-11-03. **Planning horizon:** milestone based, without invented
completion dates or throughput estimates. All five research deliveries remain
open; Spec011 has an initial offline SEC parser/manifest/inspection slice.
No new acquisition adapter or job exists yet.

## Priority and backlog relationship

This user-selected sequence supersedes the previous **next-source** Cboe →
Nasdaq breadth → OCC discovery proposal and older NY Fed-first recommendations.
Those candidates, CFTC TFF, OFR and the other existing source IDs remain in the
master backlog; they are deferred rather than removed. NAAIM stays paused.

| Rank | Delivery | Why this order | First practical deliverable | Required predecessor gate |
| --- | --- | --- | --- | --- |
| 1 | [Spec011 settlement stress](../../specs/spec011/README.md) | Security-level settlement and threshold evidence establishes the most direct equity-finance research base | One validated FTD partition plus observed Nasdaq/NYSE membership timeline, with honest mapping gaps | None |
| 2 | [Spec012 daily short volume](../../specs/spec012/README.md) | Adds scoped trading flow alongside settlement outcomes | Complete selected facility/day partitions and matched short/total ratios | Spec011 research-delivery gate |
| 3 | [Spec013 OTC transparency](../../specs/spec013/README.md) | Adds venue/participant context to the first two deliveries | Complete selected weekly security/participant reports and coverage-labelled metrics | Spec012 research-delivery gate |
| 4 | [Spec014 N-PORT](../../specs/spec014/README.md) | Adds disclosed fund holdings and lending context after simpler file/version semantics are established | One quarter's admitted public filing subset with amendment-safe holdings/lending outputs | Spec013 research-delivery gate |
| 5 | [Spec015 primary dealers](../../specs/spec015/README.md) | Completes macro collateral and dealer-financing context without mixing it into security-level claims | Selected current-regime financing/fails series with exact metadata | Spec014 research-delivery gate |

The predecessor gate controls delivery focus only. None of the latter sources
requires another's raw tables to parse or validate its own feed. Source-review
research may be prepared in advance; simultaneous implementation is not the
default build plan. If the selected free route cannot support the intended use,
record a blocked source contract and obtain a revised order rather than quietly
changing providers or excluding the hard portion of a delivery.

## Gate definitions used for every delivery

| Gate | Required evidence | What it unlocks |
| --- | --- | --- |
| G0 — contract ready | Official access/usage evidence; source schema/sample; selected universe/history; grain and clocks; explicit remaining gaps | Provider-shaped invented fixtures and bounded offline implementation |
| G1 — local slice ready | Raw archive, typed Silver, local Gold, audit and atomic publication; malformed and incomplete inputs fail visibly | Broader fixture coverage and observed-revision/replay implementation |
| G2 — research delivery | Offline tests; unchanged/revised/reverted/removed rows; honest observed-as-of/replay; bounded export; manual CLI/health/recovery; bounded transport tested offline; stopped job/schedule definitions; real-source conformance via permitted fetch or accepted actual local file | Acceptance of this workstream and start of the next workstream |
| G3 — recurring operation activation | Current source terms/calendar, explicit live configuration, operator decision to enable the delivered stopped schedule, service availability and actual tick/run check | Deliberate recurring acquisition for the selected source |
| G4 — consumer integration | Accepted artifact contract and audience/use terms, separate QuantMeridian change and verification | Local downstream presentation |

G2 is the required research completion milestone. It includes implementation
of bounded transport tested with injected responses, no-request health/recovery
and **stopped** orchestration definitions; actual schedule activation is G3.
The source task lists' final delivery checkpoints close G2 after these code and
evidence tasks pass. Creating a stopped definition does not start a service.

A source can complete real-source conformance using approved actual local files
when remote acquisition is unresolved, provided provenance and selected scope
are validated and the owner explicitly accepts that local mode. Only dependent
live access remains disabled. SSV-T12/OTC-T12 and the other source conformance
tasks accept that path; their injected-transport tests still run. A fixture-only
prototype is **not** verified real coverage. G3/G4, durable deployment and
consumer UI are not prerequisites to the next source. Required selected source
coverage must be met or the owner must explicitly revise the selected scope.

## Delivery 1 — settlement evidence

Start with the SEC FTD public file contract and safe ZIP parser. Deliver one
recent post-2008 semi-monthly partition offline, immutable balances and honest
missing-row coverage, then corrections/replay. Add separate exchange threshold
snapshots with market/date validation and membership episode states; only then
join them through dated identifier evidence. The first useful report shows two
evidence timelines even when security mapping remains unresolved.

Historical schema phases: modern semi-monthly files first; older monthly files
and the pre-2008 reporting threshold are separately gated extensions. Nasdaq
and NYSE keep distinct contracts and completeness; one exchange working does
not complete the combined threshold coverage. A recent Nasdaq notice announces
December 2026 changes to file availability, so effective-date scheduling and
trade-date interpretation require explicit review before activation.

## Delivery 2 — reported short-sale activity

Use FINRA's approved free file route if suitable for the intended use; reconcile
the general terms and dataset-specific permissions first. Begin one admitted
reporting scope and date. Model short-exempt as an included subset of short
volume, preserve facility scope and partition completeness, then expand the
facility universe and corrections. Keep a consolidated result and its component
facility files as alternative coverage paths; never add both.

The delivered ratio describes its reported trading population. A whole-market
short-interest or borrow-demand measure is not an output. Any later cross-feed
research joins use availability cutoffs, declared date alignment and unresolved
symbol mapping states.

## Delivery 3 — OTC venue context

Start with a single weekly report type/tier and documented security/participant
records. Expand ATS and non-ATS independently, preserving disclosure lags,
de-minimis aggregation and current historical file conventions. Revisions to a
week are new observations. Only compute venue share/concentration after the
selected participant population is complete and the denominator is explicitly
defined. Report tiers, blocks, monthly summaries and weekly detail are not
automatically additive or interchangeable.

## Delivery 4 — disclosed fund lending and holdings

Inventory an archive's metadata and relational tables before loading values.
Implement the filing/accession and fund-series hierarchy, then holdings and
securities-lending details with join-count assertions. Process amendments and
filing/public availability clocks before comparing fund reports. Publish only
observations from the public population; no reconstruction of nonpublic months.

A bounded archive/fund universe keeps the first pass manageable. Profile actual
ZIP expansion, table sizes and processing memory before committing to a wider
backfill budget. Disclosed holdings, collateral and on-loan values do not
establish available stock-loan inventory or utilization without another source.

## Delivery 5 — aggregate collateral/funding context

Select a small explicit list of primary-dealer series from official metadata in
the current regime. Validate units and whether each measure is a weekly
cumulative amount, average or point-in-time balance. Retain suppression reasons
and revisable observations. Add historical reporting regimes in separate
partitions with equivalence evidence before a continuous comparison is offered.

This delivery remains aggregate macro context. It cannot identify a security's
repo specialness, a broker's exposure or a specific securities loan. Existing
macro coverage should be inventoried read-only before building a duplicate
connector; the source-owned contract and gaps determine the final boundary.

## Readiness tracker

| Delivery | G0 | G1 | G2 | G3 | G4 | Current status / next task |
| --- | --- | --- | --- | --- | --- | --- |
| 011 | Open: exchange gates | Offline parser/sample check complete; storage pending | Not started | Not started | Deferred | T02 reconciles both approved September2026 SEC archives; next T04 Bronze/SQLite/audit and T03 rejected-input retention |
| 012 | Open | Not started | Not started | Not started | Deferred | Await011; resolve free-route use/retention scope |
| 013 | Open | Not started | Not started | Not started | Deferred | Await012; confirm report family and file/API contract |
| 014 | Open | Not started | Not started | Not started | Deferred | Await013; pin current schema and public availability regime |
| 015 | Open | Not started | Not started | Not started | Deferred | Await014; inventory overlap and select series/regime |

Update a gate only with a link to reproducible evidence in the source tasks or
handoff. Record fixture count, selected real partitions, exclusions and actual
verification date. An unselected history/universe remains explicitly outside
the delivered coverage.
