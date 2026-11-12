# Specification index

Updated: 2026-11-12. Numbers identify documents, not automatic build priority.
FOMC T01–T05 are accepted for the bounded 1990-onward foundation; current pickup is
[FOMC-T06 bounded acquisition/operations](spec017/todo.md#fomc-t06--bounded-acquisition-and-operations-next).
The remaining securities-finance order is **011 → 012 → 013 → 014 → 015**;
see the [securities-finance program](../docs/securities_finance/README.md).

| Spec | Capability ID | Contract and status |
| --- | --- | --- |
| [001](spec001/README.md) | positioning-market-structure | Original broader design; CFTC, FINRA and OFR remain planned. |
| [002](spec002/README.md) | aaii-initial-slice | Existing AAII ingestion contract; see the current handoffs for verification status. |
| [003](spec003/README.md) | aaii-completion | AAII local completion implemented and verified; recurring acquisition and production deployment remain gated. |
| [004](spec004/README.md) | fx-reference-rates | Initial scope accepted; [MacroMedallion overlap/gap review](../docs/reviews/spec004_macro_medallion/README.md) completed 2026-10-30. All nine ECB currencies already stored upstream; local connector deferred. Temporal, precision/metadata and FX Gold gaps identified. |
| [005](spec005/README.md) | company-web-traffic | New draft: company/competitor visit estimates, free-first feasibility, optional paid Similarweb access. No connector implemented. |
| [006](spec006/README.md) | cftc-tff-futures-only | Draft provider contract for the CFTC TFF Futures Only report. API dataset/schema, timing, identity, correction and access boundaries verified; no connector implemented. |
| [007](spec007/README.md) | naaim-exposure-public-delayed | Local delayed feed, health, source-owned schedule and versioned local research export implemented. User paused NAAIM on 2026-11-03; schedule stopped. Local QuantMeridian artifact use is documented; public/current-data scope remains separate. |
| [008](spec008/README.md) | occ-options-volume-query | Draft OCC options put/call-volume query contract. Filtered scope, CSV schema, terms, history and rate behavior require review; no connector implemented. |
| [009](spec009/README.md) | cboe-published-put-call-ratios | Draft Cboe published daily put/call-ratio contract. Date-selection, history, terms and source shape require review; no connector implemented. |
| [010](spec010/README.md) | nasdaq-daily-market-breadth | Draft Nasdaq Trader advances/declines/unchanged-count contract. Year coverage, universe semantics, terms and source shape require review; no connector implemented. |
| [011](spec011/README.md) | settlement-stress | First: SEC FTD plus Nasdaq/NYSE Reg SHO; bounded native-source delivery accepted through PR #8, with PostgreSQL/local Delta, replay and stopped jobs. See its acceptance record and master handoff for scope limits. |
| [012](spec012/README.md) | finra-short-sale-volume | Second: T01–T04 admission, three-TRF registry, pure JSON parser and exact quantity/ratio helpers complete through PR #10. Next T05 raw/rejected-capture receipts; acquisition, warehouse and jobs absent. |
| [013](spec013/README.md) | finra-otc-transparency | Third: delayed weekly ATS/non-ATS security/participant activity, tier/coverage-safe metrics. Full planning set; connector absent. |
| [014](spec014/README.md) | sec-nport | Fourth: public fund holdings/lending disclosures with accession-safe joins, amendments and availability eras. Full planning set; connector absent. |
| [015](spec015/README.md) | nyfed-primary-dealers | Fifth: aggregate dealer positions/transactions/financing/fails with exact series metadata, units, suppression and definition eras. Full planning set; connector absent. |
| [016](spec016/README.md) | presidential-research-data | Draft multi-repository port and complete PRES integration contract/plan; target package, bundle publication and consumer additions absent. |
| [017](spec017/README.md) | fomc-meeting-minutes | T01–T05 accepted for the 1990-onward bounded archive/native-text/chunk foundation: 12 representations, 74 PDF pages, 241 chunks (166 eligible, 75 review-flagged), coherent mirrors and replay. Next T06 bounded acquisition/operations. Full backfill/accuracy review and optional embeddings remain separate. |

## Presidential pipeline integration planning

[Spec016](spec016/README.md) is the 2026-11-04 user-requested multi-repository
specification and plan for porting the research website's presidential pipeline
into MarketMedallion and completely integrating its outputs into QuantMeridian
PRES. It includes the data contract, staged rollout, source inventory and task
tracker. The target package and consumer integration remain unimplemented.
This is a planning addition; existing source-build priority and the NAAIM pause
remain as recorded below.

## Current priority: securities-finance program

The user's 2026-11-03 request selects all five workstreams in the order above.
FTD and Reg SHO are one research delivery with two independent source packages,
so the five workstreams preserve all six source families discussed. Read the
[capability map](../docs/securities_finance/README.md),
[common contract](../docs/securities_finance/contract.md),
[program plan](../docs/securities_finance/plan.md),
[coordination tasks](../docs/securities_finance/todo.md) and
[delivery roadmap](../docs/securities_finance/roadmap.md).

Delivery specs own `README.md`, `data_contract.md`, `plan.md` and `todo.md`;
Spec017 has an inventory contract and [implementation task handoff](spec017/todo.md).
The [master handoff](../docs/handoffs/project_bootstrap.md) records accepted
Spec011 delivery and completed Spec012 T01–T04. FINRA's continuation is **SSV-T05**
from its [next-agent handoff](spec012/README.md#next-agent-handoff). The newly selected
FOMC build now continues at **FOMC-T06** after accepted T01–T05 archive/text/chunk checks. The source sequence supersedes older discovery priorities;
the following historical choices remain context, not a competing next-build
instruction. Keep NAAIM paused unless explicitly resumed by the user.

## Capability boundaries and proposed sequence

Specs 004 through 010 are independent source capabilities. All depend on the
existing Bronze, source-specific Silver/Gold, audit, replay, Parquet and
warehouse boundaries described in [architecture](../docs/deployment/architecture.md).
No proposed pipeline depends on another, and none changes AAII semantics.

The FX overlap review supersedes the proposed new-ingester sequence: address
MacroMedallion temporal correctness → exact values/metadata → FX-specific Gold.
The existing Spec 004 contract supplies acceptance criteria for that follow-up.
Web traffic source feasibility can proceed independently; acquisition waits for a source with suitable access and
retention rights. Spec 006 preserves the reviewed CFTC contract. Spec 007's
delayed-public research capability has a local slice and an attributed local
QuantMeridian research artifact, and is currently paused. The former proposal
Spec009 (Cboe ratios) → Spec010 (Nasdaq breadth) → Spec008 (OCC volumes) is
retained as deferred discovery order. The new securities-finance sequence is
the next source-build program. There is no shared-platform rebuild prerequisite.

The user selected **daily official reference rates**, **company/competitor
traffic estimates**, and **prefer free sources; document paid option** on
2026-10-30. The user subsequently confirmed Spec 004's proposed initial FX
scope covers the need; its acceptance record distinguishes settled coverage
from remaining engineering and operational choices. Spec 005's provider/universe
choices remain proposals. No purchase, live acquisition or schedule enablement
is authorized by these documents; no implementation was requested in the scope
confirmation exchange. These future specs do not change the AAII completion scope or
silently reprioritize the existing CFTC/FINRA/OFR backlog. The user explicitly
reprioritized the NAAIM provider discovery in Spec 007 on 2026-10-31; its
manual local capture does not authorize a purchase, recurring API use or
production integration. Specs 008–010 document new potential pipelines only;
each must clear its own terms, schema, coverage and fixture gates before
implementation.

## Picking up a build

Read the root [agent guidance](../AGENTS.md), [master handoff](../docs/handoffs/project_bootstrap.md)
and the selected numbered spec. Reconcile the current working tree and handoff
before creating a source-specific implementation plan. Keep active AAII tasks in
`tasks/plan.md` and `tasks/todo.md` intact. Use a separate branch/worktree and
source-scoped task files when implementation is authorized; reserve new spec
numbers by checking this directory first.

The [roadmap](../docs/roadmap.md) retains the original source backlog. Its
historical count of 14 additions refers to that original inventory; the FX,
web-traffic, NAAIM, OCC, Cboe and Nasdaq proposals are separately tracked
numbered capabilities.
