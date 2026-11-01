# Specification index

Updated: 2026-11-01. Numbers identify documents, not automatic build priority.

| Spec | Capability ID | Contract and status |
| --- | --- | --- |
| [001](spec001/README.md) | positioning-market-structure | Original broader design; CFTC, FINRA and OFR remain planned. |
| [002](spec002/README.md) | aaii-initial-slice | Existing AAII ingestion contract; see the current handoffs for verification status. |
| [003](spec003/README.md) | aaii-completion | AAII local completion implemented and verified; recurring acquisition and production deployment remain gated. |
| [004](spec004/README.md) | fx-reference-rates | Initial scope accepted; [MacroMedallion overlap/gap review](../docs/reviews/spec004_macro_medallion/README.md) completed 2026-10-30. All nine ECB currencies already stored upstream; local connector deferred. Temporal, precision/metadata and FX Gold gaps identified. |
| [005](spec005/README.md) | company-web-traffic | New draft: company/competitor visit estimates, free-first feasibility, optional paid Similarweb access. No connector implemented. |
| [006](spec006/README.md) | cftc-tff-futures-only | Draft provider contract for the CFTC TFF Futures Only report. API dataset/schema, timing, identity, correction and access boundaries verified; no connector implemented. |
| [007](spec007/README.md) | naaim-exposure-public-delayed | Local Bronze/Silver/delayed-latest slice, one manual smoke run and a read-only local health check verified for NAAIM's three-month-delayed internal research feed. Scheduling remains disabled; it cannot supply QuantMeridian. |
| [008](spec008/README.md) | occ-options-volume-query | Draft OCC options put/call-volume query contract. Filtered scope, CSV schema, terms, history and rate behavior require review; no connector implemented. |
| [009](spec009/README.md) | cboe-published-put-call-ratios | Draft Cboe published daily put/call-ratio contract. Date-selection, history, terms and source shape require review; no connector implemented. |
| [010](spec010/README.md) | nasdaq-daily-market-breadth | Draft Nasdaq Trader advances/declines/unchanged-count contract. Year coverage, universe semantics, terms and source shape require review; no connector implemented. |

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
delayed-public research capability has a local slice and one manual smoke run,
while written permission evidence remains an operational prerequisite. Among
the three new proposals, discovery order is Spec 009 (Cboe ratios) → Spec 010
(Nasdaq breadth) → Spec 008 (OCC volumes). This is a provider-contract
sequence, not authorization for live acquisition or a change to the active
NAAIM/CFTC backlog. QuantMeridian remains a separate Program Partner/API
capability. There is no new shared-platform rebuild prerequisite.

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
