# Securities-finance data program — five ordered deliveries

**Decision date:** 2026-11-03. **Status:** comprehensive specification and
planning set; none of these new connectors, commands or jobs is implemented.
**Owner:** MarketMedallion. **Consumer:** local research and a future separately
scoped QuantMeridian reader. **Priority:** the user's next source-build program.

The user requested all five workstreams in order. The preceding discussion
listed six source families; this program treats **SEC FTD plus Nasdaq/NYSE Reg
SHO threshold history as one settlement-research workstream**. The two feeds
retain separate packages and grains. The other four workstreams follow the
listed order. This grouping preserves every source discussed.

## Capability map and delivery order

| Order | Stable workstream ID | Responsibility | Owning spec | Proposed package(s) | Data dependency |
| --- | --- | --- | --- | --- | --- |
| 1 | `settlement-stress` | SEC outstanding equity fails and exchange threshold membership/history | [011](../../specs/spec011/README.md) | `sec_fails_to_deliver/`, `reg_sho_thresholds/` | None for raw feeds; the optional joined output requires both |
| 2 | `finra-short-sale-volume` | Facility-scoped daily short and total reported volumes | [012](../../specs/spec012/README.md) | `finra_short_sale_volume/` | None |
| 3 | `finra-otc-transparency` | Weekly ATS/non-ATS security and participant activity | [013](../../specs/spec013/README.md) | `finra_otc_transparency/` | None |
| 4 | `sec-nport` | Public fund holdings and securities-lending disclosures | [014](../../specs/spec014/README.md) | `sec_nport/` | None |
| 5 | `nyfed-primary-dealers` | Aggregate dealer positions, transactions, financing and fails | [015](../../specs/spec015/README.md) | `nyfed_primary_dealers/` | None |

Delivery order is **011 → 012 → 013 → 014 → 015**. It is an execution priority,
not a dependency that forces one provider to import another's Python internals
or query its database. Source review can reveal a terms or availability blocker;
record it explicitly and request a reprioritization before skipping a workstream.

The optional joined settlement projection belongs to the FTD source's
`research.py` and consumes the published threshold artifact plus an explicit
dated mapping artifact. It does not read the threshold pipeline's private store.
Its named owner and input versions are defined in Spec011's contract.

The user has authorized this documentation set and sequence. The detailed
technical choices remain reviewable proposals where marked. Creating these
documents does not itself run an acquisition, enable a schedule, buy access or
implement a QuantMeridian module. NAAIM remains **paused** under the top-level
[handoff decision](../handoffs/project_bootstrap.md#immediate-operational-priority--naaim-paused).

## How to use the documents

1. Read this map, [common contract](contract.md) and [delivery roadmap](roadmap.md).
2. Read the selected spec's `README.md`, `data_contract.md`, `plan.md` and
   `todo.md`. Each source owns its canonical implementation task list there;
   the existing root `tasks/plan.md` and `tasks/todo.md` continue to own AAII.
3. Start at the first unchecked provider-contract task. Distinguish documented
   facts, observed samples, proposed normalized fields and unresolved evidence.
4. Build a narrow offline vertical slice and prove replay/revisions before
   enlarging history or enabling live acquisition. Record real verification
   evidence in the selected task file; documentation completion is not delivery.
5. Update the source register, roadmap and handoff at each completed gate. The
   [program plan](plan.md) and [program tasks](todo.md) track coordination and
   cross-source checks without duplicating source implementation tasks.

## Outcome and research value

The intended product is inspectable evidence for settlement operations,
securities lending and collateral research: dated failures, observed threshold
membership, scoped short-sale flow, venue concentration, disclosed fund
lending/holdings and aggregate dealer financing. Each result must explain
coverage, information delay and revision basis. This program does not assume
these sources measure borrow fees, live inventory, locates, utilization, recall
instructions or counterparty-level exposures. Those fields remain unavailable
unless an additional suitable source is separately specified.

Proposed first reports, all contingent on completed data contracts:

| Workstream | Initial useful research output | Essential interpretation limit |
| --- | --- | --- |
| Settlement | Security timeline of reported FTD balances and observed threshold episodes | The sources do not identify fail age, cause or an individual short seller |
| Daily short volume | Short-volume fraction within each admitted reporting scope and trading date | A daily trading flow does not establish an outstanding short position |
| OTC transparency | Weekly observed venue/participant concentration and average reported trade size | Delayed reported coverage does not establish price impact or total-market liquidity |
| N-PORT | Fund/security lending flags, disclosed values and ownership concentration in the admitted filing population | Holdings are not available-to-lend inventory; portfolio dates precede public knowledge |
| Primary dealers | Same-definition historical financing/fails context with publication/revision evidence | Aggregate community totals cannot identify a borrow special or a counterparty |

## Document status and evidence

Official provider pages and technical documentation were reviewed on
**2026-11-03**. Sources and material uncertainties are linked in each spec.
No complete production schema, historical coverage or permission regime is
certified by this research pass. The first tasks require a bounded provider
sample, pinned schema/terms evidence and explicit decisions for remaining gaps.
All test examples to be committed will use invented values and identifiers.

The plans assume Python 3.12, the existing manifest-driven medallion layout,
offline pytest, source-owned SQLite/Parquet, atomic publication, local manual
operation and stopped Dagster schedule definitions. PostgreSQL is an extension
after the source semantics pass; a hosted service is not a completion
prerequisite for the research delivery. See the common contract for what each
research milestone must actually prove.
