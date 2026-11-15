# Spec012 branch review and regression verification

Reviewed `feat/finra-research-orchestration` against current `origin/main`
`1daf6b5`, starting at `3f0043a`. This review covers T14 interchange, T15
orchestration, T16 evidence and root composition. Findings below are fixed;
no unresolved blocking finding remains from this review. Merge/remote CI and
G3 activation are separate from these local results.

## Findings and fixes

1. **P1 — publication/export generation race.** A second publisher could advance
   the current pointer between the job's publication and export. The job would
   report generation A but write generation B. A deterministic interleaving test
   reproduced successful materialization of that mismatch. The exporter now
   accepts an optional `expected_generation_id`; Dagster always supplies its
   published generation. A mismatch fails before output writes, preserving both
   committed generations. CLI callers can use `--expected-generation-id` too.
2. **P2 — contradictory consumer lineage and coverage.** Readback accepted a
   value first seen after its observation capture, inconsistent origin sequence/
   receipt metadata, invalid row/symbol types, or COMPLETE/VALID_EMPTY claims
   inconsistent with reported rows. Reproducing fixture mutations drove checks
   for canonical archive clocks, capture identity/sequence/receipt consistency,
   bounded positive row/sequence integers, raw symbols and partition coverage.
   Reordered repeat captures still retain their distinct observation row index.
3. **P2 — incomplete or superseded consumer membership.** Readback could accept
   a dropped symbol, an invented all-missing symbol, or an old version despite
   retaining a newer version in its history. Four failing reproductions drove
   reconstruction of the latest value/removal membership at each declared
   current/prior capture, including prior-only symbols. Rows/components must match that membership and its complete current
   symbol population. The reconstruction is linear in the bounded history;
   it makes no mutable warehouse or network lookup.

## Regression and equivalence evidence

**24 tests added:** 12 failing bug reproductions, four generation/width boundary
checks, seven full-flow equivalence scenarios and one prior-window negative case.
Final totals: **53 export tests, 22 Dagster tests and 8 equivalence tests**;
**756 FINRA tests** and **1,777 full-suite passes / 92 optional integration skips**.
The CLI subprocess test also verifies matching/mismatching generation flags.
Repository Ruff lint and formatting pass (330 Python files). Package build and
exact source/wheel readback are checked before this review commit.

The seven equivalence scenarios compare native research results to the frozen
export, then replay the full captured source into a fresh empty target and compare
all artifact fields except the new publication event identity:

| Scenario | Invariant checked |
| --- | --- |
| Unchanged capture with reordered rows | Value origin stays unchanged; observation capture/row indices update correctly. |
| Correction followed by reversion | Every observed version survives and the latest reversion is selected. |
| Removal and missing facility rows | Removal lineage survives; no denominator or zero is fabricated. |
| Time cutoff | Later changes are excluded while prior complete membership survives. |
| Same-clock sequence cutoff | Equal timestamps do not collapse independently observed capture sequences. |
| Valid empty population | All empty/current/prior partition evidence survives without invented rows. |
| Exact fractional exponents | Quantity text, units, ratios and lineage survive query/publication/export/replay. |

The independent frozen invented consumer fixture is exercised for accepted
readback and contradictory metadata. Existing arithmetic tests retain their
independent rational references; equivalence alone is not an arithmetic oracle.
All tests use invented data or injected HTTP, pinned clocks and isolated roots.

## Existing real-source artifact equivalence

[Review conformance](evidence/branch_review_conformance.json) binds to the earlier
[T16 acquisition evidence](evidence/t16_delivery_conformance.json). Offline checks
reread every frozen source/replay table, validate the accepted artifacts with the
stricter consumer, and reproduce both research files byte for byte with explicit
generation binding. The existing accepted scope remains the three TRFs on
2026-11-09: 25,230 source observations and 12,355 research rows. All 115 source
and 88 replay files retain their prior inventory hashes. No provider request,
Slack message or schedule activation occurred.

## Limits and pickup

This is local verification for the selected SQLite/API profile. The 92 optional
integration skips remain explicit; they do not establish new PostgreSQL, Delta,
cloud or recurring-service support. API release timing remains unknown, the
reviewed default calendar ends 2026, and broader history is outside acceptance.
No separately staffed/model review or remote CI is claimed. The next step is
review/merge of this branch, then Spec013 OTC-T01 from latest main. Keep schedules
unactivated and NAAIM paused.
