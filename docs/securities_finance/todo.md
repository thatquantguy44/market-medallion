# Program coordination tasks

**Updated:** 2026-11-03. Source tasks remain canonical in each numbered spec;
these tasks coordinate the program. All implementation boxes remain open.

## SF-P01 — Accept source boundaries and first delivery contract

**Dependencies:** none. **Scope:** S (1–2 documentation files).

- [ ] Confirm the five-workstream map and record any owner correction in README.
- [ ] Review Spec011's first selected FTD partition and Nasdaq/NYSE scope.
- [ ] Separate the research G2 milestone from optional schedule/UI delivery.

**Verify:** inspect the [capability map](README.md) and Spec011 evidence table;
every discussed source has exactly one owner and a next task. This documents
the user's requested sequence; technical approval is recorded when reviewed.

## SF-P02 — Close the first research-delivery gate

**Dependencies:** SF-P01 and Spec011 tasks through G2. **Scope:** M (3 docs).

- [ ] Link actual fixture and bounded live/local-file verification evidence.
- [ ] Record exact FTD/list date coverage and unmapped identifier limitations.
- [ ] Mark011 G2 only if both selected source slices and the research export pass.

**Verify:** reconcile Spec011 acceptance IDs against tests/replay/export checks;
review roadmap and pipeline register for consistent state. Likely files:
`specs/spec011/todo.md`, this program's roadmap, `docs/pipelines.md`.

## SF-P03 — Close FINRA short-volume delivery

**Dependencies:** SF-P02 and Spec012 tasks through G2. **Scope:** M (3 docs).

- [ ] Record admitted FINRA access/use scope and selected reporting facilities.
- [ ] Link exact denominator/subset and consolidated-overlap regression evidence.
- [ ] Record any unavailable historical facilities before opening013 build.

**Verify:** source artifact validates; reported-volume checks use matching
facility/date/symbol populations. Update Spec012 task record and readiness row.

## SF-P04 — Close OTC transparency delivery

**Dependencies:** SF-P03 and Spec013 tasks through G2. **Scope:** M (3 docs).

- [ ] Record complete selected ATS/non-ATS reports and publication-lag evidence.
- [ ] Confirm de-minimis participants, revisions and population-labelled metrics.
- [ ] Link the bounded export and reopen/read/replay verification.

**Verify:** test evidence covers incomplete weeks and denominator scope; the
readiness row and source register agree on actual report families and history.

## SF-P05 — Close N-PORT delivery

**Dependencies:** SF-P04 and Spec014 tasks through G2. **Scope:** M (3 docs).

- [ ] Record selected archives/fund universe and applicable schema/public regime.
- [ ] Link cardinality, amendment and public-availability regression evidence.
- [ ] Confirm research outputs label disclosures and never imply live lendable supply.

**Verify:** accession/fund/security totals reconcile and bounded consumer
artifacts carry period/knowledge/coverage metadata. Update G2 evidence links.

## SF-P06 — Close primary-dealer delivery

**Dependencies:** SF-P05 and Spec015 tasks through G2. **Scope:** M (3 docs).

- [ ] Record series IDs/units/regime and the pre-build overlap decision.
- [ ] Link suppression, revisions and cross-era rejection evidence.
- [ ] Record same-definition output coverage and remaining historical extensions.

**Verify:** selected source metadata and export values reconcile without mixing
weekly balances/averages/cumulative fails. Update the delivery ledger.

## SF-P07 — Design a combined research report contract

**Dependencies:** SF-P02–SF-P06. **Scope:** M (3–5 documentation/fixture files).

- [ ] Specify a report that consumes each committed source artifact and lists
  source-specific dates/delays/coverage with no shared freshness fiction.
- [ ] Define permitted dated identifier and availability joins plus missing-source
  behavior; treat macro dealer data as context rather than a security-level join.
- [ ] Confirm consumer audience/terms and open a separate QuantMeridian spec for
  the accepted display; no automatic sibling-repository writes.

**Verify:** invented complete/missing/ambiguous/revised inputs can be explained
by the contract without fabricated rates, inventory, scores or signals. Likely
files: a later report spec, contract fixture and program roadmap.

## SF-P08 — Review optional recurring operation

**Dependencies:** each source's delivered G3 preparation; independent of SF-P07.
**Scope:** M (3–5 documentation/config-example files).

- [ ] Review proposed release-calendar schedules, source retention and health
  controls individually; do not inherit NAAIM settings or enable all jobs at once.
- [ ] Record manual operating owner and restart/rollback procedure per source.
- [ ] If requested, activate only the explicitly selected delivered schedules;
  preserve NAAIM's pause until the user specifically resumes it.

**Verify:** Dagster register matches actual state and a no-request inspection
shows exactly the intended schedule defaults/activation. No hosted-service
requirement is introduced into the research delivery.
