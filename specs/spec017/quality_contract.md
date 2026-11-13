# FOMC extraction-quality contract

Status: **Q01 accepted; Q02/Q03 implementation in progress**.
[Q01 acceptance](quality_report_acceptance.md). Selected 2026-11-13 after
[PR #18](https://github.com/thatquantguy44/market-medallion/pull/18) merged.
Delivery branch: `feat/fomc-text-quality`, based on merged main `1bd4ab5`.
This package scopes the next three slices under Spec017; it does not reopen
T01–T06 or repurpose optional-embeddings T07.

## Objective and baseline

Make the archived minutes' extraction limitations inspectable, compare selected
cases with their original pages, and repair only demonstrated extraction defects.
The [accepted backfill](1997_2026_acceptance.md) is the immutable starting point:
1,081 captures, 522 selected text representations, 3,200 PDF pages, 9,790 chunks,
167 review-required representations and 1,520 review-flagged chunks. These are
archive-wide acceptance counts, not a promise that every flag requires OCR.
A fresh report must pin and reconcile its actual selected catalogs rather than
hard-code this dated baseline. Active source scope remains 1990 onward.

## Capability map and order

| Slice | Stable capability | Deliverable | Depends on |
| --- | --- | --- | --- |
| FOMC-Q01 | `quality-report` | Deterministic offline report and CLI/script entrypoint | Accepted text/chunk catalogs |
| FOMC-Q02 | `source-page-review` | Reproducible case selection, source-page review evidence and dispositions | Q01 report |
| FOMC-Q03 | `extraction-remediation` | Evidence-backed fixes, regression fixtures and verified derivative publication | Q02 confirmed defects |

Build and review order: **Q01 → Q02 → Q03**. Each slice has its own tests and
acceptance record. The [plan](quality_plan.md) owns implementation decisions;
[tasks](quality_tasks.md) owns the checklist. A report is not a visual review,
and a visual review is not automatically an eligibility override.

## FOMC-Q01 — offline quality report

**FOMC-QAC01:** Read one coherent, explicitly pinned inventory/text/chunk catalog
selection through the existing verified FOMC readers. Include the catalog IDs,
report schema/profile, scope and source identity. Missing/corrupt mirrors,
changed or inconsistent pointers, invalid associations and unknown issue codes
must fail visibly; do not fall back to directory globbing or a newer orphan.

**FOMC-QAC02:** Emit deterministic JSON with per-year, representation-format,
extraction-profile and issue-code summaries plus drill-down rows. A finding's
grain is observed source-version ID + extraction ID + unit ordinal + issue code; retain its capture/source
hash, source URL, meeting IDs/dates, page/section, exact unit offsets and affected
chunk IDs. Preserve all meeting associations for shared/bundled references.
Distinguish unique representation/unit/chunk totals from overlapping issue/year
memberships. Report union character coverage rather than summing overlapping
chunk text. Empty or failed units remain visible even when they have no chunks.
Unknown release time remains unknown; report generation never backdates capture
or first-seen clocks.

**FOMC-QAC03:** Every selected representation and chunk is accounted for; summary
counts reconcile with the pinned catalogs. Include unchanged eligible content as
explicit denominator totals. Keep `image_content`, `missing_native_text` and
`replacement_character` distinct, with extraction failures separate. Counts are
diagnostics, not an OCR or semantic-quality certificate. Same verified input and
profile yield identical report bytes/ID; wall-clock execution metadata lives
outside the content identity.

**FOMC-QAC04:** Expose the report through the existing pipeline CLI and
`scripts/run_fomc.sh`, with other users supplying their own configuration. The
command reads local files/PostgreSQL only: no provider HTTP/probe, OCR/model call,
source write, migration, catalog publication, Dagster launch or alert delivery.
Output files are written only to an explicit report destination outside canonical
original/text artifact directories; existing differing outputs are refused.

## FOMC-Q02 — representative source-page review

**FOMC-QAC05:** Deterministically select cases from Q01, covering each nonempty
issue-code × format × extraction-profile stratum. Include early/late cases and
image-heavy HTML/SEP, image-bearing PDFs, empty-text and replacement-character
cases where present. Cap a review batch at 12 cases; record uncovered strata and
remaining cases explicitly. Sampling does not certify all 167 representations.
Every case binds the report/catalog IDs, source hash, extraction/profile ID,
unit/page/section and chunk citations. Never silently substitute a newer capture.

**FOMC-QAC06:** Compare each selected case against the archived original. Render
PDF pages and inspect HTML with its archived substantive assets; retain rendering
tool/version, page identity, local evidence hashes and bounded text comparisons.
Rendering uses archived inputs with network disabled; external page resources
must not be fetched. Record tables, footnotes, punctuation, numbers and source
redactions in the review checklist. Preserve source notices and exact original
bytes. Keep raw documents/renders and operational paths in ignored local storage;
commit only sanitized review summaries and reduced regression fixtures.

**FOMC-QAC07:** Each inspected case has a disposition and evidence: text matches
sampled source; chart/image-only content; confirmed native extraction defect;
candidate OCR work; or unresolved. Include reviewer/tool identity and actual
review time. Dispositions describe their checked scope and never automatically
clear extractor flags, promote chunk eligibility, infer missing text, or certify
OCR accuracy. Unknown/unreviewed cases remain explicit. Reviewed chart content
is not silently converted into prose or tabular facts.

## FOMC-Q03 — confirmed extraction remediation

**FOMC-QAC08:** For every selected confirmed defect, first add a minimal offline
fixture/assertion that fails with the baseline. Demonstrate the failure and the
fix, then compare the derivative with the reviewed source evidence. Preserve
wording, numerals, punctuation, page order, tables/footnotes, citations and
source-owned redactions. Do not remove text to improve a count or suppress a
flag. If Q02 finds no confirmed native-extractor defect, record that result and
close this bounded slice with evidence; do not invent a code change. Unresolved
or OCR-only cases remain a named backlog with explicit eligibility.

**FOMC-QAC09:** A changed extraction algorithm/profile produces new immutable
derivatives and chunk sets. Historical results/IDs remain readable and unchanged;
extractor changes are not provider revisions. Compare unchanged-source old/new
text, unit offsets, issues, chunk spans and citations. Explain every changed
representation; unchanged profiles/inputs must reproduce their existing results.
Publish a coherent inventory/text/chunk selection only after all selected artifacts
pass verification. A failure leaves the prior published pointers intact.

**FOMC-QAC10:** Before any operational derivative publication, take and actually
restore a fresh full-schema/canonical-file checkpoint into disposable targets.
After publication, verify Bronze receipt bytes/clocks, append-only history,
text/chunk spans, healthy publication and empty-target offline replay; restore-test
the resulting state and compare it. Retain old artifacts/catalogs and a verified
pointer-selection rollback route. Use the source-operation advisory lock for
stable verification; remove disposable targets without changing other namespaces.
Record acceptance and sanitized evidence before claiming improved coverage.

## Engineering and scope boundaries

**FOMC-QAC11:** Keep implementation under
`src/market_data_pipeline/pipelines/fomc_meeting_minutes/`, with pure reporting/
classification transforms separated from I/O. Reuse the pipeline's config,
verified readers, immutable identities, canonical JSON, finite limits and script
entrypoint. Add no dependency or schema solely for the report/review ledger.
Any required OCR engine, model, new dependency/schema or eligibility policy must
first be described and reviewed as a separate contract decision; this package
does not silently enable such a service. Verify official documentation before
coding any new provider/engine integration.

**FOMC-QAC12:** Offline fixture tests cover reporting/cardinality, empty units,
shared references, review selection/dispositions, malicious/unexpected artifact
paths, corrupt mirrors and no-network/no-write behavior. Isolated PostgreSQL
integration covers coherent reads, concurrent-pointer changes, compatibility,
rollback and replay. Never use the operational schema as a test target. A
remediation must have pre-fix failure evidence and meaningful regression tests.
Keep lint, build, script, documentation and pinned-library checks current.

No new acquisition, schedule activation, Slack test message, external backup
service or embedding/search consumer is part of these slices. Both configured
FOMC status sensors retain their current operating state; other schedules stay
stopped and NAAIM stays paused. Historical absence/unavailable source claims
remain independent of extraction-quality findings. Off-machine backup storage
remains configurable future work.

## Completion evidence

Each acceptance record maps its QAC IDs to actual tests/evidence and states what
remains unresolved. Each remaining slice is planned until its own gates pass. Preserve the
[backfill acceptance](1997_2026_acceptance.md) as historical evidence rather than
rewriting its counts after later quality work. Update the master register,
Spec017 pickup and runbook at each implemented checkpoint.
