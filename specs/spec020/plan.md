# Spec 020 — First implementation plan

**Status:** planned, no implementation started. Branch: `feat/ici-weekly-flows-foundation`, based on `docs/spec020-ici-weekly-flows` so the draft [requirements](README.md) are present. The branch is a planning/work branch; no PR, live acquisition or schedule is created by this plan.

## Objective and first checkpoint

Build an inspectable **offline foundation** for the two separate ICI weekly series: ETF estimated net issuance and long-term mutual fund estimated net new cash flow. The checkpoint ends when source/use admission, exact source-shape contracts, a strict pure parser and immutable local capture behavior are verified with invented fixtures. No PostgreSQL, Parquet, Dagster, export or live request is needed for this checkpoint.

The source owner's authorization is the first gate. If it does not clear, finish the pure admission rule and documentary source review, then leave source-shaped artifact review and dependent implementation blocked. Public visibility of ICI release pages is not permission for automated retrieval, storage or republication.

## Task order and dependencies

```text
ICI-T01 rights and route admission
    └── ICI-T02 admitted ETF and mutual fund artifact shapes
            └── ICI-T03 strict pure parser
                    └── ICI-T04 immutable local capture and rejection receipts
                            └── Checkpoint A: offline foundation
```

The [task list](todo.md) owns the detailed acceptance criteria, proposed files and focused verification. T02 may be documented with the exact blocked state if permission or artifacts are unavailable; T03 and T04 then remain unstarted. Invented fixtures model the admitted layout only after its shape has been verified. A public HTML table must not silently stand in for the selected XLS contract.

## Design boundaries for this first set

- Preserve separate `source_series`, `fund_type`, `measure` and release/capture/first-seen clocks. Week-ended date is not a publication timestamp.
- Pin source column/cell locations, units, category labels/hierarchy and workbook/version fingerprints before normalizing. Treat a changed layout or unknown category as a new review gate.
- Capture exact permitted local bytes and a sanitized receipt before parsing. Rejected input remains auditable; no restricted source file or credential is committed to Git.
- Use source-owned code under `pipelines/ici_weekly_flows/`; keep the parser pure and local/replay paths network-free.
- Do not add a generic ICI HTTP client, inferred backfill vintages, combined-series facts, monthly values or a schedule in this slice.

## Later slices after Checkpoint A

1. Versioned Silver membership, corrections/reversions/removals and as-of cutoffs.
2. Isolated PostgreSQL primary and coherent typed Parquet publication with recovery.
3. Network-free replay, health and a rights-compatible research artifact.
4. Bounded acquisition and stopped Dagster definitions, only after the separate source-access and operation decisions.

Each later slice needs its own bounded tasks and evidence. The current project handoff still identifies OTC-T08 as the standing implementation priority; creating this branch does not alter that queue or NAAIM's pause.

## Verification and exit

Checkpoint A requires the dated permission/route record, admitted artifact descriptions for both series, strict invented-fixture parser tests, immutable Bronze/rejection tests, and proof that all checkpoint commands make zero ICI requests. Run the focused tests named in [todo.md](todo.md), then the repository's relevant lint/format/build checks when code exists. Record actual counts and failures at closeout; none are claimed here.

## Open questions

- Which agreement permits the proposed file retrieval, storage, internal use and retention, and who owns approval?
- Which exact ETF and mutual fund XLS/HTML artifacts are the selected inputs, and can an authorized operator supply non-Git samples?
- What are the actual workbook schema, category/definition eras and historical coverage?
- Does the user intend ICI work to displace OTC-T08 in the standing build queue? This plan leaves the queue unchanged until directed.
