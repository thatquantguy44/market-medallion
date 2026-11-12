# FOMC 1992 bounded backfill acceptance

Status: **in progress** on 2026-11-12, branch `feat/fomc-1992-backfill`, from
merged PR #13 (`41e9d0c06a4669b1ef6e9aa308c93f829034176f`). This slice runs
1992 only through the existing `scripts/run_fomc.sh` entrypoint and the unchanged
[T06 operations contract](operations_contract.md). The [1991 acceptance](1991_acceptance.md)
remains the accepted prior checkpoint.

The no-request preflight is healthy and the current archived inventory lists
16 primary 1992 references. Preserve the 1990 floor, explicit finite operation
settings, original capture clocks, prior snapshots and all review flags. Do not
infer publication dates or replace unavailable minutes with other products.

Acceptance gates: verified pre-batch backup/restore; bounded 1992 acquisition;
coherent inventory/text/chunk promotion; exact original/text/chunk readbacks and
page/span coverage; preservation of old append-only rows and originals; verified
post-batch backup/restore; empty-target offline replay of retained artifacts and
latest operation audit; current health, regression, lint/format and build checks.
Record actual source outcomes, requests/bytes, retained gaps and any inventory
claim changes without inferring meeting creation or cancellation.

No runtime/schema change, OCR/model service or schedule activation is planned.
Keep NAAIM paused and FINRA at SSV-T05. Acceptance is pending the actual batch and
recovery checks; full history and text/OCR accuracy remain incomplete.
