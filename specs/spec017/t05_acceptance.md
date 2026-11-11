# FOMC-T05 acceptance — deterministic chunks

Accepted locally on 2026-11-11 on `feat/spec017-fomc-inventory` for the selected
**1990-onward committed-text sample**. [Chunks contract](chunks_contract.md) and
[sanitized evidence](evidence/t05_chunks.json) define the verified scope. T03/T04
originals, native text, source clocks and inventory/text pointers remain unchanged.

## FOMC-AC07 — complete and traceable chunks

The pinned profile is `fomc-unit-paragraph-chunks/1`, with tokenizer
`unicode-word-punctuation-pieces/1`, exact Python/Unicode versions and finite
limits. Defaults are 650 tokens maximum, 75-token overlap, and a preference for
paragraph/line boundaries after 500 tokens. Counts use this named core tokenizer;
future embedding profiles define their model-specific input limits.

Every chunk retains its observed document-version ID, extraction ID, profile,
ordinal, exact Unicode half-open offsets, text/hash and source page/section/unit
citation. Chunks stay inside units. Oversized passages/word runs split without
truncation. Tables and footnotes remain linked to their source unit/full text.
Repeated processing is idempotent; changes to observed version, extraction or
profile produce distinct IDs. A→B→A observed versions remain distinct.

Coverage merges overlapping spans. Fixtures verify Unicode/combining/emoji text,
long passages, unbroken runs, paragraph boundaries, page/section citations,
tables/footnotes, overlap, complete coverage, blank/review units, corrupt spans,
missing chunks and finite failure bounds. Unattributed review issues exclude all
units conservatively. Review text remains stored with exact spans and reasons.
Native issue-free units are eligible under this profile; that policy does not
certify OCR or text accuracy.

## FOMC-AC12 — immutable mirrors and coherent publication

Four explicit owned PostgreSQL tables mirror chunk sets, complete chunk text/rows,
generations and the selected pointer. Immutable artifacts under `text/chunks/`
and `text/chunk_catalogs/` are verified against their full source extraction.
A generation pins one text catalog, one chunker profile and exactly one selected
extraction per observed document version. Multiple extraction profiles require
explicit selection. Current/history and eligibility filters retain source clocks,
bundled-meeting/addendum links and exact citations.

A missing/corrupt original, failed chunk write, failed generation write or text
pointer change during a default build leaves the previous generation selected.
Local artifacts support retries; retrying an older generation cannot move the
pointer backward. Readers verify the pinned base and all mirrors. Empty-target
replay uses retained originals/text/chunks, preserving IDs, rows and eligibility
with HTTP, extraction, tokenization and chunk generation disabled.

## Actual local result

All **12 selected observed document/extraction versions** have chunk sets:
**241 chunks**, with **166 eligible** and **75 review-flagged**. All **616,520
source-unit characters** are covered, including **474,979 eligible** and **141,541
review characters**; the extraction's **192 inter-unit separator characters** are
accounted for separately. Full text remains **616,712 characters**. All **74 PDF
pages** have exact citations. Overlap introduces no extra coverage credit.

The five upstream representation review cases, 460 unacquired document references,
49 not-listed outcomes and one unavailable 1994 href remain explicit in the pinned
text/chunk catalog. Full historical acquisition and OCR/text accuracy review remain
incomplete. No embedding, model/OCR call, provider request, job or schedule is added.

A fresh pre-migration backup restored all **22 existing tables**. The post-migration
backup restored all **26 tables**, including all full text, chunks, metadata and
pointers exactly. All 72 original receipts/bytes/clocks and all 22 pre-existing
tables remain unchanged. Actual empty-target replay reproduced the complete chunk
generation, row text/eligibility and 72 original receipt bytes. Disposable restore/
replay databases and the replay root were removed; other namespaces are unchanged.

**945 regression tests passed with 56 optional skips; all 243 FOMC checks passed
with PostgreSQL enabled**, including 46 isolated backend checks. Lint, format,
diff, build and private-artifact checks passed. The evidence records these results
and the independent source-span reconciliation.

**Next: T06 bounded acquisition and operations** for the 1990-onward archive.
Embeddings remain a separately selected optional T07 enhancement.
