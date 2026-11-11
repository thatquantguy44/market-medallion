# FOMC-T05 — deterministic chunks and coherent local generations

Traces FOMC-AC07/12. Scope: the selected 1990-onward committed text catalog,
using archived originals/derivatives on `feat/spec017-fomc-inventory`.

## Chunk profile and grain

Keep the core pure Python. Pin `fomc-unit-paragraph-chunks/1` and a standard-library
Unicode word/punctuation tokenizer, including the Python/Unicode database versions.
Use `re.finditer(r"\w+|[^\w\s]", text)` and split word runs into at most 32 Unicode
code points. Counts are this named tokenizer's counts; future embedding profiles
own their model-specific token limits. Default maximum is 650 tokens with 75-token
overlap, preferring paragraph/line boundaries after at least 500 tokens. Short
units retain their full text; oversized passages/word runs split without truncation.
No normalization changes source punctuation, Unicode or whitespace.

A chunk set belongs to one **observed document version + extraction version +
chunker profile**. Each chunk retains its exact full-text half-open Unicode offsets,
text/hash, ordinal, unit/page/section citation, token window and eligibility reasons.
Boundaries never cross extraction units. Chunk identity includes the observed
version, extraction, profile and span; A→B→A versions remain distinct even when
byte-identical derivatives recur. Full extraction text is unchanged.

Coverage is the union of spans, independently of overlap. Cover every character
of each nonempty unit, including review-required text. Report empty/unextractable
units and the extraction's inserted inter-unit separators separately; separators
are not source content. Retain per-unit covered/eligible/review character counts.
Native nonempty units without issues/assets are eligible under this profile.
Image/missing-text/replacement-character/unknown review issues are ineligible;
a document-level issue without a unit attribution conservatively excludes all
units. Chunk coverage never certifies OCR/text accuracy. Finite character/token/
chunk bounds reject the operation rather than publish truncated success.

## Storage, catalog and replay

Immutable JSON under `text/chunks/` mirrors chunk-set profiles, coverage and rows
in pipeline-owned PostgreSQL tables. Chunk text and exact row metadata are checked
against the original extraction on every read. Original source clocks and release
precision remain in the pinned text catalog; generation time is separate.

A chunk generation pins one text catalog, one chunker profile and exactly one
selected extraction per observed document version. If multiple extraction profiles
exist, require explicit selection. Include all observed versions and retain the
base catalog's unacquired/unavailable/review gaps and all meeting/addendum links.
The generation records selected extraction IDs; it does not imply full history.
Readers resolve the committed PostgreSQL generation, verify all mirrors, and
select current versions/eligible chunks explicitly. They never glob or silently
join a new text pointer to an older chunk generation.

Publish only after all selected chunk/text/original mirrors verify. Filesystem
artifacts precede transactional database writes; failures retain reusable artifacts
and leave the old chunk pointer intact. Repeated old-generation retries cannot
roll it backward. Keep the text catalog pointer unchanged. A separate owned
migration marker protects four additive chunk tables; preserve all existing rows.
Back up and restore-verify before applying the local migration.

Empty-target replay reuses verified original/text/chunk artifacts, preserving
receipt bytes, source clocks, profile/set/chunk/generation IDs, versions and
eligibility. It calls no network, extraction or chunker. Reject populated targets;
partial replay evidence is retained for review and a fresh target is required.

## Acceptance sequence

1. Offline fixtures: deterministic IDs, changed extraction/profile/version,
   Unicode/combining/emoji/long runs, exact offsets, citations, paragraph cuts,
   overlap, full union coverage, blank/review units and tampering/bounds.
2. Isolated PostgreSQL: exact mirrors, retry/corruption/rollback, current-pointer
   preservation, extraction selection, source-version filters and offline replay.
3. Existing archived sample: reconcile all selected extraction text and citations;
   preserve 72 originals, 22 existing tables, source/text pointers and review gaps.
4. Verify before/after backups and replay, run regression/lint/build/artifact checks,
   update acceptance/evidence/register/handoff, commit and push the same branch.

T06 owns wider bounded acquisition/operations. Embeddings remain independently
selected. No job/schedule, OCR/model call or new provider request is added.

Sources checked 2026-11-11:
- https://docs.python.org/3.12/library/re.html#re.finditer
- https://docs.python.org/3.12/library/unicodedata.html#unicodedata.unidata_version
