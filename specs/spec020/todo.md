# Tasks — ICI weekly ETF and mutual fund flows

**Status:** ICI-T01 documentary review and pure admission are implemented offline;
the real-data permission decision is blocked pending source-owner evidence.
ICI-T02–T04 are not started. This is the first offline checkpoint for
[Spec 020](README.md) on `feat/ici-weekly-flows-foundation`. Paths under
`src/market_data_pipeline/pipelines/ici_weekly_flows/` are source-owned.
Each task is a small, reviewable change; split it if the likely file count
exceeds five. T02–T04 test commands are future targets.

## ICI-T01 — Decide source rights and enforce admission

**Depends:** none. **Traces:** REQ-01, NFR-02, AC-01.

- [x] Record a [dated source/use matrix](source_review.md) for both weekly routes, published permission limits and unresolved operation grants.
- [ ] Name the approving source owner and record a nonsecret ICI agreement/permission reference with permitted operations for each series. No such evidence is currently supplied.
- [x] Define a pure source-use profile that rejects missing/unsupported operation grants before any future file acquisition or export. Keep credential material out of the manifest and logs.
- [x] Distinguish fixture-only development from an authorized operator-supplied file and later automated retrieval. A public release page is not treated as blanket permission.

**Verify:** documentary review against ICI's current [permissions](https://www.ici.org/permissions) and [copyright policy](https://www.ici.org/copyright-and-linking-policies); `.venv/bin/python -m pytest -q tests/test_ici_manifest.py` passes 18 offline cases, including missing/disallowed grants and no-I/O checks. Ruff lint/format and source/wheel builds pass; the ICI package files are present in the wheel.

**Likely files:** `specs/spec020/source_review.md`, `pipelines/ici_weekly_flows/manifest.py`, `tests/test_ici_manifest.py` (and package `__init__.py` if needed).

**Gate:** BLOCKED for real artifacts and T02 pending an approving source owner and operation-specific permission evidence. The validator does not independently verify ICI rights, and no real-use profile is shipped.

## ICI-T02 — Pin both source artifact shapes

**Depends:** ICI-T01 admission for the selected use. **Traces:** REQ-02, REQ-04, REQ-05, AC-01, AC-02.

- [ ] Inspect one authorized, retained-out-of-Git artifact for **each** weekly series. Record file format, worksheet/table names, header and date orientation, unit text, numeric/missing tokens, category labels and hierarchy, notes and source location rules.
- [ ] Record the selected artifact URL or local origin, schema fingerprint, coverage window, definition era and any access or revision limitations; do not infer an XLS layout from the public HTML release.
- [ ] Create small **invented** source-shaped fixtures for both populations, including different latest week dates and one altered layout. Mark them synthetic.

**Verify:** source review reconciles the documented fields with both artifacts and lists all unresolved fields; fixture review shows no ICI observation values or restricted bytes entered Git. No source request is made by tests.

**Likely files:** `specs/spec020/source_review.md`, `specs/spec020/data_contract.md`, `tests/fixtures/ici_weekly_flows/README.md` and up to two invented fixture files.

**Gate:** Do not declare a parser schema or historical coverage until both artifact shapes are evidenced.

## ICI-T03 — Parse one admitted weekly profile offline

**Depends:** ICI-T02. **Traces:** REQ-02, REQ-04, REQ-05, REQ-07, AC-02.

- [ ] Build a pure parser for the pinned profile that returns typed source-series, category, week-ended and exact signed `Decimal` values with original text and cell/row provenance.
- [ ] Preserve measure and fund-type distinctions, category parent/child relationships and source unit. Reject duplicate observation keys, bad dates/numbers, missing required fields, unknown categories and layout drift.
- [ ] Keep source-stated release date optional; never infer it from week-ended date or file mtime.

**Verify:** future focused test `.venv/bin/python -m pytest -q tests/test_ici_parser.py` covers valid ETF/mutual-fund fixtures, signed values, missing/zero distinction, hierarchy, duplicate, malformed and changed-layout cases. Parser tests perform no I/O or network calls.

**Likely files:** `pipelines/ici_weekly_flows/data/weekly.py`, `pipelines/ici_weekly_flows/data/categories.py`, `tests/test_ici_parser.py`, and at most two fixture updates.

## ICI-T04 — Preserve local bytes and rejection evidence

**Depends:** ICI-T03. **Traces:** REQ-03, REQ-06, AC-03, NFR-05.

- [ ] Archive exact permitted operator-supplied bytes, SHA-256, source origin, schema/manifest identity and capture clock before parser invocation; record a sanitized receipt and explicit success/rejection outcome.
- [ ] Make duplicate identical capture idempotent while keeping distinct changed captures and their immutable evidence. Do not assign historical first-seen time to backfilled content.
- [ ] Reject unsafe path/origin metadata and prove a parse failure cannot erase an earlier capture or claim a complete publication.

**Verify:** future focused test `.venv/bin/python -m pytest -q tests/test_ici_bronze.py` covers readback, duplicate, changed bytes, invalid input, interruption and redaction. Local import and replay-style readback make zero ICI requests.

**Likely files:** `pipelines/ici_weekly_flows/io/bronze.py`, `pipelines/ici_weekly_flows/pipeline.py`, `tests/test_ici_bronze.py` and one fixture/manifest adjustment.

### Checkpoint A — Offline source foundation

- [ ] T01–T04 have dated evidence and focused tests; any blocked gate is explicit rather than checked off.
- [ ] Both source measures remain distinct, exact permitted bytes and rejected captures survive readback, and no test or local command contacts ICI.
- [ ] Run relevant Ruff/format/package checks and record actual results. No warehouse, Parquet, export, Dagster or live acquisition is claimed.
