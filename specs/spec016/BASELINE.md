# PR016-R01/R02 donor baseline

Recorded 2026-11-13. The donor research website checkout was clean at commit
`37c9307e86711ffadc391b6cc49079812fb09c14`; its 29 offline unittest cases
passed without source requests. The target MarketMedallion branch starts at
`1bd4ab5` and the separate QuantMeridian branch starts at `68f794b6`.
Existing checkout branches and any unrelated work are not modified.

`tests/fixtures/presidential_research/baseline.json` records SHA-256 digests of
10 relevant donor code/config/test files and wholly invented factor inputs.
Independent decimal arithmetic checks `2.00% + 0.10% = 0.021`,
`-1.00% + 0.05% = -0.0095`, and `(102 + 12/12) / 100 - 1 = 0.03`.
These are software expectations, not historical observations. The source
catalog and donor source-decision notes retain seven source IDs; candidate
Congress rows are review-only and the two control-evidence bundles are manual.
Their historical source rights/captures are not imported by this baseline.

The local QuantSmith checkout does not match its reference pin. The pinned Git
commit and tree were available locally; the engineering, point-in-time,
provenance, testing and file-ingestion guidance used here was read from that
commit and individually matched against the reference SHA-256 inventory.
The pinned agent-skills snapshot verified without network access.

The target parser/config/quality port passes 10 tests, including donor golden
factor values and source-role checks. It performs no provider requests; the
retained historical catalog locators are not authorization for future fetches.
The donor code is attributed to That Quant Guy research under its MIT metadata.

The assembly port passes eight donor-output hash comparisons on generated
fictional inputs, including both series, comparison, fundamentals, audit and
cycle outputs. It preserves source metadata while writing its derived Shiller
intermediate to private temporary storage instead of overwriting an input
directory; callers must explicitly publish returned monthly rows. Targeted
parser/assembly tests: 26 passed. No real source was imported or published.

T04 adds explicitly selected local raw capture, source-catalog bytes and parser
identity before offline parsing. It records destination admission/first-seen
separately from optional evidenced retrieval time, retains failed-parse receipts,
checks byte lengths/hashes and rejects symlinked payloads and oversized ZIP
expansion. Six targeted archive tests pass without network. No historical raw
input has yet been admitted to the new pipeline.

T05 ports reviewed workbook promotion into an immutable local review version.
Only evidenced APPROVED rows from eligible manual-control sources promote;
missing/invalid review fields, overlaps, source roles, incomplete coverage and
workbook expansion failures are rejected. The version stores separate workbook,
catalog, controls and manifest hashes, and its loader rechecks them. Source
workbooks and existing control files are never overwritten. Review timestamps
provided only as dates remain date-level evidence, not fabricated intraday times.
