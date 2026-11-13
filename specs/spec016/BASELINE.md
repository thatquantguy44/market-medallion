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

T06 publishes a single strict v1 baseline from an explicitly selected raw capture
and reviewed control version. Source and review kinds must match, fictional
inputs carry fictional labels and cannot use the frozen historical profile,
and a verified snapshot is atomically selected. Tests cover staged writes,
interrupted publication boundaries, pointer failure, corrupted raw/review/output
and idempotent publication. The presidential test group passed 75 tests; a built
wheel executed the publication fixture tests outside the source and donor
checkouts. No real historical data has been admitted or published. The shared
Python/TypeScript contract still requires the independently pinned T07 vectors.

T07 pins a structural schema and exact boundary-vector bytes in both producer
and consumer. Python and TypeScript tests agree on valid and adversarial records,
source-byte hashes, canonical hashes, escaped duplicate keys, UTF-16 text limits,
invalid dates and unsafe integers. The Python presidential suite passed 77 tests;
the QuantMeridian contract, engine and export suites passed 83 tests and its
typecheck passed. This is a format/validation boundary, not a source-authenticity
claim, a deployed API, or a loaded PRES dataset.

T08 adds disabled-by-default loopback-only catalog and immutable snapshot routes
in QuantMeridian. The socket peer, origin and request parameters are checked;
reads are bounded, reject symlinks and changed files, verify hashes, and pin the
selected generation. Thirty-two focused contract/reader/route tests pass. The
actual dev and built-server registries returned the Python-produced fictional
fixture over localhost, and a cross-origin built-server request returned 403.
No real source history was admitted by this check. The routes require explicit
local environment configuration and are not enabled in hosted environments.

T09 adds an explicit catalog check and pinned dataset load to PRES. A failed,
late or superseded load retains the current analysis and notes; update checks
do not replace it. The generated fixture is visibly labeled fictional in the
workspace, CSV, figure exports and v1 manifest limitations. Ninety-six focused
consumer tests and all seven PRES browser tests passed, including CSV/SVG/PNG
exports and responsive checks; typecheck and production build passed. This is
a one-dataset fixture integration, not real-source qualification or a v2 replay.

T10 adds source-owned observed-version SQLite facts and typed Parquet exports.
Identical captures retain audit links without duplicating versions; corrections
and reversions create new versions, equal-clock ordering uses capture sequence,
and ambiguous backwards admission clocks are rejected. Original source tokens,
missing policy values and review-only political candidates remain inspectable;
Shiller workbook numbers are explicitly labeled as binary-decoded. Exact
decimal bounds reject unrepresentable/rounded tokens. Queries recheck selected
facts against archived inputs; corrupt inputs fail closed. The presidential suite
passed 90 tests and an installed-wheel archive/publication/warehouse run passed
32 tests. These are fictional tests, not historical admission or a live feed.

T11 is in progress. A private, non-publishing assembly step now requires the
three exact admitted captures, matching configuration and reviewed controls,
then creates all five datasets and seven evidence artifacts. Its fictional
fixture produces 3/2/2/2/2 windows and one cycle row; independent calculations
check both return constructions and sample/fundamental pairing. Frozen-profile
admission, complete atomic bundle publication and consumer support remain open.
The complete producer presidential test group passed 93 tests.

The T11 fictional complete bundle now publishes all five datasets and seven
artifacts in one generation. It journals the pending/published state in SQLite,
rechecks every file, and retains or recovers a coherent pointer across injected
interruptions. Missing artifacts block replacement. The producer presidential
suite passed 100 tests. The application reader still supports only the
single-dataset generation, and the frozen real-input profile remains blocked;
T11 and its consumer acceptance are not complete.

QuantMeridian now reads the complete fictional generation, verifies all five
dataset hashes and seven evidence hashes, and renders matched common-sample and
inherited-condition comparisons. Both series run independently through the PRES
engine and reconcile to the evidence; unequal windows or inputs fail closed.
The full paired summary remains visible alongside symmetric sensitivity results.
The 103 focused consumer tests, typecheck, build and complete-fixture browser
check passed. T11/T12 remain open until the frozen source/profile bindings and
real-input qualification are completed; no historical results were claimed here.

The fixture cycle view now recomputes year-end assignments, transition and
succession flags, group statistics and deterministic ties, including the 1936
exclusion and unavailable one-observation dispersion. PRES can export labeled
paired/cycle CSV and SVG/PNG, and v2 replay embeds verified exact catalog,
dataset, sidecar and evidence bytes plus source/input provenance. Browser
tests restored paired and cycle views while the pipeline routes were blocked;
legacy v1 replay and corrupted-replay rejection also passed. The focused
consumer suite passed 118 tests; typecheck and build passed. T13–T15 remain
conditional on the required real frozen-input parity and migration gates.
