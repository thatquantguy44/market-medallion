# SSV-T09 — scope-safe research Gold

Selected 2026-11-13 for the existing `feat/finra-local-revisions` / PR #23.
Traces SSV-REQ09 / SSV-AC09 and common C04–C08. Reuse the accepted T06–T08
read-only SQLite snapshot, scope registry, exact quantities and fixed T04 ratios.
Implement/test pure metrics first, then snapshot/temporal queries and the CLI.
No warehouse schema, ingestion, raw capture or publication-pointer change is needed.

## Population and arithmetic

Require an explicit nonoverlapping selection through the existing registry.
`listed_trfs` expands to the three verified TRFs; selecting it alongside a member
or duplicating a member refuses before I/O. Consolidated/ADF/ORF/TXT, raw-code
aliases and facilities outside the manifest remain excluded. This is a selected
TRF population, not a claim of market-wide coverage or a provider consolidated file.

A metric's grain is `(trade_date, selected disjoint facility set, symbol_raw)`.
Each facility must have a committed complete partition at the same cutoff and
the symbol must have a reported row in every selected facility. Sum corresponding
short, exempt and total quantities exactly; divide summed short/total and exempt/
total using T04's 34-digit half-even method, unit 1. Never average facility ratios.
All-zero reported totals retain zero quantities and null ratios with ZERO_TOTAL.
A complete empty facility or absent symbol does not supply zero: expose
NO_REPORTED_ROW and null population quantities/ratios. A missing complete partition
makes the population INCOMPLETE_POPULATION; known symbols retain unavailable metric
rows and missing-facility evidence. Never shrink or renormalize the requested set.
A fully empty observed population yields no synthetic symbol rows.

Source numeric budgets and stored text stay unchanged. Exact sums across at most
three individually validated rows use an explicit larger bounded Decimal context
with inexact/rounded traps. Aggregate quantities may exceed one source row's
4096-digit budget and must not be sent back through that source-row codec.
Serialize exact sums and derived ratios deterministically, independent of ambient
precision, traps or capitalization. Only derived ratios and ratio differences
round under the fixed method; source quantities and sums never round.

## Temporal policy

Pin all requested/current/prior selections to one read-only SQLite transaction.
Use the existing actual local capture time and original sequence cutoff. Select
latest committed complete membership per date/facility; expose each capture/version
and source-row lineage. Different facility captures are not claimed to be a single
provider query cohort. Later partial/failed attempts do not replace committed data;
operational freshness remains independently inspectable through coverage/audit.

The prior date is the nearest earlier date within the explicit lookback having
complete committed partitions for every requested facility at the same cutoff.
Do not expand/shrink the population across dates. Report observed incomplete dates
skipped while searching. If the symbol is missing from the selected prior date,
return unavailable prior values; do not skip that complete date to find an older
symbol value. A removed current row cannot manufacture a zero observation.

`calendar_day_gap` is the difference between source date labels, including weekends.
No trading-calendar, holiday, missing-business-day, interpolation or daily-continuity
claim is made. No prior complete date gives null date/gap and NO_PRIOR_COMPLETE_DATE.
Expose prior ratios and current-minus-prior ratio changes only when both are defined.
Historical imports remain CURRENT_VINTAGE / LOCAL_CAPTURE knowledge, never source
historical release vintages. Revisions, retractions and equal-time sequence cutoffs
must affect both the current membership and historical comparison correctly.

## Interface, bounds and verification

Add a separate `research-gold` local command and source-owned read function; keep
existing `gold` output and ingest behavior compatible. Inputs: ISO trade date,
explicit scopes, optional aware `as_of` plus sequence, `lookback_days` (default366,
range1–3660) and `max_symbols` (default15000, range1–250000). Only known current
symbols are returned; prior history does not invent rows in today's population.
Missing scopes, invalid bounds/cutoffs and resource excess refuse explicitly.
Querying creates no files, performs no network probe and loads no credentials.

Characterize invented asymmetric facility ratios, exact large/fractional sums,
zero totals, missing/empty constituents, overlapping scopes, calendar gaps,
missing prior membership, changing coverage, revised/removed rows and cutoff
coherence. Compare arithmetic to independent rational expectations. Verify a real
CLI process, unchanged raw/warehouse bytes, all FINRA regressions, full offline
suite, lint/format and package source readback. Commit/push to PR #23, update its
final description and track current-head checks/reviews. T10 Parquet and later
replay/acquisition/export/jobs remain open; G1/G2 and real-source coverage remain open.

Official references rechecked before coding:
[FINRA API docs](https://developer.finra.org/docs),
[Equity terms](https://developer.finra.org/specific-terms-equity-data) and
[Python Decimal arithmetic](https://docs.python.org/3.12/library/decimal.html).
No source schema or admission change is inferred from these references.
