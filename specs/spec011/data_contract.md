# Settlement facts and threshold membership contract

**State:** normalized design proposal; source facts explicitly noted below.
**Date:** 2026-11-03. Inherits [C01–C11](../../docs/securities_finance/contract.md).
The initial parser implements the six-column/trailer local profile version2,
reconciled against two approved September2026 archives after an unsuccessful
HTTP request. See the [source checkpoint](source_review.md) for sample hashes,
explicit bounds, trailer checks and coverage limitations. Persistence remains
unimplemented.

## 1. Provider structures and grains

SEC documentation names these six fields; exact byte headers/ASCII encoding are
pinned for the two local samples: `SETTLEMENT DATE`, `CUSIP`, `SYMBOL`,
`QUANTITY (FAILS)`, `DESCRIPTION`, `PRICE`. Raw rows remain available in Bronze.

The samples have two terminal ASCII records, in order: `Trailer record count N`
and `Trailer total quantity of shares Q`. Both must match the parsed rows;
missing, malformed, duplicated or nonterminal trailers block acceptance. They
are integrity metadata, excluded from facts and fact-row budgets.

Proposed FTD fact key: `(dataset, reporting_regime, settlement_date, cusip)`.
Both September samples prove uniqueness within their observed scope. A conflicting duplicate is a blocked
partition, not an instruction to sum quantities. An identical duplicate can be
diagnosed and rejected unless an explicit schema contract permits deduplication.
Source symbol/description stay attributes with revision history.

Nasdaq's linked current text header was observed as:

```text
Symbol|Security Name|Market Category|Reg SHO Threshold Flag|Rule 3210|Filler
```

A final 14-digit creation token was observed; preserve and validate it separately.
The historical pilot-list field documentation is not a substitute for the
threshold file's current schema. NYSE's exact download format remains open;
reserve no fabricated provider column names. Provider adapters normalize into a
common membership contract only after their independent schema/sample checks.

Membership fact key: `(provider, market, list_date, raw_symbol, rule_family)`;
where the source publishes a stable issue ID, retain it as an additional
identifier and evaluate a versioned key change. Provider/market/rule scope must
not collapse into a single exchange-independent symbol boolean.

## 2. Proposed normalized FTD fields

| Field | Type | Meaning / invariant |
| --- | --- | --- |
| source, dataset, reporting_regime | string | SEC identity and applicable reporting/format regime |
| settlement_date | ISO date | Source settlement label; never shifted to trade date |
| cusip | string | Exact nine-character identifier incl. leading zeroes; preserve unusual source token for diagnostics |
| symbol_raw, description_raw | string/null | Supplied source attributes; no identity inference |
| quantity_fails | nonnegative integer | Reported outstanding shares; no imputation |
| prior_close_price | decimal/null | Source price; `.` becomes null with `SOURCE_PRICE_UNAVAILABLE` |
| price_basis | enum | `PROVIDER_PREVIOUS_CLOSE`; price-date identity remains unknown unless independently verified |
| row_state | enum | `REPORTED`, `WITHDRAWN`; absence lookup has separate coverage state |
| partition_id, raw_member, raw_row_number | string/int | Provenance within the selected archive |
| content_hash, version_id | hash | Normalized content identity and observed version identity |
| provider_published_at, source_updated_at | timestamp/null | Only when source evidence supplies the clock and semantics |
| retrieved_at, first_seen_at | UTC timestamp | Capture clock and first observation of this normalized version |
| capture_sequence, run_id, raw_sha256 | int/string/hash | Stable replay ordering and immutable raw lineage |

Dates from a semi-monthly file must lie within its declared period. Weekend or
holiday labels are diagnostic pending the admitted settlement calendar; do not
silently drop or shift them. Integer overflow and nonfinite/negative prices are
blocking validation errors under the selected format contract.

Bounds are explicit configuration, never inferred from filename suffix alone.
The reconciled September files contain Sep1–14 and Sep15–30 respectively.
The manifest accepts starts1/15/16, with end14/15 for start1 and month-end for
start15/16, within the same month. This bounded validation does not establish
which split a different month uses or certify an expected settlement calendar.

## 3. Proposed normalized list and coverage fields

One coverage record per `(provider, market, list_date, capture)`:

- `requested_date`, `source_date`, `source_created_at_raw`, parsed source clock
  when its time zone is known, `calendar_version` and `source_schema_version`;
- `coverage_state`: COMPLETE, COMPLETE_EMPTY, NOT_PUBLISHED, UNAVAILABLE, PARTIAL
  or FAILED; expected/received row counts and validation evidence;
- receipt/capture sequence, first-observed knowledge time and current version.

Membership includes provider/market/list date/raw symbol/rule-family, source
name/market category/flags as available, source row pointer and common lineage
clocks. Preserve unknown flag tokens and block promotion until a reviewed
schema version defines them. A NYSE market selector is not inferred from an
issuer's current listing or from the symbol alone.

## 4. Missingness and removal decision table

| Situation | Required behavior |
| --- | --- |
| Partition never captured or failed/truncated | UNKNOWN/UNAVAILABLE; no absence event, zero or exit |
| Complete modern SEC partition, eligible date, absent CUSIP | NOT_REPORTED_IN_COMPLETE_PARTITION with coverage evidence; optional explicitly derived zero only under an admitted universe/date contract |
| Pre-2008 SEC absence | BELOW_REPORTING_THRESHOLD_OR_UNKNOWN, not zero |
| New complete exchange list excludes a previously present symbol | Observed non-membership at that list date; confirmed exit only with admitted adjacent-date/calendar continuity |
| Complete replacement of the same partition omits an older reported row | Versioned withdrawal at replacement capture time; earlier as-of result preserved |
| Complete-empty list validates schema and date | Explicit empty membership population; different from no file |
| Symbol moves between markets | Separate source facts plus dated mapping evidence; no invented seamless episode |

Partition coverage is historical metadata too. A later file repair may establish
coverage only from its capture time; earlier cutoffs cannot inherit that repair.

## 5. Clocks and calendars

The SEC settlement date is an economic label. Expected publication occurs after
the reporting period, but the release schedule does not backdate first-seen.
Nasdaq file date, created time, publication and retrieval remain distinct.
Date validation must account for the announced December 2026 change rather
than assuming creation-day equals trading-day across all eras.

Use UTC for knowledge cutoffs; use the admitted settlement/list calendar for
adjacency. Store calendar source/version and holiday exceptions. At an equal
capture timestamp, a persisted monotonic `capture_sequence` per root orders
events; replay reuses it. Ambiguous imported ordering fails rather than sorting
random run IDs into an invented history.

## 6. Identity and the optional research projection

Crosswalk grain: `(namespace, raw_identifier, effective_from, effective_to,
mapping_version)`, with target security ID, evidence, first_seen_at and match
status. Preserve many-to-many candidates; admit only the explicitly validated
single security mapping for a requested date and knowledge cutoff.

The FTD-owned optional projection consumes versioned native artifacts and an
explicit mapping artifact. It must declare whether it aligns list_date with
settlement_date for descriptive comparison or selects the latest available
observation at a knowledge cutoff. These are separate modes. No carry-forward
is implicit; any as-of carry must include original date and age/maximum age.

Projection metadata includes input snapshot IDs, mapping/calendar versions,
join mode, cutoff, raw/joined/unmatched/ambiguous counts and row-count/cardinality
checks. A failed mapping may suppress joined metrics while preserving the two
native timelines. Counts from SEC/threshold populations are never denominators
for one another without a separately documented population contract.

## 7. Episode and feature definitions

An observed episode starts with a present list row. It can be called an observed
entry only when the preceding expected complete list excludes that issue under
the same rule/market definition. Its last-observed date, observed present-list
count, calendar span and censoring flags are separate fields. Missing expected
dates split certainty; publish a gap flag rather than bridge the missing data.

FTD previous-observation change uses current selected balance minus the prior
available selected balance and includes both dates plus gap. It remains a
balance change, not newly failed shares. A rolling maximum or positive-report
count requires explicit selected settlement dates and coverage; never use a
calendar-day forward fill to manufacture a daily history.

## 8. Export and failure contract

Native artifacts contain schema/method versions, source identity, generated
time, knowledge mode/cutoff, complete partition inventory, reported history,
missing/withdrawn states, units and revision tokens, with attribution and source
links. Consumer limits are explicit: proposed default 104 observations per
selected security and a required selected-security list; partition inventories
can be summarized with bounded pagination. Maximum file bytes are validated.

Reject duplicate dates/keys, malformed source identity, ambiguous unit/mapping
or changed required schema. Do not leave a prior artifact looking freshly
generated after failure. Retain its actual old generation time plus health
failure evidence; consumers decide whether dated prior results are usable.
