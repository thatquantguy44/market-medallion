# SEC N-PORT data contract

Status: **proposed normalized contract; runtime absent**. Reviewed 2026-11-03.
Owner/capability: `sec-nport`. Version proposal: `sec-nport-research/1`.
The [specification](README.md) owns requirements; the
[program contract](../../docs/securities_finance/contract.md) supplies common
rules. Upstream spelling below comes from documentation, not a captured ZIP.

## Admission matrix

| Item | Verified documentary evidence | Still required at SEC014-T01/T02 |
| --- | --- | --- |
| Packaging | SEC publishes quarterly public ZIP datasets. | Actual edition URL, HTTP headers, archive/member inventory and bounds. |
| Format | Readme describes UTF-8 TSV tables and table metadata. | Exact headers/encoding/quoting/null representation per selected era. |
| Filing scope | Public submissions include amendments. | Full vs partial amendment behavior and report selection on actual examples. |
| Disclosure timing | 2024 monthly-public amendments were delayed; 2026 proposal remains proposed. | Effective rules on implementation date and actual dissemination evidence. |
| Lending fields | C.12 flags and loan/collateral values are documented. | Conditional null behavior, units/currency and era changes in source files. |
| Identity | Accessions, holding IDs, fund/series and issuer identifiers have distinct uses. | Cardinality/uniqueness from sample metadata; no global surrogate-ID assumption. |

Sources: [dataset page](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets),
[readme](https://www.sec.gov/files/nport_readme.pdf),
[current form](https://www.sec.gov/files/formn-port.pdf),
[2025 delay](https://www.sec.gov/files/rules/final/2025/ic-35538.pdf),
[2026 proposal](https://www.sec.gov/files/rules/proposed/2026/ic-35962.pdf).
Capture the relevant evidence and its checksum when selecting a schema era.

## Upstream tables and safe join plan

| Table | Documentary key or relationship | Use |
| --- | --- | --- |
| `SUBMISSION` | `ACCESSION_NUMBER` | Filing/report metadata and amendment type. |
| `REGISTRANT` | `ACCESSION_NUMBER` | Registrant context. |
| `FUND_REPORTED_INFO` | `ACCESSION_NUMBER` | Fund/series context and reported portfolio totals. |
| `FUND_REPORTED_HOLDING` | `ACCESSION_NUMBER`, `HOLDING_ID` | Core holding facts. |
| `IDENTIFIERS` | `HOLDING_ID`, identifier row ID | Multiple identifiers; retain separately or aggregate to an array before a fact join. |
| `SECURITIES_LENDING` | `HOLDING_ID` in the documented table | C.12 holding-level flags/values; resolve the parent in the same archive/filing context. |
| `BORROWER` | `ACCESSION_NUMBER`, `BORROWER_ID` | Fund-level borrower aggregate, not a holding-to-borrower allocation. |
| `BORROW_AGGREGATE` | `ACCESSION_NUMBER`, aggregate row ID | Categories of non-cash collateral. |

Every staging key includes `archive_edition_id` and table identity. Resolve a
holding-only child key to a single admitted parent before storing its accession.
If multiple parents remain possible, reject the join and retain the row for
diagnosis. Surrogate IDs are not cross-quarter security identifiers.
Do not inner-join away holdings with no child table row. Do not join borrower
rows to every holding and then sum values.

The documented C.12 fields are `IS_CASH_COLLATERAL`,
`CASH_COLLATERAL_AMOUNT`, `IS_NON_CASH_COLLATERAL`,
`NON_CASH_COLLATERAL_VALUE`, `IS_LOAN_BY_FUND` and `LOAN_VALUE`.
They concern different reported facts; preserve the original field and label.
Schema-era admission determines their exact runtime types and conditions.

## Proposed normalized entities

**Archive edition** has `edition_id`, provider archive period, source URL,
retrieval UTC, SHA-256, compressed/expanded bytes, parser/schema versions,
member inventory/checksums, and `SUCCEEDED|PARTIAL|FAILED` audit outcome.
URLs must be public and credential-free. Raw archive bytes remain immutable.

**Filing** grain is `(accession_number, observed_version)`; fields include
fund/series identity, registrant identity, submission type, report date,
provider filing-date label, nullable acceptance/dissemination timestamps,
selection/coverage status and raw lineage. Store date-only source fields as
dates. The posting quarter is never the fund's observation date.

**Holding** grain is `(accession_number, holding_id, observed_version)` within
the admitted archive lineage. Preserve instrument name, identity array,
security classification, currency, reported quantity/unit and monetary value
only where actually supplied. `issuer_id` is not interchangeable with
`security_id`; unresolved instrument identity is retained.

**Holding lending fact** shares its parent's accession/holding grain and carries
each C.12 flag and amount independently. Add `field_state` and units/currency
evidence. An absent optional table row is `NOT_REPORTED`, not automatically
`IS_LOAN_BY_FUND=N`.

**Borrower** grain is `(accession_number, borrower_id, observed_version)`.
Keep borrower name, nullable LEI, reported aggregate value and currency basis.
An unresolved LEI prevents entity consolidation, not preservation of the row.
Borrower and holding values can be reconciled at the declared filing universe
only when their field definitions and currency basis match.

**Non-cash collateral category** has accession, source row ID/category,
principal/value/units and lineage; it does not replace a holding loan value.

## Types and nulls

- Accession/CIK/series/holding/borrower/LEI/CUSIP/ISIN are strings. Preserve
  leading zeros and source punctuation; store normalized alternatives explicitly.
- Money uses decimal text in SQLite and a declared decimal schema in Parquet;
  precision and scale must round-trip without binary float conversion.
- Counts/units use field-specific types. Do not treat a principal amount as shares.
- Flags retain true/false/unknown with the original value. Reject unknown codes
  until classified by the admitted schema; no generic truthiness conversion.
- Unknown, not reported, not applicable, confidential and invalid are separate
  states only when the evidence supports those distinctions. Otherwise use
  `UNKNOWN` plus a reason; do not label an unexplained absence confidential.
- Currency conversion, equity share adjustment and issuer-level aggregation
  require separate source/model contracts; none is implicit in this release.

## Time and revision model

`report_date` describes holdings. `provider_filing_date` may have day resolution.
`provider_accepted_at` and `provider_disseminated_at` remain null unless reliable
EDGAR evidence is captured. `retrieved_at` and `first_seen_at` are actual UTC
pipeline clocks. `archive_posted_at` is separate and may also be unknown.

Append changed normalized content under the same observed source key. A
provider amendment has its own accession; an archive correction may alter a
previous accession's extracted content. Preserve both mechanisms and retain
original raw sources. A latest filing selection must cite its method version,
candidate accessions, eligibility and selection reason.

Follow common C06: persist the original monotonic capture sequence under the
source-root lock, order equal timestamps by that sequence and preserve it in
replay. Reject inconsistent clock regression or ambiguous imported ordering
under the documented policy; do not sort UUIDs to guess amendment precedence.

Do not assume amendments are automatically complete replacement portfolios.
Until characterized, an ambiguous candidate is excluded from a single-current
portfolio selection while remaining inspectable as a filing. A missing accession
in one refreshed ZIP is not a withdrawal unless complete replacement semantics
have been established. Proven withdrawals create a versioned tombstone event;
the historical observation is never erased.

Observed-as-of selection first filters on `first_seen_at <= cutoff`, then applies
the documented filing selector. Loading 2020 holdings today does not create
2020 knowledge. A public filing date alone cannot establish pipeline knowledge.

## Publication and completeness

Required members must all validate before a complete edition is promoted.
Optional unavailable entities can yield partial diagnostic output, but the
consumer artifact must state excluded funds/filings/tables and cannot carry a
complete flag. SQLite fact/version/audit writes are transactional. Cross-store
publication follows common C07's explicit journal: stage/fsync immutable files,
commit DB facts and a pending snapshot, finalize/verify files, publish the
atomic pointer, then mark the journal published. Readers pin one generation;
restart reconciles pending records under the root lock. Tests interrupt every
boundary, including pointer publication before the final journal mark, and
prove a coherent generation without a false success watermark.

Research artifact metadata includes source/schema, generated UTC, report-date
coverage, selected filing count, instrument identity coverage, table coverage,
selection/method versions, source clocks, stable input-version tokens and
interpretation limits. It contains only the approved bounded output, not a
copy of the ZIP or runtime configuration. The artifact manifest records a hash
and may be reproduced from Bronze.

## Fixture and validation cases

Invented TSV/ZIP fixtures must include a fund with multiple share classes,
multiple identifiers for one holding, two borrowers, null LEI, zero/positive
loans, missing child row, conditional flag/value conflict, huge IDs, decimal
precision, non-USD monetary facts, amendment, archive correction, ambiguous
surrogate reuse, orphan child, missing required member, unexpected header,
late public filing, corrupt/truncated archive and a proven withdrawal event.
Offline fixtures include no real fund holdings or borrowed provider payload.

An independently calculated expected output must catch join amplification and
ensure a changed filing does not silently double the portfolio. Validate
round-trip types, exact totals under a documented field-level tolerance,
null-versus-zero behavior, output lineage and deterministic equal-clock capture
ordering. Injected transport cases cover exact bounded response capture,
status/content mismatch, transient retries and terminal failure. The full
definition of done is SEC014-AC01–AC13 in the [specification](README.md).
