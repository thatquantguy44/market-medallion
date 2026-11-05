# Spec 014 — SEC N-PORT holdings and securities lending

Status: **planned; connector, package, fixtures, warehouse and jobs absent**.
Created/reviewed: **2026-11-03**, America/New_York.
Capability ID: `sec-nport`; proposed package: `pipelines/sec_nport/`.
Program order: **4 of 5**, after Spec 013 and before Spec 015.

Read the [program](../../docs/securities_finance/README.md),
[common contract](../../docs/securities_finance/contract.md),
[data contract](data_contract.md), [plan](plan.md) and [tasks](todo.md).
Numbered tasks remain unchecked until implementation evidence exists.

## Objective and users

Give a securities-finance researcher reproducible views of publicly disclosed
fund holdings, securities on loan, borrowers and collateral classifications.
The delivery must explain which filings support an aggregate and which
portfolios, dates or fields are missing. It should make it possible to inspect
fund participation in lending, borrower concentration, collateral reinvestment
and historical changes without flattening the distinct disclosure grains.

The initial consumer is local SQL/research and a versioned exchange artifact
that QuantMeridian can later admit in its own repository. The source package
owns acquisition, parsing, revisions, validation, publication and operations.
There is no need to share a database, runtime configuration or secret with a
terminal application.

Reported holdings and securities on loan do **not** measure the complete
lendable inventory, available-to-borrow shares, current utilization, recall
probability, executable borrow price or street-wide loan balance. This source
will produce disclosed positions and explicit research measures only.

## Evidence and source admission

The [SEC dataset page](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets)
offers public quarterly ZIP datasets beginning in 2019. Only publicly
disseminated filings appear; the bulk posting period and a fund's report date
are separate dimensions. This review inspected documentation, not a live ZIP.

The [SEC readme](https://www.sec.gov/files/nport_readme.pdf) describes UTF-8
tab-delimited tables, amendments and keys. Relevant tables include
`SUBMISSION`, `REGISTRANT`, `FUND_REPORTED_INFO`, `FUND_REPORTED_HOLDING`,
`IDENTIFIERS`, `BORROWER`, `BORROW_AGGREGATE` and `SECURITIES_LENDING`.
The holding join and borrower aggregation must remain separate.

The [2025 final delay](https://www.sec.gov/files/rules/final/2025/ic-35538.pdf)
moved the 2024 N-PORT amendments' dates to 2027/2028. The
[February 2026 proposal](https://www.sec.gov/files/rules/proposed/2026/ic-35962.pdf)
is a proposal, not an implemented monthly public-feed contract. Do not infer
that every fund month is public or impose a universal 60-day monthly lag.
At implementation, record the effective disclosure regime supported by actual
filings and the current official rules. A later rule change gets a new era.

Before acquisition, SEC014-T01 must record the current archive URL pattern,
edition metadata, schema/version, public availability, fair-access requirements,
content size and allowed use. No endpoint, column addition or redistribution
right is inferred from a third-party wrapper. A missing or changed table
blocks dependent publication rather than triggering a guessed schema.

## Scope and release boundary

The required research release includes:

- A manifest and strict source-era schema for the minimum linked table set.
- One local archive-to-Silver-to-SQLite-to-Parquet fixture workflow first.
- Deterministic filing identity, amendments, overlap refresh and observed-as-of
  selection from captured versions.
- Separate fund, holding, borrower and non-cash collateral outputs with
  coverage and reconciliation diagnostics.
- An explicitly bounded archive acquisition and historical load only after
  offline behavior passes and the acquisition scope is recorded.
- A versioned local research artifact, local health command and documented
  manual refresh/recovery procedure.
- Source-owned Dagster definitions if delivered, with schedules stopped.

PostgreSQL, expanded derivative tables, EDGAR filing-by-filing acceleration,
current lending availability, dashboard/UI changes, public deployment and any
predictive score are later scope. SEC N-MFP remains a separate money-market
fund pipeline. No NAAIM run or schedule is resumed by this program.

## Functional requirements

| ID | Required behavior |
| --- | --- |
| SEC014-R01 | Archive source metadata, schema era, access terms and report/publication distinction before runtime extraction. |
| SEC014-R02 | Validate the archive and stream an explicit member allowlist under byte/member/row limits; preserve exact raw bytes and checksums. |
| SEC014-R03 | Parse the declared tables using exact identifier/date/decimal/null rules and preserve every input row's source location. |
| SEC014-R04 | Validate accession/holding/borrower relationships without join multiplication or cross-edition surrogate-key collision. |
| SEC014-R05 | Retain immutable observed versions, amendments and withdrawal evidence; repeated input remains idempotent. |
| SEC014-R06 | Publish disclosed holdings and loan/collateral facts with units, field-presence and fund/date coverage. |
| SEC014-R07 | Publish only reconciled research aggregates, with method version, sample universe and unresolved exclusions. |
| SEC014-R08 | Support local replay, bounded backfill and overlapping archive refresh with atomic promotion and explicit partial status. |
| SEC014-R09 | Emit a versioned local research artifact with clocks, lineage, provenance and interpretation limits. |
| SEC014-R10 | Provide local health, audit and stopped-by-default orchestration without background provider requests. |
| SEC014-R11 | Preserve unknown/absent/suppressed/confidential distinctions where evidence supports them; never impute zero. |
| SEC014-R12 | Document actual verification, unresolved schema/rights decisions and the handoff to Spec 015. |

## Non-functional requirements

| ID | Contract |
| --- | --- |
| SEC014-N01 | Pure Python core on Python 3.12; existing locked dependencies first; I/O isolated from transforms. |
| SEC014-N02 | Streaming memory behavior; configurable archive/expansion/record limits and bounded temporary storage. |
| SEC014-N03 | Same Bronze, manifest and parser produce identical normalized records/content hashes. |
| SEC014-N04 | Failed extraction/joins/DQ leave the last committed publication pointer intact. |
| SEC014-N05 | No source credentials, raw provider data or local environment enters Git or consumer logs. |
| SEC014-N06 | Every exported aggregate traces to accession-level input versions and an explicit completeness result. |

## Research outputs and interpretation

1. **Fund disclosure inventory:** series/fund identifier, report date, filing
   identity/type, selected edition, selection reason and completeness.
2. **Holding-level loan facts:** provider instrument identifiers, reported
   holding value, on-loan flag/value and collateral-related fields separately.
3. **Borrower exposure:** disclosed aggregate loan value by borrower within a
   filing, with LEI when provided and an unresolved-identity bucket.
4. **Collateral views:** reinvested cash collateral and reported non-cash
   collateral grouped under their source classifications, with no cash/loan
   balance conflation.
5. **Research aggregates:** disclosed loan totals by selected fund/date,
   contributing fund counts and measured coverage. A loan-value-to-holding-value
   ratio is admitted only after checking a common currency and population; a
   zero/missing denominator returns null with a reason. Do not call it market
   utilization or clamp exceptions to a convenient range.

Fund-report dates will vary. A cross-fund view must show the distribution of
report dates and lag rather than pretending all holdings are simultaneous.
Share classes must not duplicate a portfolio. Identifier matches require the
evidence effective at the selected date; ticker/name matches alone are
insufficient to merge holdings across funds or periods.

## Architecture and ownership

Proposed files are source-owned under
`src/market_data_pipeline/pipelines/sec_nport/`: `manifest.py`, `source.py`,
`archive.py`, `data/tables.py`, `data/joins.py`, `data/gold.py`,
`io/warehouse.py`, `io/parquet.py`, `pipeline.py`, `replay.py`,
`research_export.py`, `operations.py`, `cli.py` and `dagster_defs.py`.
Create modules as each bounded task requires them, not as empty placeholders.
Manifests, invented fixtures, SQL and runbooks remain source-scoped.

Archive receipt → member receipts → typed tables → relationship validation →
filing selection → research facts → committed SQLite/Parquet snapshot → local
artifact. The root orchestration module composes working pipeline definitions
only after the source slice exists; NAAIM stays paused.

Use type annotations, immutable records and `Decimal` for monetary values.
Preserve SEC identifiers as strings, including numerical-looking surrogate
IDs. A field parser must distinguish missing from invalid input:

```python
from decimal import Decimal

def optional_amount(raw: str | None) -> Decimal | None:
    if raw is None or raw == "":
        return None
    value = Decimal(raw)
    if not value.is_finite():
        raise ValueError("non-finite SEC amount")
    return value
```

The sample illustrates style; the implementation must enforce each field's
specific sign/scale rules from the admitted schema.

## Acceptance and traceability

| Acceptance ID | Evidence required | Requirements |
| --- | --- | --- |
| SEC014-AC01 | A dated access/schema matrix distinguishes verified fields from proposed normalized names and records public disclosure eras. | R01 |
| SEC014-AC02 | Invented ZIPs prove member limits, traversal/symlink rejection, truncation and corrupt-member detection before promotion. | R02, N02 |
| SEC014-AC03 | Typed fixtures preserve large IDs, decimal precision, nulls, dates, source rows and exact checksums. | R03, N01, N03 |
| SEC014-AC04 | One-to-many identifier/borrower rows do not multiply holding values; orphan/cross-edition keys block dependent output. | R04 |
| SEC014-AC05 | Same archive adds no fact versions; amendments/corrections/reversions remain observable by cutoff; equal capture clocks preserve the original monotonic sequence during replay. | R05 |
| SEC014-AC06 | Fund/share-class boundaries, holding loans and borrower/collateral facts remain distinct and reconcile within the documented tolerance. | R06, R07 |
| SEC014-AC07 | Unknown field, missing table, confidential coverage and absent loan flag cannot silently become zero or a negative lending finding. | R11 |
| SEC014-AC08 | Failure injection at every C07 journal boundary selects one coherent SQLite/file generation after restart; partial archives cannot advance a complete watermark. | R08, N04 |
| SEC014-AC09 | Empty-root replay reproduces selected facts and source knowledge clocks with no network access. | R08, N03 |
| SEC014-AC10 | Artifact validation preserves source/date/clock/coverage labels and rejects a simulated consumer input. | R09, N05, N06 |
| SEC014-AC11 | Local health diagnoses missing publication and schema/coverage failure; any created schedule is stopped. | R10 |
| SEC014-AC12 | Runbook and source evidence identify limitations, checks passed and the next delivery gate without claiming a deployed service. | R12 |
| SEC014-AC13 | Injected bounded-fetch tests archive exact responses and reject truncation, oversized bodies, 200 HTML challenges, terminal HTTP errors and exhausted transient retries before publication. | R02, R08, N02, N04 |

## Commands and completion

Existing repository checks are executable today:

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m build --no-isolation
```

Proposed command surface after implementation: `python -m
market_data_pipeline.pipelines.sec_nport` with `validate`, `ingest --input
<local.zip> --root <owned-root>`, `fetch --period <YYYYQn> --root <owned-root>`,
`replay --source-root <archive-root> --root <empty-root>`, `check --root
<owned-root>` and `export-research --root <owned-root> --destination <path>`.
These commands do not exist yet; the future CLI task must replace this section
with exact verified invocations and bounded flags.

Always preserve source evidence, missingness, versions and table grain. Resolve
new dependencies, historical volume, retention/public reuse and deployment
scope before those operations. Never infer securities-lending revenue or borrow
fees from loan values, reconstruct undisclosed fund months, copy sibling
secrets or mutate a filing to make a reconciliation pass.

Completion means AC01–AC13 have recorded evidence and the source output is
usable locally. A lack of complete public coverage is an explicit output
characteristic, not a reason to fabricate observations. Delivery of Spec 014
opens the ordered Spec 015 gate; there is no runtime dependency between them.
