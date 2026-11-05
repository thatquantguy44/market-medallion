# Spec011 T17 — bounded research-delivery acceptance

Reviewed 2026-11-05 on `chore/spec011-t17-acceptance`, from merged `main`
`5b092af` (PR #7). This record closes the selected native-source delivery;
recurring service operation and consumer integration remain separate gates.

## Scope and operator decisions

The user requested the remaining T17 items and authentic local threshold files.
They then confirmed existing provider permission under a **verbal agreement**
and stated that no document reference would be supplied. The authorization basis
is **USER_ATTESTED_VERBAL_PROVIDER_PERMISSION**, not independently verified
provider documentation. It covers this bounded Nasdaq/NASDAQ and NYSE/NYSE
October 26–30 capture, retained local copies and internal research. The local
attestation is retained with the sample package. No written license, public
redistribution grant or recurring-operation activation is inferred.

The original two approved September SEC local archives remain the selected SEC
sample. Their original retrieval/publication clocks are unknown. The selected
threshold dates are exactly October 26–30, 2026, for NASDAQ and NYSE. NYSE
American/Arca/National, other dates, broader historical formats and full exchange
calendars are outside this evidence. The earlier November 3 documentary samples
are historical context and do not substitute for this ten-file reconciliation.

T14's joined SEC/threshold projection remains an explicitly deferred optional
extension. This acceptance covers the two native artifacts and dated mapping
boundary; it does not claim a delivered joined view or universal security master.
G3 schedules and G4 consumer UI/public display were not requested in this closeout.

## Official source-use review

- [Nasdaq's notice](https://www.nasdaqtrader.com/Trader.aspx?id=CopyDisclaimMain)
  restricts copying, storage, transformation and distribution, with narrow
  personal/noncommercial exceptions. The general notice alone does not establish
  permission for the intended archive and research workflow. This bounded use
  relies on the user's attestation above; that attestation is recorded separately
  from the public notice and technical conformance evidence.
- [ICE's terms linked by NYSE](https://www.ice.com/privacy-security-center/terms-of-use)
  limit the general personal/noncommercial download license and exclude robots
  and similar extraction methods from it. They also restrict systematic
  compilation and sharing. The operator attestation is the authorization basis
  for this bounded capture; website reachability is not used as permission proof.
- [SEC's FTD contract](https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data)
  describes outstanding balances, six fields and the source price-null token.
  The [developer guidance](https://www.sec.gov/about/developer-resources) requires
  identified, efficient access. This closeout reuses approved local SEC files
  and makes no new SEC data request.

## Real-source reconciliation

The [SEC aggregate record](evidence/sec-september-2026.json) pins both original
archive/member/manifest hashes and independently compares every parsed field
and both trailers. It also compares every source field and raw lineage with
both facts and Gold in the retained coherent snapshot. All **117,025** facts
match; the original files, current pointer and snapshot files remain unchanged.
Both September partitions retain **PARTIAL / NOT_CERTIFIED** population coverage.
The 663 unavailable prices and one missing symbol remain null with source semantics.
First-seen remains the actual November capture; September dates are not
historical publication vintages.

The [threshold aggregate record](evidence/threshold-october-2026.json) independently
compares every original field, flag, row position, trailer, Bronze digest,
normalized membership and coverage/audit capture clock. The files are authentic
HTTPS responses, preserved unaltered locally; no invented data fills a gap.

| List date | Nasdaq rows | NYSE rows | NYSE file state |
| --- | ---: | ---: | --- |
| 2026-10-26 | 81 | 1 | COMPLETE |
| 2026-10-27 | 82 | 1 | COMPLETE |
| 2026-10-28 | 82 | 1 | COMPLETE |
| 2026-10-29 | 80 | 1 | COMPLETE |
| 2026-10-30 | 79 | 0 | COMPLETE_EMPTY |

All ten selected files reconcile: **408 membership rows**, ten coverage rows
and ten successful capture audits. Source creation tokens remain raw because
their timezone is unknown; retrieval and first-seen are actual November 5 clocks.
The [local round-trip record](evidence/threshold-local-roundtrip.json) confirms
network-free replay and native export: 408 membership rows, ten coverage rows,
93 coverage-qualified observed episodes and an exact-schema Delta staging snapshot.
The bounded calendar is the five configured/observed list dates, not a full
holiday service or a reconstruction of earlier membership.

Local originals, HTTPS receipts, initial failed attempts, authorization record,
warehouse, replay and research artifact are under ignored
`data/spec011-t17/thresholds/`. The ZIP
`threshold-samples-20261026-20261030.zip` contains the ten original files, their
hash/capture manifest and operator-attestation record. Provider rows, runtime
configuration and credentials are excluded from Git; tracked evidence contains
aggregate counts, hashes and provenance metadata only.

## NYSE acquisition defect found during conformance

Initial NYSE startup HEAD requests returned 404 for all five exact selected
Download URLs; these attempts were recorded as failures, not empty lists.
A bounded one-file diagnosis found that the official source page accepts HEAD,
the Download endpoint accepts GET, and unrestricted content negotiation succeeds
where an explicit `text/plain` preference returns 406. The diagnostic used the
same declared client identity, official host and verified run-scoped HTTP client.

The NYSE-owned adapter now probes its published source page once, uses `Accept:
*/*`, and reuses that selected client for the exact configured Download GET.
Acquisition receipts record the probe URL. Provider/market manifest mismatch
fails before probing; startup HTTP rejection still prevents the data request.
Shared route fallback, TLS, retries and other providers' probes are unchanged.
Offline regressions reproduce the old failure and verify exact bytes, one probe,
startup rejection and provider-profile gating. All five subsequent NYSE captures
succeeded, including the independently validated empty October 30 envelope.

## Acceptance traceability

| Criterion | Result and concrete evidence |
| --- | --- |
| A01 | PASS — `tests/test_sec_ftd_storage.py`, SEC parser/storage tests: string IDs, exact quantities/prices, source null tokens and malformed input. |
| A02 | PASS — `tests/test_sec_ftd_bronze.py`, `tests/test_sec_ftd_pipeline.py`: unsafe/oversized/error/invalid input cannot promote facts. |
| A03 | PASS — threshold parser/pipeline/episode tests distinguish missing, failed and valid empty lists; gaps do not imply exits. |
| A04 | PASS — `tests/test_reg_sho_parser.py`, NYSE HTTP regressions and all ten actual files reconcile source date, market, header and trailer. |
| A05 | PASS — `tests/test_sec_ftd_revisions.py`, threshold pipeline and Delta tests retain unchanged/corrected/reverted/withdrawn versions at true cutoffs. |
| A06 | PASS — partial replacements cannot withdraw absent rows; the real SEC samples retain PARTIAL coverage and no derived zeros. Older reporting regimes remain excluded. |
| A07 | PASS at native mapping boundary — `tests/test_spec011_identity_features.py` covers knowledge/effective dates, reused symbols, overlapping candidates and unknown CUSIPs without selecting an ambiguous target. Optional joined output is deferred. |
| A08 | PASS — episode tests cover an explicit holiday/weekend gap in the calendar, missing coverage, entry/exit and censoring; only observed expected dates are counted. |
| A09 | PASS — both replay suites and real ten-capture threshold replay preserve normalized history and original clocks/ordering without network. Retained SEC replay state is reconciled back to its source archives. |
| A10 | PASS — `tests/test_sec_ftd_publication.py` and actual Delta tests cover staging/commit/finalize/pointer failures and coherent recovery. |
| A11 | PASS for the two native contracts — source export tests and real threshold export verify independent generation/budget/coverage behavior. Joined projection is explicitly deferred. |
| A12 | PASS — source CLI/operations/Dagster tests enforce bounded explicit acquisition, no-request local paths and stopped schedule defaults; NAAIM remains paused. |
| A13 | PASS for selected scope — both SEC archives and all ten authentic threshold files reconcile; excluded markets, dates and population claims are explicit above. |
| A14 | PASS — full suite with actual disposable PostgreSQL 17.6 enables both source parity/replay/concurrency/native-type tests; no integration skip substitutes for evidence. |
| A15 | PASS — actual local Spark 4.0.1 / Delta 4.0.1 tests for both publishers verify exact types/keys/clocks, repeated publication and interrupted multi-table recovery; threshold empty tables and sealed-generation retry also pass. |

The [verification record](evidence/verification.json) records the final full-suite
result, engine versions and hashes of the changed implementation/tests/review
scripts. Ruff, formatting, dependency checks, package builds and relative document
links are checked separately. CI's disposable service and DSN now include an
explicit test password, enabling both source-specific PostgreSQL suites that
previously skipped when the DSN omitted that field. The repository-local workflow snapshot verifies;
QuantSmith's external checkout still differs from its pin and is not counted
as verified guidance. Its checkout and pin were left untouched.

## Delivery decision and reproducibility

**G2 is complete for this bounded internal-research delivery**, subject to review
and merge of this branch. This is an operator-attested source-use decision and
verified engineering/data evidence, not independent confirmation of a provider
license. G3 remains not activated; G4 remains deferred. The next source build is
Spec012 FINRA daily short-sale volume after this closeout merges.

To recheck the local real samples without provider access:

```bash
.venv/bin/python scripts/verify_spec011_sec_samples.py \
  --output data/spec011-t17/sec-recheck.json
.venv/bin/python scripts/verify_spec011_threshold_samples.py \
  --config data/spec011-t17/thresholds/pipeline.local.yml \
  --output data/spec011-t17/threshold-recheck.json
```

The source bytes must already be present. These commands perform no HTTP and
make no changes to the retained warehouses. Fresh clones receive only the
sanitized evidence and scripts, not provider data or runtime configuration.
Enable `MARKET_DATA_TEST_POSTGRES_DSN` only for an isolated disposable database,
and `MARKET_DATA_TEST_DELTA=true` with a suitable JDK for the actual engine tests.
Databricks workspace publication was not run. Both checked-in acquisition
settings and the local sample config are disabled after capture; every schedule
remains stopped, and NAAIM remains paused. No cloud object or production database
was provisioned.
