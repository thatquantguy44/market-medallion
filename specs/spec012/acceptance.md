# Spec012 — bounded research delivery acceptance

T14 → T15 → T16 were implemented in that order on
`feat/finra-research-orchestration`, from merged main `1daf6b5`.
T14 is commit `2b6fc3b`; T15 is `c6b025a`. T16 verification and evidence are
recorded here. This is local delivery evidence; PR review/CI/merge are separate.

## Selected real coverage and gates

The user explicitly authorized all three selected TRFs for **2026-11-09**, using
this project's configured Public API access without enabling a schedule. A fresh
isolated in-process `finra_short_sale_volume_job` succeeded on 2026-11-15:

| Facility | Complete source rows |
| --- | ---: |
| Nasdaq Carteret TRF (`NQTRF` / `Q`) | 12,349 |
| Nasdaq Chicago TRF (`NCTRF` / `B`) | 5,849 |
| NYSE TRF (`NYTRF` / `N`) | 7,032 |
| Total disjoint facility observations | 25,230 |

All multi-page partitions have two complete agreeing passes. Fourteen original
page captures plus three derived collection captures are retained. The run used
18 charged request-budget units and 11,119,037 response bytes, within explicit
one-day/three-partition limits. These are budget units, not a claim of18 actual
provider HTTP requests. The publication has 25,230 Silver versions,25,230 facility
Gold rows and 12,355 scoped research rows. A symbol missing from one selected TRF
has NO_REPORTED_ROW and null aggregate quantities; complete partitions do not
imply every symbol was reported at every facility.

The T14 artifact has 12,355 research rows and 25,230 version lineage records with
internal-use/source labels. Source quantities retain exact decimal text, local
capture/first-seen clocks remain actual, and unknown provider publication stays
null. Credentials, permission references, run attempts, private paths and raw
payloads are excluded. No downstream application or redistribution is enabled.

A fresh empty-target replay reproduces every published table and research field
except the new publication event identity. Read-only source/replay health reports
UNKNOWN_API_TIMING, with all three committed partitions present, publication
PUBLISHED, no unresolved capture and zero deletable bytes. Source refresh is
SUCCESS; replay is LOCAL_ONLY. All 115 source files and 88 replay files remain
unchanged by these read-only checks. UNKNOWN describes the unresolved API release
SLA; it is not silently relabelled healthy or late.

**G1 and G2 are accepted locally for this explicit three-TRF one-date API scope**
after the deterministic and real-source checks below. Broader historical coverage,
additional dates/facilities, TXT/ADF/ORF/consolidated routes and older schemas are
outside this delivery. G3 recurring activation and G4 consumer integration remain
unstarted. Every acquisition schedule and both new FINRA alert sensors remain
unactivated by this work; NAAIM remains paused. Spec013 OTC-T01 is the next
implementation after this branch's review/merge.

## Acceptance matrix

| ID | Deterministic evidence | Actual/local evidence and policy boundary |
| --- | --- | --- |
| SSV-AC01 | `test_ssv_manifest.py`, `test_ssv_export.py` admission before I/O and output-use refusal | Existing user-attested Public API terms; explicit acquire/transform/local-import/internal-export grants in ignored config. Fixture rights cannot authorize provider use. |
| SSV-AC02 | `test_ssv_registry.py`, `test_ssv_completeness.py` | Three literal facility/market pairs verified; API version 1 only. Unknown/overlapping/ADF/ORF/consolidated selections refuse. |
| SSV-AC03 | `test_ssv_parser.py` literal fields, malformed/partial/schema/date cases | All 25,230 real rows parsed, every original page retained; no mock observations. |
| SSV-AC04 | `test_ssv_semantics.py`, `test_ssv_gold.py`, `test_ssv_export.py` | Whole/fractional quantities, scale and unit-1 ratios preserved; exempt is included in short, never added twice. |
| SSV-AC05 | `test_ssv_bronze.py`, [T05](t05_acceptance.md) | Seventeen real captures with payload/receipt hashes and capture clocks; accepted pages and verification inputs unchanged. |
| SSV-AC06 | `test_ssv_pipeline.py`, `test_ssv_cli.py`, [T06–08](t06_t08_acceptance.md) | Real transactional warehouse and explicit committed publication; failure fixtures preserve prior state. |
| SSV-AC07 | `test_ssv_versions.py`, `test_ssv_gold_query.py` | Identical/revised/reverted values and local cutoff membership tested with invented captures; no fabricated historical vintages in real backfill. |
| SSV-AC08 | `test_ssv_versions.py`, `test_ssv_gold_query.py`, `test_ssv_export.py` | Complete removal/valid-empty/absence differ from failed/partial inputs; exported missing rows retain nulls. |
| SSV-AC09 | `test_ssv_gold.py`, `test_ssv_gold_query.py`, [T09](t09_acceptance.md) | Matched disjoint research population for the three actual partitions; calendar gaps stay explicit. |
| SSV-AC10 | `test_ssv_parquet_types.py`, `test_ssv_publication.py`, [T10](t10_acceptance.md) | Actual coherent publication/readback/recovery and replay table parity. Stage/commit/finalize/pointer/mark failure-restart tests pass. |
| SSV-AC11 | `test_ssv_replay.py`, `test_ssv_replay_archive.py`, `test_ssv_replay_cli.py` | All 17 actual captures replay with original clocks, membership, versions and audit; every published table equals source. |
| SSV-AC12 | `test_ssv_acquisition_*.py`, `test_ssv_source.py`, `test_ssv_page_collection.py` | Finite approved one-day/three-TRF request; two agreeing page passes per partition; failure/retry/truncation/HTML fixtures do not promote. |
| SSV-AC13 | `test_ssv_operations.py`, [T13](t13_acceptance.md) | Offline release/capture/refresh/publication checks,203 files unchanged and PRESERVE_ALL with no deletion. API timing remains unknown; calendar ends 2026 unless reviewed. |
| SSV-AC14 | `test_ssv_export.py` (38 tests), [T14](t14_acceptance.md) | Versioned admitted export and independent invented consumer fixture; real research readback/replay parity. Final review also rejects repeat membership beyond the declared cutoff. |
| SSV-AC15 | `test_ssv_dagster.py` (21 tests), [T15](t15_acceptance.md), root entrypoint/sensor regression tests | Default-stopped schedule/sensors, worker live and scheduled kill gates, no orchestration retries, sanitized failure metadata, actual isolated manual job success, [operating runbook](../../docs/deployment/ssv_runbook.md). |

Machine-readable aggregate [conformance evidence](evidence/t16_delivery_conformance.json)
contains identities, hashes, bounds/counts, replay and CLI results. Original provider
payloads and runtime settings remain ignored under this project's data root.
Earlier task acceptances remain separate historical evidence, not claims about
the final branch's CI or present service health.

## Verification and review

T14 added37 tests; T15 added21; T16's consumer-cutoff regression adds one.
Final export coverage is38 tests and focused FINRA coverage is732 tests.
The full offline regression, package readback and portable CLI results are listed
in the aggregate evidence with their actual outcomes. Optional integration skips
are reported separately; no new PostgreSQL, Delta or cloud-backend support is
claimed for FINRA's selected SQLite delivery.

Review covered source ownership, admission before I/O, exact arithmetic and
capture-time cutoffs, output field whitelists, atomic no-overwrite behavior,
publication recovery, execution-time kill switches, error-context sanitization
and source/replay parity. The first full regression caught two fixed root-inventory
expectations for the new schedule/sensors. A new failing cutoff regression drove
the final consumer fix, followed by a passing export suite and full rerun.
No dependency/vendor changes, sibling data/config, real Slack sends or schedule
activation occurred. Pinned agent-skills 188 entries verify. QuantSmith's checkout
still has known revision/tree drift; all 61 pinned Git guidance objects verify
without modifying or repinning that external checkout.

## Subsequent branch review

[Branch review](branch_review.md) records the fixed generation race and consumer
lineage/membership gaps, 24 additional regressions/equivalence tests, 756 FINRA
checks and 1,777 full-suite passes / 92 optional skips. The original T16 counts
above describe its earlier checkpoint. [New offline conformance](evidence/branch_review_conformance.json)
reproduces the accepted source/replay artifact bytes under the corrected reader,
with all 203 store files unchanged and no new provider requests or activation.
