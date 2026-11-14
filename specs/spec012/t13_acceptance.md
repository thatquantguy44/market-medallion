# SSV-T13 acceptance — release health and retention dry run

Date:2026-11-14. Branch:`feat/finra-health-retention`, from merged T11–T12 main
`12edd0f`. Traces:SSV-NFR02 / SSV-AC13. [Contract](operations_contract.md) /
[aggregate real-store evidence](evidence/t13_operations_conformance.json).

## Accepted implementation

`health` and `retention-dry-run` run through the existing portable script/local
config, with optional separate strict operations YAML. Read-only nonblocking locks,
SQLite transactions and existing raw/warehouse/catalog verifiers reconcile source
runs, ingestion, committed membership and frozen publications. Bounds refuse
oversize/corrupt evidence without initialization, network, repair or deletion.

Health retains requested/observed partition counts, last local/source success,
failed/no-response refresh above valid prior Gold, interrupted/unresolved work,
valid empties, stale capture and current publication lag/recovery. The FINRA2026
calendar handles closures/early closes without extrapolation. Unknown API timing
is explicit; a configured evidence-bound deadline distinguishes pending/overdue
and pre-release captures. ADF remains excluded, with a regression proving an
empty admitted TRF response does not admit ADF or impute another facility's rows.

Retention holds all data under PRESERVE_ALL. Every file has byte/hash accounting
and KEEP reasons; captures/versions explain current partitions, origins,
memberships, audit, source journal, frozen generations and both-pass collection
inputs. Unknown artifacts and unresolved work remain held. There is no deletion
option, finite approved retention/backup policy or inherited NAAIM365-day rule.

## Deterministic verification

47 new tests in `tests/test_ssv_operations.py` cover:

- Holiday, early close, unknown year/API timing, deadline boundary, early/post-release
  empty, stale capture and absent-root non-creation.
- Actual source success/failure/recovery, no-response, RUNNING, local partial/rejected
  refresh, unresolved raw capture and replay's missing source-run evidence.
- Current/older/lagging/COMMITTED publication and frozen capture/version references.
- Both passes of page collections; unchanged observation memberships, removals and
  version references; exact physical byte accounting and unknown-artifact holds.
- Missing/corrupt raw/receipt/outcome/binding/plan/events/results, invalid budgets,
  busy/missing/special locks, strict policy loading and actual process-level CLI.

The complete FINRA suite and full offline regression, Ruff/style, pinned library
integrity and package readback are recorded in the final verification checkpoint.
Optional PostgreSQL coverage is separate from local-only FINRA state.

## Read-only real-data conformance

No new provider request was made. Four actual portable-script commands read the
already accepted T12 isolated Chicago TRF2026-11-09 source and replay stores.
Each reconciles5 captures,5849 versions and4 protected input pages across both
collection passes; both publications are valid. Source refresh is SUCCESS with
last acquisition completion `2026-11-13T22:18:45.495588Z`; replay reports LOCAL_ONLY
and null original-network success. Both honestly report UNKNOWN_API_TIMING.

All53 source files and40 replay files remain byte-identical. Retention accounts
for42,896,700 source bytes and42,891,897 replay bytes, with zero deletable bytes.
Tracked evidence contains only aggregate status/counts/hashes; private raw rows
and detailed reports remain ignored. No other store, service, schedule, Slack
message or sibling configuration was touched.

## Delivery boundary and pickup

SSV-T13 is implemented for the selected three-TRF API scope. This does not certify
an API release SLA, complete all-facility/history coverage, approve deletion,
close G1/G2 or activate orchestration. NAAIM stays paused. Next is SSV-T14,
rights-admitted research interchange with a versioned invented consumer contract;
T15 stopped jobs and T16 delivery closeout follow. No merge is implied.
