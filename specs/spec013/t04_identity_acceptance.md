# OTC-T04 — participant identity acceptance

Implemented offline on `feat/finra-otc-contract-parser` after T01–T03 commit
`041b9bee4b8d63dbba5e56ad5aa4e84eafc20781`. The user asked to continue on this
branch. This is the T04 record at `568610d`; it closes OTC-REQ04 / OTC-AC04.
[Current Spec013 status](README.md#current-implementation-and-pickup) owns later
work, including the subsequent [T05 Bronze slice](t05_bronze_acceptance.md).

## Contract and evidence

[Identity contract](data_contract.md#otc-t04-identity-contract) and
`data/identity.py` preserve supplied names, MPID and CRD as source evidence.
Keys carry a versioned SHA-256 namespace with route, schema and product/tier
scope. Supplied MPID precedes CRD as key basis; both raw IDs remain inspectable.
An MPID-based label rename keeps its source key without asserting that a firm
or its owners remain the same through time. Names and supporting IDs remain
content for later observed-revision storage, not discarded key ingredients.

Name-only and explicitly grouped keys additionally include the reporting week.
A reused display name does not establish cross-period firm continuity. Grouping
requires explicit reviewed source evidence; the text "De Minimis Firms" alone
is not a classifier. Unknown attribution has no key. Missing/ambiguous evidence,
conflicting duplicates or mixed population/route/schema contexts cannot pass
`require_unique_participants`. Unknown/grouped rows with individual firm IDs
are contradictory and rejected. A group key identifies a bucket, not its members.

The ATS/T1 fixture parser now attaches this identity record and validates all
rows before returning a partition. Raw labels and leading-zero identifiers stay
available through row attributes. No other parser, real schema, entity lookup,
identity crosswalk or acquisition route is introduced.

## Verification

`tests/test_otc_identity.py` covers identifier kinds/leading zeros, provider-ID
renames, same-name/different-ID cases, week-scoped name/group keys, unknown
refusal, contradictory or malformed evidence, mixed context, duplicate keys
and integration through the existing invented ATS/T1 file. The missing module
failed before implementation; the focused identity/parser tests pass afterward.
Focused verification: **35 new identity tests, 179 total OTC tests pass**.
Repository Ruff lint/format and the canonical 8-job/8-schedule/12-sensor check
pass. Source/wheel byte readback passes for all 191 runtime modules; an isolated
wheel import parses the exact fixture quantities with its identity records.
The first build attempt failed on a missing backend temporary JSON output; an
unchanged retry succeeded without rewriting dependencies. The cause was not
established. Full offline regression: **2,006 passed, 92 existing optional skips**
in 628.62s, including the slow dependency/migration imports observed during this
run. No code/dependency change or test restart was needed for the suite to pass.

```bash
.venv/bin/python -m pytest -q tests/test_otc_manifest.py tests/test_otc_catalog.py tests/test_otc_parser.py tests/test_otc_identity.py
```

## Pickup and limits

Next is **OTC-T05 / OTC-AC05**: archive immutable exact bytes and source-owned
partition/discovery receipts, retain schema/manifest hashes and requested scope,
and preserve rejected/missing/partial outcomes before normalization. Local
capture planning and replay must not probe a provider. Continue to distinguish
parsed empty syntax from verified complete-empty membership. T06 will then add
the local panel and all six required fixture populations.

No production source coverage, longitudinal firm reconciliation, complete-panel
publication, storage/replay acceptance, live access or schedule activation is
claimed. No source requests, credentials, Slack messages or NAAIM changes occur.
