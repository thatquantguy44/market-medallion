# SSV-T14 — research interchange acceptance

Implemented on `feat/finra-research-orchestration`, from merged main `1daf6b5`.
Traces SSV-REQ12 / SSV-AC14 and SSV-AC01.

`export-research` reads one verified PUBLISHED T10 generation, refuses pending
recovery and exact-scope mismatches, and writes the versioned research JSON.
Consumer validation checks admitted audience, source/units/methods, exact quantity
text, matched arithmetic, dates, capture/version lineage and missingness. Source
clocks remain actual first-seen; nullable retrieval/publication stays null.
Whitelist projection excludes attempts, permission references, run IDs, private
paths, raw payloads, runtime settings and credentials. Invented exports cannot
masquerade as provider artifacts. Source stores remain unchanged.

37 deterministic tests in `tests/test_ssv_export.py` pass, including actual CLI
subprocess/readback, independent frozen invented consumer fixture, 4096-digit
quantities, fractional scale, zero/null/empty/removal/missing rows, altered
arithmetic/lineage, scope/use refusal, bounded resources, corruption/recovery
refusal and atomic output interruption. **710 FINRA tests pass**. Changed files
pass Ruff lint/format. Full-suite/build evidence is recorded at T16. No provider
request or schedule activation occurred in T14.

The artifact is an internal interchange, not consumer integration or permission
to redistribute. See [wire contract](data_contract.md#ssv-t14-research-interchange-implementation-contract).
