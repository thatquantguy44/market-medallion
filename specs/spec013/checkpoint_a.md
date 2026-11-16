# Spec013 Checkpoint A — OTC-T01 through OTC-T03

Implemented locally **2026-11-15** on `feat/finra-otc-contract-parser`, from
latest main `1c256bf75fdf4fe1863346fee1b6f0bde6a8f85a` (merged Dagster PR #29).
This accepts the **offline fixture checkpoint**, not a complete pipeline or
actual provider coverage. This is the T01–T03 checkpoint at `041b9be`;
[current Spec013 status](README.md#current-implementation-and-pickup) owns later work,
including the subsequent [T04 identity slice](t04_identity_acceptance.md).

| Requirement / acceptance | Delivered evidence |
| --- | --- |
| OTC-REQ01 / OTC-AC01 | [Independent route/use review](source_review.md), exact approved-use gate and admitted/denied tests in `tests/test_otc_manifest.py`. Missing grants and wrong-source/mock/commercial/public capabilities fail closed. |
| OTC-REQ02 / OTC-AC02 | Six product/tier contracts in the [manifest](../../manifests/finra-otc-transparency.yml), catalog and `tests/test_otc_catalog.py`. Explicit bounded Monday selections reject duplicate/overlapping populations and summary/monthly/block aliases. |
| OTC-REQ03 / OTC-AC03 | [Pinned fixture grammar](data_contract.md#atst1-invented-pipe-profile), three invented files and `tests/test_otc_parser.py`. Literal header, exact counts, raw identifiers, date-only update and explicit context survive parsing. |

## What can be run now

The package is source-owned under
`src/market_data_pipeline/pipelines/finra_otc_transparency/`. It provides pure
Python admission/catalog/parser calls plus a strict local manifest loader.
It has no CLI, HTTP adapter, warehouse, publication, research artifact or job.

```bash
.venv/bin/python -m pytest -q tests/test_otc_manifest.py tests/test_otc_catalog.py tests/test_otc_parser.py
```

For a local fixture inspection:

```python
from pathlib import Path
from market_data_pipeline.pipelines.finra_otc_transparency.catalog import select_partitions
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import load_manifest
from market_data_pipeline.pipelines.finra_otc_transparency.data.weekly import parse_weekly

manifest = load_manifest(Path("manifests/finra-otc-transparency.yml"))
partition, = select_partitions([{
    "reporting_week_start": "2026-10-19", "product": "ATS_WEEKLY_DETAIL", "tier": "T1"
}])
profile = next(p for p in manifest.profiles if (p.product, p.tier) == (partition.product, partition.tier))
result = parse_weekly(
    Path("tests/fixtures/finra_otc_transparency/ats_t1_weekly.txt").read_bytes(),
    partition=partition, schema_profile=profile.schema_profile,
)
assert len(result.rows) == 3
assert result.rows[0].weekly_shares == 9007199254740993
assert result.completeness == "UNVERIFIED"
```

## Fixtures, verification and limits

All symbols, participants, MPIDs, quantities and dates in the three `.txt` files
are invented. The header is based on the older official v0.4 ATS file sample.
`ats_t1_weekly.txt` covers two venues, raw suffixes/leading-zero ID, non-ASCII
names, above-float-precision counts and reported zero. `ats_t1_empty.txt` is
header-only syntax. `ats_t1_changed_schema.txt` is deliberately unsupported.
No downloaded observations or credentials are included.

The 144 focused tests pass, including per-profile dated review provenance. Red/green evidence includes missing admission,
catalog and parser modules followed by working behavior, plus five reproduced
identifier-whitespace cases fixed to reject ambiguous tokens. Checks include
schema/type/count/date errors, conflicting duplicate keys, finite row/byte and
partition budgets, line-ending equivalence and a no-file/no-network pure path.
Full offline regression: **1,971 passed, 92 existing optional skips** (106.84s).
Repository Ruff lint/format, canonical Dagster inventory and diff checks pass.
Source/wheel builds and byte readback pass for all 190 runtime modules; the source
archive retains the exact OTC manifest and all three invented fixtures.

Header-only produces `PARSED_EMPTY` with `UNVERIFIED` completeness. Context comes
from explicit selection, not a provider discovery receipt. No real-file or API
conformance, stable name-only/aggregated identity, availability calendar,
persistence/replay equivalence or historical coverage is claimed. No provider
agreement was accepted, source request made, Slack sent or automation activated.
Known external QuantSmith working-tree revision drift remains; all 61 pinned Git
guidance objects and the 188-entry agent-skills snapshot verify.

## Exact next slice

**OTC-T04 / OTC-REQ04 / OTC-AC04:** preserve supplied MPID/CRD, name-only,
grouped and unknown attribution with evidence-scoped participant keys; test
renames and ambiguous labels without fabricating firm matches. Start with this
fixture/profile contract. T05 then adds immutable bytes/coverage receipts and
T06 the local panel, with all six selected populations required for that later
acceptance. Actual route/schema/complete extraction remains T12 work.
