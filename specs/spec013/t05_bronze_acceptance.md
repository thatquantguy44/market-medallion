# OTC-T05 — immutable Bronze and partition receipts

Implemented offline on `feat/finra-otc-contract-parser` after T04 commit
`568610d8813dd9e304cb4498f30e47a5ed1d4531`, following the user's explicit T05
continuation. This is the T05 record at `6cb0376`; it closes OTC-REQ05 / OTC-AC05.
[Current Spec013 status](README.md#current-implementation-and-pickup) and
[T06 acceptance](t06_local_panel_acceptance.md) own subsequent work.

**Verified:** 2026-11-16, America/New_York.

## Delivered contract

`io/receipts.py` defines the bounded canonical metadata and `Part` envelope;
`io/bronze.py` owns `archive_capture`, `read_capture` and `validate_capture`.
All work is source-owned and network free. Exact bytes are durable before a
parser is invoked; no rejected payload is rewritten into a normalized substitute.

```text
<root>/.writer.lock
<root>/bronze/format.json
<root>/bronze/sequences/<20-digit-sequence>.json
<root>/bronze/captures/<capture-uuid>/receipt.json
<root>/bronze/captures/<capture-uuid>/parts/<part-id>.bin
<root>/bronze/captures/<capture-uuid>/discovery.bin       # optional
<root>/bronze/outcomes/<capture-uuid>/<binding-sha>.json
```

The root lock is compatible with later source-owned writers. Fsynced sequence
reservations precede atomic capture-directory publication. Failed writes may
leave an unused reservation; no partially staged capture becomes readable.
Retries with unchanged capture ID/content/lineage preserve the original clock
and bytes. New capture IDs retain repeated payloads as separate observations.
Equal clocks have distinct durable sequences; new clock regressions are rejected.

Receipts embed canonical manifest/schema snapshots and hashes, requested
week/product/tier, expected/received part IDs, source UUIDs and UTC capture clock,
byte counts/hashes, optional discovery evidence and bounded public HTTP metadata.
The full use-grant reference is not stored; its hash and route/operation are.
Auth/cookie headers are dropped and unsafe credential/query-bearing URLs rejected.
Publication time remains null; HTTP dates never become historical first-seen.
Only invented fixtures can override the archive clock; real retention always
records the worker's actual UTC archive time.

| Submitted evidence | Raw coverage / outcome | Membership and row-count treatment |
| --- | --- | --- |
| No expected data part received | `MISSING` | `UNVERIFIED`; no zero row count invented. |
| Missing, unexpected or truncated part; truncated discovery | `PARTIAL` | Bytes retained; parsing withheld; no complete population claim. |
| All declared bytes received | `RECEIVED_ALL` | This is a ledger observation, not provider completeness proof. |
| Valid invented ATS/T1 whole file | `PARSED` | Typed rows returned; membership remains `UNVERIFIED`. |
| Header-only invented file | `PARSED_EMPTY` | Parsed zero rows; not an established complete-empty provider release. |
| HTTP failure, unsupported route/profile/contract, invalid syntax | `REJECTED` with a fixed reason code | Exact raw bytes retained; nonparsed row count remains null. |

Readback checks the storage marker, reservation, receipt/hash, typed snapshots,
file inventory and exact body hashes without creating a root or lock. Retained
unknown schema IDs can remain byte-verifiable; they do not gain a current parser.
Unsupported snapshots are not silently upgraded. Symlink/nonregular archive
components, traversal names, false resealed coverage and corrupt outcomes fail
without automatic repair. Operators own the selected root and its parent paths.

## Verification and reproducible local use

**63 new Bronze tests / 242 total OTC tests pass.** Tests include original
byte/discovery readback, absence/truncation, parser rejection, header-only syntax,
separate retention/transform admission, safe metadata, all five unsupported
profiles, clock ordering, immutable retries, concurrent writers, failed writes,
forged/corrupt receipts, symlinks/FIFOs, finite inventories and input snapshots.
The missing module failed before implementation. Review fixtures reproduced
nine coverage/storage cases, four typed/mutable-input cases and two real-clock
cases before fixes. Full offline regression: **2,069 passed, 92 existing optional
skips** in 142.92s. Ruff lint/format and canonical Dagster parity pass.

A separate native Python smoke on invented data produced PARSED, PARTIAL,
REJECTED and PARSED_EMPTY with row counts 3/null/null/0 and sequences 1–4. Exact
large quantities and the leading-zero MPID survived. All 19 file hashes and
modification times remained unchanged across an idempotent capture retry, reads
and repeated validation. Every outcome retained UNVERIFIED membership.

The 188-entry agent-skills snapshot and all 61 pinned QuantSmith Git guidance
objects verify. The known external checkout revision/tree drift remains; no
reference or dependency was rewritten. Source/wheel builds and byte readback
pass for all **194 runtime modules**. An isolated wheel verifies the four retained
synthetic captures/outcomes while preserving all 19 file hashes and mtimes.

```bash
.venv/bin/python -m pytest -q tests/test_otc_bronze.py
.venv/bin/python -m pytest -q tests/test_otc_manifest.py tests/test_otc_catalog.py tests/test_otc_parser.py tests/test_otc_identity.py tests/test_otc_bronze.py
```

The following is an invented local fixture example, not an active real-data grant:

```python
from datetime import date
from pathlib import Path
from uuid import uuid4
from market_data_pipeline.pipelines.finra_otc_transparency.catalog import Partition
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import Part, archive_capture, validate_capture
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import load_manifest

use = {
    "dataset": "finra-otc-transparency", "purpose": "personal_noncommercial_internal_research",
    "route": "invented_fixture", "authorization": "pending", "permission_reference": None,
    "credential_type": None, "provider_dataset": None, "attribution": None,
    "operations": ["fixture"],
}
root = Path("data/otc-fixture-demo")
receipt = archive_capture(
    root, partition=Partition(date(2026, 10, 19), "ATS_WEEKLY_DETAIL", "T1"),
    manifest=load_manifest(Path("manifests/finra-otc-transparency.yml")),
    approved_use=use, run_id=str(uuid4()), expected_parts=["file"],
    parts=[Part("file", Path("tests/fixtures/finra_otc_transparency/ats_t1_weekly.txt").read_bytes())],
)
result = validate_capture(root, receipt["capture_id"], approved_use=use)
assert result.outcome["status"] == "PARSED" and result.outcome["row_count"] == 3
assert result.outcome["membership"] == "UNVERIFIED"
```

## Exact next slice

**OTC-T06 / OTC-REQ06 / OTC-AC06:** build a transactional source-owned SQLite
weekly panel and CLI around admitted captures, identity and explicit completeness.
The selected acceptance scope includes all six ATS/non-ATS × T1/T2/OTCE fixture
profiles; only ATS/T1 currently has a parser. Split a growing task before widening
its file scope, and keep older capture snapshots inspectable. Negative/partial
or unsupported captures cannot replace membership or produce a success watermark.

No real source observation, credential, permission acceptance, HTTP request,
warehouse/Gold publication, CLI, replay service, Slack message, schedule or
NAAIM runtime change is part of T05. Public Query API remains a future candidate;
actual route/schema/complete extraction is still T12 work.
