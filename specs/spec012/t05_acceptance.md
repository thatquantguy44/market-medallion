# SSV-T05 acceptance — immutable local API-response Bronze

Accepted 2026-11-13 on `feat/finra-daily-short-sale-volume`, based on merged main
`b580689`; delivered in [PR #20](https://github.com/thatquantguy44/market-medallion/pull/20). Traces **SSV-REQ05 / SSV-AC05**, with **SSV-AC01** admission.
[Contract](bronze_contract.md), [tasks](todo.md) and
[sanitized conformance](evidence/t05_bronze_conformance.json).

## Delivered behavior

`io.bronze.archive_input` admits the operation and exact public API request scope,
then stores exact bounded data-response bytes and an immutable, hashed receipt
before any dataset parsing. `read_capture` verifies bytes, manifest/schema,
receipt, original sequence and clocks. `parse_capture` invokes the unchanged pure
parser and appends a versioned, sanitized parsed/rejected outcome. Rejected data
remains inspectable; unexpected execution failures leave originals retryable.

Capture UUIDs are idempotency keys. Exact retries retain original sequence/clocks;
conflicting evidence refuses overwrite. New capture IDs retain repeat observations.
Atomic directory publication, exclusive private files, writer locking and sequence
reservations prevent partial captures and order concurrent/equal-time inputs.
Interrupted capture publication can leave a sequence gap; that ordinal is not reused.
Corruption is refused, not silently repaired. This is local POSIX storage for
cooperating writers, not a tamper-proof or off-machine backup service.

Receipts retain validated public request scope and safe response headers only.
Opaque diagnostics, auth/cookie/proxy headers and arbitrary permission references
are excluded. Unsafe request identities and recognizable credential bodies refuse
before persistence. This security screen is not a universal secret detector;
only market-data responses belong at this boundary. Incomplete-body evidence,
invalid response claims and parser failures cannot become successful outcomes.

`archived_at` is local archive observation under the writer lock before atomic
publication. It does not claim exact fsync completion, provider publication or
retrieval completion. `retrieved_at` is explicit and nullable, publication stays
unknown, and T02 request-start `captured_at` is never borrowed as first-seen.
No Silver clock, accepted rows, coverage watermark or publication pointer is written.

## Verification

- **49 Bronze tests** cover exact retention before parsing, valid fractional and
  empty responses, malformed/HTML/partial input, redaction, admission before I/O,
  provider/fixture separation before reading payloads, immutable conflicts,
  metadata/payload/outcome corruption, symlinks, interrupted publication, sequence
  gaps, concurrent writers, queued capture clocks, bounded resources and retry.
- **315 focused FINRA tests** pass. All five prior core modules remain identical
  to merged main: manifest, registry, parser, quantity codec and ratio helpers.
- Full offline regression: **1,169 passed, 92 optional skips**. External database
  variables were unset. Ruff lint/format and wheel/sdist builds pass; all three
  new source modules are byte-identical in both packages.
- Independent invented-only conformance checked **four captures / 18 files** in
  a temporary root: valid, fractional, empty 204 and malformed JSON. Every raw
  readback and retry matched; no diagnostic credential sentinel persisted.
  Source package checks exclude private environment/runtime files.
- Vendored agent-skills verified at its existing pin (188 entries). The existing
  QuantSmith checkout has revision/tree drift; all **61 pinned Git guidance
  objects** were checksum-verified without changing the checkout or pin.

The new test suite was first run against missing modules. Further failing tests
exposed fixture/provider read admission, unrecognized receipt metadata and clock
ordering; the implementation now passes those boundaries. Fixtures are invented,
not copied from ignored provider samples. No provider-data request, database,
Slack notification, acquisition schedule or existing operational-store change
was part of this slice.

## Runnable fixture example

After normal repository setup, run from the repository root:

```bash
.venv/bin/python -m pytest -q tests/test_ssv_bronze.py
```

The public library API can also be exercised without credentials or persistent
runtime configuration:

```python
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import load_manifest
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
    archive_input, read_capture, parse_capture,
)

manifest = load_manifest(Path("manifests/finra-short-sale-volume.yml"))
payload = Path("tests/fixtures/finra_short_sale_volume/invented-valid.json").read_bytes()
grant = {
    "purpose": "personal_noncommercial_internal_research",
    "route": "invented_fixture", "authorization": "pending",
    "permission_reference": None, "operations": ["fixture"], "credential_type": None,
}
receipt = {
    "url": "https://api.finra.org/data/group/otcMarket/name/regShoDaily",
    "method": "POST", "request_data_version": 1,
    "request_payload": {"limit": 2, "offset": 0, "compareFilters": [
        {"compareType": "EQUAL", "fieldName": "tradeReportDate", "fieldValue": "2026-11-09"},
        {"compareType": "EQUAL", "fieldName": "marketCode", "fieldValue": "N"},
    ]},
    "http_status": 200,
    "headers": {"Content-Type": "application/json", "Data-Version": "1",
                "Record-Total": "2", "Record-Offset": "0", "Record-Limit": "2"},
    "bytes": len(payload), "sha256": sha256(payload).hexdigest(),
}
with TemporaryDirectory() as root:
    saved = archive_input(root, payload, manifest, receipt, approved_use=grant,
                          operation="fixture", run_id=str(uuid4()))
    capture_id = saved["capture_id"]
    assert read_capture(root, capture_id, manifest, approved_use=grant,
                        operation="fixture") == (saved, payload)
    page, outcome = parse_capture(root, capture_id, manifest, approved_use=grant,
                                  operation="fixture")
    assert len(page.rows) == 2 and outcome["status"] == "parsed"
    assert outcome["published"] is False
```

## Next bounded work

**SSV-T06:** one invented complete API partition through Bronze/Silver/Gold and
explicit run audit in SQLite, with validation/write failures preserving prior
committed publication. No CLI, warehouse, acquisition adapter, export, Parquet
snapshot, replay command or Dagster job exists for FINRA yet. Those tasks and
G1/G2 remain open; T05 is not whole-pipeline production acceptance. Keep the
three-TRF API scope, current admission and fractional quantities. Schedules stay
stopped and NAAIM stays paused.
