"""OTC-T06 bounded offline fixture capture -> transactional local weekly panel."""

import hashlib
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import Partition, profile_for
from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import (
    fixture_assertion,
    prepare,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import (
    Part,
    archive_capture,
    read_capture,
    validate_capture,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import (
    DEFAULT_BYTES,
    encoded,
    limits,
)
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import (
    Manifest,
    require_approved_use,
)


def _fixture(approved_use):
    # Real grants cannot authorize synthetic local membership, even without HTTP.
    require_approved_use(approved_use, operation="fixture")


def publish_capture(
    root: Path | str, capture_id: str, *, approved_use: object, max_bytes: int = DEFAULT_BYTES
) -> dict:
    from market_data_pipeline.pipelines.finra_otc_transparency.io.warehouse import persist

    _fixture(approved_use)
    result = validate_capture(root, capture_id, approved_use=approved_use, max_bytes=max_bytes)
    capture = read_capture(root, capture_id, approved_use=approved_use, max_bytes=max_bytes)
    status, reason, coverage, rows = prepare(capture, result)
    return persist(root, result, status=status, reason=reason, coverage=coverage, rows=rows)


def run_fixture(
    root: Path | str,
    *,
    payload: bytes,
    partition: Partition,
    manifest: Manifest,
    approved_use: object,
    expected_rows: int | None = None,
    run_id: str | None = None,
    capture_id: str | None = None,
    body_complete: bool = True,
    max_bytes: int = DEFAULT_BYTES,
    max_rows: int = 10000,
) -> dict:
    """Import explicitly invented bytes; expected_rows is a synthetic assertion.

    Omit the assertion to retain an UNVERIFIED attempt without publication. This
    API cannot admit real files/API responses or establish provider completeness.
    """
    _fixture(approved_use)
    limits(max_bytes, max_rows)
    if max_rows > 10000:
        raise ValueError("OTC local panel supports at most 10000 rows")
    discovery = None
    if expected_rows is not None:
        if not isinstance(payload, bytes):
            raise ValueError("OTC fixture input must be exact bytes")
        assertion = fixture_assertion(
            partition,
            profile_for(partition.product, partition.tier).schema_profile,
            hashlib.sha256(payload).hexdigest(),
            expected_rows,
        )
        discovery = Part("fixture-population", encoded(assertion))
    receipt = archive_capture(
        root,
        partition=partition,
        manifest=manifest,
        approved_use=approved_use,
        run_id=run_id if run_id is not None else str(uuid4()),
        capture_id=capture_id,
        expected_parts=["file"],
        parts=[Part("file", payload, body_complete=body_complete)],
        discovery=discovery,
        max_bytes=max_bytes,
        max_rows=max_rows,
    )
    return publish_capture(
        root, receipt["capture_id"], approved_use=approved_use, max_bytes=max_bytes
    )


def inspect_panel(
    root: Path | str,
    *,
    partitions: list[Partition] | tuple[Partition, ...],
    approved_use: object,
    max_rows: int = 10000,
) -> dict:
    from market_data_pipeline.pipelines.finra_otc_transparency.io.warehouse import inspect

    _fixture(approved_use)
    return inspect(root, partitions=partitions, max_rows=max_rows)
