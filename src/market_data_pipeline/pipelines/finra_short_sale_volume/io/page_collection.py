"""SSV-AC12: a derived two-pass collection binds immutable provider page captures."""

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import ParsedPage, parse_page
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.data.versions import observation
from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze
from market_data_pipeline.shared.files import writer_lock

CONTRACT = bronze.COLLECTION
SORT = ["securitiesInformationProcessorSymbolIdentifier"]


def _assemble(root, body, manifest, *, approved_use, operation, max_bytes, max_rows, parent=None):
    if (
        not isinstance(body, dict)
        or set(body) != {"schema_id", "passes", "max_pages"}
        or body["schema_id"] != CONTRACT
        or type(body["max_pages"]) is not int
        or not 1 <= body["max_pages"] <= 20
        or not isinstance(body["passes"], list)
        or len(body["passes"]) != 2
    ):
        raise ValueError("FINRA collection manifest is invalid")
    seen = set()
    bytes_used = 0
    previous = None
    complete = []
    first = None
    for members in body["passes"]:
        if not isinstance(members, list) or not 1 <= len(members) <= body["max_pages"]:
            raise ValueError("FINRA collection page budget exceeded")
        rows = []
        symbols = set()
        expected = 0
        total = None
        scope = None
        for capture_id in members:
            bronze._identity(capture_id)
            if capture_id in seen:
                raise ValueError("FINRA collection requires independent page captures")
            seen.add(capture_id)
            path = Path(root) / "bronze/captures" / capture_id
            if path.is_symlink() or not path.is_dir():
                raise ValueError("FINRA collection page directory is unsafe")
            kind = bronze._json(path / "receipt.json").get("schema_id")
            if kind not in {bronze.CONTRACT, bronze.EVIDENCE}:
                raise ValueError("FINRA collection cannot nest or refer to unknown captures")
            saved, payload = bronze.read_capture(
                root,
                capture_id,
                manifest,
                approved_use=approved_use,
                operation=operation,
                max_bytes=max_bytes,
            )
            point = (saved["capture_sequence"], saved["archived_at"])
            if previous is not None and (point[0] <= previous[0] or point[1] < previous[1]):
                raise ValueError("FINRA collection capture order regressed")
            if parent and (
                point[0] >= parent["capture_sequence"] or point[1] > parent["archived_at"]
            ):
                raise ValueError("FINRA collection refers to future page evidence")
            previous = point
            if (
                saved["receipt_issues"]
                or saved["api_receipt"]["request_payload"].get("sortFields") != SORT
            ):
                raise ValueError("FINRA collection requires valid sorted original pages")
            bytes_used += len(payload)
            if bytes_used > max_bytes:
                raise ValueError("FINRA collection byte budget exceeded")
            page = parse_page(
                payload,
                manifest,
                saved["api_receipt"],
                max_bytes=max_bytes,
                max_rows=saved["capture_limits"]["max_rows"],
            )
            current = (page.trade_date, page.facility_id)
            if (
                page.offset != expected
                or (total is not None and page.record_total != total)
                or (scope and scope != current)
            ):
                raise ValueError("FINRA collection has missing pages or changing scope/count")
            if first is None:
                first = saved
            total, scope = page.record_total, current
            if total > max_rows:
                raise ValueError("FINRA collection row budget exceeded")
            for row in page.rows:
                if row.symbol_raw in symbols:
                    raise ValueError("FINRA collection contains duplicate symbols")
                symbols.add(row.symbol_raw)
                rows.append(replace(row, source_row=page.offset + row.source_row))
            expected += len(page.rows)
        if expected != total:
            raise ValueError("FINRA collection is incomplete")
        complete.append((scope, rows))
    if complete[0][0] != complete[1][0] or digest(
        [observation(r) for r in complete[0][1]]
    ) != digest([observation(r) for r in complete[1][1]]):
        raise ValueError("FINRA collection verification pass disagrees")
    return complete[0], first


def parse(root, saved, payload, manifest, *, approved_use, operation, max_bytes, max_rows):
    bronze._admit(manifest, approved_use, operation)
    try:
        body = json.loads(payload)
        if encoded(body) != payload or len(payload) > 65536 or saved["receipt_issues"]:
            raise ValueError
        (scope, rows), first = _assemble(
            root,
            body,
            manifest,
            approved_use=approved_use,
            operation=operation,
            max_bytes=max_bytes,
            max_rows=max_rows,
            parent=saved,
        )
        if saved["api_receipt"] != first["api_receipt"]:
            raise ValueError
        return ParsedPage(
            tuple(rows),
            scope[0],
            scope[1],
            0,
            len(rows),
            len(rows),
            hashlib.sha256(payload).hexdigest(),
            manifest.schema_id,
            manifest.data_version,
            first["api_receipt"]["http_status"],
        )
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ValueError("FINRA collection integrity or completeness mismatch") from None


def archive(
    root,
    manifest,
    passes,
    *,
    approved_use,
    operation,
    run_id,
    archived_at,
    max_pages,
    page_bytes,
    page_rows,
):
    kind = bronze._admit(manifest, approved_use, operation, {"fixture", "acquire", "local_import"})
    if (
        type(max_pages) is not int
        or not 1 <= max_pages <= 20
        or type(page_bytes) is not int
        or not 1 <= page_bytes <= 20 * 1024**2
        or type(page_rows) is not int
        or not 1 <= page_rows <= 5000
    ):
        raise ValueError("FINRA collection requires bounded page limits")
    body = dict(schema_id=CONTRACT, passes=passes, max_pages=max_pages)
    payload = encoded(body)
    if len(payload) > 65536:
        raise ValueError("FINRA collection metadata budget exceeded")
    max_bytes = page_bytes * max_pages * 2
    max_rows = page_rows * max_pages
    (_, _rows), first = _assemble(
        root,
        body,
        manifest,
        approved_use=approved_use,
        operation=operation,
        max_bytes=max_bytes,
        max_rows=max_rows,
    )
    run_id = bronze._identity(run_id)
    clock = bronze._clock(archived_at)
    root = Path(root)
    if (root / ".writer.lock").is_symlink():
        raise ValueError("FINRA collection writer lock is unsafe")
    with writer_lock(root):
        base = bronze._base(root)
        capture_id = str(uuid4())
        sequence = bronze._reserve(base, capture_id)
        saved = bronze._receipt(
            manifest,
            first["api_receipt"],
            [],
            capture_id=capture_id,
            run_id=run_id,
            sequence=sequence,
            clock=clock,
            retrieved=None,
            kind=kind,
            grant=approved_use,
            operation=operation,
            limits=dict(max_bytes=max_bytes, max_rows=max_rows),
            payload=payload,
            capture_schema=CONTRACT,
        )
        parse(
            root,
            saved,
            payload,
            manifest,
            approved_use=approved_use,
            operation=operation,
            max_bytes=max_bytes,
            max_rows=max_rows,
        )
        bronze._publish_directory(
            base / "captures", capture_id, {"receipt.json": encoded(saved), "payload.bin": payload}
        )
        verified, _ = bronze.read_capture(
            root,
            capture_id,
            manifest,
            approved_use=approved_use,
            operation=operation,
            max_bytes=max_bytes,
        )
        if saved != verified:
            raise ValueError("FINRA collection readback mismatch")
        return saved
