"""SSV-AC12/C06: complete collections preserve original pages and replay exactly."""

import json

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_parser import FIXTURES, receipt
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume import pipeline
from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze, page_collection

__all__ = ["manifest"]
OPTIONS = dict(approved_use=fixture_grant(), operation="fixture")
SORT = ["securitiesInformationProcessorSymbolIdentifier"]


def page(root, manifest, offset, *, changed=False, evidence=True):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    if offset:
        rows = json.loads(payload)
        rows = rows[:1]
        rows[0]["securitiesInformationProcessorSymbolIdentifier"] = "ZZSYN"
        if changed:
            rows[0]["shortParQuantity"] = 50
        payload = json.dumps(rows).encode()
    wire = receipt(payload, total=3, offset=offset)
    wire["request_payload"]["sortFields"] = SORT
    saved = bronze.archive_input(
        root,
        payload,
        manifest,
        wire,
        **OPTIONS,
        run_id="00000000-0000-0000-0000-000000000001",
        archived_at=NOW,
        evidence_only=evidence,
    )
    result = pipeline.ingest_capture(
        root, saved["capture_id"], manifest, **OPTIONS, clock=lambda: NOW
    )
    assert result["status"] == "PARTIAL"
    return saved["capture_id"]


def test_ssv_ac12_collection_is_derived_and_recomputes_complete_membership(tmp_path, manifest):
    passes = [[page(tmp_path, manifest, 0), page(tmp_path, manifest, 2)] for _ in range(2)]
    saved = page_collection.archive(
        tmp_path,
        manifest,
        passes,
        **OPTIONS,
        run_id="00000000-0000-0000-0000-000000000001",
        archived_at=NOW,
        max_pages=3,
        page_bytes=20 * 1024**2,
        page_rows=5000,
    )
    assert saved["schema_id"] == page_collection.CONTRACT
    result = pipeline.ingest_capture(
        tmp_path,
        saved["capture_id"],
        manifest,
        **OPTIONS,
        max_bytes=saved["capture_limits"]["max_bytes"],
        max_rows=saved["capture_limits"]["max_rows"],
        clock=lambda: NOW,
    )
    assert result["status"] == "APPLIED" and result["accepted_rows"] == 3
    gold = pipeline.read_gold(
        tmp_path, manifest, **OPTIONS, trade_date="2026-11-09", facility_id="nyse_trf"
    )
    assert [r["source_row"] for r in gold["rows"]] == [1, 3, 2]
    assert len(list((tmp_path / "bronze/captures").iterdir())) == 5


def test_ssv_ac12_disagreeing_passes_never_promote_collection(tmp_path, manifest):
    passes = [
        [page(tmp_path, manifest, 0), page(tmp_path, manifest, 2)],
        [page(tmp_path, manifest, 0), page(tmp_path, manifest, 2, changed=True)],
    ]
    with pytest.raises(ValueError, match="collection"):
        page_collection.archive(
            tmp_path,
            manifest,
            passes,
            **OPTIONS,
            run_id="00000000-0000-0000-0000-000000000001",
            archived_at=NOW,
            max_pages=3,
            page_bytes=20 * 1024**2,
            page_rows=5000,
        )
    assert len(list((tmp_path / "bronze/captures").iterdir())) == 4
    assert (
        pipeline.read_gold(
            tmp_path, manifest, **OPTIONS, trade_date="2026-11-09", facility_id="nyse_trf"
        )["rows"]
        == []
    )


@pytest.mark.parametrize("damage", ["repeat", "missing", "scope", "nested", "payload"])
def test_ssv_ac12_collection_refuses_incomplete_or_ambiguous_provenance(tmp_path, manifest, damage):
    passes = [[page(tmp_path, manifest, 0), page(tmp_path, manifest, 2)] for _ in range(2)]
    if damage == "repeat":
        passes[1] = passes[0]
    elif damage == "missing":
        passes[0] = passes[0][:1]
    else:
        target = tmp_path / "bronze/captures" / passes[1][-1]
        if damage == "payload":
            (target / "payload.bin").write_bytes(b"[]")
        else:
            from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import (
                digest,
                encoded,
            )

            value = json.loads((target / "receipt.json").read_bytes())
            if damage == "nested":
                value["schema_id"] = page_collection.CONTRACT
            else:
                value["api_receipt"]["request_payload"]["compareFilters"][0]["fieldValue"] = (
                    "2026-11-10"
                )
            value.pop("receipt_sha256")
            value["receipt_sha256"] = digest(value)
            (target / "receipt.json").write_bytes(encoded(value))
    with pytest.raises(ValueError):
        page_collection.archive(
            tmp_path,
            manifest,
            passes,
            **OPTIONS,
            run_id="00000000-0000-0000-0000-000000000001",
            archived_at=NOW,
            max_pages=3,
            page_bytes=20 * 1024**2,
            page_rows=5000,
        )
    assert len(list((tmp_path / "bronze/captures").iterdir())) == 4


def test_ssv_ac12_evidence_only_complete_page_never_promotes_membership(tmp_path, manifest):
    raw = (FIXTURES / "invented-valid.json").read_bytes()
    saved = bronze.archive_input(
        tmp_path,
        raw,
        manifest,
        receipt(raw),
        **OPTIONS,
        run_id="00000000-0000-0000-0000-000000000001",
        archived_at=NOW,
        evidence_only=True,
    )
    result = pipeline.ingest_capture(
        tmp_path, saved["capture_id"], manifest, **OPTIONS, clock=lambda: NOW
    )
    assert result["status"] == "PARTIAL" and result["accepted_rows"] == 0
    assert (
        pipeline.read_gold(
            tmp_path, manifest, **OPTIONS, trade_date="2026-11-09", facility_id="nyse_trf"
        )["capture_id"]
        is None
    )
