"""SSV-AC06: invented raw-to-SQLite partitions, exact readback and transaction failures."""

import json
import socket
from datetime import timedelta
from uuid import uuid4

import pytest
from test_ssv_bronze import NOW, fixture_grant, provider_grant
from test_ssv_parser import FIXTURES, receipt
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume.pipeline import (
    audit,
    coverage,
    ingest_capture,
    read_gold,
    run,
)

__all__ = ["manifest"]


def ingest(
    root,
    manifest,
    *,
    payload=None,
    total=2,
    status=200,
    offset=0,
    day="2026-11-09",
    clock=NOW,
    fault=None,
    capture_id=None,
):
    payload = payload if payload is not None else (FIXTURES / "invented-valid.json").read_bytes()
    return run(
        root,
        payload,
        manifest,
        receipt(payload, total=total, status=status, offset=offset, day=day),
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=str(uuid4()),
        capture_id=capture_id,
        clock=lambda: clock,
        fault=fault,
    )


def gold(root, manifest, **kwargs):
    return read_gold(
        root,
        manifest,
        approved_use=fixture_grant(),
        operation="fixture",
        trade_date="2026-11-09",
        facility_id="nyse_trf",
        **kwargs,
    )


def state(root, manifest, **kwargs):
    return coverage(
        root,
        manifest,
        approved_use=fixture_grant(),
        operation="fixture",
        trade_date="2026-11-09",
        facility_id="nyse_trf",
        **kwargs,
    )


def audits(root, manifest):
    return audit(root, manifest, approved_use=fixture_grant(), operation="fixture")


def test_ssv_ac06_exact_bronze_silver_gold_reopen_and_audit(tmp_path, manifest, monkeypatch):
    monkeypatch.setattr(
        socket, "create_connection", lambda *a, **k: pytest.fail("network attempted")
    )
    result = ingest(tmp_path, manifest)
    snapshot = gold(tmp_path, manifest)
    assert result["status"] == "APPLIED" and result["new_versions"] == 2
    assert snapshot["capture_id"] == result["capture_id"] and len(snapshot["rows"]) == 2
    row = snapshot["rows"][0]
    assert (
        row["symbol_raw"],
        row["short_volume"],
        row["short_exempt_volume"],
        row["total_volume"],
    ) == ("000SYN.A", "40", "5", "100")
    assert row["short_share"] == "0.4" and row["short_exempt_share"] == "0.05"
    assert snapshot["rows"][1]["short_volume"] == str(2**80 + 1)
    assert row["first_seen_at"] == "2026-11-13T15:00:00.000000Z"
    assert row["provider_published_at"] is None and row["retrieved_at"] is None
    assert (
        snapshot["knowledge_basis"] == "CURRENT_VINTAGE"
        and snapshot["clock_basis"] == "LOCAL_CAPTURE"
    )
    assert audits(tmp_path, manifest)[0]["status"] == "APPLIED"
    assert state(tmp_path, manifest)["latest_attempt_status"] == "COMPLETE"
    saved = json.loads(next((tmp_path / "bronze" / "captures").glob("*/receipt.json")).read_text())
    assert (
        ingest_capture(
            tmp_path,
            result["capture_id"],
            manifest,
            approved_use=fixture_grant(),
            operation="fixture",
        )
        == result
    )
    assert gold(tmp_path, manifest) == snapshot and len(audits(tmp_path, manifest)) == 1
    assert saved["capture_id"] == row["capture_id"]


@pytest.mark.parametrize(
    "name,total,status",
    [("invented-fractional.json", 1, 200), ("invented-empty.json", 0, 200), (None, 0, 204)],
)
def test_ssv_ac06_fractional_and_empty_partitions(tmp_path, manifest, name, total, status):
    payload = (FIXTURES / name).read_bytes() if name else b""
    result = ingest(tmp_path, manifest, payload=payload, total=total, status=status)
    snapshot = gold(tmp_path, manifest)
    assert result["status"] == "APPLIED" and len(snapshot["rows"]) == total
    if total:
        assert snapshot["rows"][0]["short_volume"] == "12.345678"
    else:
        assert state(tmp_path, manifest)["latest_attempt_status"] == "VALID_EMPTY"


@pytest.mark.parametrize("payload,total,expected", [(b"[{", 2, "REJECTED"), (None, 20, "PARTIAL")])
def test_ssv_ac06_invalid_or_partial_never_publishes(tmp_path, manifest, payload, total, expected):
    result = ingest(tmp_path, manifest, payload=payload, total=total)
    assert result["status"] == expected and result["accepted_rows"] == 0
    assert gold(tmp_path, manifest)["capture_id"] is None
    assert gold(tmp_path, manifest)["rows"] == []
    assert (tmp_path / "bronze" / "captures" / result["capture_id"] / "payload.bin").exists()
    assert audits(tmp_path, manifest)[0]["status"] == expected


@pytest.mark.parametrize(
    "stage", ["after_start", "after_versions", "after_membership", "before_commit"]
)
def test_ssv_ac06_failed_writes_preserve_committed_snapshot_and_retry(tmp_path, manifest, stage):
    first = ingest(tmp_path, manifest)
    old = gold(tmp_path, manifest)
    capture_id = str(uuid4())

    def fail(point):
        if point == stage:
            raise OSError("invented private diagnostic")

    payload = (FIXTURES / "invented-valid.json").read_bytes().replace(b"2026-11-09", b"2026-11-10")
    with pytest.raises(RuntimeError, match="original capture retained") as error:
        ingest(
            tmp_path,
            manifest,
            payload=payload,
            day="2026-11-10",
            clock=NOW + timedelta(seconds=1),
            fault=fail,
            capture_id=capture_id,
        )
    assert "private" not in str(error.value)
    assert gold(tmp_path, manifest) == old and audits(tmp_path, manifest)[0]["status"] == "FAILED"
    result = ingest_capture(
        tmp_path, capture_id, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert result["status"] == "APPLIED" and result["capture_id"] != first["capture_id"]
    assert len(audits(tmp_path, manifest)) == 3


def test_ssv_ac06_admission_precedes_any_root_io(tmp_path, manifest):
    root = tmp_path / "refused"
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    with pytest.raises(PermissionError):
        run(
            root,
            payload,
            manifest,
            receipt(payload),
            approved_use=fixture_grant(),
            operation="local_import",
        )
    assert not root.exists()
    grant = provider_grant()
    grant["operations"] = ["local_import"]
    with pytest.raises(PermissionError):
        run(root, payload, manifest, receipt(payload), approved_use=grant, operation="local_import")
    assert not root.exists()


@pytest.mark.parametrize("damage", ["missing_db", "identity", "symlink", "corrupt_db"])
def test_ssv_ac06_bound_store_damage_refuses_recreation(tmp_path, manifest, damage):
    ingest(tmp_path, manifest)
    directory = tmp_path / "warehouse"
    db = directory / "ssv.sqlite"
    if damage == "missing_db":
        db.unlink()
    elif damage == "identity":
        (directory / "identity.json").write_text("{}")
    elif damage == "symlink":
        saved = tmp_path / "outside.sqlite"
        db.rename(saved)
        db.symlink_to(saved)
    else:
        db.write_bytes(b"broken")
    before = {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    with pytest.raises(ValueError):
        gold(tmp_path, manifest)
    assert {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()} == before


def test_ssv_ac06_current_provider_profile_cannot_read_fixture_store(tmp_path, manifest):
    ingest(tmp_path, manifest)
    with pytest.raises(PermissionError):
        read_gold(
            tmp_path,
            manifest,
            approved_use=provider_grant(),
            operation="transform",
            trade_date="2026-11-09",
            facility_id="nyse_trf",
        )


def test_ssv_ac06_read_missing_root_has_no_side_effect(tmp_path, manifest):
    root = tmp_path / "absent"
    with pytest.raises(ValueError):
        gold(root, manifest)
    assert not root.exists()
