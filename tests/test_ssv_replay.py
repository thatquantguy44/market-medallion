"""SSV-AC11/C06: replay must preserve original knowledge and full logical history."""

import json
import socket
from datetime import timedelta
from uuid import uuid4

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_parser import manifest as manifest
from test_ssv_pipeline import audits, gold, ingest, state

from market_data_pipeline.pipelines.finra_short_sale_volume import replay
from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse
from market_data_pipeline.pipelines.finra_short_sale_volume.pipeline import ingest_capture

__all__ = ["manifest"]
OPTIONS = dict(approved_use=fixture_grant(), operation="fixture")


def rebuild(source, destination, manifest, **kwargs):
    return replay.replay(source, destination, manifest, **OPTIONS, **kwargs)


def inventory(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def logical(root, manifest):
    with warehouse.connect(root, manifest, **OPTIONS) as db:
        return {
            name: [
                tuple(row)
                for row in db.execute(
                    f"SELECT * FROM {name} ORDER BY "
                    + ("trade_date,facility_id,symbol_raw" if name == "gold" else "rowid")
                )
            ]
            for name in ("attempts", "captures", "versions", "capture_rows", "partitions", "gold")
        }


def test_ssv_ac11_replay_recomputes_exact_history_audit_and_cutoffs(
    tmp_path, manifest, monkeypatch
):
    source, target = tmp_path / "source", tmp_path / "target"
    original = ingest(source, manifest)
    ingest(source, manifest, clock=NOW)  # unchanged, equal time; sequence must disambiguate
    payload = next((source / "bronze/captures").glob("*/payload.bin")).read_bytes()
    changed = ingest(
        source,
        manifest,
        payload=payload.replace(b'"shortParQuantity": 40', b'"shortParQuantity": 50'),
        clock=NOW + timedelta(seconds=1),
    )
    assert changed["new_versions"] == 1
    ingest(source, manifest, clock=NOW + timedelta(seconds=2))  # reversion
    ingest(source, manifest, payload=b"[]", total=0, clock=NOW + timedelta(seconds=3))
    ingest(source, manifest, total=20, clock=NOW + timedelta(seconds=4))  # partial
    ingest(source, manifest, payload=b"[{", clock=NOW + timedelta(seconds=5))  # rejected
    before = inventory(source)
    monkeypatch.setattr(
        socket, "create_connection", lambda *a, **k: pytest.fail("network attempted")
    )
    result = rebuild(source, target, manifest)
    assert result["status"] == "SUCCEEDED" and result["replayed"] == 7
    assert result["source_warehouse_id"] != result["warehouse_id"]
    assert logical(target, manifest) == logical(source, manifest)
    assert audits(target, manifest) == audits(source, manifest)
    assert state(target, manifest) == state(source, manifest)
    for cutoff, seq in [
        (NOW, original["capture_sequence"]),
        (NOW, 2),
        (NOW + timedelta(seconds=1), changed["capture_sequence"]),
        (NOW + timedelta(seconds=3), 5),
    ]:
        assert gold(target, manifest, as_of=cutoff, as_of_sequence=seq) == gold(
            source, manifest, as_of=cutoff, as_of_sequence=seq
        )
    assert inventory(source) == before
    saved = json.loads((target / "replay.json").read_bytes())
    assert saved == result and str(source) not in json.dumps(saved)
    assert not (target / "parquet").exists()
    # Original slots remain usable by the normal ingestion allocator after replay.
    later = ingest(target, manifest, clock=NOW + timedelta(seconds=6))
    assert later["capture_sequence"] == 8 and later["new_versions"] == 2


def test_ssv_ac11_replay_retains_failed_attempt_before_terminal_retry(tmp_path, manifest):
    source, target = tmp_path / "source", tmp_path / "target"
    capture_id = str(uuid4())

    def fail(at):
        if at == "after_versions":
            raise OSError

    with pytest.raises(RuntimeError):
        ingest(source, manifest, capture_id=capture_id, fault=fail)
    ingest_capture(source, capture_id, manifest, **OPTIONS)
    rebuild(source, target, manifest)
    assert logical(target, manifest) == logical(source, manifest)
    assert [row["status"] for row in audits(target, manifest)] == ["APPLIED", "FAILED"]


@pytest.mark.parametrize("phase", ["copied", "ingested", "verified", "sealed", "before_install"])
def test_ssv_ac11_failure_removes_only_private_stage(tmp_path, manifest, phase):
    source, target = tmp_path / "source", tmp_path / "target"
    ingest(source, manifest)
    target.mkdir()
    before = inventory(source)

    def fail(at):
        if at == phase:
            raise OSError("invented private failure")

    with pytest.raises(OSError):
        rebuild(source, target, manifest, _fault=fail)
    assert list(target.iterdir()) == [] and inventory(source) == before
    assert not list(tmp_path.glob(".target.replay-*"))


@pytest.mark.parametrize(
    "damage",
    [
        "payload",
        "receipt",
        "outcome",
        "missing_outcome",
        "missing_capture",
        "orphan",
        "pending",
        "failed",
    ],
)
def test_ssv_ac11_source_damage_or_unresolved_capture_refuses_before_target(
    tmp_path, manifest, damage
):
    source, target = tmp_path / "source", tmp_path / "target"
    saved = ingest(source, manifest)
    capture = source / "bronze/captures" / saved["capture_id"]
    if damage == "payload":
        (capture / "payload.bin").write_bytes(b"changed")
    elif damage == "receipt":
        (capture / "receipt.json").write_text("{}")
    elif damage in {"outcome", "missing_outcome"}:
        path = next((source / "bronze/outcomes" / saved["capture_id"]).glob("*.json"))
        path.unlink() if damage == "missing_outcome" else path.write_text("{}")
    elif damage == "missing_capture":
        (capture / "payload.bin").unlink()
    elif damage == "orphan":
        (source / "bronze/captures" / str(uuid4())).mkdir()
    else:
        import sqlite3

        with sqlite3.connect(source / "warehouse/ssv.sqlite") as db:
            db.execute(
                "UPDATE attempts SET status=?", ("STARTED" if damage == "pending" else "FAILED",)
            )
    before = inventory(source)
    with pytest.raises((ValueError, RuntimeError)):
        rebuild(source, target, manifest)
    assert not target.exists() and inventory(source) == before


def test_ssv_ac11_admission_and_destination_guards(tmp_path, manifest):
    source, target = tmp_path / "source", tmp_path / "target"
    with pytest.raises(PermissionError):
        replay.replay(source, target, manifest, approved_use=fixture_grant(), operation="transform")
    assert not source.exists() and not target.exists()
    ingest(source, manifest)
    target.mkdir()
    (target / "owned.txt").write_text("preserve")
    for destination in (source, source / "nested", tmp_path, target):
        with pytest.raises(ValueError):
            rebuild(source, destination, manifest)
    assert (target / "owned.txt").read_text() == "preserve"
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError):
        rebuild(source, link, manifest)


@pytest.mark.parametrize(
    "limit", [{"max_captures": 1}, {"max_total_bytes": 10}, {"max_state_rows": 1}]
)
def test_ssv_ac11_preflight_budgets_leave_no_destination(tmp_path, manifest, limit):
    source, target = tmp_path / "source", tmp_path / "target"
    ingest(source, manifest)
    ingest(source, manifest, clock=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="budget"):
        rebuild(source, target, manifest, **limit)
    assert not target.exists()
