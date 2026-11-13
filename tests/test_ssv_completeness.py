"""SSV-AC08 / C06: complete replacements, explicit absence and failure recovery."""

import json
import shutil
import sqlite3
from datetime import timedelta
from uuid import uuid4

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_parser import FIXTURES, receipt
from test_ssv_parser import manifest as manifest
from test_ssv_pipeline import audits, gold, ingest, state
from test_ssv_versions import trail

from market_data_pipeline.pipelines.finra_short_sale_volume.pipeline import (
    ingest_capture,
    read_gold,
    run,
)

__all__ = ["manifest"]


def one_row():
    return json.dumps(json.loads((FIXTURES / "invented-valid.json").read_bytes())[:1]).encode()


def test_ssv_ac08_complete_replacement_removes_only_absent_rows(tmp_path, manifest):
    first = ingest(tmp_path, manifest)
    result = ingest(
        tmp_path, manifest, payload=one_row(), total=1, clock=NOW + timedelta(seconds=1)
    )
    assert result["removed_rows"] == 1 and result["new_versions"] == 1
    assert [r["symbol_raw"] for r in gold(tmp_path, manifest)["rows"]] == ["000SYN.A"]
    versions = trail(tmp_path, manifest, "tSt/P-A")
    assert len(versions) == 2 and versions[-1]["removed"] is True
    assert all(
        versions[-1][k] is None
        for k in (
            "short_volume",
            "short_exempt_volume",
            "total_volume",
            "short_share",
            "short_exempt_share",
        )
    )
    assert len(gold(tmp_path, manifest, as_of=NOW)["rows"]) == 2
    assert state(tmp_path, manifest, symbol="tSt/P-A")["symbol_status"] == "NO_REPORTED_ROW"
    assert gold(tmp_path, manifest)["rows"][0]["capture_id"] == first["capture_id"]


def test_ssv_ac08_empty_repeat_and_return_preserve_membership_at_each_cutoff(tmp_path, manifest):
    ingest(tmp_path, manifest)
    empty = ingest(tmp_path, manifest, payload=b"[]", total=0, clock=NOW + timedelta(seconds=1))
    repeat = ingest(
        tmp_path, manifest, payload=b"", total=0, status=204, clock=NOW + timedelta(seconds=2)
    )
    assert empty["removed_rows"] == 2 and repeat["new_versions"] == 0
    assert state(tmp_path, manifest)["latest_attempt_status"] == "VALID_EMPTY"
    assert gold(tmp_path, manifest)["rows"] == []
    returned = ingest(tmp_path, manifest, clock=NOW + timedelta(seconds=3))
    assert returned["new_versions"] == 2 and returned["removed_rows"] == 0
    versions = trail(tmp_path, manifest)
    assert [v["removed"] for v in versions] == [False, True, False]
    assert len({v["version_id"] for v in versions}) == 3
    assert [
        len(gold(tmp_path, manifest, as_of=NOW + timedelta(seconds=n))["rows"]) for n in range(4)
    ] == [2, 0, 0, 2]


def test_ssv_ac08_partial_page_cannot_change_values_or_remove_rows(tmp_path, manifest):
    first = ingest(tmp_path, manifest)
    old = gold(tmp_path, manifest)
    payload = one_row().replace(b'"shortParQuantity": 40', b'"shortParQuantity": 50')
    result = ingest(tmp_path, manifest, payload=payload, total=20)
    assert (
        result["status"] == "PARTIAL"
        and result["accepted_rows"] == result["new_versions"] == result["removed_rows"] == 0
    )
    assert gold(tmp_path, manifest) == old
    assert state(tmp_path, manifest)["latest_attempt_status"] == "PARTIAL"
    assert state(tmp_path, manifest)["committed_capture_id"] == first["capture_id"]
    assert len(trail(tmp_path, manifest)) == 1


@pytest.mark.parametrize(
    "payload,status", [(b"[", 200), (b"<html>challenge</html>", 200), (b"", 200), (b"[]", 503)]
)
def test_ssv_ac08_rejected_refresh_never_establishes_absence(tmp_path, manifest, payload, status):
    ingest(tmp_path, manifest)
    old = gold(tmp_path, manifest)
    result = ingest(tmp_path, manifest, payload=payload, status=status)
    assert result["status"] == "REJECTED" and gold(tmp_path, manifest) == old
    assert state(tmp_path, manifest, symbol="tSt/P-A")["symbol_status"] == "REPORTED"
    assert state(tmp_path, manifest)["latest_attempt_status"] == "FAILED"


def test_ssv_ac08_other_partition_and_facility_are_independent(tmp_path, manifest):
    ingest(tmp_path, manifest)
    payload = (
        (FIXTURES / "invented-valid.json")
        .read_bytes()
        .replace(b'"marketCode": "N"', b'"marketCode": "Q"')
        .replace(b'"NYTRF"', b'"NQTRF"')
    )
    other = run(
        tmp_path,
        payload,
        manifest,
        receipt(payload, market="Q"),
        approved_use=fixture_grant(),
        operation="fixture",
        clock=lambda: NOW,
    )
    before = read_gold(
        tmp_path,
        manifest,
        approved_use=fixture_grant(),
        operation="fixture",
        trade_date="2026-11-09",
        facility_id="nasdaq_carteret_trf",
    )
    ingest(tmp_path, manifest, payload=b"[]", total=0)
    after = read_gold(
        tmp_path,
        manifest,
        approved_use=fixture_grant(),
        operation="fixture",
        trade_date="2026-11-09",
        facility_id="nasdaq_carteret_trf",
    )
    assert (
        before == after and after["capture_id"] == other["capture_id"] and len(after["rows"]) == 2
    )


def test_ssv_ac08_missing_pending_and_unknown_are_local_evidence_states(tmp_path, manifest):
    ingest(
        tmp_path,
        manifest,
        day="2026-11-10",
        payload=(FIXTURES / "invented-valid.json")
        .read_bytes()
        .replace(b"2026-11-09", b"2026-11-10"),
    )
    assert state(tmp_path, manifest, symbol="000SYN.A")["latest_attempt_status"] == "MISSING"
    assert state(tmp_path, manifest, symbol="000SYN.A")["symbol_status"] == "UNKNOWN"
    capture_id = str(uuid4())

    def interrupt(stage):
        if stage == "before_commit":
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        ingest(tmp_path, manifest, fault=interrupt, capture_id=capture_id)
    assert state(tmp_path, manifest, symbol="000SYN.A")["latest_attempt_status"] == "PENDING"
    assert state(tmp_path, manifest, symbol="000SYN.A")["symbol_status"] == "UNKNOWN"
    with pytest.raises(RuntimeError, match="pending"):
        ingest(tmp_path, manifest)
    result = ingest_capture(
        tmp_path, capture_id, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert (
        result["status"] == "APPLIED"
        and state(tmp_path, manifest)["latest_attempt_status"] == "COMPLETE"
    )
    assert [a["status"] for a in audits(tmp_path, manifest)][:2] == ["APPLIED", "INTERRUPTED"]


@pytest.mark.parametrize(
    "stage", ["after_versions", "after_membership", "before_commit", "after_commit"]
)
def test_ssv_ac08_interrupted_empty_publication_is_coherent_and_retryable(
    tmp_path, manifest, stage
):
    ingest(tmp_path, manifest)
    original = gold(tmp_path, manifest)
    capture_id = str(uuid4())

    def fail(point):
        if point == stage:
            raise OSError("invented-private-diagnostic")

    with pytest.raises(RuntimeError):
        ingest(tmp_path, manifest, payload=b"[]", total=0, fault=fail, capture_id=capture_id)
    if stage == "after_commit":
        assert (
            gold(tmp_path, manifest)["rows"] == []
            and audits(tmp_path, manifest)[0]["status"] == "APPLIED"
        )
    else:
        assert gold(tmp_path, manifest) == original and len(trail(tmp_path, manifest)) == 1
    result = ingest_capture(
        tmp_path, capture_id, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert result["removed_rows"] == 2 and gold(tmp_path, manifest)["rows"] == []
    assert len(trail(tmp_path, manifest)) == 2


def test_ssv_ac08_unchanged_capture_at_version_budget_does_not_require_extra_versions(
    tmp_path, manifest, monkeypatch
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import warehouse

    ingest(tmp_path, manifest)
    monkeypatch.setattr(warehouse, "MAX_VERSIONS", 2)
    result = ingest(tmp_path, manifest)
    assert result["new_versions"] == 0
    before = gold(tmp_path, manifest)
    with pytest.raises(RuntimeError):
        ingest(tmp_path, manifest, payload=b"[]", total=0)
    assert gold(tmp_path, manifest) == before


def test_ssv_ac08_missing_bound_store_directory_is_not_recreated(tmp_path, manifest):
    result = ingest(tmp_path, manifest)
    shutil.rmtree(tmp_path / "warehouse")
    with pytest.raises(ValueError):
        ingest_capture(
            tmp_path,
            result["capture_id"],
            manifest,
            approved_use=fixture_grant(),
            operation="fixture",
        )
    assert not (tmp_path / "warehouse").exists()


def test_ssv_ac08_copy_to_a_different_root_does_not_inherit_warehouse_identity(tmp_path, manifest):
    root = tmp_path / "first"
    ingest(root, manifest)
    copied = tmp_path / "copied"
    shutil.copytree(root, copied)
    with pytest.raises(ValueError, match="identity"):
        gold(copied, manifest)


@pytest.mark.parametrize("damage", ["terminal", "schema", "version"])
def test_ssv_ac08_sql_evidence_corruption_refuses_read_or_retry(tmp_path, manifest, damage):
    result = ingest(tmp_path, manifest)
    with sqlite3.connect(tmp_path / "warehouse/ssv.sqlite") as db:
        if damage == "schema":
            db.execute("CREATE TABLE foreign_table(value TEXT)")
        elif damage == "terminal":
            row = db.execute("SELECT body FROM captures").fetchone()
            body = json.loads(row[0])
            body["new_versions"] = 900
            db.execute(
                "UPDATE captures SET body=?",
                (json.dumps(body, sort_keys=True, separators=(",", ":")),),
            )
        else:
            row = db.execute("SELECT version_id,body FROM versions LIMIT 1").fetchone()
            body = json.loads(row[1])
            body["short_volume"] = "41"
            db.execute(
                "UPDATE versions SET body=? WHERE version_id=?",
                (json.dumps(body, sort_keys=True, separators=(",", ":")), row[0]),
            )
    with pytest.raises(ValueError):
        if damage == "terminal":
            ingest_capture(
                tmp_path,
                result["capture_id"],
                manifest,
                approved_use=fixture_grant(),
                operation="fixture",
            )
        else:
            gold(tmp_path, manifest)


@pytest.mark.parametrize("damage", ["membership", "partition", "journal_symlink"])
def test_ssv_ac08_membership_scope_and_sqlite_sidecars_are_verified(tmp_path, manifest, damage):
    ingest(tmp_path, manifest)
    db_path = tmp_path / "warehouse/ssv.sqlite"
    if damage == "journal_symlink":
        outside = tmp_path / "outside"
        outside.write_bytes(b"keep this file")
        db_path.with_name(db_path.name + "-journal").symlink_to(outside)
    else:
        with sqlite3.connect(db_path) as db:
            if damage == "membership":
                db.execute("UPDATE capture_rows SET source_row=500")
            else:
                db.execute("UPDATE partitions SET facility_id='nasdaq_carteret_trf'")
    with pytest.raises(ValueError):
        if damage == "partition":
            read_gold(
                tmp_path,
                manifest,
                approved_use=fixture_grant(),
                operation="fixture",
                trade_date="2026-11-09",
                facility_id="nasdaq_carteret_trf",
            )
        else:
            gold(tmp_path, manifest)
