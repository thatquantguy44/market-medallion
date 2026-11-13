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


@pytest.mark.parametrize("damage", ["missing_profile", "boolean_slot"])
def test_ssv_ac11_malformed_but_self_hashed_metadata_refuses(tmp_path, manifest, damage):
    import sqlite3

    from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded

    source, target = tmp_path / "source", tmp_path / "target"
    saved = ingest(source, manifest)
    if damage == "missing_profile":
        with sqlite3.connect(source / "warehouse/ssv.sqlite") as db:
            body = json.loads(db.execute("SELECT body FROM captures").fetchone()[0])
            body.pop("profile")
            body.pop("result_sha256")
            body["result_sha256"] = digest(body)
            db.execute("UPDATE captures SET body=?", (encoded(body).decode(),))
    else:
        path = source / "bronze/sequences" / f"{saved['capture_sequence']:020d}.json"
        value = json.loads(path.read_bytes())
        value["capture_sequence"] = True
        path.write_bytes(encoded(value))
    with pytest.raises(ValueError):
        rebuild(source, target, manifest)
    assert not target.exists()


def test_ssv_ac11_install_response_failure_leaves_a_complete_verified_target(tmp_path, manifest):
    source, target = tmp_path / "source", tmp_path / "target"
    ingest(source, manifest)

    def fail(at):
        if at == "installed":
            raise OSError("invented response failure")

    with pytest.raises(OSError):
        rebuild(source, target, manifest, _fault=fail)
    assert logical(source, manifest) == logical(target, manifest)
    assert json.loads((target / "replay.json").read_bytes())["status"] == "SUCCEEDED"
    with pytest.raises(ValueError, match="empty destination"):
        rebuild(source, target, manifest)


def test_ssv_ac11_concurrent_destination_population_is_preserved(tmp_path, manifest):
    source, target = tmp_path / "source", tmp_path / "target"
    ingest(source, manifest)

    def competing_writer(at):
        if at == "before_install":
            target.mkdir()
            (target / "owned.txt").write_text("preserve concurrent content")

    with pytest.raises(ValueError, match="empty destination"):
        rebuild(source, target, manifest, _fault=competing_writer)
    assert (target / "owned.txt").read_text() == "preserve concurrent content"
    assert len(list(target.iterdir())) == 1 and not list(tmp_path.glob(".target.replay-*"))


def test_ssv_ac11_replay_retains_allocator_only_slots_and_extra_parse_profiles(tmp_path, manifest):
    from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import encoded
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    source, target = tmp_path / "source", tmp_path / "target"
    saved = ingest(source, manifest)
    bronze.parse_capture(source, saved["capture_id"], manifest, **OPTIONS, max_rows=500)
    slot = dict(schema_id=bronze.CONTRACT, capture_sequence=2, capture_id=str(uuid4()))
    (source / "bronze/sequences/00000000000000000002.json").write_bytes(encoded(slot))
    before = inventory(source)
    rebuild(source, target, manifest)
    assert inventory(source) == before
    assert (target / "bronze/sequences/00000000000000000002.json").read_bytes() == encoded(slot)
    assert len(list((target / "bronze/outcomes").glob("*/*.json"))) == 2
    assert ingest(target, manifest, clock=NOW + timedelta(seconds=1))["capture_sequence"] == 3


@pytest.mark.parametrize(
    "fixture,total",
    [("invented-fractional.json", 1), ("invented-valid.json", 2), ("4096digits", 2)],
)
def test_ssv_ac11_replay_preserves_wide_and_fractional_quantities(
    tmp_path, manifest, fixture, total
):
    from test_ssv_parser import FIXTURES

    source, target = tmp_path / "source", tmp_path / "target"
    raw = (FIXTURES / ("invented-valid.json" if fixture == "4096digits" else fixture)).read_bytes()
    if fixture == "4096digits":
        raw = raw.replace(b'"shortParQuantity": 40', b'"shortParQuantity": ' + b"9" * 4096)
        raw = raw.replace(b'"totalParQuantity": 100', b'"totalParQuantity": ' + b"9" * 4096)
    ingest(source, manifest, payload=raw, total=total)
    rebuild(source, target, manifest)
    assert gold(source, manifest) == gold(target, manifest)
    assert logical(source, manifest) == logical(target, manifest)


def test_ssv_ac11_research_and_new_publication_match_across_dates_and_facilities(
    tmp_path, manifest
):
    from test_ssv_parser import FIXTURES, receipt

    from market_data_pipeline.pipelines.finra_short_sale_volume import (
        pipeline,
        publication,
        research,
    )

    source, target = tmp_path / "source", tmp_path / "target"
    raw = (FIXTURES / "invented-valid.json").read_bytes()
    for index, (day, facility) in enumerate(
        [
            ("2026-11-06", "nyse_trf"),
            ("2026-11-06", "nasdaq_carteret_trf"),
            ("2026-11-09", "nyse_trf"),
            ("2026-11-09", "nasdaq_carteret_trf"),
        ]
    ):
        selected = next(f for f in manifest.facilities if f.id == facility)
        payload = (
            raw.replace(b"2026-11-09", day.encode())
            .replace(b'"NYTRF"', json.dumps(selected.reporting_facility_code).encode())
            .replace(b'"N"', json.dumps(selected.market_code).encode())
        )
        pipeline.run(
            source,
            payload,
            manifest,
            receipt(payload, day=day, market=selected.market_code),
            **OPTIONS,
            clock=lambda index=index: NOW + timedelta(seconds=index),
        )
    rebuild(source, target, manifest)
    request = dict(trade_date="2026-11-09", scopes=["nyse_trf", "nasdaq_carteret_trf"])
    assert research.research_gold(source, manifest, **OPTIONS, **request) == research.research_gold(
        target, manifest, **OPTIONS, **request
    )
    first = publication.publish(source, manifest, **OPTIONS, **request)
    second = publication.publish(target, manifest, **OPTIONS, **request)
    assert first["generation_id"] != second["generation_id"]
    assert (
        publication.read(source, manifest, **OPTIONS)["tables"]
        == publication.read(target, manifest, **OPTIONS)["tables"]
    )


@pytest.mark.parametrize(
    "limit",
    [
        {"max_captures": 0},
        {"max_captures": True},
        {"max_captures": 10001},
        {"max_total_bytes": 0},
        {"max_state_rows": True},
    ],
)
def test_ssv_ac11_invalid_limits_refuse_before_root_io(tmp_path, manifest, limit):
    with pytest.raises(ValueError):
        rebuild(tmp_path / "absent", tmp_path / "target", manifest, **limit)
    assert list(tmp_path.iterdir()) == []


def test_ssv_ac11_populated_replay_and_fixture_kind_relabelling_refuse(tmp_path, manifest):
    from test_ssv_bronze import provider_grant

    source, target = tmp_path / "source", tmp_path / "target"
    ingest(source, manifest)
    with pytest.raises(PermissionError):
        replay.replay(
            source, target, manifest, approved_use=provider_grant(), operation="transform"
        )
    assert not target.exists()
    rebuild(source, target, manifest)
    before = inventory(target)
    with pytest.raises(ValueError, match="empty destination"):
        rebuild(source, target, manifest)
    assert inventory(target) == before


def test_ssv_ac11_capture_limit_preflight_precedes_state_materialization(
    tmp_path, manifest, monkeypatch
):
    source, target = tmp_path / "source", tmp_path / "target"
    ingest(source, manifest)
    ingest(source, manifest, clock=NOW + timedelta(seconds=1))
    monkeypatch.setattr(replay, "_state", lambda *a: pytest.fail("oversized ledger materialized"))
    with pytest.raises(ValueError, match="budget"):
        rebuild(source, target, manifest, max_captures=1)
    assert not target.exists()
