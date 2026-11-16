"""OTC-AC06: atomic six-population local fixture panel and failure evidence."""

import json
import sqlite3
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest
from test_otc_bronze import MANIFEST, USE

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import PROFILES, Partition
from market_data_pipeline.pipelines.finra_otc_transparency.pipeline import (
    inspect_panel,
    publish_capture,
    run_fixture,
)

FIXTURES = Path(__file__).parent / "fixtures/finra_otc_transparency"


def payload(profile):
    name = (
        "ats_t1_weekly.txt" if profile.product == "ATS_WEEKLY_DETAIL" else "non_ats_t1_invented.txt"
    )
    return (FIXTURES / name).read_bytes().replace(b"\nT1|", b"\n" + profile.tier.encode() + b"|")


def partition(profile):
    return Partition(date(2026, 10, 19), profile.product, profile.tier)


def run(root, profile=PROFILES[0], **kw):
    values = dict(
        payload=payload(profile),
        partition=partition(profile),
        manifest=MANIFEST,
        approved_use=USE,
        expected_rows=3,
    )
    values.update(kw)
    return run_fixture(root, **values)


def inspect(root, profiles=PROFILES):
    return inspect_panel(root, partitions=[partition(p) for p in profiles], approved_use=USE)


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_complete_fixture_readback_preserves_scope_exact_counts_and_coverage(tmp_path, profile):
    root = tmp_path / "panel"
    result = run(root, profile)
    assert result["status"] == "PUBLISHED"
    panel = inspect(root, [profile])
    assert panel["status"] == "COMPLETE_FIXTURE"
    (item,) = panel["partitions"]
    assert item["publication"]["coverage"]["status"] == "COMPLETE_FIXTURE"
    assert item["latest_attempt"]["capture_id"] == result["capture_id"]
    assert len(panel["rows"]) == 3
    first = panel["rows"][0]
    assert first["origin"] == "INVENTED_FIXTURE"
    assert first["product"] == profile.product and first["tier"] == profile.tier
    assert all(isinstance(r["weekly_shares"], str) for r in panel["rows"])
    assert "12345678901234567890" in [r["weekly_shares"] for r in panel["rows"]]
    assert first["provider_published_at"] is None
    assert first["first_seen_at"] == result["captured_at"]
    assert first["first_seen_at"] > "2026-10-19"
    assert first["schema_profile"] == profile.schema_profile
    with sqlite3.connect(root / "otc.sqlite3") as db:
        assert db.execute("select count(*) from otc_rows").fetchone()[0] == 3
        assert db.execute("select status from otc_attempts").fetchone()[0] == "PUBLISHED"


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
@pytest.mark.parametrize("boundary", ["rows", "coverage", "status", "commit"])
def test_persistence_failures_never_publish_rows_membership_or_success(
    tmp_path, monkeypatch, profile, boundary
):
    from market_data_pipeline.pipelines.finra_otc_transparency.io import warehouse

    root = tmp_path / "panel"
    # Create the warehouse through an explicitly incomplete local attempt.
    run(root, profile, expected_rows=None)
    if boundary == "commit":
        original = warehouse._commit
        calls = 0

        def fail(db):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise sqlite3.OperationalError("injected commit failure")
            return original(db)

        monkeypatch.setattr(warehouse, "_commit", fail)
    else:
        statements = {
            "rows": (
                "CREATE TRIGGER fail BEFORE INSERT ON otc_rows "
                "BEGIN SELECT RAISE(ABORT,'rows'); END"
            ),
            "coverage": (
                "CREATE TRIGGER fail BEFORE INSERT ON otc_publications "
                "BEGIN SELECT RAISE(ABORT,'coverage'); END"
            ),
            "status": (
                "CREATE TRIGGER fail BEFORE UPDATE OF status ON otc_attempts "
                "WHEN NEW.status='PUBLISHED' BEGIN SELECT RAISE(ABORT,'status'); END"
            ),
        }
        with sqlite3.connect(root / "otc.sqlite3") as db:
            db.execute(statements[boundary])
    with pytest.raises(sqlite3.Error):
        run(root, profile)
    panel = inspect(root, [profile])
    assert panel["rows"] == []
    assert panel["partitions"][0]["publication"] is None
    assert panel["partitions"][0]["latest_attempt"]["status"] == "FAILED"
    with sqlite3.connect(root / "otc.sqlite3") as db:
        assert db.execute("select count(*) from otc_rows").fetchone()[0] == 0
        assert db.execute("select count(*) from otc_publications").fetchone()[0] == 0
    assert len(list((root / "bronze/captures").iterdir())) == 2


def test_six_disjoint_populations_are_selected_without_combining_denominators(tmp_path):
    for p in PROFILES:
        run(tmp_path / "panel", p)
    panel = inspect(tmp_path / "panel")
    assert panel["status"] == "COMPLETE_FIXTURE" and len(panel["rows"]) == 18
    assert len({r["participant_key"] for r in panel["rows"]}) >= 12
    for p, item in zip(PROFILES, panel["partitions"], strict=True):
        assert item["partition"]["product"] == p.product
        assert item["publication"]["coverage"]["expected_delay_weeks"] == p.expected_delay_weeks
        counts = item["publication"]["coverage"]["attribution_rows"]
        assert sum(counts.values()) == 3
        if p.product == "NON_ATS_WEEKLY_DETAIL":
            assert counts == {
                "PROVIDER_ID": 1,
                "SOURCE_ROW_LABEL": 1,
                "AGGREGATED": 1,
                "UNKNOWN": 0,
            }


@pytest.mark.parametrize("mode", ["unverified", "partial", "bad_count", "rejected", "replacement"])
def test_later_nonpublishable_attempt_keeps_old_publication_and_visible_failure(tmp_path, mode):
    root = tmp_path / "panel"
    first = run(root)
    options = {
        "unverified": {"expected_rows": None},
        "partial": {"body_complete": False},
        "bad_count": {"expected_rows": 4},
        "rejected": {"payload": b"<html>SECRET denied</html>"},
        "replacement": {},
    }
    second = run(root, **options[mode])
    panel = inspect(root, [PROFILES[0]])
    assert panel["partitions"][0]["publication"]["capture_id"] == first["capture_id"]
    assert panel["partitions"][0]["latest_attempt"]["capture_id"] == second["capture_id"]
    assert second["status"] in ("UNVERIFIED", "PARTIAL", "REJECTED", "REPLACEMENT_DEFERRED")
    assert panel["status"] == "DEGRADED_FIXTURE" and len(panel["rows"]) == 3
    assert "SECRET" not in json.dumps(panel)


@pytest.mark.parametrize("profile", PROFILES)
def test_header_only_needs_separate_explicit_empty_fixture_assertion(tmp_path, profile):
    raw = payload(profile).split(b"\n")[0] + b"\n"
    result = run(tmp_path / "panel", profile, payload=raw, expected_rows=0)
    assert result["status"] == "PUBLISHED"
    panel = inspect(tmp_path / "panel", [profile])
    assert panel["rows"] == []
    assert panel["partitions"][0]["publication"]["coverage"]["status"] == "COMPLETE_EMPTY_FIXTURE"


def test_capture_retry_is_immutable_and_does_not_duplicate_local_rows(tmp_path):
    root = tmp_path / "panel"
    result = run(root, capture_id=str(uuid4()), run_id=str(uuid4()))
    files = {
        str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    }
    assert publish_capture(root, result["capture_id"], approved_use=USE) == result
    assert {
        str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    } == files


def test_inspect_is_read_only_bounded_and_reports_explicit_missing_populations(
    tmp_path, monkeypatch
):
    import socket

    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: pytest.fail("network"))
    root = tmp_path / "panel"
    run(root)
    before = {
        str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()
    }
    panel = inspect(root)
    assert panel["status"] == "PARTIAL_FIXTURE"
    assert sum(p["publication"] is None for p in panel["partitions"]) == 5
    with pytest.raises(ValueError):
        inspect_panel(root, partitions=[partition(PROFILES[0])], approved_use=USE, max_rows=2)
    assert before == {
        str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()
    }
    with pytest.raises(ValueError):
        inspect(tmp_path / "missing")
    assert not (tmp_path / "missing").exists()


def test_denied_use_never_creates_a_panel(tmp_path):
    with pytest.raises(PermissionError):
        run(tmp_path / "denied", approved_use=None)
    assert not (tmp_path / "denied").exists()


@pytest.mark.parametrize("bad", [True, -1, 1000001, "3"])
def test_invalid_fixture_assertion_is_rejected_before_any_store(tmp_path, bad):
    with pytest.raises(ValueError):
        run(tmp_path / "invalid", expected_rows=bad)
    assert not (tmp_path / "invalid").exists()


def test_noncanonical_boolean_discovery_cannot_claim_complete_empty_fixture(tmp_path):
    import hashlib

    from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import fixture_assertion
    from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import (
        Part,
        archive_capture,
    )
    from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import encoded

    p = partition(PROFILES[0])
    root = tmp_path / "panel"
    raw = payload(PROFILES[0]).split(b"\n")[0] + b"\n"
    assertion = fixture_assertion(p, PROFILES[0].schema_profile, hashlib.sha256(raw).hexdigest(), 0)
    assertion["expected_rows"] = False
    receipt = archive_capture(
        root,
        partition=p,
        manifest=MANIFEST,
        approved_use=USE,
        run_id=str(uuid4()),
        expected_parts=["file"],
        parts=[Part("file", raw)],
        discovery=Part("index", encoded(assertion)),
    )
    result = publish_capture(root, receipt["capture_id"], approved_use=USE)
    assert result["status"] == "REJECTED"
    assert inspect(root, [PROFILES[0]])["rows"] == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "COMPLETE_EMPTY_FIXTURE"),
        ("row_count", True),
        ("attribution_rows", {"PROVIDER_ID": 0}),
    ],
)
def test_corrupt_coverage_cannot_be_read_as_a_complete_panel(tmp_path, field, value):
    root = tmp_path / "panel"
    run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        capture, raw = db.execute(
            "SELECT capture_id,coverage_json FROM otc_publications"
        ).fetchone()
        coverage = json.loads(raw)
        coverage[field] = value
        db.execute(
            "UPDATE otc_publications SET coverage_json=? WHERE capture_id=?",
            (json.dumps(coverage), capture),
        )
    with pytest.raises(ValueError):
        inspect(root, [PROFILES[0]])


@pytest.mark.parametrize("target", ["database", "journal", "lock"])
def test_symlink_storage_targets_are_refused_without_mutating_target(tmp_path, target):
    root = tmp_path / "panel"
    run(root, expected_rows=None)
    outside = tmp_path / "outside"
    outside.write_bytes(b"untouched")
    paths = {
        "database": root / "otc.sqlite3",
        "journal": root / "otc.sqlite3-journal",
        "lock": root / ".writer.lock",
    }
    path = paths[target]
    path.unlink(missing_ok=True)
    path.symlink_to(outside)
    with pytest.raises(ValueError):
        run(root)
    assert outside.read_bytes() == b"untouched"


def test_sqlite_other_pipeline_identity_is_never_adopted_or_changed(tmp_path):
    root = tmp_path / "panel"
    run(root, expected_rows=None)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        db.execute("PRAGMA application_id=123")
    before = (root / "otc.sqlite3").read_bytes()
    with pytest.raises(ValueError):
        run(root)
    assert (root / "otc.sqlite3").read_bytes() == before


def test_panel_row_limit_is_rejected_before_raw_capture(tmp_path):
    with pytest.raises(ValueError):
        run(tmp_path / "invalid", max_rows=10001)
    assert not (tmp_path / "invalid").exists()


def test_concurrent_writers_publish_one_initial_population(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    root = tmp_path / "panel"
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: run(root), range(4)))
    assert sorted(r["status"] for r in results) == ["PUBLISHED"] + ["REPLACEMENT_DEFERRED"] * 3
    panel = inspect(root, [PROFILES[0]])
    assert len(panel["rows"]) == 3
    with sqlite3.connect(root / "otc.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM otc_attempts").fetchone()[0] == 4
        assert db.execute("SELECT count(*) FROM otc_publications").fetchone()[0] == 1


def test_reader_cannot_see_half_publication_and_pending_attempt_can_resume(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.finra_otc_transparency.io import warehouse

    root = tmp_path / "panel"
    run(root, expected_rows=None)
    seen = []
    original = warehouse._commit
    calls = 0

    def fail(db):
        nonlocal calls
        calls += 1
        if calls == 2:
            seen.append(inspect(root, [PROFILES[0]]))
            raise KeyboardInterrupt("simulated worker termination before commit")
        return original(db)

    capture_id = str(uuid4())
    with monkeypatch.context() as patch:
        patch.setattr(warehouse, "_commit", fail)
        with pytest.raises(KeyboardInterrupt):
            run(root, capture_id=capture_id)
    assert seen[0]["rows"] == [] and seen[0]["partitions"][0]["publication"] is None
    assert seen[0]["partitions"][0]["latest_attempt"]["status"] == "PENDING"
    result = publish_capture(root, capture_id, approved_use=USE)
    assert result["status"] == "PUBLISHED"
    assert len(inspect(root, [PROFILES[0]])["rows"]) == 3


@pytest.mark.parametrize("target", ["row", "status", "attempt_context"])
def test_inconsistent_warehouse_records_fail_readback_without_repair(tmp_path, target):
    root = tmp_path / "panel"
    run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        if target == "row":
            db.execute("UPDATE otc_rows SET row_json='{}'")
        elif target == "status":
            db.execute("UPDATE otc_attempts SET status='PENDING'")
        else:
            db.execute("UPDATE otc_attempts SET context_json='[]'")
    before = (root / "otc.sqlite3").read_bytes()
    with pytest.raises(ValueError):
        inspect(root, [PROFILES[0]])
    assert before == (root / "otc.sqlite3").read_bytes()


def test_real_approved_use_cannot_enter_fixture_pipeline(tmp_path):
    from test_otc_manifest import api_use

    with pytest.raises(PermissionError):
        run(tmp_path / "denied", approved_use=api_use())
    assert not (tmp_path / "denied").exists()


@pytest.mark.parametrize("corruption", ["row", "selection"])
def test_successful_retry_verifies_existing_publication_before_reporting_success(
    tmp_path, corruption
):
    root = tmp_path / "panel"
    result = run(root)
    with sqlite3.connect(root / "otc.sqlite3") as db:
        if corruption == "row":
            db.execute("DELETE FROM otc_rows WHERE symbol_raw=?", ("ZXQ.A",))
        else:
            db.execute("DELETE FROM otc_publications")
    before = (root / "otc.sqlite3").read_bytes()
    with pytest.raises(ValueError):
        publish_capture(root, result["capture_id"], approved_use=USE)
    assert (root / "otc.sqlite3").read_bytes() == before
