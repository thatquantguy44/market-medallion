"""OTC-AC07 immutable value versions and authoritative replacement membership."""

from copy import deepcopy

import pytest
from test_otc_bronze import USE, archive
from test_otc_pipeline import PROFILES

from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import prepare
from market_data_pipeline.pipelines.finra_otc_transparency.data.versions import (
    content,
    make_version,
    tokens,
)
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import (
    read_capture,
    validate_capture,
)


def sample(tmp_path):
    root = tmp_path / "raw"
    import hashlib

    from test_otc_bronze import VALID, WEEK

    from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import fixture_assertion
    from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import Part
    from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import encoded

    receipt = archive(
        root,
        discovery=Part(
            "index",
            encoded(
                fixture_assertion(
                    WEEK,
                    "finra-otc-ats-weekly-pipe-fixture/1",
                    hashlib.sha256(VALID).hexdigest(),
                    3,
                )
            ),
        ),
    )
    raw = read_capture(root, receipt["capture_id"], approved_use=USE)
    result = validate_capture(root, receipt["capture_id"], approved_use=USE)
    return prepare(raw, result)[3][0], receipt


def test_content_hash_excludes_capture_transport_and_original_integer_formatting(tmp_path):
    row, receipt = sample(tmp_path)
    copy = deepcopy(row)
    for key in (
        "capture_id",
        "run_id",
        "receipt_sha256",
        "manifest_sha256",
        "schema_sha256",
        "captured_at",
        "first_seen_at",
        "capture_sequence",
    ):
        copy[key] = "different" if key != "capture_sequence" else 999
    copy["weekly_trades_text"] = "00042"
    assert content(copy) == content(row)
    assert tokens(copy) == {"weekly_shares_text": "9007199254740993", "weekly_trades_text": "00042"}


def test_reversion_and_removal_have_distinct_occurrences_and_never_invent_zero(tmp_path):
    row, receipt = sample(tmp_path)
    key = (row["symbol_raw"], row["participant_key"])
    a = make_version(receipt, key, content(row), None)
    assert make_version(receipt, key, content(row), a) is None
    corrected = {**content(row), "weekly_shares": "7"}
    b = make_version({**receipt, "capture_sequence": 2}, key, corrected, a)
    c = make_version({**receipt, "capture_sequence": 3}, key, content(row), b)
    removed = make_version({**receipt, "capture_sequence": 4}, key, None, c)
    revived = make_version({**receipt, "capture_sequence": 5}, key, content(row), removed)
    assert a["content_sha256"] == c["content_sha256"] == revived["content_sha256"]
    assert len({v["version_id"] for v in (a, b, c, removed, revived)}) == 5
    assert removed["state"] == "REMOVED" and removed["content"] is None
    assert removed["previous_version_id"] == c["version_id"]
    assert make_version(receipt, key, None, removed) is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("participant_name_raw", "Invented Rename"),
        ("issue_name_raw", "Invented Changed Issue"),
        ("provider_update_date", "2026-11-10"),
        ("mpid", "0002"),
        ("firm_crd", "000123"),
        ("schema_profile", "invented-changed-schema/1"),
    ],
)
def test_semantic_source_changes_are_not_erased_from_content_identity(tmp_path, field, value):
    row, receipt = sample(tmp_path)
    changed = {**row, field: value}
    key = (row["symbol_raw"], row["participant_key"])
    first = make_version(receipt, key, content(row), None)
    revision = make_version({**receipt, "capture_sequence": 2}, key, content(changed), first)
    assert revision["content_sha256"] != first["content_sha256"]
    assert revision["previous_version_id"] == first["version_id"]


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_six_profiles_preserve_repeats_corrections_reversions_and_complete_removals(
    tmp_path, profile
):
    from test_otc_pipeline import partition, payload, run

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline

    root = tmp_path / "panel"
    p = partition(profile)
    raw = payload(profile)
    first = run(root, profile)
    repeated = run(root, profile)
    assert repeated["status"] == "PUBLISHED"

    def panel(**kw):
        return pipeline.inspect_panel(root, partitions=[p], approved_use=USE, **kw)

    before = panel()
    assert all(r["first_seen_at"] == first["captured_at"] for r in before["rows"])
    assert (
        len(pipeline.observation_history(root, partitions=[p], approved_use=USE)["versions"]) == 3
    )
    corrected = raw.replace(b"12345678901234567890", b"12345678901234567891")
    correction = run(root, profile, payload=corrected)
    reverted = run(root, profile)
    full = panel()
    assert any(r["weekly_shares"] == "12345678901234567890" for r in full["rows"])
    short = b"\n".join(raw.splitlines()[:-1]) + b"\n"
    partial = run(root, profile, payload=short, expected_rows=2, body_complete=False)
    assert partial["status"] == "PARTIAL" and panel()["rows"] == full["rows"]
    removal = run(root, profile, payload=short, expected_rows=2)
    assert removal["status"] == "PUBLISHED"
    assert len(panel()["rows"]) == 2
    historical = panel(as_of=reverted["captured_at"], as_of_sequence=reverted["capture_sequence"])
    assert historical["rows"] == full["rows"]
    assert (
        len(
            panel(as_of=correction["captured_at"], as_of_sequence=correction["capture_sequence"])[
                "rows"
            ]
        )
        == 3
    )
    empty = run(root, profile, payload=raw.splitlines()[0] + b"\n", expected_rows=0)
    assert empty["status"] == "PUBLISHED"
    assert panel()["rows"] == []
    revived = run(root, profile)
    assert revived["status"] == "PUBLISHED"
    assert len(panel()["rows"]) == 3
    versions = pipeline.observation_history(root, partitions=[p], approved_use=USE)["versions"]
    assert len(versions) == 11
    assert sum(v["state"] == "REMOVED" for v in versions) == 3
    assert all(v["content"] is None for v in versions if v["state"] == "REMOVED")
    assert len({v["version_id"] for v in versions}) == 11


def test_payload_row_order_line_endings_and_count_format_do_not_create_value_versions(tmp_path):
    from test_otc_pipeline import partition, payload, run

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline

    root = tmp_path / "panel"
    first = run(root)
    raw = payload(PROFILES[0])
    lines = raw.splitlines()
    changed = b"\r\n".join([lines[0], *reversed(lines[1:])]) + b"\r\n"
    changed = changed.replace(b"|42\r\n", b"|00042\r\n")
    repeated = run(root, payload=changed)
    panel = pipeline.inspect_panel(root, partitions=[partition(PROFILES[0])], approved_use=USE)
    assert all(r["first_seen_at"] == first["captured_at"] for r in panel["rows"])
    assert all(r["membership_capture_id"] == repeated["capture_id"] for r in panel["rows"])
    assert any(r["weekly_trades_text"] == "00042" for r in panel["rows"])
    assert (
        len(
            pipeline.observation_history(
                root, partitions=[partition(PROFILES[0])], approved_use=USE
            )["versions"]
        )
        == 3
    )


def archived(root, profile, raw, stamp):
    import hashlib
    from uuid import uuid4

    from test_otc_pipeline import MANIFEST, partition

    from market_data_pipeline.pipelines.finra_otc_transparency.data.panel import fixture_assertion
    from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import (
        Part,
        archive_capture,
    )
    from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import encoded

    p = partition(profile)
    return archive_capture(
        root,
        partition=p,
        manifest=MANIFEST,
        approved_use=USE,
        run_id=str(uuid4()),
        captured_at=stamp,
        expected_parts=["file"],
        parts=[Part("file", raw)],
        discovery=Part(
            "index",
            encoded(
                fixture_assertion(p, profile.schema_profile, hashlib.sha256(raw).hexdigest(), 3)
            ),
        ),
    )


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_equal_capture_clocks_use_original_durable_sequence_at_both_cutoffs(tmp_path, profile):
    from datetime import UTC, datetime

    from test_otc_pipeline import partition, payload

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline

    root = tmp_path / "panel"
    stamp = datetime(2026, 11, 16, 15, tzinfo=UTC)
    first = archived(root, profile, payload(profile), stamp)
    second = archived(root, profile, payload(profile).replace(b"12345678901234567890", b"7"), stamp)
    pipeline.publish_capture(root, first["capture_id"], approved_use=USE)
    pipeline.publish_capture(root, second["capture_id"], approved_use=USE)
    a = pipeline.inspect_panel(
        root,
        partitions=[partition(profile)],
        approved_use=USE,
        as_of=stamp,
        as_of_sequence=first["capture_sequence"],
    )
    b = pipeline.inspect_panel(
        root,
        partitions=[partition(profile)],
        approved_use=USE,
        as_of=stamp,
        as_of_sequence=second["capture_sequence"],
    )
    assert any(r["weekly_shares"] == "12345678901234567890" for r in a["rows"])
    assert any(r["weekly_shares"] == "7" for r in b["rows"])
    assert a["partitions"][0]["latest_attempt"]["capture_id"] == first["capture_id"]
    assert b["partitions"][0]["latest_attempt"]["capture_id"] == second["capture_id"]


def test_late_processing_of_older_complete_capture_cannot_rewrite_newer_membership(tmp_path):
    from datetime import UTC, datetime

    from test_otc_pipeline import partition, payload

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline

    root = tmp_path / "panel"
    p = PROFILES[0]
    stamp = datetime(2026, 11, 16, 15, tzinfo=UTC)
    first = archived(root, p, payload(p), stamp)
    second = archived(root, p, payload(p).replace(b"12345678901234567890", b"7"), stamp)
    pipeline.publish_capture(root, second["capture_id"], approved_use=USE)
    assert (
        pipeline.publish_capture(root, first["capture_id"], approved_use=USE)["status"]
        == "OUT_OF_ORDER"
    )
    assert (
        pipeline.inspect_panel(
            root,
            partitions=[partition(p)],
            approved_use=USE,
            as_of=stamp,
            as_of_sequence=first["capture_sequence"],
        )["rows"]
        == []
    )
    assert (
        len(
            pipeline.observation_history(root, partitions=[partition(p)], approved_use=USE)[
                "versions"
            ]
        )
        == 3
    )


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
@pytest.mark.parametrize("boundary", ["versions", "members", "snapshot", "status", "commit"])
def test_failed_replacement_rolls_back_versions_removals_and_membership(
    tmp_path, monkeypatch, profile, boundary
):
    import sqlite3

    from test_otc_pipeline import partition, payload, run

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline
    from market_data_pipeline.pipelines.finra_otc_transparency.io import warehouse

    root = tmp_path / "panel"
    run(root, profile)
    selected = pipeline.inspect_panel(root, partitions=[partition(profile)], approved_use=USE)
    before = pipeline.observation_history(root, partitions=[partition(profile)], approved_use=USE)
    if boundary == "commit":
        original = warehouse._commit
        calls = 0

        def fail(db):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise sqlite3.OperationalError("injected commit failure")
            original(db)

        monkeypatch.setattr(warehouse, "_commit", fail)
    else:
        tables = {
            "versions": "otc2_versions",
            "members": "otc2_members",
            "snapshot": "otc2_snapshots",
        }
        statement = (
            "CREATE TRIGGER fail BEFORE UPDATE OF status ON otc2_attempts "
            "WHEN NEW.status='PUBLISHED' BEGIN SELECT RAISE(ABORT,'status'); END"
            if boundary == "status"
            else "CREATE TRIGGER fail BEFORE INSERT ON "
            + tables[boundary]
            + " BEGIN SELECT RAISE(ABORT,'write'); END"
        )
        with sqlite3.connect(root / "otc.sqlite3") as db:
            db.execute(statement)
    raw = payload(profile).replace(b"12345678901234567890", b"7")
    raw = b"\n".join(raw.splitlines()[:-1]) + b"\n"
    with pytest.raises(sqlite3.Error):
        run(root, profile, payload=raw, expected_rows=2)
    after = pipeline.inspect_panel(root, partitions=[partition(profile)], approved_use=USE)
    assert after["rows"] == selected["rows"]
    assert after["partitions"][0]["latest_attempt"]["status"] == "FAILED"
    assert (
        pipeline.observation_history(root, partitions=[partition(profile)], approved_use=USE)
        == before
    )


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.product + "-" + p.tier)
def test_supplied_id_rename_keeps_identity_and_creates_source_content_revisions(tmp_path, profile):
    from test_otc_pipeline import partition, payload, run

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline

    root = tmp_path / "panel"
    run(root, profile)
    before = pipeline.inspect_panel(root, partitions=[partition(profile)], approved_use=USE)["rows"]
    label = (
        b"EXAMPLE ALPHA ATS" if profile.product == "ATS_WEEKLY_DETAIL" else b"Invented Named Firm"
    )
    run(root, profile, payload=payload(profile).replace(label, b"Invented Source Rename"))
    after = pipeline.inspect_panel(root, partitions=[partition(profile)], approved_use=USE)["rows"]
    assert {(r["symbol_raw"], r["participant_key"]) for r in after} == {
        (r["symbol_raw"], r["participant_key"]) for r in before
    }
    versions = pipeline.observation_history(
        root, partitions=[partition(profile)], approved_use=USE
    )["versions"]
    assert len(versions) == (5 if profile.product == "ATS_WEEKLY_DETAIL" else 4)
    assert all(v["state"] == "OBSERVED" for v in versions)


@pytest.mark.parametrize(
    "profile", [p for p in PROFILES if p.product == "NON_ATS_WEEKLY_DETAIL"], ids=lambda p: p.tier
)
@pytest.mark.parametrize("label", [b"Invented Unidentified Label", b"Invented Group"])
def test_name_and_group_rename_changes_membership_without_inventing_firm_continuity(
    tmp_path, profile, label
):
    from test_otc_pipeline import partition, payload, run

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline

    root = tmp_path / "panel"
    run(root, profile)
    run(root, profile, payload=payload(profile).replace(label, b"Invented Changed Label"))
    versions = pipeline.observation_history(
        root, partitions=[partition(profile)], approved_use=USE
    )["versions"]
    assert len(versions) == 5 and sum(v["state"] == "REMOVED" for v in versions) == 1
    rows = pipeline.inspect_panel(root, partitions=[partition(profile)], approved_use=USE)["rows"]
    changed = next(r for r in rows if r["participant_name_raw"] == "Invented Changed Label")
    assert changed["mpid"] is None and changed["firm_crd"] is None


@pytest.mark.parametrize(
    "clock,sequence",
    [
        (None, 1),
        ("2026-11-16", None),
        ("2026-11-16T12:00:00", None),
        ("2026-11-16T12:00:00Z", True),
        ("2026-11-16T12:00:00Z", -1),
        ("2026-11-16T12:00:00Z", 100001),
    ],
)
def test_invalid_cutoffs_are_rejected_without_creating_or_touching_stores(
    tmp_path, clock, sequence
):
    from test_otc_pipeline import partition

    from market_data_pipeline.pipelines.finra_otc_transparency import pipeline

    with pytest.raises(ValueError, match="cutoff|sequence"):
        pipeline.inspect_panel(
            tmp_path / "absent",
            partitions=[partition(PROFILES[0])],
            approved_use=USE,
            as_of=clock,
            as_of_sequence=sequence,
        )
    assert not (tmp_path / "absent").exists()
