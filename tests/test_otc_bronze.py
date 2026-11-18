"""OTC-AC05: exact immutable bytes and truthful offline partition receipts."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import Partition
from market_data_pipeline.pipelines.finra_otc_transparency.io import bronze
from market_data_pipeline.pipelines.finra_otc_transparency.io.bronze import (
    Part,
    archive_capture,
    read_capture,
    validate_capture,
)
from market_data_pipeline.pipelines.finra_otc_transparency.manifest import load_manifest

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = load_manifest(ROOT / "manifests/finra-otc-transparency.yml")
VALID = (ROOT / "tests/fixtures/finra_otc_transparency/ats_t1_weekly.txt").read_bytes()
EMPTY = (ROOT / "tests/fixtures/finra_otc_transparency/ats_t1_empty.txt").read_bytes()
CLOCK = datetime(2026, 11, 15, 18, tzinfo=UTC)
WEEK = Partition(datetime(2026, 10, 19).date(), "ATS_WEEKLY_DETAIL", "T1")
RUN = "11111111-1111-4111-8111-111111111111"
USE = {
    "dataset": "finra-otc-transparency",
    "purpose": "personal_noncommercial_internal_research",
    "route": "invented_fixture",
    "authorization": "pending",
    "permission_reference": None,
    "credential_type": None,
    "provider_dataset": None,
    "attribution": None,
    "operations": ["fixture"],
}


def archive(root, *, payload=VALID, **kwargs):
    values = dict(
        partition=WEEK,
        manifest=MANIFEST,
        approved_use=USE,
        run_id=RUN,
        expected_parts=["file"],
        parts=[Part("file", payload)],
        captured_at=CLOCK,
    )
    values.update(kwargs)
    return archive_capture(root, **values)


def state(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_raw_bytes_discovery_manifest_schema_and_context_survive_roundtrip(tmp_path):
    import hashlib

    discovery = Part(
        "index",
        b'{"invented_index":["file"]}',
        source_url="https://www.finra.org/filing-reporting/otc-transparency",
    )
    part = Part(
        "file",
        VALID,
        http_status=200,
        retrieved_at=CLOCK - timedelta(seconds=1),
        response_headers={
            "Content-Type": "text/plain",
            "Set-Cookie": "SECRET",
            "Authorization": "Bearer SECRET",
        },
    )
    receipt = archive(tmp_path / "store", parts=[part], discovery=discovery)
    stored = read_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    assert stored.parts == {"file": VALID}
    assert stored.discovery == discovery.payload
    assert stored.receipt == receipt
    assert receipt["partition"] == {
        "reporting_week_start": "2026-10-19",
        "product": "ATS_WEEKLY_DETAIL",
        "tier": "T1",
    }
    assert receipt["captured_at"] == "2026-11-15T18:00:00.000000Z"
    assert receipt["provider_published_at"] is None
    assert receipt["coverage"] == "RECEIVED_ALL" and receipt["membership"] == "UNVERIFIED"
    assert receipt["parts"][0]["sha256"] == hashlib.sha256(VALID).hexdigest()
    assert receipt["parts"][0]["headers"] == {"content-type": "text/plain"}
    assert receipt["manifest_sha256"] == bronze.digest(receipt["manifest"])
    assert receipt["schema_sha256"] == bronze.digest(receipt["schema"])
    assert "SECRET" not in str(receipt)
    assert all("SECRET" not in p.read_text() for p in (tmp_path / "store").rglob("*.json"))


def test_success_and_header_only_keep_unverified_membership(tmp_path):
    for index, payload in enumerate((VALID, EMPTY)):
        root = tmp_path / str(index)
        receipt = archive(root, payload=payload)
        result = validate_capture(root, receipt["capture_id"], approved_use=USE)
        assert result.outcome["status"] == ("PARSED" if index == 0 else "PARSED_EMPTY")
        assert result.outcome["row_count"] == (3 if index == 0 else 0)
        assert result.outcome["membership"] == "UNVERIFIED"
        assert result.outcome["receipt_sha256"] == receipt["receipt_sha256"]
        assert result.parsed is not None
        before = state(root)
        assert (
            validate_capture(root, receipt["capture_id"], approved_use=USE).outcome
            == result.outcome
        )
        assert state(root) == before


@pytest.mark.parametrize(
    "parts,expected,coverage",
    [
        ([], ["page-0"], "MISSING"),
        ([Part("page-0", VALID)], ["page-0", "page-1"], "PARTIAL"),
        ([Part("page-0", VALID, body_complete=False)], ["page-0"], "PARTIAL"),
        ([Part("extra", VALID)], ["page-0"], "PARTIAL"),
    ],
)
def test_missing_truncated_or_unexpected_parts_preserve_negative_receipts(
    tmp_path, parts, expected, coverage
):
    receipt = archive(tmp_path / "store", parts=parts, expected_parts=expected)
    result = validate_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    assert receipt["coverage"] == result.outcome["status"] == coverage
    assert result.outcome["row_count"] is None and result.parsed is None
    assert read_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE).parts == {
        p.part_id: p.payload for p in parts
    }
    assert result.outcome["membership"] == "UNVERIFIED"


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"<html>challenge SECRET</html>",
        b"bad\x00\xff",
        VALID.replace(b"9007199254740993", b"-1"),
    ],
)
def test_parser_rejection_keeps_original_binary_and_sanitized_outcome(tmp_path, payload):
    receipt = archive(tmp_path / "store", payload=payload)
    result = validate_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    assert result.outcome["status"] == "REJECTED"
    assert result.outcome["reason"] == "PARSER_REJECTED"
    assert result.outcome["row_count"] is None
    assert "SECRET" not in str(result.outcome)
    assert (
        read_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE).parts["file"]
        == payload
    )


def test_http_failure_and_unsupported_multipart_are_not_parsed(tmp_path):
    failed = archive(tmp_path / "http", parts=[Part("file", VALID, http_status=503)])
    result = validate_capture(tmp_path / "http", failed["capture_id"], approved_use=USE)
    assert result.outcome["reason"] == "HTTP_STATUS"
    both = archive(
        tmp_path / "multi", expected_parts=["a", "b"], parts=[Part("a", VALID), Part("b", VALID)]
    )
    result = validate_capture(tmp_path / "multi", both["capture_id"], approved_use=USE)
    assert (
        result.outcome["status"] == "REJECTED"
        and result.outcome["reason"] == "MULTIPART_UNSUPPORTED"
    )
    assert result.outcome["row_count"] is None


def test_same_id_is_idempotent_conflicts_never_overwrite_and_same_bytes_are_new_events(tmp_path):
    root = tmp_path / "store"
    capture_id = str(uuid4())
    first = archive(root, capture_id=capture_id)
    before = state(root)
    assert archive(root, capture_id=capture_id, captured_at=None) == first
    assert state(root) == before
    for changes in (
        {"payload": EMPTY},
        {"run_id": str(uuid4())},
        {"captured_at": CLOCK + timedelta(seconds=1)},
    ):
        with pytest.raises(ValueError):
            archive(root, capture_id=capture_id, **changes)
        assert state(root) == before
    second = archive(root)
    assert second["capture_id"] != first["capture_id"]
    assert second["capture_sequence"] == first["capture_sequence"] + 1
    assert second["parts"][0]["sha256"] == first["parts"][0]["sha256"]
    with pytest.raises(ValueError, match="clock"):
        archive(root, captured_at=CLOCK - timedelta(seconds=1))


def test_denied_profiles_and_invalid_lineage_create_no_store(tmp_path):
    for changes in (
        {"approved_use": None},
        {"run_id": "../escape"},
        {"capture_id": "../escape"},
        {"expected_parts": ["../escape"]},
        {"expected_parts": ["file", "file"]},
        {"captured_at": CLOCK.replace(tzinfo=None)},
        {"max_bytes": True},
        {"max_rows": 0},
    ):
        root = tmp_path / str(uuid4())
        with pytest.raises((PermissionError, ValueError)):
            archive(root, **changes)
        assert not root.exists()


def test_byte_budget_and_url_credentials_are_rejected_before_writes(tmp_path):
    with pytest.raises(ValueError):
        archive(tmp_path / "bound", max_bytes=len(VALID) - 1)
    assert not (tmp_path / "bound").exists()
    for url in (
        "https://user:SECRET@www.finra.org/file",
        "https://api.finra.org/data?token=SECRET",
        "https://elsewhere.example/file",
    ):
        root = tmp_path / str(uuid4())
        with pytest.raises(ValueError):
            archive(root, parts=[Part("file", VALID, source_url=url)])
        assert not root.exists()


def test_read_and_permission_failures_are_read_only_and_network_free(tmp_path, monkeypatch):
    import socket

    def forbidden(*args, **kwargs):
        raise AssertionError("OTC Bronze attempted network access")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    absent = tmp_path / "absent"
    with pytest.raises(ValueError):
        read_capture(absent, str(uuid4()), approved_use=USE)
    assert not absent.exists()
    receipt = archive(tmp_path / "store")
    before = state(tmp_path / "store")
    read_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    with pytest.raises(PermissionError):
        read_capture(tmp_path / "store", receipt["capture_id"], approved_use=None)
    assert state(tmp_path / "store") == before


def test_tampered_bytes_receipt_and_sequence_are_refused_without_repair(tmp_path):
    for name in ("parts/file.bin", "receipt.json", "reservation"):
        root = tmp_path / str(uuid4())
        receipt = archive(root)
        target = root / "bronze/captures" / receipt["capture_id"] / name
        if name == "reservation":
            target = root / "bronze/sequences" / f"{receipt['capture_sequence']:020d}.json"
        target.write_bytes(target.read_bytes() + b"changed")
        before = state(root)
        with pytest.raises(ValueError):
            read_capture(root, receipt["capture_id"], approved_use=USE)
        assert state(root) == before


def test_failed_publication_reserves_sequence_but_exposes_no_partial_capture(tmp_path, monkeypatch):
    root = tmp_path / "store"
    first = archive(root)
    original = bronze._write

    def fail(path, body):
        if path.name == "receipt.json":
            raise OSError("injected disk failure")
        return original(path, body)

    with monkeypatch.context() as patch:
        patch.setattr(bronze, "_write", fail)
        with pytest.raises(OSError):
            archive(root)
    assert [p.name for p in (root / "bronze/captures").iterdir() if not p.name.startswith(".")] == [
        first["capture_id"]
    ]
    second = archive(root)
    assert second["capture_sequence"] == first["capture_sequence"] + 2
    assert read_capture(root, first["capture_id"], approved_use=USE).parts["file"] == VALID


def test_parallel_writers_have_unique_durable_sequences(tmp_path):
    root = tmp_path / "store"
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(lambda _: archive(root), range(8)))
    assert sorted(r["capture_sequence"] for r in receipts) == list(range(1, 9))
    for receipt in receipts:
        assert read_capture(root, receipt["capture_id"], approved_use=USE).receipt == receipt


@pytest.mark.parametrize(
    "product,tier",
    [
        ("ATS_WEEKLY_DETAIL", "T2"),
        ("ATS_WEEKLY_DETAIL", "OTCE"),
        ("NON_ATS_WEEKLY_DETAIL", "T1"),
        ("NON_ATS_WEEKLY_DETAIL", "T2"),
        ("NON_ATS_WEEKLY_DETAIL", "OTCE"),
    ],
)
def test_catalog_only_profiles_are_explicitly_unsupported_not_bad_data(tmp_path, product, tier):
    receipt = archive(
        tmp_path / "store", partition=Partition(WEEK.reporting_week_start, product, tier)
    )
    from market_data_pipeline.pipelines.finra_otc_transparency.catalog import legacy_manifest
    from market_data_pipeline.pipelines.finra_otc_transparency.io.receipts import schema_snapshot

    # Historical candidate descriptors stay unsupported even after T06 fixtures ship.
    receipt["manifest"] = legacy_manifest()
    receipt["schema"] = schema_snapshot(
        Partition(WEEK.reporting_week_start, product, tier), receipt["manifest"]
    )
    reseal(tmp_path / "store/bronze/captures" / receipt["capture_id"] / "receipt.json", receipt)
    result = validate_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    assert result.outcome["reason"] == "UNSUPPORTED_PROFILE"
    assert result.parsed is None and result.outcome["row_count"] is None
    assert (
        read_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE).parts["file"]
        == VALID
    )


def test_incomplete_discovery_is_not_a_fabricated_missing_data_part(tmp_path):
    receipt = archive(
        tmp_path / "store",
        expected_parts=["discovery"],
        parts=[Part("discovery", VALID)],
        discovery=Part("index", b"partial index", body_complete=False),
    )
    assert receipt["coverage"] == "PARTIAL"
    assert receipt["truncated_parts"] == []
    result = validate_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    assert result.outcome["status"] == "PARTIAL" and result.parsed is None


def test_foreign_bronze_identity_is_refused_without_creating_a_lock(tmp_path):
    root = tmp_path / "foreign"
    (root / "bronze").mkdir(parents=True)
    (root / "bronze/format.json").write_text('{"format":"another-pipeline"}\n')
    before = state(root)
    with pytest.raises(ValueError):
        archive(root)
    assert state(root) == before


def test_symlink_writer_lock_is_rejected_as_an_unsafe_store(tmp_path):
    root = tmp_path / "store"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_bytes(b"untouched")
    (root / ".writer.lock").symlink_to(outside)
    with pytest.raises(ValueError):
        archive(root)
    assert outside.read_bytes() == b"untouched"
    assert not (root / "bronze").exists()


def test_ignored_temporary_reservations_cannot_bypass_inventory_budget(tmp_path, monkeypatch):
    root = tmp_path / "store"
    first = archive(root)
    for _ in range(2):
        (root / "bronze/sequences" / (".otc-write-" + uuid4().hex + ".tmp")).write_bytes(b"")
    monkeypatch.setattr(bronze, "MAX_SEQUENCES", 2)
    with pytest.raises(ValueError):
        archive(root)
    assert read_capture(root, first["capture_id"], approved_use=USE).parts["file"] == VALID


@pytest.mark.parametrize(
    "field,value",
    [
        ("coverage", "COMPLETE"),
        ("membership", "COMPLETE"),
        ("manifest_sha256", "0" * 64),
        ("schema_sha256", "0" * 64),
    ],
)
def test_resealed_semantically_false_metadata_does_not_promote_a_partition(tmp_path, field, value):
    import json

    root = tmp_path / "store"
    receipt = archive(root)
    path = root / "bronze/captures" / receipt["capture_id"] / "receipt.json"
    changed = json.loads(path.read_bytes())
    changed[field] = value
    changed.pop("receipt_sha256")
    changed["receipt_sha256"] = bronze.digest(changed)
    path.write_bytes(bronze.encoded(changed))
    before = state(root)
    with pytest.raises(ValueError):
        read_capture(root, receipt["capture_id"], approved_use=USE)
    assert state(root) == before


def test_unexpected_payload_file_and_corrupt_outcome_do_not_get_silently_repaired(tmp_path):
    root = tmp_path / "store"
    receipt = archive(root)
    validate_capture(root, receipt["capture_id"], approved_use=USE)
    (outcome_file,) = (root / "bronze/outcomes" / receipt["capture_id"]).iterdir()
    outcome_file.write_bytes(b"corrupt")
    with pytest.raises(ValueError):
        validate_capture(root, receipt["capture_id"], approved_use=USE)
    assert outcome_file.read_bytes() == b"corrupt"
    extra = root / "bronze/captures" / receipt["capture_id"] / "parts/extra.bin"
    extra.write_bytes(b"undeclared")
    with pytest.raises(ValueError):
        read_capture(root, receipt["capture_id"], approved_use=USE)
    assert extra.read_bytes() == b"undeclared"


@pytest.mark.parametrize(
    "target", ["root", "bronze", "capture", "parts", "payload", "discovery", "reservation"]
)
def test_symlink_components_cannot_redirect_archive_reads(tmp_path, target):
    root = tmp_path / "store"
    receipt = archive(root, discovery=Part("index", b"index"))
    outside = tmp_path / "outside"
    capture = root / "bronze/captures" / receipt["capture_id"]
    paths = {
        "root": root,
        "bronze": root / "bronze",
        "capture": capture,
        "parts": capture / "parts",
        "payload": capture / "parts/file.bin",
        "discovery": capture / "discovery.bin",
        "reservation": root / "bronze/sequences" / f"{receipt['capture_sequence']:020d}.json",
    }
    path = paths[target]
    path.rename(outside)
    path.symlink_to(outside, target_is_directory=outside.is_dir())
    with pytest.raises(ValueError):
        read_capture(root, receipt["capture_id"], approved_use=USE)


def test_fifo_payloads_and_locks_fail_without_blocking(tmp_path):
    import os

    root = tmp_path / "store"
    receipt = archive(root)
    payload = root / "bronze/captures" / receipt["capture_id"] / "parts/file.bin"
    payload.unlink()
    os.mkfifo(payload)
    with pytest.raises(ValueError):
        read_capture(root, receipt["capture_id"], approved_use=USE)
    other = tmp_path / "other"
    other.mkdir()
    os.mkfifo(other / ".writer.lock")
    with pytest.raises(ValueError):
        archive(other)


@pytest.mark.parametrize(
    "changes",
    [
        {"max_bytes": 0},
        {"max_bytes": 64 * 1024 * 1024 + 1},
        {"max_rows": True},
        {"max_rows": 1000001},
        {"parts": [Part("file", "not bytes")]},
        {"parts": [Part("file", VALID), Part("file", VALID)]},
        {"expected_parts": []},
        {"expected_parts": ["p"] * 65},
        {"parts": [Part("file", VALID, retrieved_at=CLOCK + timedelta(seconds=1))]},
        {"parts": [Part("file", VALID, http_status=True)]},
        {"parts": [Part("file", VALID, body_complete="true")]},
        {
            "parts": [
                Part("file", VALID, response_headers={"Content-Type": "x", "content-type": "y"})
            ]
        },
        {"parts": [Part("file", VALID, response_headers={"Content-Type": "bad\nvalue"})]},
    ],
)
def test_invalid_receipt_inputs_fail_before_creating_a_root(tmp_path, changes):
    root = tmp_path / "store"
    with pytest.raises(ValueError):
        archive(root, **changes)
    assert not root.exists()


def test_separate_retention_and_transform_admission_and_updated_grant_readback(tmp_path):
    grant = {
        **USE,
        "route": "approved_local_file",
        "authorization": "provider_permission",
        "permission_reference": "invented-test-permission",
        "attribution": "FINRA",
        "operations": ["retain"],
    }
    root = tmp_path / "store"
    receipt = archive(root, approved_use=grant, captured_at=None)
    assert "invented-test-permission" not in str(receipt)
    before = state(root)
    with pytest.raises(PermissionError):
        validate_capture(root, receipt["capture_id"], approved_use=grant)
    assert state(root) == before
    renewed = {
        **grant,
        "permission_reference": "invented-renewed-permission",
        "operations": ["retain", "transform"],
    }
    assert read_capture(root, receipt["capture_id"], approved_use=renewed).receipt == receipt
    result = validate_capture(root, receipt["capture_id"], approved_use=renewed)
    assert result.outcome["reason"] == "UNSUPPORTED_ROUTE"
    assert read_capture(root, receipt["capture_id"], approved_use=renewed).parts["file"] == VALID


def reseal(path, value):
    value["manifest_sha256"] = bronze.digest(value["manifest"])
    value["schema_sha256"] = bronze.digest(value["schema"])
    value.pop("receipt_sha256")
    value["receipt_sha256"] = bronze.digest(value)
    path.write_bytes(bronze.encoded(value))


@pytest.mark.parametrize(
    "field,value", [("schema_profile", None), ("expected_delay_weeks", True), ("reviewed_on", 123)]
)
def test_resealed_malformed_schema_snapshot_is_not_accepted_as_history(tmp_path, field, value):
    import json

    root = tmp_path / "store"
    receipt = archive(root)
    path = root / "bronze/captures" / receipt["capture_id"] / "receipt.json"
    altered = json.loads(path.read_bytes())
    profile = altered["manifest"]["profiles"][0]
    profile[field] = value
    altered["schema"]["profile"] = profile
    altered["schema"]["schema_profile"] = profile["schema_profile"]
    reseal(path, altered)
    with pytest.raises(ValueError):
        read_capture(root, receipt["capture_id"], approved_use=USE)


def test_retained_unknown_schema_can_be_verified_without_admitting_a_current_parser(tmp_path):
    import json

    root = tmp_path / "store"
    receipt = archive(root)
    path = root / "bronze/captures" / receipt["capture_id"] / "receipt.json"
    altered = json.loads(path.read_bytes())
    profile = altered["manifest"]["profiles"][0]
    profile["schema_profile"] = "finra-otc-ats-weekly-pipe-fixture/0"
    altered["schema"]["profile"] = profile
    altered["schema"]["schema_profile"] = profile["schema_profile"]
    reseal(path, altered)
    assert read_capture(root, receipt["capture_id"], approved_use=USE).parts["file"] == VALID
    result = validate_capture(root, receipt["capture_id"], approved_use=USE)
    assert result.outcome["status"] == "REJECTED" and result.outcome["row_count"] is None


def test_capture_snapshots_mutable_request_inputs_before_validation_yields(tmp_path, monkeypatch):
    headers = {"Content-Type": "text/plain"}
    parts, expected = [Part("file", VALID, response_headers=headers)], ["file"]
    original, changed = bronze.make_receipt, False

    def mutate_after_first_validation(**kwargs):
        nonlocal changed
        result = original(**kwargs)
        if not changed:
            changed = True
            parts.clear()
            expected.append("not-requested")
            headers["Content-Type"] = "changed"
        return result

    monkeypatch.setattr(bronze, "make_receipt", mutate_after_first_validation)
    receipt = archive(tmp_path / "store", parts=parts, expected_parts=expected)
    capture = read_capture(tmp_path / "store", receipt["capture_id"], approved_use=USE)
    assert capture.parts == {"file": VALID}
    assert receipt["expected_parts"] == ["file"]
    assert receipt["parts"][0]["headers"] == {"content-type": "text/plain"}


def test_reader_budget_is_enforced_and_failed_clock_does_not_rewrite_store(tmp_path):
    root = tmp_path / "store"
    receipt = archive(root)
    before = state(root)
    with pytest.raises(ValueError):
        read_capture(root, receipt["capture_id"], approved_use=USE, max_bytes=len(VALID) - 1)
    with pytest.raises(ValueError, match="clock"):
        archive(root, captured_at=CLOCK - timedelta(days=1))
    assert state(root) == before


def test_default_capture_clock_is_not_report_week_or_http_last_modified(tmp_path, monkeypatch):
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return CLOCK

    monkeypatch.setattr(bronze, "datetime", FixedClock)
    receipt = archive(
        tmp_path / "store",
        captured_at=None,
        parts=[
            Part("file", VALID, response_headers={"Last-Modified": "Mon, 19 Oct 2026 00:00:00 GMT"})
        ],
    )
    assert receipt["captured_at"] == "2026-11-15T18:00:00.000000Z"
    assert receipt["provider_published_at"] is None
    assert receipt["parts"][0]["retrieved_at"] is None


@pytest.mark.parametrize("route", ["approved_local_file", "finra_query_api"])
def test_real_retention_cannot_backdate_the_archive_clock(tmp_path, route):
    grant = {
        **USE,
        "route": route,
        "permission_reference": "invented-clock-test-permission",
        "attribution": "FINRA",
        "operations": ["retain"],
        "authorization": "provider_permission",
    }
    if route == "finra_query_api":
        grant.update(
            authorization="accepted_api_terms",
            credential_type="public",
            provider_dataset="weeklySummary",
        )
    root = tmp_path / "store"
    with pytest.raises(ValueError, match="clock"):
        archive(root, approved_use=grant, captured_at=CLOCK)
    assert not root.exists()
