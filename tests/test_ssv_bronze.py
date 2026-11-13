"""SSV-AC05: invented raw captures, sanitized evidence and rejection durability."""

import json
import socket
from datetime import UTC, datetime

import pytest
from test_ssv_manifest import profile
from test_ssv_parser import FIXTURES, receipt
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import (
    public_receipt,
    reject_credentials,
)

__all__ = ["manifest"]
NOW = datetime(2026, 11, 13, 15, tzinfo=UTC)
RUN = "00000000-0000-0000-0000-000000000001"
CAPTURE = "00000000-0000-0000-0000-000000000002"
SECRET = "invented-only-sensitive-123456789"


def fixture_grant():
    return profile()


def provider_grant():
    return profile(
        "finra_query_api",
        authorization="accepted_api_terms",
        permission_reference="invented-permission",
        credential_type="public",
        operations=["local_import", "transform", "acquire"],
    )


def test_ssv_ac05_sanitizes_opaque_credentials_without_changing_public_query(manifest):
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    value = receipt(payload)
    value.update(
        authorization="Bearer " + SECRET,
        proxy_url="https://user:" + SECRET + "@proxy.test",
        captured_at=SECRET,
    )
    value["headers"].update(
        {
            "Authorization": "Bearer " + SECRET,
            "Set-Cookie": SECRET,
            "Location": "https://x.test/?token=" + SECRET,
            "X-Diagnostic": SECRET,
        }
    )
    public, issues = public_receipt(value, payload, manifest, max_rows=5000)
    assert issues == [] and public["request_payload"] == value["request_payload"]
    assert set(public) == {
        "url",
        "method",
        "request_data_version",
        "request_payload",
        "http_status",
        "headers",
        "bytes",
        "sha256",
    }
    assert SECRET not in json.dumps(public)
    assert set(public["headers"]) == {
        "content-type",
        "data-version",
        "record-total",
        "record-offset",
        "record-limit",
    }


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(
            url="https://user:" + SECRET + "@api.finra.org/data/group/otcMarket/name/regShoDaily"
        ),
        lambda r: r.update(url="https://api.finra.org/oauth/token"),
        lambda r: r["request_payload"].update(client_secret=SECRET),
        lambda r: r["request_payload"]["compareFilters"][1].update(fieldValue="D"),
    ],
)
def test_ssv_ac05_unadmitted_request_scope_is_not_sanitized_into_valid_data(manifest, change):
    payload = b"[]"
    value = receipt(payload, total=0)
    change(value)
    with pytest.raises(ValueError) as error:
        public_receipt(value, payload, manifest, max_rows=5000)
    assert SECRET not in str(error.value)


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(sha256=SECRET),
        lambda r: r.update(bytes=True),
        lambda r: r["headers"].update({"Record-Limit": SECRET}),
        lambda r: r["headers"].update({"record-limit": "2"}),
        lambda r: r["headers"].update({"Content-Type": "application/json; token=" + SECRET}),
        lambda r: r.update(http_status=SECRET),
    ],
)
def test_ssv_ac05_bad_response_evidence_is_flagged_without_persisting_secret(manifest, change):
    payload = b"[]"
    value = receipt(payload, total=0)
    change(value)
    public, issues = public_receipt(value, payload, manifest, max_rows=5000)
    assert issues and SECRET not in json.dumps(public)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"access_token":"' + SECRET.encode() + b'"}',
        b'{"nested":{"clientSecret":"' + SECRET.encode() + b'"}}',
        b'{"api_client_secret":"' + SECRET.encode() + b'"}',
        b'{"FINRA_API_CLIENT_SECRET":"' + SECRET.encode() + b'"}',
        b'{"\\u0061ccess_token":"' + SECRET.encode() + b'"}',
        b"Bearer " + SECRET.encode(),
        b"Basic aW52ZW50ZWQ6c2VjcmV0",
    ],
)
def test_ssv_ac05_credential_bodies_are_refused_without_echo(payload):
    with pytest.raises(ValueError) as error:
        reject_credentials(payload)
    assert SECRET not in str(error.value)


def test_ssv_ac05_security_scan_does_not_normalize_or_reject_data_values():
    for payload in (
        (FIXTURES / "invented-valid.json").read_bytes(),
        b"not JSON",
        b'[{"ordinary":"password"}]',
    ):
        assert reject_credentials(payload) is None


def archive(root, payload, manifest, **changes):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import archive_input

    args = {
        "approved_use": fixture_grant(),
        "operation": "fixture",
        "run_id": RUN,
        "capture_id": CAPTURE,
        "archived_at": NOW,
    }
    args.update(changes)
    return archive_input(root, payload, manifest, receipt(payload), **args)


def test_ssv_ac05_complete_capture_precedes_parser_and_preserves_exact_quantities(
    tmp_path, manifest, monkeypatch
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    saved = archive(tmp_path, payload, manifest)
    original = bronze.parse_page

    def parse(raw, m, r, **limits):
        stored, body = bronze.read_capture(
            tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
        )
        assert stored == saved and body == raw == payload
        return original(raw, m, r, **limits)

    monkeypatch.setattr(bronze, "parse_page", parse)
    monkeypatch.setattr(
        socket, "create_connection", lambda *a, **k: pytest.fail("Bronze attempted network")
    )
    page, outcome = bronze.parse_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert outcome["status"] == "parsed" and outcome["page"]["row_count"] == 2
    assert outcome["published"] is False and page.rows[0].short_volume == 40
    assert saved["provider_published_at"] is None and saved["retrieved_at"] is None
    assert saved["archived_at"] == "2026-11-13T15:00:00.000000Z" and "first_seen_at" not in saved
    assert (
        saved["manifest"] == manifest.to_dict() and saved["source_schema_id"] == manifest.schema_id
    )
    assert bronze.parse_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    ) == (page, outcome)
    assert len(list((tmp_path / "bronze" / "outcomes" / CAPTURE).glob("*.json"))) == 1


@pytest.mark.parametrize(
    "payload", [b"[{", b"<html>challenge</html>", b"not JSON", b"", b'[{"unreviewed":true}]']
)
def test_ssv_ac05_parse_rejection_keeps_exact_original_bytes(tmp_path, manifest, payload):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
        parse_capture,
        read_capture,
    )

    saved = archive(tmp_path, payload, manifest)
    page, outcome = parse_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert (
        page is None
        and outcome["status"] == "rejected"
        and outcome["error_code"] == "parse_rejected"
    )
    assert outcome["published"] is False
    assert read_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    ) == (saved, payload)


def test_ssv_ac05_invalid_response_claims_are_archived_but_not_promoted(tmp_path, manifest):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
        archive_input,
        parse_capture,
    )

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    r = receipt(payload)
    r["sha256"] = SECRET
    archive_input(
        tmp_path,
        payload,
        manifest,
        r,
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=RUN,
        capture_id=CAPTURE,
        archived_at=NOW,
    )
    page, outcome = parse_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert page is None and outcome["error_code"] == "receipt_rejected"
    assert not any(SECRET.encode() in p.read_bytes() for p in tmp_path.rglob("*") if p.is_file())


def test_ssv_ac05_admission_and_security_refuse_before_creating_root(tmp_path, manifest):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
        archive_input,
        parse_capture,
        read_capture,
    )

    root = tmp_path / "absent"
    payload = b"[]"
    for fn in [
        lambda: archive_input(
            root,
            payload,
            manifest,
            receipt(payload, total=0),
            approved_use=None,
            operation="local_import",
            run_id=RUN,
        ),
        lambda: read_capture(root, CAPTURE, manifest, approved_use=None, operation="local_import"),
        lambda: parse_capture(root, CAPTURE, manifest, approved_use=None, operation="transform"),
    ]:
        with pytest.raises(PermissionError):
            fn()
        assert not root.exists()
    with pytest.raises(ValueError):
        archive(root, b'{"access_token":"' + SECRET.encode() + b'"}', manifest)
    assert not root.exists()
    with pytest.raises(ValueError):
        archive(root, b"[]", manifest, max_bytes=1)
    assert not root.exists()


def test_ssv_ac05_provider_and_fixture_captures_cannot_be_relabelled(tmp_path, manifest):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import read_capture

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    saved = archive(
        tmp_path, payload, manifest, approved_use=provider_grant(), operation="local_import"
    )
    assert saved["input_kind"] == "provider_api"
    assert (
        read_capture(
            tmp_path, CAPTURE, manifest, approved_use=provider_grant(), operation="transform"
        )[1]
        == payload
    )
    with pytest.raises(PermissionError):
        read_capture(tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture")


def test_ssv_ac05_retry_preserves_clock_sequence_and_refuses_conflicting_capture(
    tmp_path, manifest
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import archive_input

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    saved = archive(tmp_path, payload, manifest)
    repeated = archive_input(
        tmp_path,
        payload,
        manifest,
        receipt(payload),
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=RUN,
        capture_id=CAPTURE,
    )
    assert repeated == saved and saved["capture_sequence"] == 1
    before = {
        str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
    }
    with pytest.raises(ValueError):
        archive(tmp_path, payload + b" ", manifest)
    assert {
        str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
    } == before
    next_saved = archive(
        tmp_path, payload, manifest, capture_id="00000000-0000-0000-0000-000000000003"
    )
    assert (
        next_saved["capture_sequence"] == 2
        and next_saved["payload_sha256"] == saved["payload_sha256"]
    )


@pytest.mark.parametrize("name", ["payload.bin", "receipt.json"])
def test_ssv_ac05_corrupt_capture_is_refused_not_repaired(tmp_path, manifest, name):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import read_capture

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    archive(tmp_path, payload, manifest)
    path = tmp_path / "bronze" / "captures" / CAPTURE / name
    path.write_bytes(b"corrupt")
    with pytest.raises(ValueError):
        read_capture(tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture")
    assert path.read_bytes() == b"corrupt"


@pytest.mark.parametrize("identity", ["../escape", "not-a-uuid", CAPTURE.upper(), True])
def test_ssv_ac05_invalid_capture_paths_are_refused(tmp_path, manifest, identity):
    # This UUID contains digits only; use an uppercase UUID with letters for that case.
    if identity == CAPTURE.upper():
        identity = "ABCDEFAB-0000-0000-0000-000000000002"
    with pytest.raises(ValueError):
        archive(tmp_path, b"[]", manifest, capture_id=identity)
    assert not (tmp_path / "bronze").exists()


def test_ssv_ac05_unexpected_parser_failure_keeps_raw_and_can_resume(
    tmp_path, manifest, monkeypatch
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    archive(tmp_path, payload, manifest)
    original = bronze.parse_page

    def fail(*args, **kwargs):
        raise RuntimeError(SECRET)

    monkeypatch.setattr(bronze, "parse_page", fail)
    with pytest.raises(RuntimeError) as error:
        bronze.parse_capture(
            tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
        )
    assert SECRET not in str(error.value)
    assert not list((tmp_path / "bronze" / "outcomes").rglob("*.json"))
    monkeypatch.setattr(bronze, "parse_page", original)
    assert (
        bronze.parse_capture(
            tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
        )[1]["status"]
        == "parsed"
    )


def test_ssv_ac05_provider_payload_is_not_read_under_fixture_admission(
    tmp_path, manifest, monkeypatch
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    archive(tmp_path, payload, manifest, approved_use=provider_grant(), operation="local_import")
    original = bronze._read

    def read(path, limit):
        if path.name == "payload.bin":
            pytest.fail("fixture grant read provider payload")
        return original(path, limit)

    monkeypatch.setattr(bronze, "_read", read)
    with pytest.raises(PermissionError):
        bronze.read_capture(
            tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
        )


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r.update(opaque_diagnostic=SECRET),
        lambda r: r["admission"].update(operation="transform"),
        lambda r: r["admission"].update(profile_sha256="bad"),
        lambda r: r.update(archived_at="1990-01-01"),
    ],
)
def test_ssv_ac05_rehashed_but_invalid_metadata_is_still_refused(tmp_path, manifest, change):
    from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import read_capture

    saved = archive(tmp_path, (FIXTURES / "invented-valid.json").read_bytes(), manifest)
    change(saved)
    saved.pop("receipt_sha256")
    saved["receipt_sha256"] = digest(saved)
    (tmp_path / "bronze" / "captures" / CAPTURE / "receipt.json").write_bytes(encoded(saved))
    with pytest.raises(ValueError):
        read_capture(tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture")


def test_ssv_ac05_interrupted_capture_publish_leaves_gap_and_no_partial_capture(
    tmp_path, manifest, monkeypatch
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    original = bronze.os.rename

    def fail(source, target):
        if target.parent.name == "captures":
            raise OSError("invented publish failure")
        return original(source, target)

    monkeypatch.setattr(bronze.os, "rename", fail)
    with pytest.raises(OSError):
        archive(tmp_path, payload, manifest)
    assert not (tmp_path / "bronze" / "captures" / CAPTURE).exists()
    assert not list((tmp_path / "bronze" / "captures").iterdir())
    monkeypatch.setattr(bronze.os, "rename", original)
    assert archive(tmp_path, payload, manifest)["capture_sequence"] == 2


def test_ssv_ac05_concurrent_captures_have_distinct_monotonic_sequences(tmp_path, manifest):
    from concurrent.futures import ThreadPoolExecutor

    payload = (FIXTURES / "invented-valid.json").read_bytes()

    def save(index):
        return archive(
            tmp_path, payload, manifest, capture_id=f"00000000-0000-0000-0000-{index:012d}"
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        values = list(pool.map(save, range(10, 18)))
    assert sorted(v["capture_sequence"] for v in values) == list(range(1, 9))
    assert len({v["capture_id"] for v in values}) == 8
    assert len({v["payload_sha256"] for v in values}) == 1


def test_ssv_ac05_historical_input_and_explicit_retrieval_completion_are_not_backdated(
    tmp_path, manifest
):
    from datetime import timedelta

    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
        archive_input,
        parse_capture,
    )

    payload = (FIXTURES / "invented-valid.json").read_bytes().replace(b"2026-11-09", b"1990-01-02")
    r = receipt(payload, day="1990-01-02")
    r["captured_at"] = "1990-01-02T12:00:00Z"
    saved = archive_input(
        tmp_path,
        payload,
        manifest,
        r,
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=RUN,
        capture_id=CAPTURE,
        archived_at=NOW,
        retrieved_at=NOW - timedelta(seconds=2),
    )
    assert saved["archived_at"].startswith("2026-11-13")
    assert saved["retrieved_at"] == "2026-11-13T14:59:58.000000Z"
    assert "captured_at" not in json.dumps(saved)
    page, _ = parse_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert page.trade_date.isoformat() == "1990-01-02"
    with pytest.raises(ValueError):
        archive(tmp_path / "future", payload, manifest, retrieved_at=NOW + timedelta(seconds=1))
    assert not (tmp_path / "future").exists()


def test_ssv_ac05_incomplete_response_and_partial_page_never_publish(tmp_path, manifest):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
        archive_input,
        parse_capture,
    )

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    r = receipt(payload)
    r["body_complete"] = False
    saved = archive_input(
        tmp_path,
        payload,
        manifest,
        r,
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=RUN,
        capture_id=CAPTURE,
        archived_at=NOW,
    )
    assert "incomplete_body" in saved["receipt_issues"]
    assert (
        parse_capture(
            tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
        )[1]["status"]
        == "rejected"
    )
    r = receipt(payload, total=200)
    other = "00000000-0000-0000-0000-000000000003"
    archive_input(
        tmp_path,
        payload,
        manifest,
        r,
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=RUN,
        capture_id=other,
        archived_at=NOW,
    )
    page, outcome = parse_capture(
        tmp_path, other, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert not page.partition_complete and not outcome["page"]["page_partition_complete"]
    assert outcome["published"] is False


def test_ssv_ac05_default_capture_time_is_recorded_after_writer_lock_wait(
    tmp_path, manifest, monkeypatch
):
    from contextlib import contextmanager
    from datetime import timedelta

    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    locked = False
    original_lock = bronze.writer_lock

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = NOW + (timedelta(minutes=5) if locked else timedelta())
            return cls.fromisoformat(value.isoformat())

    @contextmanager
    def delayed_lock(root):
        nonlocal locked
        with original_lock(root):
            locked = True
            try:
                yield
            finally:
                locked = False

    monkeypatch.setattr(bronze, "datetime", Clock)
    monkeypatch.setattr(bronze, "writer_lock", delayed_lock)
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    saved = bronze.archive_input(
        tmp_path,
        payload,
        manifest,
        receipt(payload),
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=RUN,
        capture_id=CAPTURE,
    )
    assert saved["archived_at"] == "2026-11-13T15:05:00.000000Z"


def test_ssv_ac05_symlinks_outcome_corruption_and_reservation_limit_are_refused(
    tmp_path, manifest, monkeypatch
):
    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    payload = (FIXTURES / "invented-valid.json").read_bytes()
    archive(tmp_path, payload, manifest)
    raw_path = tmp_path / "bronze" / "captures" / CAPTURE / "payload.bin"
    outside = tmp_path / "outside.bin"
    outside.write_bytes(payload)
    raw_path.unlink()
    raw_path.symlink_to(outside)
    with pytest.raises(ValueError):
        bronze.read_capture(
            tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
        )
    raw_path.unlink()
    raw_path.write_bytes(payload)
    _, outcome = bronze.parse_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    path = tmp_path / "bronze" / "outcomes" / CAPTURE / (outcome["profile_sha256"] + ".json")
    path.write_bytes(b"corrupt")
    with pytest.raises(ValueError):
        bronze.parse_capture(
            tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
        )
    assert path.read_bytes() == b"corrupt"
    monkeypatch.setattr(bronze, "MAX_RESERVATIONS", 1)
    with pytest.raises(ValueError, match="reservation limit"):
        archive(tmp_path, payload, manifest, capture_id="00000000-0000-0000-0000-000000000003")


@pytest.mark.parametrize(
    "fixture_name,status,total", [("invented-fractional.json", 200, 1), (None, 204, 0)]
)
def test_ssv_ac05_fractional_and_no_content_captures_roundtrip_privately(
    tmp_path, manifest, fixture_name, status, total
):
    from decimal import Decimal
    from stat import S_IMODE

    from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
        archive_input,
        parse_capture,
        read_capture,
    )

    payload = (FIXTURES / fixture_name).read_bytes() if fixture_name else b""
    saved = archive_input(
        tmp_path,
        payload,
        manifest,
        receipt(payload, status=status, total=total),
        approved_use=fixture_grant(),
        operation="fixture",
        run_id=RUN,
        capture_id=CAPTURE,
        archived_at=NOW,
    )
    page, outcome = parse_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    )
    assert read_capture(
        tmp_path, CAPTURE, manifest, approved_use=fixture_grant(), operation="fixture"
    ) == (saved, payload)
    assert page.partition_complete and len(page.rows) == total and outcome["status"] == "parsed"
    if fixture_name:
        assert page.rows[0].short_volume.as_tuple() == Decimal("12.345678").as_tuple()
        assert page.rows[0].short_exempt_volume.as_tuple() == Decimal("0.000001").as_tuple()
    for path in (tmp_path / "bronze").rglob("*"):
        assert S_IMODE(path.stat().st_mode) == (0o700 if path.is_dir() else 0o600)
