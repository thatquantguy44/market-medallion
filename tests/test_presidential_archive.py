"""PR016-R02/N02: immutable offline capture and rejection evidence."""

import hashlib
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture
from market_data_pipeline.pipelines.presidential_research.storage import load_capture

CATALOG = "config/presidential_research/sources.toml"
NOW = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)


def test_malformed_parse_preserves_exact_capture_and_separate_admission_clock(tmp_path):
    payload = b"observation_date,FEDFUNDS\n1970-01-01,NaN\n"
    with patch("socket.socket", side_effect=AssertionError("network must not be used")):
        receipt, outcome = import_local_capture(
            tmp_path,
            load_config(CATALOG),
            "FEDFUNDS_001",
            payload,
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            admitted_at=NOW,
            retrieved_at=None,
        )
    saved, original, config_bytes = load_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])
    assert original == payload
    assert saved["retrieved_at"] is None
    assert saved["admitted_at"] == "2026-11-13T12:00:00.000000Z"
    assert saved["first_seen_at"] == saved["admitted_at"]
    assert saved["capture_sequence"] == 1
    assert outcome["status"] == "REJECTED"
    assert hashlib.sha256(config_bytes).hexdigest() == saved["config_sha256"]


def test_bad_expected_digest_and_tampered_bytes_fail_closed(tmp_path):
    payload = b"observation_date,FEDFUNDS\n1970-01-01,8.98\n"
    config = load_config(CATALOG)
    with pytest.raises(ValueError, match="digest"):
        import_local_capture(
            tmp_path, config, "FEDFUNDS_001", payload, expected_sha256="0" * 64, admitted_at=NOW
        )
    receipt, outcome = import_local_capture(
        tmp_path,
        config,
        "FEDFUNDS_001",
        payload,
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        admitted_at=NOW,
        retrieved_at=datetime(2020, 1, 1, tzinfo=UTC),
    )
    assert outcome["status"] == "PARSED" and outcome["row_count"] == 1
    assert receipt["retrieved_at"] == "2020-01-01T00:00:00.000000Z"
    (tmp_path / "bronze" / "FEDFUNDS_001" / receipt["capture_id"] / "payload").write_bytes(
        b"tampered"
    )
    with pytest.raises(ValueError, match="digest"):
        load_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])


def test_unknown_source_and_path_escape_are_rejected(tmp_path):
    config = load_config(CATALOG)
    with pytest.raises(ValueError):
        import_local_capture(tmp_path, config, "../outside", b"x", admitted_at=NOW)
    with pytest.raises(ValueError):
        load_capture(tmp_path, "FEDFUNDS_001", "../../outside")


def test_future_retrieval_and_tampered_receipt_are_rejected(tmp_path):
    config = load_config(CATALOG)
    with pytest.raises(ValueError, match="retrieval"):
        import_local_capture(
            tmp_path,
            config,
            "FEDFUNDS_001",
            b"fixture",
            admitted_at=NOW,
            retrieved_at=datetime(2026, 11, 14, tzinfo=UTC),
        )
    receipt, _ = import_local_capture(
        tmp_path,
        config,
        "FEDFUNDS_001",
        b"fixture",
        admitted_at=NOW,
    )
    path = tmp_path / "bronze" / "FEDFUNDS_001" / receipt["capture_id"] / "receipt.json"
    import json

    value = json.loads(path.read_text())
    value["first_seen_at"] = "1930-01-01T00:00:00.000000Z"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="receipt"):
        load_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])


def test_factor_archive_rejects_excessive_expansion():
    import io
    from zipfile import ZIP_DEFLATED, ZipFile

    from market_data_pipeline.pipelines.presidential_research import parsers

    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("factors.csv", b"x" * 200)
    with patch.object(parsers, "MAX_FACTOR_TABLE_BYTES", 100):
        with pytest.raises(parsers.ParseError, match="size"):
            parsers.parse_fama_french_monthly_zip(buffer.getvalue(), "FF_MKT_001")


def test_capture_paths_reject_symlink_and_sequence_advances(tmp_path):
    config = load_config(CATALOG)
    first, _ = import_local_capture(tmp_path, config, "FEDFUNDS_001", b"x", admitted_at=NOW)
    second, _ = import_local_capture(tmp_path, config, "FEDFUNDS_001", b"y", admitted_at=NOW)
    assert [first["capture_sequence"], second["capture_sequence"]] == [1, 2]
    capture = tmp_path / "bronze" / "FEDFUNDS_001" / first["capture_id"]
    (capture / "payload").unlink()
    (capture / "payload").symlink_to(capture / "config.toml")
    with pytest.raises(ValueError, match="Symlink"):
        load_capture(tmp_path, "FEDFUNDS_001", first["capture_id"])
