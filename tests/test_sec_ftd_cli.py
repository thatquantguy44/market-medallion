"""Local documentary-profile inspection is separate from pipeline ingestion."""

import io
import json
import zipfile
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.cli import main

MANIFEST = Path("manifests/sec-fails-to-deliver.yml")


def test_validate_requires_no_data_input_and_reports_unverified_conformance(capsys):
    assert main(["validate", "--manifest", str(MANIFEST)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == "sec-ftd-six-column/1"
    assert result["provider_conformance"] == "UNVERIFIED"


def test_inspect_reads_a_bounded_local_archive_without_claiming_source_coverage(tmp_path, capsys):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "cnsfails202610a.txt",
            Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes(),
        )
    path = tmp_path / "invented.zip"
    path.write_bytes(buffer.getvalue())
    assert main(["inspect", "--manifest", str(MANIFEST), "--input", str(path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["row_count"] == 2
    assert result["observed_date_count"] == 2
    assert result["provider_conformance"] == "UNVERIFIED"
    assert result["source_coverage"] == "NOT_CERTIFIED"
    assert "FIX.A" not in json.dumps(result)
    assert sorted(item.name for item in tmp_path.iterdir()) == ["invented.zip"]


def test_inspect_failure_returns_nonzero_without_a_success_summary(tmp_path, capsys):
    path = tmp_path / "challenge.zip"
    path.write_bytes(b"<html>challenge</html>")
    assert main(["inspect", "--manifest", str(MANIFEST), "--input", str(path)]) == 1
    result = capsys.readouterr()
    assert not result.out
    assert "error" in json.loads(result.err)


def test_invalid_manifest_fails_before_trying_to_open_the_input(tmp_path, capsys):
    path = tmp_path / "invalid.yml"
    path.write_text("source: other\n")
    assert main(["inspect", "--manifest", str(path), "--input", str(tmp_path / "missing.zip")]) == 1
    result = capsys.readouterr()
    assert "manifest" in json.loads(result.err)["error"]


def test_missing_input_is_reported_as_a_local_file_error(tmp_path, capsys):
    assert (
        main(["inspect", "--manifest", str(MANIFEST), "--input", str(tmp_path / "missing.zip")])
        == 1
    )
    assert json.loads(capsys.readouterr().err)["error"] == "local input is unavailable"
