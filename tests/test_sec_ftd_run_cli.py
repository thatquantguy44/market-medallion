"""T04 implemented operator commands use only explicit pipeline configuration."""

import io
import json
import zipfile
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.cli import main


def config_file(tmp_path, *, valid=True):
    source = tmp_path / "fixture.zip"
    payload = Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes()
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    source.write_bytes(stream.getvalue() if valid else b"<html>invented challenge</html>")
    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    value.update(
        warehouse="sqlite",
        root=str(tmp_path / "root"),
        partitions={
            "fixture": {
                "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
                "input": str(source),
            }
        },
    )
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_run_gold_and_config_validation_report_only_implemented_capabilities(tmp_path, capsys):
    path = config_file(tmp_path)
    assert main(["validate-config", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["storage_implemented"] is True
    assert main(["run", "--config", str(path), "--partition", "fixture"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["inserted_versions"] == 2 and result["source_coverage"] == "NOT_CERTIFIED"
    assert "FIX.A" not in json.dumps(result)
    assert main(["gold", "--config", str(path), "--limit", "2"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["row_count"] == 2
    assert result["rows"][1]["quantity_fails"] == str(2**70)
    assert result["rows"][0]["prior_close_price_text"] == "12.3400"


def test_rejected_input_has_nonzero_exit_with_durable_audit_code(tmp_path, capsys):
    path = config_file(tmp_path, valid=False)
    assert main(["run", "--config", str(path), "--partition", "fixture"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["error_code"] == "INVALID_ARCHIVE"
    assert result["row_count"] == 0


def test_gold_before_ingestion_does_not_create_an_empty_warehouse(tmp_path, capsys):
    path = config_file(tmp_path)
    assert main(["gold", "--config", str(path), "--limit", "20"]) == 1
    captured = capsys.readouterr()
    assert not captured.out and "error" in json.loads(captured.err)
    assert not (tmp_path / "root").exists()
