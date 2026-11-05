"""S11-T07: coherent typed Parquet generations and deterministic recovery."""

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import run
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import (
    current_snapshot,
    publish_config,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import SQLiteWarehouse


def configured(tmp_path):
    source = tmp_path / "fixture.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            "cnsfails202610a.txt",
            Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes(),
        )
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
    return path, source


def test_run_publishes_exact_typed_generation_and_database_watermark(tmp_path):
    path, _ = configured(tmp_path)
    result = run(path, "fixture")
    config = load_config(path)
    snapshot = current_snapshot(config.root)
    assert result["snapshot"] == str(snapshot)
    manifest = json.loads((snapshot / "manifest.json").read_text())
    assert manifest["publication_version"] == "sec-ftd-parquet/1"
    assert manifest["run_id"] == result["run_id"]
    assert manifest["row_counts"] == {"audit": 1, "coverage": 1, "facts": 2, "gold": 2}
    for name in ("facts", "gold", "coverage", "audit"):
        table = pq.read_table(snapshot / f"{name}.parquet")
        assert table.num_rows == manifest["row_counts"][name]
        assert table.schema.metadata[b"storage_contract_id"].decode() == config.storage.contract_id
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        publication = warehouse.select_publications()[-1]
        assert publication["status"] == "PUBLISHED"
        assert publication["published_at"] is not None


def test_partial_file_generation_does_not_change_current_pointer(tmp_path, monkeypatch):
    path, source = configured(tmp_path)
    run(path, "fixture")
    config = load_config(path)
    pointer = (config.root / "parquet/current.json").read_bytes()
    payload = (
        Path("tests/fixtures/sec_fails_to_deliver/modern.txt")
        .read_bytes()
        .replace(b"12.3400", b"12.3500")
    )
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    source.write_bytes(stream.getvalue())
    original = pq.write_table

    def fail_coverage(table, where, **kwargs):
        if Path(where).name == "coverage.parquet":
            raise OSError("injected parquet failure")
        return original(table, where, **kwargs)

    monkeypatch.setattr(pq, "write_table", fail_coverage)
    with pytest.raises(OSError, match="injected"):
        run(path, "fixture")
    assert (config.root / "parquet/current.json").read_bytes() == pointer
    monkeypatch.undo()
    repaired = publish_config(path)
    assert repaired["status"] == "PUBLISHED"
    assert (config.root / "parquet/current.json").read_bytes() != pointer
    assert pq.read_table(current_snapshot(config.root) / "facts.parquet").num_rows == 3


def test_committed_generation_is_finished_after_pointer_failure(tmp_path, monkeypatch):
    path, source = configured(tmp_path)
    run(path, "fixture")
    config = load_config(path)
    pointer = (config.root / "parquet/current.json").read_bytes()
    payload = (
        Path("tests/fixtures/sec_fails_to_deliver/modern.txt")
        .read_bytes()
        .replace(b"12.3400", b"12.3500")
    )
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    import market_data_pipeline.pipelines.sec_fails_to_deliver.publication as publication

    def fail_pointer(*_args, **_kwargs):
        raise OSError("injected pointer failure")

    monkeypatch.setattr(publication, "_publish_pointer", fail_pointer)
    with pytest.raises(OSError, match="pointer"):
        run(path, "fixture")
    assert (config.root / "parquet/current.json").read_bytes() == pointer
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert warehouse.pending_publications()[-1]["status"] == "COMMITTED"
    monkeypatch.undo()
    repaired = publish_config(path)
    assert repaired["recovered"] is True
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        assert warehouse.pending_publications() == []
        assert warehouse.select_publications()[-1]["status"] == "PUBLISHED"


def test_pointer_written_before_watermark_failure_is_idempotently_recovered(tmp_path, monkeypatch):
    path, source = configured(tmp_path)
    run(path, "fixture")
    payload = (
        Path("tests/fixtures/sec_fails_to_deliver/modern.txt")
        .read_bytes()
        .replace(b"12.3400", b"12.3500")
    )
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    from market_data_pipeline.pipelines.sec_fails_to_deliver import publication

    original = SQLiteWarehouse.transition_publication

    def fail_published(self, snapshot_id, status, published_at=None):
        if status == "PUBLISHED":
            raise OSError("injected watermark failure")
        return original(self, snapshot_id, status, published_at)

    monkeypatch.setattr(SQLiteWarehouse, "transition_publication", fail_published)
    with pytest.raises(OSError, match="watermark"):
        run(path, "fixture")
    selected = publication.current_snapshot(load_config(path).root)
    assert pq.read_table(selected / "facts.parquet").num_rows == 3
    monkeypatch.undo()
    assert publish_config(path)["recovered"] is True


def test_next_capture_recovers_prior_generation_then_publishes_its_own_state(tmp_path, monkeypatch):
    path, source = configured(tmp_path)
    run(path, "fixture")
    payload = (
        Path("tests/fixtures/sec_fails_to_deliver/modern.txt")
        .read_bytes()
        .replace(b"12.3400", b"12.3500")
    )
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    import market_data_pipeline.pipelines.sec_fails_to_deliver.publication as publication

    def fail_pointer(*_args, **_kwargs):
        raise OSError("injected pointer failure")

    monkeypatch.setattr(publication, "_publish_pointer", fail_pointer)
    with pytest.raises(OSError):
        run(path, "fixture")
    monkeypatch.undo()
    latest = run(path, "fixture")
    manifest = json.loads((current_snapshot(load_config(path).root) / "manifest.json").read_text())
    assert manifest["run_id"] == latest["run_id"]
    assert manifest["capture_sequence"] == latest["capture_sequence"] == 3


def test_corrupt_staged_generation_is_abandoned_without_moving_pointer(tmp_path):
    path, _ = configured(tmp_path)
    run(path, "fixture")
    config = load_config(path)
    pointer = (config.root / "parquet/current.json").read_bytes()
    current = current_snapshot(config.root)
    malformed = json.loads((current / "manifest.json").read_text())
    malformed.update(snapshot_id="f" * 32, row_counts=1)
    payload = json.dumps(malformed).encode()
    stage = config.root / "parquet/.staging" / ("f" * 32)
    stage.mkdir(parents=True)
    (stage / "manifest.json").write_bytes(payload)
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        latest = warehouse.select_runs()[-1]
        row = {
            "source": "sec",
            "dataset": "sec-fails-to-deliver",
            "reporting_regime": "semi-monthly-2009+",
            "snapshot_id": "f" * 32,
            "run_id": latest["run_id"],
            "status": "STAGED",
            "generated_at": latest["completed_at"],
            "published_at": None,
            "manifest_sha256": hashlib.sha256(payload).hexdigest(),
            "snapshot_path": f"parquet/snapshots/{'f' * 32}",
        }
        warehouse.record_publication(row)
    result = publish_config(path)
    assert result["recovered"] is False
    assert (config.root / "parquet/current.json").read_bytes() != pointer
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        statuses = {row["snapshot_id"]: row["status"] for row in warehouse.select_publications()}
        assert statuses["f" * 32] == "ABANDONED"
