"""S11-T21 SEC FTD Delta publication contract and actual local round trip."""

import io
import json
import os
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.delta import (
    DeltaPathTarget,
    publish_configured_delta,
    publish_delta_snapshot,
    read_delta_generation,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import run
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import current_snapshot

pytestmark = pytest.mark.delta
CLOCK = datetime(2026, 11, 5, 12, tzinfo=UTC)


def _config(tmp_path):
    source = tmp_path / "fixture.zip"
    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    value.update(
        warehouse="sqlite",
        root=str(tmp_path / "warehouse"),
        partitions={
            "fixture": {
                "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
                "input": str(source),
                "coverage_evidence": None,
                "source_url": None,
            }
        },
    )
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path, source


def _bundle(path, payload):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    path.write_bytes(stream.getvalue())


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    if os.environ.get("MARKET_DATA_TEST_DELTA") != "true":
        pytest.skip("set MARKET_DATA_TEST_DELTA=true to run the local Spark/Delta integration")
    pyspark = pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("market-medallion-sec-ftd-delta-test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def test_delta_target_rejects_unsafe_prefix_and_nonempty_unbound_root(tmp_path):
    with pytest.raises(ValueError, match="prefix"):
        DeltaPathTarget(tmp_path / "delta", "bad-prefix", "0" * 64)
    root = tmp_path / "occupied"
    root.mkdir()
    (root / "unrelated.txt").write_text("do not replace")
    with pytest.raises(ValueError, match="nonempty"):
        DeltaPathTarget(root, "sec_ftd", "0" * 64).bind()
    link = tmp_path / "linked"
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic"):
        DeltaPathTarget(link, "sec_ftd", "0" * 64)


def test_configured_delta_publisher_requires_explicit_enablement(tmp_path):
    config, _source = _config(tmp_path)
    with pytest.raises(ValueError, match="disabled"):
        publish_configured_delta(None, config)


def test_actual_delta_round_trip_is_idempotent_and_recovers_partial_publication(tmp_path, spark):
    config, source = _config(tmp_path)
    fixture = Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes()
    _bundle(source, fixture)
    run(config, "fixture", clock=lambda: CLOCK)
    first_snapshot = current_snapshot(tmp_path / "warehouse")
    target = DeltaPathTarget(
        tmp_path / "delta", "sec_ftd", load_config(config).storage.contract_id
    ).bind()

    first = publish_delta_snapshot(spark, config, first_snapshot, target)
    assert first["status"] == "PUBLISHED"
    assert first["reused"] is False
    current_before = json.loads((target.root / "current.json").read_text())
    frames = read_delta_generation(spark, target)
    assert {name: frame.count() for name, frame in frames.items()} == {
        "facts": 2,
        "coverage": 1,
        "audit": 1,
    }
    fact_rows = {row.cusip: row for row in frames["facts"].collect()}
    assert set(fact_rows) == {"000000001", "000000002"}
    assert fact_rows["000000001"].quantity_fails == Decimal(125)
    assert fact_rows["000000001"].quantity_fails_text == "125"
    assert fact_rows["000000001"].prior_close_price == Decimal("12.340000000000")
    assert fact_rows["000000002"].quantity_fails == Decimal("1180591620717411303424")
    assert fact_rows["000000002"].quantity_fails > 2**63
    assert fact_rows["000000002"].prior_close_price is None
    assert fact_rows["000000002"].prior_close_price_text == "."
    assert fact_rows["000000002"].price_missing_reason == "SOURCE_PRICE_UNAVAILABLE"
    first_seen_micros = (
        frames["facts"]
        .filter("cusip = '000000001'")
        .selectExpr("unix_micros(first_seen_at) AS value")
        .first()["value"]
    )
    assert first_seen_micros == int(CLOCK.timestamp() * 1_000_000)

    other_root = tmp_path / "other-root"
    other_root.mkdir()
    other_config, other_source = _config(other_root)
    _bundle(other_source, fixture)
    run(other_config, "fixture", clock=lambda: CLOCK)
    with pytest.raises(ValueError, match="different source warehouse"):
        publish_delta_snapshot(
            spark,
            other_config,
            current_snapshot(other_root / "warehouse"),
            target,
        )

    repeated = publish_delta_snapshot(spark, config, first_snapshot, target)
    assert repeated["reused"] is True
    assert json.loads((target.root / "current.json").read_text()) == current_before

    _bundle(source, fixture.replace(b"12.3400", b"12.3500"))
    run(config, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))
    second_snapshot = current_snapshot(tmp_path / "warehouse")
    with pytest.raises(RuntimeError, match="injected"):
        publish_delta_snapshot(
            spark,
            config,
            second_snapshot,
            target,
            fail_after_table="facts",
        )

    assert json.loads((target.root / "current.json").read_text()) == current_before
    pinned = read_delta_generation(spark, target)
    assert pinned["facts"].count() == 2

    recovered = publish_delta_snapshot(spark, config, second_snapshot, target)
    assert recovered["status"] == "PUBLISHED"
    assert recovered["reused"] is False
    latest = read_delta_generation(spark, target)
    assert latest["facts"].count() == 3
    assert latest["coverage"].count() == 2
    assert latest["audit"].count() == 2
    generation = json.loads(
        (
            target.root / json.loads((target.root / "current.json").read_text())["generation"]
        ).read_text()
    )
    assert generation["source_snapshot_id"] == second_snapshot.name
    assert (
        generation["source_warehouse_id"]
        == json.loads((second_snapshot / "manifest.json").read_text())["warehouse_id"]
    )
    assert set(generation["tables"]) == {"facts", "coverage", "audit"}
    assert all(type(table["version"]) is int for table in generation["tables"].values())
    assert generation["engine"] == {
        "spark_version": "4.0.1",
        "delta_spark_version": "4.0.1",
        "target_mode": "path",
    }
