"""S11-T22/A15 actual Spark/Delta history, coverage and recovery evidence."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from test_reg_sho_pipeline import _config

from market_data_pipeline.pipelines.reg_sho_thresholds import delta as publisher
from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.delta_snapshot import (
    stage_delta_snapshot,
    validate_snapshot,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.pipeline import gold, run
from market_data_pipeline.pipelines.reg_sho_thresholds.storage import storage_schemas

pytestmark = pytest.mark.delta
CLOCK = datetime(2026, 11, 5, 12, 0, 0, 123456, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    if os.environ.get("MARKET_DATA_TEST_DELTA") != "true":
        pytest.skip("set MARKET_DATA_TEST_DELTA=true for actual local Spark/Delta verification")
    pyspark = pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    builder = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("market-medallion-threshold-delta-test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("spark-warehouse")))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


def _normalized(rows):
    def convert(value):
        if isinstance(value, datetime):
            return value.astimezone(UTC).isoformat()
        return value

    return sorted(
        json.dumps({key: convert(value) for key, value in row.items()}, sort_keys=True, default=str)
        for row in rows
    )


def _assert_exact(spark, target, config, snapshot):
    manifest, tables = validate_snapshot(load_config(config), snapshot)
    frames = publisher.read_delta_generation(spark, target)
    for name, frame in frames.items():
        assert _normalized([row.asDict() for row in frame.collect()]) == _normalized(
            tables[name].to_pylist()
        ), name
    return manifest, frames


def _latest(frame, keys, sequence):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy(*keys).orderBy(F.desc("first_seen_at"), F.desc("capture_sequence"))
    return (
        frame.filter(F.col("capture_sequence") <= sequence)
        .withColumn("rank", F.row_number().over(window))
        .filter("rank = 1")
        .drop("rank")
    )


def test_actual_delta_exact_history_cutoffs_and_interrupted_generations(
    tmp_path, spark, monkeypatch
):
    config, source = _config(tmp_path)
    original = source.read_bytes()
    value = yaml.safe_load(config.read_text())
    value["partitions"]["nyse"] = {
        "provider": "nyse",
        "market": "NYSE American",
        "list_date": "2026-11-03",
        "manifest": str(Path("manifests/reg-sho-nyse.yml").resolve()),
        "input": str(tmp_path / "nyse.txt"),
        "source_url": None,
    }
    nyse = tmp_path / "nyse.txt"
    nyse.write_bytes(
        Path("tests/fixtures/reg_sho_thresholds/nyse-american-complete.txt").read_bytes()
    )
    for day in (4, 5, 6, 7):
        value["partitions"][f"day{day}"] = {
            **value["partitions"]["fixture"],
            "list_date": f"2026-11-{day:02d}",
            "input": str(tmp_path / f"day{day}.txt"),
            "source_url": None,
        }
    config.write_text(yaml.safe_dump(value))
    run(config, "fixture", clock=lambda: CLOCK)
    run(config, "nyse", clock=lambda: CLOCK)
    first = stage_delta_snapshot(config)
    target = publisher.DeltaPathTarget(
        tmp_path / "delta", "reg_sho", storage_schemas()["membership"].contract_id
    ).bind()
    assert publisher.publish_delta_snapshot(spark, config, first, target)["reused"] is False
    _manifest, frames = _assert_exact(spark, target, config, first)
    assert frames["membership"].count() == 4
    assert frames["episodes"].count() == 4
    assert {row.provider for row in frames["membership"].collect()} == {"nasdaq", "nyse"}
    assert frames["membership"].selectExpr(
        "unix_micros(first_seen_at) AS clock"
    ).first().clock == int(CLOCK.timestamp() * 1_000_000)
    pointer = (target.root / "current.json").read_bytes()
    assert publisher.publish_delta_snapshot(spark, config, first, target)["reused"] is True
    assert (target.root / "current.json").read_bytes() == pointer

    source.write_bytes(
        original.replace(b"ALFA INVENTED CORPORATION", b"ALFA REVISED").replace(
            b"BETA|BETA INVENTED FUND|G|Y|Y|\n", b""
        )
    )
    run(config, "fixture", clock=lambda: CLOCK)
    run(config, "day4", clock=lambda: CLOCK)  # missing is UNAVAILABLE
    (tmp_path / "day5.txt").write_bytes(original.replace(b"20261103", b"20261105"))
    run(config, "day5", clock=lambda: CLOCK)
    (tmp_path / "day6.txt").write_bytes(original.splitlines(keepends=True)[0] + b"20261106230004\n")
    run(config, "day6", clock=lambda: CLOCK)
    (tmp_path / "day7.txt").write_bytes(b"invalid provider bytes")
    run(config, "day7", clock=lambda: CLOCK)
    nyse.write_bytes(nyse.read_bytes().splitlines(keepends=True)[0] + b"20261103220201\n")
    run(config, "nyse", clock=lambda: CLOCK)
    second = stage_delta_snapshot(config)
    with pytest.raises(RuntimeError, match="injected"):
        publisher.publish_delta_snapshot(
            spark, config, second, target, fail_after_table="membership"
        )
    assert (target.root / "current.json").read_bytes() == pointer
    _assert_exact(spark, target, config, first)
    publisher.publish_delta_snapshot(spark, config, second, target)
    manifest, frames = _assert_exact(spark, target, config, second)
    assert manifest["capture_sequence"] == 8
    assert frames["membership"].count() == 10
    assert {row.coverage_state for row in frames["coverage"].collect()} == {
        "COMPLETE",
        "COMPLETE_EMPTY",
        "UNAVAILABLE",
        "FAILED",
    }
    episodes = [row.asDict() for row in frames["episodes"].collect()]
    assert len(episodes) == 3
    assert any(row["gap_after"] and row["right_censored"] for row in episodes)
    assert any(row["gap_before"] and row["exit_confirmed"] for row in episodes)
    observed = gold(config, as_of=CLOCK, as_of_sequence=2)
    members = _latest(
        frames["membership"], ["provider", "market", "list_date", "raw_symbol", "rule_family"], 2
    ).filter("row_state = 'REPORTED'")
    assert _normalized([row.asDict() for row in members.collect()]) == _normalized(observed["rows"])
    coverage = _latest(frames["coverage"], ["partition_id"], 2)
    assert _normalized([row.asDict() for row in coverage.collect()]) == _normalized(
        observed["coverage"]
    )
    with pytest.raises(ValueError, match="roll back"):
        publisher.publish_delta_snapshot(spark, config, first, target)

    # Reversion is another observed version, even when all capture clocks tie.
    source.write_bytes(original)
    run(config, "fixture", clock=lambda: CLOCK)
    third = stage_delta_snapshot(config)
    prior_pointer = (target.root / "current.json").read_bytes()
    atomic_json = publisher.atomic_json

    def fail_pointer(path, value):
        if path == target.root / "current.json":
            raise OSError("injected pointer failure")
        return atomic_json(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(publisher, "atomic_json", fail_pointer)
        with pytest.raises(OSError, match="pointer failure"):
            publisher.publish_delta_snapshot(spark, config, third, target)
    assert (target.root / "current.json").read_bytes() == prior_pointer
    _assert_exact(spark, target, config, second)
    sealed = (target.root / "generations" / f"{third.name}.json").read_bytes()
    assert publisher.publish_delta_snapshot(spark, config, third, target)["reused"] is True
    assert (target.root / "generations" / f"{third.name}.json").read_bytes() == sealed
    _assert_exact(spark, target, config, third)

    other = tmp_path / "other"
    other.mkdir()
    other_config, _ = _config(other)
    run(other_config, "fixture", clock=lambda: CLOCK)
    with pytest.raises(ValueError, match="different source warehouse"):
        publisher.publish_delta_snapshot(
            spark, other_config, stage_delta_snapshot(other_config), target
        )


def test_actual_delta_empty_membership_and_episode_tables(tmp_path, spark, monkeypatch):
    config, source = _config(tmp_path, "nasdaq-empty.txt")
    source.write_bytes(source.read_bytes().replace(b"20261104", b"20261103"))
    run(config, "fixture", clock=lambda: CLOCK)
    snapshot = stage_delta_snapshot(config)
    target = publisher.DeltaPathTarget(
        tmp_path / "delta", "reg_sho", storage_schemas()["membership"].contract_id
    ).bind()
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try:
        with pytest.raises(ValueError, match="UTC"):
            publisher.publish_delta_snapshot(spark, config, snapshot, target)
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    try:
        with pytest.raises(ValueError, match="schema evolution"):
            publisher.publish_delta_snapshot(spark, config, snapshot, target)
    finally:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
    installed_version = publisher.importlib.metadata.version

    def bundled_metadata(name):
        if name == "delta-spark":
            raise publisher.importlib.metadata.PackageNotFoundError(name)
        return installed_version(name)

    monkeypatch.setattr(publisher.importlib.metadata, "version", bundled_metadata)
    publisher.publish_delta_snapshot(spark, config, snapshot, target)
    generation = json.loads((target.root / "generations" / f"{snapshot.name}.json").read_text())
    assert generation["engine"]["delta_spark_version"] is None
    _manifest, frames = _assert_exact(spark, target, config, snapshot)
    assert frames["membership"].count() == frames["episodes"].count() == 0
    assert frames["coverage"].first().coverage_state == "COMPLETE_EMPTY"
    assert publisher.publish_delta_snapshot(spark, config, snapshot, target)["reused"] is True
