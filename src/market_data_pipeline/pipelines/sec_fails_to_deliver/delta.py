"""Coherent SEC FTD publication to Delta Lake or Unity Catalog.

Delta's transaction log versions make each table snapshot addressable, while a
pipeline-owned generation manifest pins one facts/coverage/audit combination.

Official references:
https://docs.delta.io/quick-start/
https://docs.databricks.com/aws/en/delta/merge
https://docs.databricks.com/aws/en/tables/history
"""

import hashlib
import importlib.metadata
import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import (
    PUBLICATION_VERSION,
    current_snapshot,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import (
    IDENTITY,
    storage_schemas,
)
from market_data_pipeline.shared.files import atomic_json, writer_lock

GENERATION_VERSION = "sec-ftd-delta-generation/1"
POINTER_VERSION = "sec-ftd-delta-pointer/1"
TARGET_VERSION = "sec-ftd-delta-target/1"
SOURCE_VERSION = "sec-ftd-delta-source/1"
TABLES = {
    "facts": ("facts", "version_id", "facts_versions"),
    "coverage": ("coverage", "version_id", "coverage_versions"),
    "audit": ("audit", "run_id", "pipeline_runs"),
}


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def _json(path: Path, maximum=1024 * 1024) -> dict:
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON field")
            value[key] = item
        return value

    try:
        if path.is_symlink() or not path.is_file():
            raise ValueError("JSON artifact is unavailable")
        with path.open("rb") as stream:
            payload = stream.read(maximum + 1)
        if len(payload) > maximum:
            raise ValueError("JSON artifact exceeds its byte limit")
        value = json.loads(payload, object_pairs_hook=unique)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise ValueError("invalid JSON artifact") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON artifact requires an object")
    return value


def _prefix(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value):
        raise ValueError("Delta table prefix requires a safe lowercase identifier")
    return value


class _TargetBinding:
    root: Path
    expected: dict

    def bind(self):
        self.root.mkdir(parents=True, exist_ok=True)
        marker = self.root / ".delta-target.json"
        if self.root.is_symlink() or marker.is_symlink():
            raise ValueError("Delta target artifacts must not be symbolic links")
        with writer_lock(self.root):
            if marker.exists():
                identity = _json(marker, 65536)
                if any(identity.get(key) != value for key, value in self.expected.items()):
                    raise ValueError("Delta target belongs to a different contract")
            else:
                if any(path.name != ".writer.lock" for path in self.root.iterdir()):
                    raise ValueError("refusing a nonempty unbound Delta target")
                identity = {**self.expected, "target_id": uuid.uuid4().hex}
                atomic_json(marker, identity)
        self.identity = identity
        return self


@dataclass
class DeltaPathTarget(_TargetBinding):
    """Local path target used for the required Spark/Delta integration proof."""

    root: Path
    table_prefix: str
    storage_contract_id: str

    def __post_init__(self):
        raw_root = Path(self.root)
        if raw_root.is_symlink():
            raise ValueError("Delta target root must not be a symbolic link")
        self.root = raw_root.resolve()
        self.table_prefix = _prefix(self.table_prefix)
        if not re.fullmatch(r"[0-9a-f]{64}", self.storage_contract_id):
            raise ValueError("Delta target requires a storage contract digest")
        self.mode = "path"
        self.expected = {
            "target_version": TARGET_VERSION,
            "pipeline": IDENTITY["dataset"],
            "mode": self.mode,
            "table_prefix": self.table_prefix,
            "storage_contract_id": self.storage_contract_id,
        }

    def table_reference(self, suffix: str) -> str:
        return str((self.root / "tables" / f"{self.table_prefix}_{suffix}").resolve())


@dataclass
class DeltaCatalogTarget(_TargetBinding):
    """Unity Catalog tables plus generation metadata in a configured volume."""

    root: Path
    catalog: str
    schema: str
    table_prefix: str
    storage_contract_id: str

    def __post_init__(self):
        raw_root = Path(self.root)
        if raw_root.is_symlink():
            raise ValueError("Delta target root must not be a symbolic link")
        self.root = raw_root.resolve()
        self.catalog = _prefix(self.catalog)
        self.schema = _prefix(self.schema)
        self.table_prefix = _prefix(self.table_prefix)
        if not re.fullmatch(r"[0-9a-f]{64}", self.storage_contract_id):
            raise ValueError("Delta target requires a storage contract digest")
        self.mode = "catalog"
        self.expected = {
            "target_version": TARGET_VERSION,
            "pipeline": IDENTITY["dataset"],
            "mode": self.mode,
            "catalog": self.catalog,
            "schema": self.schema,
            "table_prefix": self.table_prefix,
            "storage_contract_id": self.storage_contract_id,
        }

    def table_reference(self, suffix: str) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_prefix}_{suffix}"


def _validate_snapshot(config, snapshot: Path):
    import pyarrow.parquet as pq

    snapshot = Path(snapshot).resolve()
    if snapshot != current_snapshot(config.root).resolve():
        raise ValueError("Delta publication requires the current committed SEC FTD snapshot")
    manifest_path = snapshot / "manifest.json"
    manifest = _json(manifest_path)
    schemas = storage_schemas(config.storage)
    required = {
        "publication_version",
        "pipeline",
        "snapshot_id",
        "run_id",
        "capture_sequence",
        "generated_at",
        "warehouse_id",
        "storage_contract_id",
        "record_schemas",
        "row_counts",
        "files",
    }
    if (
        set(manifest) != required
        or manifest["publication_version"] != PUBLICATION_VERSION
        or manifest["pipeline"] != IDENTITY["dataset"]
        or manifest["snapshot_id"] != snapshot.name
        or manifest["storage_contract_id"] != config.storage.contract_id
        or manifest["record_schemas"] != {name: schema.version for name, schema in schemas.items()}
    ):
        raise ValueError("SEC FTD snapshot manifest differs from the Delta contract")
    for name, (schema_name, _key, _suffix) in TABLES.items():
        path = snapshot / f"{name}.parquet"
        if path.is_symlink() or not path.is_file() or _digest(path) != manifest["files"].get(name):
            raise ValueError("SEC FTD snapshot checksum differs from its manifest")
        table = pq.read_table(path)
        schemas[schema_name].read_arrow(table)
        if table.num_rows != manifest["row_counts"].get(name):
            raise ValueError("SEC FTD snapshot row count differs from its manifest")
    return snapshot, manifest, schemas, _digest(manifest_path)


def _frame(spark, path: Path, schema, expected_rows: int, key: str):
    from pyspark.sql.types import StructType

    expected_schema = StructType.fromJson(schema.spark_schema_json())
    frame = spark.read.schema(expected_schema).parquet(str(path))
    observed_types = [(field.name, field.dataType.json()) for field in frame.schema.fields]
    expected_types = [(field.name, field.dataType.json()) for field in expected_schema.fields]
    if observed_types != expected_types:
        raise ValueError("Spark schema differs from the SEC FTD storage contract")
    for field in expected_schema.fields:
        if not field.nullable and frame.filter(f"{field.name} IS NULL").limit(1).count():
            raise ValueError("SEC FTD Delta source violates required-field nullability")
    if frame.filter(f"{key} IS NULL").limit(1).count():
        raise ValueError("SEC FTD Delta source contains a null revision key")
    count = frame.count()
    if count != expected_rows or frame.select(key).distinct().count() != count:
        raise ValueError("SEC FTD Delta source row count or revision keys are invalid")
    return frame


def _merge(spark, target, name, frame, key):
    from delta.tables import DeltaTable

    reference = target.table_reference(TABLES[name][2])
    if target.mode == "path":
        exists = DeltaTable.isDeltaTable(spark, reference)
        if not exists:
            frame.write.format("delta").mode("errorifexists").save(reference)
        table = DeltaTable.forPath(spark, reference)
    else:
        exists = spark.catalog.tableExists(reference)
        if not exists:
            frame.write.format("delta").mode("errorifexists").saveAsTable(reference)
        table = DeltaTable.forName(spark, reference)
    if exists:
        if table.toDF().schema.jsonValue() != frame.schema.jsonValue():
            raise ValueError("existing Delta table schema differs from the snapshot")
        (
            table.alias("target")
            .merge(frame.alias("source"), f"target.{key}=source.{key}")
            .whenNotMatchedInsertAll()
            .execute()
        )
    persisted = table.toDF()
    if persisted.select(key).distinct().count() != persisted.count():
        raise ValueError("persisted Delta table contains duplicate revision keys")
    if frame.join(persisted.select(key), key, "left_anti").limit(1).count():
        raise ValueError("Delta publication did not persist every source revision")
    version = table.history(1).select("version").first()["version"]
    return {
        "kind": target.mode,
        "reference": reference,
        "version": version,
        "row_count": persisted.count(),
        "key": key,
    }


def _generation(spark, target, snapshot, manifest, manifest_sha256, tables):
    return {
        "generation_version": GENERATION_VERSION,
        "pipeline": IDENTITY["dataset"],
        "target_id": target.identity["target_id"],
        "source_snapshot_id": snapshot.name,
        "source_manifest_sha256": manifest_sha256,
        "source_capture_sequence": manifest["capture_sequence"],
        "source_generated_at": manifest["generated_at"],
        "source_warehouse_id": manifest["warehouse_id"],
        "storage_contract_id": manifest["storage_contract_id"],
        "engine": {
            "spark_version": spark.version,
            "delta_spark_version": importlib.metadata.version("delta-spark"),
            "target_mode": target.mode,
        },
        "tables": tables,
    }


def _bind_source(target, manifest):
    """Permanently bind a target to one source warehouse before any table write."""
    marker = target.root / ".delta-source.json"
    if marker.is_symlink():
        raise ValueError("Delta source binding must not be a symbolic link")
    expected = {
        "source_version": SOURCE_VERSION,
        "pipeline": IDENTITY["dataset"],
        "target_id": target.identity["target_id"],
        "warehouse_id": manifest["warehouse_id"],
        "storage_contract_id": manifest["storage_contract_id"],
    }
    if marker.exists():
        if _json(marker, 65536) != expected:
            raise ValueError("Delta target belongs to a different source warehouse")
    else:
        atomic_json(marker, expected)


def _load_generation_metadata(target):
    pointer = _json(target.root / "current.json", 65536)
    if pointer.get("pointer_version") != POINTER_VERSION:
        raise ValueError("unsupported SEC FTD Delta pointer")
    relative = pointer.get("generation")
    if not isinstance(relative, str) or not re.fullmatch(
        r"generations/[0-9a-f]{32}\.json", relative
    ):
        raise ValueError("invalid SEC FTD Delta generation path")
    raw_generation_path = target.root / relative
    if raw_generation_path.is_symlink():
        raise ValueError("invalid SEC FTD Delta generation path")
    generation_path = raw_generation_path.resolve()
    if not generation_path.is_relative_to((target.root / "generations").resolve()):
        raise ValueError("invalid SEC FTD Delta generation path")
    if _digest(generation_path) != pointer.get("generation_sha256"):
        raise ValueError("SEC FTD Delta generation checksum mismatch")
    generation = _json(generation_path)
    if (
        generation.get("generation_version") != GENERATION_VERSION
        or generation.get("pipeline") != IDENTITY["dataset"]
        or generation.get("target_id") != target.identity["target_id"]
        or generation.get("storage_contract_id") != target.identity["storage_contract_id"]
        or set(generation.get("tables", {})) != set(TABLES)
    ):
        raise ValueError("SEC FTD Delta generation differs from the target contract")
    return pointer, generation_path, generation


def publish_delta_snapshot(
    spark,
    config_path: Path,
    snapshot: Path,
    target,
    *,
    fail_after_table: str | None = None,
):
    """Merge immutable revisions and atomically expose their pinned versions."""
    config = load_config(Path(config_path))
    if getattr(target, "identity", None) is None:
        raise ValueError("Delta target must be bound before publication")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("SEC FTD Delta publication requires a UTC Spark session")
    if spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled", "false") != "false":
        raise ValueError("SEC FTD Delta publication forbids automatic schema evolution")
    if fail_after_table is not None and fail_after_table not in TABLES:
        raise ValueError("fail_after_table requires a known Delta table")
    if target.identity.get("storage_contract_id") != config.storage.contract_id:
        raise ValueError("Delta target storage contract differs from the SEC FTD config")
    snapshot, manifest, schemas, manifest_sha256 = _validate_snapshot(config, snapshot)
    generations = target.root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    generation_path = generations / f"{snapshot.name}.json"
    with writer_lock(target.root):
        _bind_source(target, manifest)
        current_generation = None
        if (target.root / "current.json").exists():
            pointer, _current_path, current_generation = _load_generation_metadata(target)
            current_sequence = current_generation.get("source_capture_sequence")
            if type(current_sequence) is not int:
                raise ValueError("SEC FTD Delta generation has an invalid capture sequence")
            if manifest["capture_sequence"] < current_sequence:
                raise ValueError("SEC FTD Delta publication cannot roll back capture sequence")
            if (
                manifest["capture_sequence"] == current_sequence
                and current_generation.get("source_manifest_sha256") != manifest_sha256
            ):
                raise ValueError("SEC FTD Delta capture sequence identifies different content")
        if current_generation is not None and generation_path.is_file():
            if pointer.get("generation") == f"generations/{snapshot.name}.json" and pointer.get(
                "generation_sha256"
            ) == _digest(generation_path):
                if (
                    current_generation.get("source_manifest_sha256") == manifest_sha256
                    and current_generation.get("source_warehouse_id") == manifest["warehouse_id"]
                ):
                    read_delta_generation(spark, target)
                    return {
                        "status": "PUBLISHED",
                        "source_snapshot_id": snapshot.name,
                        "generation": str(generation_path),
                        "reused": True,
                    }
        tables = {}
        for name, (schema_name, key, _suffix) in TABLES.items():
            frame = _frame(
                spark,
                snapshot / f"{name}.parquet",
                schemas[schema_name],
                manifest["row_counts"][name],
                key,
            )
            tables[name] = _merge(spark, target, name, frame, key)
            if fail_after_table == name:
                raise RuntimeError("injected SEC FTD Delta publication failure")
        generation = _generation(spark, target, snapshot, manifest, manifest_sha256, tables)
        if generation_path.exists():
            if _json(generation_path) != generation:
                raise ValueError("existing Delta generation differs from the verified retry")
        else:
            atomic_json(generation_path, generation)
        atomic_json(
            target.root / "current.json",
            {
                "pointer_version": POINTER_VERSION,
                "generation": f"generations/{snapshot.name}.json",
                "generation_sha256": _digest(generation_path),
            },
        )
    return {
        "status": "PUBLISHED",
        "source_snapshot_id": snapshot.name,
        "generation": str(generation_path),
        "reused": False,
    }


def read_delta_generation(spark, target):
    """Load only the table versions pinned by the current generation pointer."""
    if getattr(target, "identity", None) is None:
        raise ValueError("Delta target must be bound before reading")
    _pointer, _generation_path, generation = _load_generation_metadata(target)
    source = _json(target.root / ".delta-source.json", 65536)
    if (
        source.get("source_version") != SOURCE_VERSION
        or source.get("pipeline") != IDENTITY["dataset"]
        or source.get("target_id") != target.identity["target_id"]
        or source.get("warehouse_id") != generation.get("source_warehouse_id")
        or source.get("storage_contract_id") != generation.get("storage_contract_id")
    ):
        raise ValueError("SEC FTD Delta generation differs from its source binding")
    frames = {}
    for name, table in generation["tables"].items():
        _schema_name, expected_key, suffix = TABLES[name]
        if (
            not isinstance(table, dict)
            or table.get("kind") != target.mode
            or table.get("reference") != target.table_reference(suffix)
            or type(table.get("version")) is not int
            or table["version"] < 0
            or type(table.get("row_count")) is not int
            or table["row_count"] < 0
            or table.get("key") != expected_key
        ):
            raise ValueError("invalid SEC FTD Delta table generation entry")
        if table.get("kind") == "path":
            frames[name] = (
                spark.read.format("delta")
                .option("versionAsOf", table["version"])
                .load(table["reference"])
            )
        elif table.get("kind") == "catalog":
            frames[name] = spark.read.option("versionAsOf", table["version"]).table(
                table["reference"]
            )
        else:
            raise ValueError("unsupported SEC FTD Delta table reference")
        if frames[name].count() != table.get("row_count"):
            raise ValueError("pinned SEC FTD Delta table row count differs")
    return frames


def publish_configured_delta(spark, config_path: Path, snapshot: Path | None = None):
    """Publish to configured Unity Catalog tables with metadata in the staging volume."""
    config = load_config(Path(config_path))
    if not config.databricks.enabled:
        raise ValueError("Databricks Delta publication is disabled in this configuration")
    volume = Path(config.databricks.staging_volume).resolve()
    selected = current_snapshot(config.root) if snapshot is None else Path(snapshot).resolve()
    if not selected.is_relative_to(volume):
        raise ValueError(
            "SEC FTD snapshot must be staged inside the configured Unity Catalog volume"
        )
    target = DeltaCatalogTarget(
        volume / "delta" / config.databricks.table_prefix,
        config.databricks.catalog,
        config.databricks.schema,
        config.databricks.table_prefix,
        config.storage.contract_id,
    ).bind()
    return publish_delta_snapshot(spark, config_path, selected, target)
