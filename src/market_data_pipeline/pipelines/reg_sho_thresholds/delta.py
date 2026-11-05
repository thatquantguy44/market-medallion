"""Coherent threshold publication to Delta Lake or Unity Catalog.

Delta's transaction log versions make each table snapshot addressable, while a
pipeline-owned generation manifest pins one membership/coverage/episodes/audit combination.

Official references:
https://docs.delta.io/quick-start/
https://docs.databricks.com/aws/en/delta/merge
https://docs.databricks.com/aws/en/tables/history
"""

import importlib.metadata
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.delta_snapshot import (
    KEYS,
    current_snapshot,
    snapshot_schemas,
    validate_snapshot,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.delta_snapshot import (
    digest as _digest,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.delta_snapshot import (
    read_json as _json,
)
from market_data_pipeline.pipelines.reg_sho_thresholds.storage import storage_schemas
from market_data_pipeline.shared.files import atomic_json, writer_lock

IDENTITY = {"dataset": "reg-sho-thresholds"}

GENERATION_VERSION = "reg-sho-delta-generation/1"
POINTER_VERSION = "reg-sho-delta-pointer/1"
TARGET_VERSION = "reg-sho-delta-target/1"
SOURCE_VERSION = "reg-sho-delta-source/1"
TABLES = {
    "membership": ("membership", "version_id", "membership_versions"),
    "coverage": ("coverage", "version_id", "coverage_versions"),
    "episodes": ("episodes", ",".join(KEYS["episodes"]), "episodes"),
    "audit": ("audit", "run_id", "pipeline_runs"),
}


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
    manifest, _tables = validate_snapshot(config, snapshot)
    return snapshot, manifest, snapshot_schemas(), _digest(snapshot / "manifest.json")


def _frame(spark, path: Path, schema, expected_rows: int, key: str):
    from pyspark.sql.types import StructType

    types = {
        "string": "string",
        "date32[day]": "date",
        "int64": "long",
        "bool": "boolean",
        "timestamp[us, tz=UTC]": "timestamp",
    }
    expected_schema = StructType.fromJson(
        {
            "type": "struct",
            "fields": [
                {
                    "name": field.name,
                    "type": types[str(field.type)],
                    "nullable": field.nullable,
                    "metadata": {},
                }
                for field in schema
            ],
        }
    )
    frame = spark.read.schema(expected_schema).parquet(str(path))
    observed_types = [(field.name, field.dataType.json()) for field in frame.schema.fields]
    expected_types = [(field.name, field.dataType.json()) for field in expected_schema.fields]
    if observed_types != expected_types:
        raise ValueError("Spark schema differs from the threshold storage contract")
    required_nulls = " OR ".join(
        f"{field.name} IS NULL" for field in expected_schema.fields if not field.nullable
    )
    if frame.filter(required_nulls).limit(1).count():
        raise ValueError("threshold Delta source violates required-field nullability")
    if frame.filter(" OR ".join(f"{column} IS NULL" for column in key.split(","))).limit(1).count():
        raise ValueError("threshold Delta source contains a null revision key")
    count = frame.count()
    if count != expected_rows or frame.select(*key.split(",")).distinct().count() != count:
        raise ValueError("threshold Delta source row count or revision keys are invalid")
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
        persisted = table.toDF()
        if persisted.schema.jsonValue() != frame.schema.jsonValue():
            raise ValueError("existing Delta table schema differs from the snapshot")
        if name == "episodes":
            # Derived episodes can change, split or disappear; pin a complete replacement.
            if (
                persisted.exceptAll(frame).limit(1).count()
                or frame.exceptAll(persisted).limit(1).count()
            ):
                writer = frame.write.format("delta").mode("overwrite")
                if target.mode == "path":
                    writer.save(reference)
                else:
                    writer.saveAsTable(reference)
        else:
            # Source revision keys are immutable, including their complete normalized values.
            if persisted.exceptAll(frame).limit(1).count():
                raise ValueError("Delta history differs from immutable source revisions")
            if frame.exceptAll(persisted).limit(1).count():
                (
                    table.alias("target")
                    .merge(frame.alias("source"), f"target.{key}=source.{key}")
                    .whenNotMatchedInsertAll()
                    .execute()
                )
    persisted = table.toDF()
    if persisted.exceptAll(frame).limit(1).count() or frame.exceptAll(persisted).limit(1).count():
        raise ValueError("persisted Delta values differ from the source snapshot")
    version = table.history(1).select("version").first()["version"]
    return {
        "kind": target.mode,
        "reference": reference,
        "version": version,
        "row_count": persisted.count(),
        "key": key,
    }


def _generation(spark, target, snapshot, manifest, manifest_sha256, tables):
    # Managed runtimes can bundle the Delta API without Python distribution metadata.
    try:
        delta_version = importlib.metadata.version("delta-spark")
    except importlib.metadata.PackageNotFoundError:
        delta_version = None
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
            "delta_spark_version": delta_version,
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
        raise ValueError("unsupported threshold Delta pointer")
    relative = pointer.get("generation")
    if not isinstance(relative, str) or not re.fullmatch(
        r"generations/[0-9a-f]{32}\.json", relative
    ):
        raise ValueError("invalid threshold Delta generation path")
    raw_generation_path = target.root / relative
    if raw_generation_path.is_symlink():
        raise ValueError("invalid threshold Delta generation path")
    generation_path = raw_generation_path.resolve()
    if not generation_path.is_relative_to((target.root / "generations").resolve()):
        raise ValueError("invalid threshold Delta generation path")
    if _digest(generation_path) != pointer.get("generation_sha256"):
        raise ValueError("threshold Delta generation checksum mismatch")
    generation = _json(generation_path)
    if (
        generation.get("generation_version") != GENERATION_VERSION
        or generation.get("pipeline") != IDENTITY["dataset"]
        or generation.get("target_id") != target.identity["target_id"]
        or generation.get("storage_contract_id") != target.identity["storage_contract_id"]
        or set(generation.get("tables", {})) != set(TABLES)
    ):
        raise ValueError("threshold Delta generation differs from the target contract")
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
        raise ValueError("threshold Delta publication requires a UTC Spark session")
    if spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled", "false") != "false":
        raise ValueError("threshold Delta publication forbids automatic schema evolution")
    if fail_after_table is not None and fail_after_table not in TABLES:
        raise ValueError("fail_after_table requires a known Delta table")
    if target.identity.get("storage_contract_id") != storage_schemas()["membership"].contract_id:
        raise ValueError("Delta target storage contract differs from the threshold config")
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
                raise ValueError("threshold Delta generation has an invalid capture sequence")
            if manifest["capture_sequence"] < current_sequence:
                raise ValueError("threshold Delta publication cannot roll back capture sequence")
            if (
                manifest["capture_sequence"] == current_sequence
                and current_generation.get("source_manifest_sha256") != manifest_sha256
            ):
                raise ValueError("threshold Delta capture sequence identifies different content")
        if generation_path.exists():
            sealed = _json(generation_path)
            if (
                sealed.get("source_manifest_sha256") != manifest_sha256
                or sealed.get("pipeline") != IDENTITY["dataset"]
                or sealed.get("source_snapshot_id") != snapshot.name
                or sealed.get("source_capture_sequence") != manifest["capture_sequence"]
                or sealed.get("source_generated_at") != manifest["generated_at"]
                or sealed.get("source_warehouse_id") != manifest["warehouse_id"]
                or sealed.get("target_id") != target.identity["target_id"]
                or sealed.get("generation_version") != GENERATION_VERSION
                or sealed.get("storage_contract_id") != storage_schemas()["membership"].contract_id
                or set(sealed.get("tables", {})) != set(TABLES)
            ):
                raise ValueError("sealed threshold generation differs from its source")
            _read_tables(spark, target, sealed)
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
                raise RuntimeError("injected threshold Delta publication failure")
        generation = _generation(spark, target, snapshot, manifest, manifest_sha256, tables)
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
        raise ValueError("threshold Delta generation differs from its source binding")
    return _read_tables(spark, target, generation)


def _read_tables(spark, target, generation):
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
            raise ValueError("invalid threshold Delta table generation entry")
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
            raise ValueError("unsupported threshold Delta table reference")
        if frames[name].count() != table.get("row_count"):
            raise ValueError("pinned threshold Delta table row count differs")
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
            "threshold snapshot must be staged inside the configured Unity Catalog volume"
        )
    target = DeltaCatalogTarget(
        volume / "delta" / config.databricks.table_prefix,
        config.databricks.catalog,
        config.databricks.schema,
        config.databricks.table_prefix,
        storage_schemas()["membership"].contract_id,
    ).bind()
    return publish_delta_snapshot(spark, config_path, selected, target)
