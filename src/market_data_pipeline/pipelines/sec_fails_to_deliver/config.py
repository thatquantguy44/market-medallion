"""SEC FTD runtime intent; loading never reads secrets or connects to services."""

import hashlib
import json
import os
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import MappingProxyType

import yaml

from market_data_pipeline.shared.http import NetworkConfig, parse_network
from market_data_pipeline.shared.yaml import UniqueLoader


def _object(value: object, fields: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"{label} requires exactly the documented fields")
    return value


def _text(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError(f"{label} requires nonempty text without control characters")
    return value


def _integer(value: object, lower: int, upper: int, label: str) -> int:
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(f"{label} requires an integer in [{lower}, {upper}]")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value):
        raise ValueError(f"{label} requires a simple lowercase SQL identifier")
    return value


def _path(value: object, base: Path, label: str) -> Path:
    path = Path(_text(value, label)).expanduser()
    return (base / path).resolve()


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    role: str | None
    schema: str
    password_env: str
    sslmode: str
    connect_timeout: int

    def conninfo(self) -> str:
        """Resolve the named password only at connection time."""
        password = os.environ.get(self.password_env)
        if not password:
            raise ValueError(f"set password environment variable {self.password_env}")
        try:
            from psycopg.conninfo import make_conninfo
        except ImportError as exc:
            raise ValueError(
                "PostgreSQL support requires the postgres optional dependency"
            ) from exc
        return make_conninfo(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=password,
            sslmode=self.sslmode,
            connect_timeout=self.connect_timeout,
        )


@dataclass(frozen=True)
class DatabricksConfig:
    enabled: bool
    catalog: str | None
    schema: str | None
    staging_volume: str | None
    table_prefix: str


@dataclass(frozen=True)
class StorageConfig:
    schema_version: str
    quantity_precision: int
    price_precision: int
    price_scale: int
    timezone: str
    overflow: str
    preserve_numeric_text: bool

    def __post_init__(self) -> None:
        if self.schema_version != "sec-ftd-storage/1":
            raise ValueError("unsupported storage.schema_version")
        _integer(self.quantity_precision, 1, 38, "storage.quantity_precision")
        _integer(self.price_precision, 1, 38, "storage.price_precision")
        _integer(self.price_scale, 0, self.price_precision, "storage.price_scale")
        if (
            self.timezone != "UTC"
            or self.overflow != "reject"
            or self.preserve_numeric_text is not True
        ):
            raise ValueError("storage requires UTC, rejected overflow and exact numeric text")

    @property
    def contract_id(self) -> str:
        """Semantic identity only; future storage must enforce this root binding."""
        payload = {"pipeline": "sec-fails-to-deliver", **asdict(self)}
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class PartitionConfig:
    manifest: Path
    input: Path
    coverage_evidence: Path | None = None


@dataclass(frozen=True)
class RuntimeConfig:
    root: Path
    warehouse: str
    postgres: PostgresConfig
    databricks: DatabricksConfig
    storage: StorageConfig
    partitions: Mapping[str, PartitionConfig]
    network: NetworkConfig = field(default_factory=NetworkConfig)


def load_config(path: Path) -> RuntimeConfig:
    """Validate explicit settings and resolve paths relative to the config file.

    This validates intent, not endpoint availability, credentials, provider
    payloads, numeric projection values or implemented backend capability.
    """
    path = Path(path).resolve()
    try:
        value = yaml.load(path.read_text(), Loader=UniqueLoader)
    except (yaml.YAMLError, TypeError, UnicodeError) as exc:
        raise ValueError("invalid SEC FTD runtime YAML") from exc
    network = (
        parse_network(value.get("network", {}), base=path.parent)
        if isinstance(value, dict)
        else NetworkConfig()
    )
    if isinstance(value, dict):
        value = {key: item for key, item in value.items() if key != "network"}
    value = _object(
        value,
        {
            "config_version",
            "pipeline",
            "root",
            "warehouse",
            "postgres",
            "databricks",
            "storage",
            "partitions",
        },
        "SEC FTD config",
    )
    _integer(value["config_version"], 1, 1, "config_version")
    if value["pipeline"] != "sec-fails-to-deliver":
        raise ValueError("config pipeline must be sec-fails-to-deliver")
    if value["warehouse"] not in ("sqlite", "postgres"):
        raise ValueError("warehouse must be sqlite or postgres")
    root = _path(value["root"], path.parent, "root")

    pg = _object(value["postgres"], set(PostgresConfig.__dataclass_fields__), "postgres")
    for key in ("host", "database", "user"):
        _text(pg[key], f"postgres.{key}")
    _integer(pg["port"], 1, 65535, "postgres.port")
    _integer(pg["connect_timeout"], 1, 300, "postgres.connect_timeout")
    _identifier(pg["schema"], "postgres.schema")
    if pg["role"] is not None:
        _identifier(pg["role"], "postgres.role")
    if not isinstance(pg["password_env"], str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", pg["password_env"]
    ):
        raise ValueError("postgres.password_env requires an environment variable name")
    if pg["sslmode"] not in ("disable", "allow", "prefer", "require", "verify-ca", "verify-full"):
        raise ValueError("unsupported postgres.sslmode")

    delta = _object(value["databricks"], set(DatabricksConfig.__dataclass_fields__), "databricks")
    if type(delta["enabled"]) is not bool:
        raise ValueError("databricks.enabled requires boolean true/false")
    _identifier(delta["table_prefix"], "databricks.table_prefix")
    for key in ("catalog", "schema"):
        if delta[key] is not None:
            _identifier(delta[key], f"databricks.{key}")
        elif delta["enabled"]:
            raise ValueError("enabled Databricks requires catalog, schema and staging_volume")
    if delta["staging_volume"] is not None:
        volume = _text(delta["staging_volume"], "databricks.staging_volume")
        parts = volume.split("/")
        if (
            len(parts) < 5
            or parts[:2] != ["", "Volumes"]
            or any(part in ("", ".", "..") for part in parts[2:])
        ):
            raise ValueError("databricks.staging_volume requires a Unity Catalog volume path")
    elif delta["enabled"]:
        raise ValueError("enabled Databricks requires catalog, schema and staging_volume")

    storage = _object(value["storage"], set(StorageConfig.__dataclass_fields__), "storage")

    raw_partitions = value["partitions"]
    if not isinstance(raw_partitions, dict) or not 1 <= len(raw_partitions) <= 100:
        raise ValueError("partitions requires between 1 and 100 explicit entries")
    partitions = {}
    for name, settings in raw_partitions.items():
        _identifier(name, "partition name")
        if isinstance(settings, dict) and "coverage_evidence" in settings:
            settings = _object(settings, {"manifest", "input", "coverage_evidence"}, "partition")
        else:
            settings = _object(settings, {"manifest", "input"}, "partition")
        partitions[name] = PartitionConfig(
            _path(settings["manifest"], path.parent, "partition.manifest"),
            _path(settings["input"], path.parent, "partition.input"),
            _path(settings["coverage_evidence"], path.parent, "partition.coverage_evidence")
            if settings.get("coverage_evidence") is not None
            else None,
        )
    return RuntimeConfig(
        root,
        value["warehouse"],
        PostgresConfig(**pg),
        DatabricksConfig(**delta),
        StorageConfig(**storage),
        MappingProxyType(partitions),
        network,
    )
