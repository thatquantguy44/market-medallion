"""Runtime configuration; secrets are resolved only when connecting."""

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from market_data_pipeline.shared.http import NetworkConfig, parse_network
from market_data_pipeline.shared.yaml import UniqueLoader


@dataclass(frozen=True)
class PostgresConfig:
    host: str = "127.0.0.1"
    port: int = 5432
    database: str = "additional_market_data"
    user: str = "market_data"
    role: str | None = None
    schema: str = "public"
    password_env: str | None = "AAII_POSTGRES_PASSWORD"
    sslmode: str = "prefer"
    connect_timeout: int = 15

    def conninfo(self) -> str:
        # libpq performs quoting; never interpolate passwords into a URI.
        # https://www.psycopg.org/psycopg3/docs/api/conninfo.html
        from psycopg.conninfo import make_conninfo

        params = dict(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            sslmode=self.sslmode,
            connect_timeout=self.connect_timeout,
            options=f"-c search_path={self.schema}",
        )
        if self.role:
            params["options"] += f" -c role={self.role}"
        if self.password_env:
            password = os.environ.get(self.password_env)
            if not password:
                raise ValueError(f"set password environment variable {self.password_env}")
            params["password"] = password
        return make_conninfo(**params)


@dataclass(frozen=True)
class AaiiOperations:
    overlap_days: int = 28
    max_age_days: int = 10
    http_attempts: int = 3
    scheduled_live_access: bool = False


@dataclass(frozen=True)
class RuntimeConfig:
    warehouse: str = "sqlite"
    root: Path = Path("data/aaii")
    postgres: PostgresConfig | None = None
    operations: AaiiOperations = field(default_factory=AaiiOperations)
    network: NetworkConfig = field(default_factory=NetworkConfig)


def load_config(path: Path) -> RuntimeConfig:
    try:
        value = yaml.load(path.read_text(), Loader=UniqueLoader)
    except yaml.YAMLError as exc:
        raise ValueError("invalid runtime YAML") from exc
    if not isinstance(value, dict) or set(value) - {
        "warehouse",
        "root",
        "postgres",
        "operations",
        "network",
    }:
        raise ValueError(
            "runtime config fields must be warehouse, root, postgres, operations, network"
        )
    backend = value.get("warehouse", "sqlite")
    if backend not in ("sqlite", "postgres"):
        raise ValueError("warehouse must be sqlite or postgres")
    raw_root = value.get("root", "data/aaii")
    if not isinstance(raw_root, str) or not raw_root.strip():
        raise ValueError("root must be a nonempty path")
    root = Path(raw_root).expanduser()
    if not root.is_absolute():
        root = (path.resolve().parent / root).resolve()
    postgres = None
    if "postgres" in value:
        settings = value["postgres"]
        if not isinstance(settings, dict) or set(settings) - set(
            PostgresConfig.__dataclass_fields__
        ):
            raise ValueError(
                "unknown PostgreSQL config fields; store passwords in environment only"
            )
        postgres = PostgresConfig(**settings)
        for key in ("host", "database", "user"):
            entry = getattr(postgres, key)
            if not isinstance(entry, str) or not entry.strip() or "\x00" in entry:
                raise ValueError(f"postgres.{key} must be a nonempty string")
        if type(postgres.port) is not int or not 1 <= postgres.port <= 65535:
            raise ValueError("postgres.port must be an integer in [1,65535]")
        if type(postgres.connect_timeout) is not int or not 1 <= postgres.connect_timeout <= 300:
            raise ValueError("postgres.connect_timeout must be an integer in [1,300]")
        for key in ("schema", "role"):
            entry = getattr(postgres, key)
            if entry is None and key == "role":
                continue
            if not isinstance(entry, str) or not re.fullmatch(r"[a-z_][a-z0-9_]*", entry):
                raise ValueError(f"postgres.{key} must be a simple lowercase identifier")
        if postgres.password_env is not None and (
            not isinstance(postgres.password_env, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", postgres.password_env)
        ):
            raise ValueError("password_env must be an environment-variable name or null")
        if postgres.sslmode not in (
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ):
            raise ValueError("unsupported postgres.sslmode")
    ops = value.get("operations", {})
    if not isinstance(ops, dict) or set(ops) - set(AaiiOperations.__dataclass_fields__):
        raise ValueError("unknown AAII operations fields")
    operations = AaiiOperations(**ops)
    for key, lower, upper in (
        ("overlap_days", 7, 90),
        ("max_age_days", 1, 90),
        ("http_attempts", 1, 5),
    ):
        entry = getattr(operations, key)
        if type(entry) is not int or not lower <= entry <= upper:
            raise ValueError(f"operations.{key} must be an integer in [{lower},{upper}]")
    if type(operations.scheduled_live_access) is not bool:
        raise ValueError("operations.scheduled_live_access must be boolean")
    return RuntimeConfig(
        backend,
        root,
        postgres,
        operations,
        parse_network(value.get("network", {}), base=path.resolve().parent),
    )


def resolve_runtime(
    *, config: RuntimeConfig | None = None, root: Path | None = None, warehouse: str | None = None
) -> tuple[Path, str, str | None]:
    config = config or RuntimeConfig()
    backend = warehouse or config.warehouse
    if backend == "sqlite":
        dsn = None
    elif backend == "postgres":
        if config.postgres:
            dsn = config.postgres.conninfo()
        else:
            dsn = os.environ.get("MARKET_DATA_POSTGRES_DSN")
            if not dsn:
                raise ValueError("postgres requires YAML settings or MARKET_DATA_POSTGRES_DSN")
    else:
        raise ValueError("warehouse must be sqlite or postgres")
    return Path(root) if root is not None else config.root, backend, dsn
