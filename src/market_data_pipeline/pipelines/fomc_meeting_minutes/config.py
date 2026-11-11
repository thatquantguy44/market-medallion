"""FOMC-owned runtime configuration; loading reads no secrets and opens no service."""

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from market_data_pipeline.shared.http import NetworkConfig, parse_network
from market_data_pipeline.shared.yaml import UniqueLoader


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


@dataclass(frozen=True)
class Config:
    root: Path
    manifest: Path
    postgres: PostgresConfig
    network: NetworkConfig


def _text(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 1024
        or any(ord(c) < 32 for c in value)
    ):
        raise ValueError("FOMC configuration requires nonempty bounded text")
    return value


def _int(value: object, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("FOMC PostgreSQL integer setting is out of range")
    return value


def config_from_dict(value: object, *, base_dir: Path) -> Config:
    if not isinstance(value, dict) or set(value) != {
        "root",
        "manifest",
        "warehouse",
        "postgres",
        "network",
    }:
        raise ValueError(
            "FOMC config requires exactly root, manifest, warehouse, postgres and network"
        )
    if value["warehouse"] != "postgres":
        raise ValueError("FOMC implemented catalog backend is postgres")
    settings = postgres_from_dict(value["postgres"])
    return Config(
        (base_dir / Path(_text(value["root"])).expanduser()).resolve(),
        (base_dir / Path(_text(value["manifest"])).expanduser()).resolve(),
        settings,
        parse_network(value["network"], base=base_dir),
    )


def postgres_from_dict(value: object) -> PostgresConfig:
    pg = value
    keys = {
        "host",
        "port",
        "database",
        "user",
        "role",
        "schema",
        "password_env",
        "sslmode",
        "connect_timeout",
    }
    if not isinstance(pg, dict) or set(pg) != keys:
        raise ValueError("FOMC postgres config requires exactly the documented nonsecret fields")
    schema = _text(pg["schema"])
    if not re.fullmatch(r"fomc_meeting_minutes(?:_test_[a-z0-9_]+)?", schema) or len(schema) > 63:
        raise ValueError(
            "FOMC tables require their own fomc_meeting_minutes schema or test namespace"
        )
    role = pg["role"]
    if role is not None and (
        not isinstance(role, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", role)
    ):
        raise ValueError("FOMC postgres role requires a simple SQL identifier or null")
    env = _text(pg["password_env"])
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env):
        raise ValueError("FOMC password_env requires an environment variable name")
    if pg["sslmode"] not in ("disable", "allow", "prefer", "require", "verify-ca", "verify-full"):
        raise ValueError("unsupported FOMC postgres sslmode")
    settings = PostgresConfig(
        _text(pg["host"]),
        _int(pg["port"], 1, 65535),
        _text(pg["database"]),
        _text(pg["user"]),
        role,
        schema,
        env,
        pg["sslmode"],
        _int(pg["connect_timeout"], 1, 300),
    )
    return settings


def load_config(path: Path) -> Config:
    try:
        return config_from_dict(
            yaml.load(path.read_text(), Loader=UniqueLoader), base_dir=path.resolve().parent
        )
    except (yaml.YAMLError, UnicodeError):
        raise ValueError("invalid FOMC runtime YAML") from None
