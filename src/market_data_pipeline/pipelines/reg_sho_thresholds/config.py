"""Strict source-owned runtime configuration for threshold lists."""

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import MappingProxyType
from urllib.parse import parse_qs, unquote, urlsplit

import yaml

from market_data_pipeline.shared.http import NetworkConfig, parse_network
from market_data_pipeline.shared.yaml import UniqueLoader


def _path(value, base, label):
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{label} requires a path")
    return (base / Path(value).expanduser()).resolve()


def _identifier(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value):
        raise ValueError(f"{label} requires a safe identifier")
    return value


@dataclass(frozen=True)
class AcquisitionConfig:
    enabled: bool
    user_agent_env: str

    def __post_init__(self):
        if type(self.enabled) is not bool:
            raise ValueError("acquisition.enabled requires boolean true/false")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.user_agent_env):
            raise ValueError("acquisition.user_agent_env requires an environment name")


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
class PartitionConfig:
    provider: str
    market: str
    list_date: date
    manifest: Path
    input: Path
    source_url: str | None


@dataclass(frozen=True)
class RuntimeConfig:
    root: Path
    warehouse: str
    postgres: PostgresConfig
    acquisition: AcquisitionConfig
    schema_version: str
    partitions: MappingProxyType
    network: NetworkConfig = field(default_factory=NetworkConfig)


def _source_url(value, provider, market, list_date):
    if value is None:
        return None
    if not isinstance(value, str) or not value or any(ord(char) < 32 for char in value):
        raise ValueError("partition.source_url requires text")
    parts = urlsplit(value)
    if parts.scheme != "https" or parts.username is not None or parts.fragment or parts.port:
        raise ValueError("partition.source_url requires an official HTTPS URL")
    stamp = list_date.strftime("%Y%m%d")
    if provider == "nasdaq":
        expected = f"/dynamic/symdir/regsho/nasdaqth{stamp}.txt"
        valid = (
            parts.hostname == "www.nasdaqtrader.com" and parts.path == expected and not parts.query
        )
    else:
        query = parse_qs(parts.query, strict_parsing=True)
        valid = (
            parts.hostname == "www.nyse.com"
            and parts.path == "/api/regulatory/threshold-securities/download"
            and set(query) == {"selectedDate", "market"}
            and query["selectedDate"] == [list_date.isoformat()]
            and [unquote(item) for item in query["market"]] == [market]
        )
    if not valid:
        raise ValueError("partition.source_url does not match its provider/date/market")
    return value


def load_config(path: Path) -> RuntimeConfig:
    path = Path(path).resolve()
    try:
        value = yaml.load(path.read_text(), Loader=UniqueLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ValueError("invalid threshold runtime YAML") from exc
    if not isinstance(value, dict) or set(value) != {
        "config_version",
        "pipeline",
        "root",
        "warehouse",
        "postgres",
        "acquisition",
        "network",
        "storage",
        "partitions",
    }:
        raise ValueError("threshold config requires exactly the documented fields")
    if value["config_version"] != 1 or type(value["config_version"]) is not int:
        raise ValueError("unsupported threshold config_version")
    if value["pipeline"] != "reg-sho-thresholds":
        raise ValueError("threshold config requires pipeline reg-sho-thresholds")
    if value["warehouse"] not in ("sqlite", "postgres"):
        raise ValueError("threshold warehouse must be sqlite or postgres")
    pg = value["postgres"]
    if not isinstance(pg, dict) or set(pg) != set(PostgresConfig.__dataclass_fields__):
        raise ValueError("postgres requires exactly the documented fields")
    for key in ("host", "database", "user"):
        if not isinstance(pg[key], str) or not pg[key].strip():
            raise ValueError(f"postgres.{key} requires nonempty text")
    if type(pg["port"]) is not int or not 1 <= pg["port"] <= 65535:
        raise ValueError("postgres.port requires an integer in [1,65535]")
    if type(pg["connect_timeout"]) is not int or not 1 <= pg["connect_timeout"] <= 300:
        raise ValueError("postgres.connect_timeout requires an integer in [1,300]")
    _identifier(pg["schema"], "postgres.schema")
    if pg["role"] is not None:
        _identifier(pg["role"], "postgres.role")
    if not isinstance(pg["password_env"], str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", pg["password_env"]
    ):
        raise ValueError("postgres.password_env requires an environment name")
    if pg["sslmode"] not in (
        "disable",
        "allow",
        "prefer",
        "require",
        "verify-ca",
        "verify-full",
    ):
        raise ValueError("unsupported postgres.sslmode")
    acquisition = value["acquisition"]
    if not isinstance(acquisition, dict) or set(acquisition) != {"enabled", "user_agent_env"}:
        raise ValueError("invalid threshold acquisition settings")
    storage = value["storage"]
    if not isinstance(storage, dict) or storage != {"schema_version": "reg-sho-storage/1"}:
        raise ValueError("unsupported threshold storage contract")
    raw = value["partitions"]
    if not isinstance(raw, dict) or not 1 <= len(raw) <= 1000:
        raise ValueError("threshold partitions require 1 to 1000 entries")
    partitions = {}
    for name, item in raw.items():
        _identifier(name, "partition name")
        if not isinstance(item, dict) or set(item) != {
            "provider",
            "market",
            "list_date",
            "manifest",
            "input",
            "source_url",
        }:
            raise ValueError("threshold partition requires exactly the documented fields")
        provider, market = item["provider"], item["market"]
        if provider not in ("nasdaq", "nyse") or not isinstance(market, str) or not market:
            raise ValueError("threshold partition requires provider and market")
        if provider == "nasdaq" and market != "NASDAQ":
            raise ValueError("Nasdaq threshold partition market must be NASDAQ")
        if provider == "nyse" and market not in (
            "NYSE",
            "NYSE American",
            "NYSE Arca",
            "NYSE National",
        ):
            raise ValueError("unsupported NYSE threshold market")
        try:
            selected_date = date.fromisoformat(item["list_date"])
        except (TypeError, ValueError) as exc:
            raise ValueError("threshold list_date requires an ISO date") from exc
        partitions[name] = PartitionConfig(
            provider,
            market,
            selected_date,
            _path(item["manifest"], path.parent, "partition.manifest"),
            _path(item["input"], path.parent, "partition.input"),
            _source_url(item["source_url"], provider, market, selected_date),
        )
    return RuntimeConfig(
        _path(value["root"], path.parent, "root"),
        value["warehouse"],
        PostgresConfig(**pg),
        AcquisitionConfig(**acquisition),
        storage["schema_version"],
        MappingProxyType(partitions),
        parse_network(value["network"], base=path.parent),
    )
