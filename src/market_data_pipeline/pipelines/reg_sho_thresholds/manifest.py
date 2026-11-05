"""Strict provider-specific Reg SHO physical manifests."""

from dataclasses import dataclass
from pathlib import Path

import yaml

from market_data_pipeline.shared.yaml import UniqueLoader


@dataclass(frozen=True)
class Manifest:
    provider: str
    schema_version: str
    encoding: str
    header: tuple[str, ...]
    allowed_markets: tuple[str, ...]
    max_bytes: int
    max_rows: int
    max_record_bytes: int
    source_page: str


def load_manifest(path: Path) -> Manifest:
    try:
        value = yaml.load(Path(path).read_text(), Loader=UniqueLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ValueError("invalid threshold manifest") from exc
    if not isinstance(value, dict) or set(value) != {
        "provider",
        "dataset",
        "schema_version",
        "format",
        "encoding",
        "header",
        "allowed_markets",
        "limits",
        "access",
    }:
        raise ValueError("threshold manifest requires exactly the documented fields")
    provider = value["provider"]
    expected = {
        "nasdaq": ("nasdaq-reg-sho-pipe/1", "ascii"),
        "nyse": ("nyse-reg-sho-pipe/1", "utf-8"),
    }
    if (
        provider not in expected
        or value["dataset"] != "reg-sho-thresholds"
        or value["schema_version"] != expected[provider][0]
        or value["format"] != "pipe-text"
        or value["encoding"] != expected[provider][1]
    ):
        raise ValueError("unsupported threshold provider/schema profile")
    header = value["header"]
    markets = value["allowed_markets"]
    if (
        not isinstance(header, list)
        or len(header) != 6
        or any(not isinstance(item, str) or not item for item in header)
        or not isinstance(markets, list)
        or not markets
        or any(not isinstance(item, str) or not item for item in markets)
    ):
        raise ValueError("invalid threshold header or market contract")
    limits = value["limits"]
    access = value["access"]
    if not isinstance(limits, dict) or set(limits) != {"bytes", "rows", "record_bytes"}:
        raise ValueError("invalid threshold limits")
    if (
        any(type(limits[key]) is not int for key in limits)
        or not 1 <= limits["bytes"] <= 16 * 1024 * 1024
        or not 1 <= limits["rows"] <= 1_000_000
        or not 64 <= limits["record_bytes"] <= 65536
    ):
        raise ValueError("threshold limits are outside supported bounds")
    if (
        not isinstance(access, dict)
        or set(access) != {"source_page", "automation"}
        or access["automation"] != "disabled-by-default"
        or not isinstance(access["source_page"], str)
        or not access["source_page"].startswith("https://")
    ):
        raise ValueError("invalid threshold access metadata")
    return Manifest(
        provider,
        value["schema_version"],
        value["encoding"],
        tuple(header),
        tuple(markets),
        limits["bytes"],
        limits["rows"],
        limits["record_bytes"],
        access["source_page"],
    )
