"""SSV-AC12 strict source-owned acquisition configuration; no credential reads or HTTP."""

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.finra_short_sale_volume.data.acquisition import Limits
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import _admit
from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import Manifest, load_manifest
from market_data_pipeline.shared.http import NetworkConfig, parse_network
from market_data_pipeline.shared.yaml import UniqueLoader


@dataclass(frozen=True)
class AcquisitionConfig:
    root: Path
    manifest: Manifest
    operation: str
    approved_use: dict
    live: bool = False
    client_id_env: str = "FINRA_API_KEY"
    client_secret_env: str = "FINRA_PASSWORD_SECRET"
    network: NetworkConfig = NetworkConfig()
    limits: Limits = Limits()

    def validate(self):
        _admit(self.manifest, self.approved_use, self.operation, {"acquire", "fixture"})
        _admit(
            self.manifest,
            self.approved_use,
            "fixture" if self.operation == "fixture" else "transform",
            {"fixture", "transform"},
        )
        if self.operation == "acquire" and self.approved_use["route"] != "finra_query_api":
            raise PermissionError("FINRA acquisition requires the admitted Public Query API route")
        if (
            type(self.live) is not bool
            or not isinstance(self.root, Path)
            or not isinstance(self.network, NetworkConfig)
            or not isinstance(self.limits, Limits)
        ):
            raise ValueError("FINRA acquisition requires validated settings")
        for value in (self.client_id_env, self.client_secret_env):
            if not isinstance(value, str) or not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]{0,127}", value
            ):
                raise ValueError("FINRA credentials must name environment variables")
        if self.client_id_env == self.client_secret_env:
            raise ValueError("FINRA credentials require distinct environment references")


def load_config(path):
    path = Path(path)
    try:
        with path.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError
        value = yaml.load(raw, Loader=UniqueLoader)
        required = {
            "schema_id",
            "root",
            "manifest",
            "operation",
            "approved_use",
            "live",
            "credentials",
            "limits",
            "network",
        }
        if (
            not isinstance(value, dict)
            or set(value) != required
            or value["schema_id"] != "finra-ssv-acquisition-config/1"
        ):
            raise ValueError
        for key in ("root", "manifest"):
            if not isinstance(value[key], str) or not value[key].strip():
                raise ValueError
        credentials = value["credentials"]
        if not isinstance(credentials, dict) or set(credentials) != {
            "client_id_env",
            "client_secret_env",
        }:
            raise ValueError
        if not isinstance(value["limits"], dict):
            raise ValueError
        result = AcquisitionConfig(
            root=(path.parent / value["root"]).resolve(),
            manifest=load_manifest((path.parent / value["manifest"]).resolve()),
            operation=value["operation"],
            approved_use=value["approved_use"],
            live=value["live"],
            **credentials,
            limits=Limits(**value["limits"]),
            network=parse_network(value["network"], base=path.parent.resolve()),
        )
        result.validate()
        return result
    except PermissionError:
        raise
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        yaml.YAMLError,
        RecursionError,
    ):
        raise ValueError("invalid FINRA acquisition configuration") from None
