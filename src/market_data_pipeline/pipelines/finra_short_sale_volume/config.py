"""SSV-AC06: strict local settings; no network, credentials or service startup."""

from dataclasses import dataclass
from pathlib import Path

import yaml

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import settings
from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import Manifest, load_manifest
from market_data_pipeline.pipelines.finra_short_sale_volume.pipeline import _operation
from market_data_pipeline.shared.yaml import UniqueLoader


@dataclass(frozen=True)
class LocalConfig:
    root: Path
    manifest: Manifest
    operation: str
    approved_use: dict
    max_bytes: int
    max_rows: int

    @property
    def transform_operation(self):
        return "fixture" if self.operation == "fixture" else "transform"


def load_config(path):
    path = Path(path)
    try:
        with path.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError
        value = yaml.load(raw, Loader=UniqueLoader)
        if (
            not isinstance(value, dict)
            or set(value)
            != {"schema_id", "root", "manifest", "operation", "approved_use", "limits"}
            or value["schema_id"] != "finra-ssv-local-config/1"
        ):
            raise ValueError
        for key in ("root", "manifest"):
            if not isinstance(value[key], str) or not value[key].strip():
                raise ValueError
        limits = value["limits"]
        if not isinstance(limits, dict) or set(limits) != {"max_bytes", "max_rows"}:
            raise ValueError
        settings(**limits)
        manifest = load_manifest((path.parent / value["manifest"]).resolve())
        _operation(manifest, value["approved_use"], value["operation"])
        return LocalConfig(
            (path.parent / value["root"]).resolve(),
            manifest,
            value["operation"],
            value["approved_use"],
            **limits,
        )
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        yaml.YAMLError,
        RecursionError,
    ):
        raise ValueError("invalid FINRA local configuration") from None
