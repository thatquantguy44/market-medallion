"""Explicit offline import; there is no provider network operation here."""

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from market_data_pipeline.shared.files import atomic_json

from .config import PipelineConfig, load_config
from .parsers import ParseError, parse_payload
from .storage import MAX_CONFIG_BYTES, archive_capture


def import_local_capture(
    root: Path,
    config: PipelineConfig,
    source_id: str,
    payload: bytes,
    *,
    admitted_at: datetime,
    input_kind: str,
    retrieved_at: datetime | None = None,
    expected_sha256: str | None = None,
) -> tuple[dict, dict]:
    """Preserve raw input before parsing, including explicit rejection evidence."""
    with config.path.open("rb") as source_file:
        config_bytes = source_file.read(MAX_CONFIG_BYTES + 1)
    if len(config_bytes) > MAX_CONFIG_BYTES:
        raise ValueError("Source catalog exceeds the size limit.")
    with NamedTemporaryFile(prefix="presidential-config-", suffix=".toml") as snapshot:
        snapshot.write(config_bytes)
        snapshot.flush()
        if replace(load_config(Path(snapshot.name)), path=config.path) != config:
            raise ValueError("Source catalog changed since selection.")
    source = config.source(source_id)
    receipt = archive_capture(
        root,
        source_id,
        payload,
        config_bytes,
        admitted_at=admitted_at,
        input_kind=input_kind,
        retrieved_at=retrieved_at,
        expected_sha256=expected_sha256,
        parser_id=source.parser,
    )
    try:
        rows = parse_payload(source.parser, payload, source_id)
        outcome = {"status": "PARSED", "row_count": len(rows), "error_code": None}
    except (ParseError, ValueError, UnicodeError):
        outcome = {"status": "REJECTED", "row_count": 0, "error_code": "PARSE_REJECTED"}
    atomic_json(Path(root) / "bronze" / source_id / receipt["capture_id"] / "parse.json", outcome)
    return receipt, outcome
