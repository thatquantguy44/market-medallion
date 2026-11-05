"""Bounded exact-partition acquisition for Nasdaq and NYSE threshold lists."""

import hashlib
import os
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.manifest import load_manifest
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock
from market_data_pipeline.shared.http import prepare_http


@dataclass(frozen=True)
class AcquiredPartition:
    path: Path
    metadata: dict


def _agent(config):
    value = os.environ.get(config.acquisition.user_agent_env, "").strip()
    if (
        not value
        or len(value) > 512
        or not re.search(r"@|https?://", value)
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError(
            f"set {config.acquisition.user_agent_env} to a declared User-Agent with contact"
        )
    return value


def acquire_partition(config_path: Path, partition: str, *, transport=None, clock=None):
    config = load_config(Path(config_path))
    if not config.acquisition.enabled:
        raise ValueError("threshold acquisition is disabled in configuration")
    if partition not in config.partitions:
        raise ValueError("partition is not present in the threshold config")
    selected = config.partitions[partition]
    if selected.source_url is None:
        raise ValueError("threshold partition has no configured source URL")
    manifest = load_manifest(selected.manifest)
    request = Request(
        selected.source_url,
        headers={"Accept": "text/plain", "User-Agent": _agent(config)},
        method="GET",
    )
    if transport is None:
        client = prepare_http(request, config.network)
        route, fallback = client.route, client.fallback_used
    else:
        client, route, fallback = transport, "injected", False
    try:
        with client(request) as response:
            status = int(getattr(response, "status", getattr(response, "code", 0)))
            if status != 200:
                raise ValueError(f"threshold acquisition returned HTTP {status}")
            length = response.headers.get("Content-Length") if response.headers else None
            invalid_length = length is not None and (
                not str(length).isdigit() or int(length) > manifest.max_bytes
            )
            if invalid_length:
                raise ValueError("threshold acquisition exceeds the manifest byte limit")
            payload = response.read(manifest.max_bytes + 1)
            if not payload or len(payload) > manifest.max_bytes:
                raise ValueError("threshold acquisition returned an empty or oversized payload")
            content_type = response.headers.get("Content-Type") if response.headers else None
    except HTTPError as exc:
        status = exc.code
        exc.close()
        raise ValueError(f"threshold acquisition returned HTTP {status}") from None
    except URLError as exc:
        raise ValueError("threshold acquisition request failed") from exc
    digest = hashlib.sha256(payload).hexdigest()
    retrieved_at = utc_timestamp((clock or (lambda: datetime.now(UTC)))())
    staging_root = config.root.parent / f".{config.root.name}.acquisition"
    directory = staging_root / partition / digest
    metadata = {
        "receipt_version": "reg-sho-acquisition/1",
        "partition": partition,
        "provider": selected.provider,
        "market": selected.market,
        "list_date": selected.list_date.isoformat(),
        "source_url": selected.source_url,
        "retrieved_at": retrieved_at,
        "http_status": status,
        "http_route": route,
        "startup_fallback_used": fallback,
        "content_type": str(content_type) if content_type is not None else None,
        "payload_bytes": len(payload),
        "payload_sha256": digest,
    }
    with writer_lock(staging_root):
        if directory.exists():
            file = directory / "payload.txt"
            staged_digest = (
                hashlib.sha256(file.read_bytes()).hexdigest()
                if file.is_file() and not file.is_symlink()
                else None
            )
            if staged_digest != digest:
                raise ValueError("threshold acquisition staging is inconsistent")
        else:
            temporary = directory.with_name(f".{digest}.{uuid.uuid4().hex}.tmp")
            temporary.mkdir(parents=True, exist_ok=False)
            try:
                with (temporary / "payload.txt").open("xb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                atomic_json(temporary / "receipt.json", metadata)
                os.replace(temporary, directory)
            finally:
                if temporary.exists():
                    for item in temporary.iterdir():
                        item.unlink()
                    temporary.rmdir()
    return AcquiredPartition(directory / "payload.txt", metadata)
