"""Bounded, exact-URL SEC FTD acquisition with run-scoped HTTP routing."""

import hashlib
import os
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import load_manifest
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock
from market_data_pipeline.shared.http import prepare_http


@dataclass(frozen=True)
class AcquiredPartition:
    path: Path
    metadata: dict


def _user_agent(config) -> str:
    value = os.environ.get(config.acquisition.user_agent_env, "").strip()
    if (
        not value
        or len(value) > 512
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
        or not re.search(r"@|https?://", value)
    ):
        raise ValueError(
            f"set {config.acquisition.user_agent_env} to a declared SEC User-Agent with contact"
        )
    return value


def _header(response, name: str) -> str | None:
    headers = getattr(response, "headers", None)
    value = headers.get(name) if headers is not None else None
    if value is None:
        return None
    value = str(value).strip()
    if len(value) > 1024 or any(ord(char) < 32 and char not in "\t" for char in value):
        raise ValueError("SEC response contains invalid metadata")
    return value


def _download(client, request: Request, maximum: int) -> tuple[bytes, int, dict]:
    try:
        with client(request) as response:
            status = int(getattr(response, "status", getattr(response, "code", 0)))
            if status != 200:
                raise ValueError(f"SEC acquisition returned HTTP {status}")
            length = _header(response, "Content-Length")
            if length is not None:
                if not length.isascii() or not length.isdigit() or int(length) > maximum:
                    raise ValueError("SEC acquisition exceeds the manifest archive limit")
            payload = response.read(maximum + 1)
            if len(payload) > maximum:
                raise ValueError("SEC acquisition exceeds the manifest archive limit")
            if not payload:
                raise ValueError("SEC acquisition returned an empty payload")
            return (
                payload,
                status,
                {
                    "content_type": _header(response, "Content-Type"),
                    "etag": _header(response, "ETag"),
                    "last_modified": _header(response, "Last-Modified"),
                },
            )
    except HTTPError as exc:
        status = exc.code
        exc.close()
        raise ValueError(f"SEC acquisition returned HTTP {status}") from None
    except URLError as exc:
        raise ValueError("SEC acquisition request failed") from exc


def acquire_partition(
    config_path: Path,
    partition: str,
    *,
    transport=None,
    clock=None,
) -> AcquiredPartition:
    """Fetch one explicitly configured official ZIP and retain a staging receipt.

    An injected transport skips route probing and is intended for deterministic
    tests. Production selects direct/proxy once through ``prepare_http``.
    """
    config = load_config(Path(config_path))
    if not config.acquisition.enabled:
        raise ValueError("SEC FTD acquisition is disabled in configuration")
    if partition not in config.partitions:
        raise ValueError("partition is not present in the SEC FTD config")
    selected = config.partitions[partition]
    if selected.source_url is None:
        raise ValueError("partition has no configured SEC source URL")
    manifest = load_manifest(selected.manifest)
    request = Request(
        selected.source_url,
        headers={
            "Accept": "application/zip, application/octet-stream",
            "User-Agent": _user_agent(config),
        },
        method="GET",
    )
    if transport is None:
        client = prepare_http(request, config.network)
        route = client.route
        fallback_used = client.fallback_used
    else:
        client = transport
        route = "injected"
        fallback_used = False
    payload, status, headers = _download(client, request, manifest.max_archive_bytes)
    digest = hashlib.sha256(payload).hexdigest()
    retrieved_at = utc_timestamp((clock or (lambda: datetime.now(UTC)))())
    # Keep pre-admission staging beside the durable root: a new warehouse root
    # must remain empty until its identity is bound transactionally.
    staging_root = config.root.parent / f".{config.root.name}.acquisition"
    directory = staging_root / partition / digest
    metadata = {
        "receipt_version": "sec-ftd-acquisition/1",
        "partition": partition,
        "source_url": selected.source_url,
        "retrieved_at": retrieved_at,
        "http_status": status,
        "http_route": route,
        "startup_fallback_used": fallback_used,
        "payload_bytes": len(payload),
        "payload_sha256": digest,
        **headers,
    }
    with writer_lock(staging_root):
        if directory.exists():
            if directory.is_symlink() or not directory.is_dir():
                raise ValueError("SEC acquisition staging path is unsafe")
            existing = directory / "payload.zip"
            existing_digest = (
                hashlib.sha256(existing.read_bytes()).hexdigest() if existing.is_file() else None
            )
            if existing_digest != digest:
                raise ValueError("SEC acquisition staging payload is inconsistent")
        else:
            temporary = directory.with_name(f".{digest}.{uuid.uuid4().hex}.tmp")
            temporary.mkdir(parents=True, exist_ok=False)
            try:
                with (temporary / "payload.zip").open("xb") as stream:
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
    return AcquiredPartition(directory / "payload.zip", metadata)
