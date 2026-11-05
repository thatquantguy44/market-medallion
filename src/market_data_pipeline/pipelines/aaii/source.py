"""Explicit AAII retrieval with bounded, observable response retries."""

import ssl
import time
import uuid
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request

from market_data_pipeline.pipelines.aaii.data.xls import selection
from market_data_pipeline.pipelines.aaii.io.bronze import archive
from market_data_pipeline.pipelines.aaii.manifest import load_manifest
from market_data_pipeline.pipelines.aaii.pipeline import MAX_INPUT_BYTES, promote_capture
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock
from market_data_pipeline.shared.http import NetworkConfig, prepare_http, tls_context


def utc_now():
    return datetime.now(UTC)


def _retry_delay(headers, attempt, now):
    value = headers.get("Retry-After")
    if value:
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = (parsedate_to_datetime(value) - now).total_seconds()
            except (ValueError, TypeError):
                return None
        return max(0, delay) if delay <= 30 else None
    return min(2**attempt, 30)


def run_live(
    manifest_path,
    root,
    *,
    start,
    end,
    dsn=None,
    transport=None,
    clock=None,
    sleeper=None,
    max_attempts=1,
    network: NetworkConfig | None = None,
):
    clock, sleeper = clock or utc_now, sleeper or time.sleep
    network = network or NetworkConfig()
    if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
        raise ValueError("max_attempts must be in [1,5]")
    manifest = load_manifest(manifest_path)
    if manifest.format != "xls":
        raise ValueError("live AAII runs require the official XLS manifest")
    bounds = selection(start, end)
    if bounds[1] > clock().date():
        raise ValueError("live date selection cannot end in the future")
    url = manifest.access["source_url"]
    request = Request(
        url, headers={"User-Agent": "market-medallion/0.1 (bounded research verification)"}
    )
    with writer_lock(root):
        context = tls_context(network)
        if transport is None:
            try:
                transport = prepare_http(request, network, context=context)
            except ValueError:
                log = root / "operations" / "transport"
                log.mkdir(parents=True, exist_ok=True)
                atomic_json(
                    log / f"{uuid.uuid4().hex}.json",
                    {
                        "captured_at": utc_timestamp(clock()),
                        "error_type": "network_startup_failed",
                        "attempt": 0,
                        "url": url,
                    },
                )
                raise
        for attempt in range(max_attempts):
            try:
                try:
                    response = transport(request, timeout=network.timeout_seconds, context=context)
                except HTTPError as exc:
                    response = exc
                with response:
                    payload = response.read(MAX_INPUT_BYTES + 1)
                    status = response.code
                    headers = {
                        key: response.headers[key]
                        for key in ("Content-Type", "Last-Modified", "ETag", "Retry-After")
                        if key in response.headers
                    }
            except (URLError, TimeoutError) as exc:
                log = root / "operations" / "transport"
                log.mkdir(parents=True, exist_ok=True)
                atomic_json(
                    log / f"{uuid.uuid4().hex}.json",
                    {
                        "captured_at": utc_timestamp(clock()),
                        "error_type": type(exc).__name__,
                        "attempt": attempt + 1,
                        "url": url,
                    },
                )
                if attempt + 1 < max_attempts and not isinstance(
                    getattr(exc, "reason", None), ssl.SSLError
                ):
                    sleeper(min(2**attempt, 30))
                    continue
                raise ValueError(f"AAII transport failed: {type(exc).__name__}") from exc
            if len(payload) > MAX_INPUT_BYTES:
                raise ValueError("AAII download exceeds 20 MiB limit")
            envelope, directory = archive(
                root,
                payload,
                manifest,
                None,
                clock(),
                transport={
                    "transport": "http",
                    "source_uri": url,
                    "http_status": status,
                    "http_headers": headers,
                    "network_route": getattr(transport, "route", "injected"),
                    "network_fallback_used": getattr(transport, "fallback_used", False),
                },
                date_selection={"start": start, "end": end},
            )
            if status in {429, 500, 502, 503, 504}:
                try:
                    promote_capture(root, envelope, payload, manifest, directory, dsn)
                except ValueError:
                    pass
                delay = _retry_delay(headers, attempt, clock())
                if attempt + 1 < max_attempts and delay is not None:
                    sleeper(delay)
                    continue
                raise ValueError(f"AAII download returned HTTP {status}; retry limit reached")
            return promote_capture(root, envelope, payload, manifest, directory, dsn)
    raise ValueError("AAII retry limit reached")
