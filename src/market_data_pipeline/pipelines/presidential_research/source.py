"""Bounded source acquisition. Every production source is presently unapproved."""

from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request

from market_data_pipeline.shared.http import NetworkConfig, prepare_http

from .pipeline import import_local_capture
from .storage import MAX_RAW_BYTES

# Admission requires a reviewed code change with source-specific evidence.
# Neither a CLI switch nor an environment variable can bypass these entries.
_APPROVED_LIVE_SOURCES: dict[str, str] = {}


def fetch_selected(root, config, source_ids, *, network=None, transport=None, clock=None):
    """Fetch a selected batch; injected transports only produce fictional fixtures.

    All authorization and URL checks happen before the shared client can probe.
    A failure for one source does not discard previously archived captures.
    """
    if not source_ids or len(source_ids) != len(set(source_ids)):
        raise ValueError("Select one or more distinct source IDs.")
    sources = [config.source(source_id) for source_id in source_ids]
    for source in sources:
        if not source.enabled:
            raise ValueError("Manual or disabled sources cannot be fetched.")
        parts = urlsplit(source.url)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username
            or parts.password
            or parts.fragment
            or parts.port not in (None, 443)
        ):
            raise ValueError("Source requires reviewed HTTPS transport.")
        if transport is None and _APPROVED_LIVE_SOURCES.get(source.id) != source.url:
            raise ValueError("SOURCE_ACCESS_NOT_REVIEWED: live acquisition is disabled.")
    requests = [
        Request(source.url, headers={"User-Agent": "MarketMedallion-PRES/1.0"}, method="GET")
        for source in sources
    ]
    client = (
        transport
        if transport is not None
        else prepare_http(requests[0], network or NetworkConfig(), allow_redirects=False)
    )
    injected = transport is not None
    results = []
    for source, request in zip(sources, requests, strict=True):
        try:
            payload, headers = _download(client, request)
            now = (clock or (lambda: datetime.now(UTC)))()
            receipt, outcome = import_local_capture(
                Path(root),
                config,
                source.id,
                payload,
                admitted_at=now,
                retrieved_at=now,
                input_kind="SYNTHETIC_FIXTURE" if injected else "CAPTURED_SOURCE",
                acquisition={
                    "transport": "injected_fixture" if injected else "https_fetch",
                    "http_status": 200,
                    "route": "injected" if injected else client.route,
                    "startup_fallback_used": False if injected else client.fallback_used,
                    **headers,
                },
            )
            results.append(
                {
                    "source_id": source.id,
                    "status": outcome["status"],
                    "capture_id": receipt["capture_id"],
                    "input_kind": receipt["input_kind"],
                }
            )
        except (HTTPError, URLError, OSError, ValueError) as exc:
            if isinstance(exc, HTTPError):
                exc.close()
            # Do not expose provider URLs, proxy credentials, or raw exception text.
            results.append(
                {
                    "source_id": source.id,
                    "status": "FETCH_FAILED",
                    "error_code": "SOURCE_REQUEST_FAILED",
                }
            )
    return {
        "status": "SUCCEEDED"
        if all(r["status"] == "PARSED" for r in results)
        else "PARTIAL_OR_FAILED",
        "sources": results,
    }


def _download(client, request):
    for attempt in range(2):
        try:
            with client(request) as response:
                status = int(getattr(response, "status", getattr(response, "code", 0)))
                if status in (500, 502, 503, 504) and attempt == 0:
                    continue
                if status != 200:
                    raise ValueError("Unexpected source response.")
                length = response.headers.get("Content-Length")
                if length is not None and (
                    not length.isascii() or not length.isdigit() or int(length) > MAX_RAW_BYTES
                ):
                    raise ValueError("Invalid or excessive response length.")
                payload = response.read(MAX_RAW_BYTES + 1)
                if not payload or len(payload) > MAX_RAW_BYTES:
                    raise ValueError("Empty or excessive source response.")
                if length is not None and len(payload) != int(length):
                    raise ValueError("Incomplete source response.")
                headers = {}
                for header, key in (
                    ("Content-Type", "content_type"),
                    ("ETag", "etag"),
                    ("Last-Modified", "last_modified"),
                ):
                    value = response.headers.get(header)
                    if value is not None and (
                        len(value) > 512 or any(ord(c) < 32 or ord(c) == 127 for c in value)
                    ):
                        raise ValueError("Invalid source response metadata.")
                    headers[key] = value
                return payload, headers
        except HTTPError as exc:
            if exc.code in (500, 502, 503, 504) and attempt == 0:
                exc.close()
                continue
            raise
        except (URLError, TimeoutError):
            if attempt:
                raise
    raise ValueError("Source request failed.")
