"""Explicit, permission-gated retrieval of NAAIM's delayed public table."""

from dataclasses import dataclass
from urllib.request import Request

from market_data_pipeline.shared.http import NetworkConfig, prepare_http, tls_context

TABLE_URL = "https://index.naaim.org/embeddable/table"
MAX_INPUT_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class FetchResponse:
    payload: bytes
    source_url: str
    status: int
    headers: dict[str, str]
    network_route: str = "injected"
    network_fallback_used: bool = False


def fetch_table(
    *, permission_reference: str, transport=None, network: NetworkConfig | None = None
) -> FetchResponse:
    """Fetch one table only when a nonsecret written-permission reference is supplied."""
    if not isinstance(permission_reference, str) or not permission_reference.strip():
        raise ValueError("permission_reference is required for a live NAAIM request")
    network = network or NetworkConfig()
    request = Request(
        TABLE_URL,
        headers={"User-Agent": "market-medallion/0.1 (permitted delayed research)"},
    )
    context = tls_context(network)
    transport = transport or prepare_http(request, network, context=context)
    with transport(request, timeout=network.timeout_seconds, context=context) as response:
        payload = response.read(MAX_INPUT_BYTES + 1)
        status = response.code
        headers = {
            key: response.headers[key]
            for key in ("Content-Type", "Last-Modified", "ETag", "Retry-After")
            if key in response.headers
        }
    if len(payload) > MAX_INPUT_BYTES:
        raise ValueError("NAAIM public table exceeds 2 MiB limit")
    if status != 200:
        raise ValueError(f"NAAIM public table returned HTTP {status}")
    return FetchResponse(
        payload=payload,
        source_url=TABLE_URL,
        status=status,
        headers=headers,
        network_route=getattr(transport, "route", "injected"),
        network_fallback_used=getattr(transport, "fallback_used", False),
    )
