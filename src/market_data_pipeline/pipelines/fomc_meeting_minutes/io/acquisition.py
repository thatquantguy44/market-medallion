"""Bounded source-owned acquisition using one shared run-scoped HTTP route."""

from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import capture
from market_data_pipeline.shared.http import _RunProxyHandler, prepare_http


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never contact a redirect target, including another official-host path.
        return None


def _opener(route, proxies, no_proxy, context):
    proxy = _RunProxyHandler(proxies, no_proxy) if route == "proxy" else ProxyHandler({})
    return build_opener(proxy, HTTPSHandler(context=context), _NoRedirect()).open


def prepare_fomc_http(source_url, manifest, settings):
    """Select/probe once; redirects are refused before contacting their targets."""
    url = manifest.canonical_url(source_url, source_page=True)
    request = Request(url, headers={"User-Agent": "MarketMedallion/1.0"}, method="GET")
    return prepare_http(request, settings, opener_factory=_opener)


def acquire_one(root, *, manifest, store, client, source_url, kind, max_bytes, now=None):
    """Capture one response; limits preserve a marked partial prefix, never claim completeness."""
    url = manifest.capture_url(source_url, kind)
    if type(max_bytes) is not int or not 1 <= max_bytes <= 50_000_000:
        raise ValueError("FOMC acquisition byte limit must be between 1 and 50000000")
    request = Request(
        url, headers={"User-Agent": "MarketMedallion/1.0", "Accept-Encoding": "identity"}
    )
    try:
        response = client(request)
    except HTTPError as exc:
        response = exc
    except (URLError, OSError):
        return capture(
            root,
            b"",
            manifest=manifest,
            store=store,
            source_url=url,
            kind=kind,
            captured_at=(now or (lambda: datetime.now(UTC)))(),
            status=None,
            headers={},
            max_bytes=max_bytes,
            body_complete=False,
            transport_error=True,
        )
    parts, size, complete = [], 0, True
    try:
        while size <= max_bytes:
            try:
                chunk = response.read(min(65536, max_bytes + 1 - size))
            except (OSError, EOFError):
                complete = False
                break
            if not chunk:
                break
            parts.append(chunk)
            size += len(chunk)
        if size > max_bytes:
            complete = False
        status = response.status if hasattr(response, "status") else response.code
        headers = dict(response.headers.items())
    finally:
        response.close()
    captured_at = (now or (lambda: datetime.now(UTC)))()
    return capture(
        root,
        b"".join(parts),
        manifest=manifest,
        store=store,
        source_url=url,
        kind=kind,
        captured_at=captured_at,
        status=status,
        headers=headers,
        max_bytes=max_bytes,
        body_complete=complete,
    )


class AcquisitionLimits:
    """Explicit per-run budgets, loaded independently of secrets and network setup."""

    def __init__(self, *, max_requests, inventory_max_bytes, document_max_bytes):
        if type(max_requests) is not int or not 1 <= max_requests <= 1000:
            raise ValueError("FOMC max_requests must be an integer in [1,1000]")
        for value in (inventory_max_bytes, document_max_bytes):
            if type(value) is not int or not 1 <= value <= 50_000_000:
                raise ValueError("FOMC byte limits must be integers in [1,50000000]")
        self.max_requests = max_requests
        self.inventory_max_bytes = inventory_max_bytes
        self.document_max_bytes = document_max_bytes


def load_acquisition_limits(path):
    from pathlib import Path

    import yaml

    from market_data_pipeline.shared.yaml import UniqueLoader

    try:
        value = yaml.load(Path(path).read_text(), Loader=UniqueLoader)
    except (yaml.YAMLError, UnicodeError):
        raise ValueError("invalid FOMC acquisition limits YAML") from None
    if not isinstance(value, dict) or set(value) != {
        "max_requests",
        "inventory_max_bytes",
        "document_max_bytes",
    }:
        raise ValueError("FOMC acquisition limits require exactly the documented fields")
    return AcquisitionLimits(**value)


class AcquisitionRun:
    def __init__(self, root, *, manifest, store, client, limits):
        if not isinstance(limits, AcquisitionLimits):
            raise ValueError("FOMC acquisition requires validated per-run limits")
        self.root, self.manifest, self.store, self.client = root, manifest, store, client
        self.limits = limits
        self.requests = 0

    def acquire(self, source_url, kind):
        if self.requests >= self.limits.max_requests:
            raise ValueError("FOMC acquisition request budget exhausted")
        if kind not in ("inventory", "document", "asset"):
            raise ValueError("unknown FOMC source kind")
        # Attempted requests count even if the provider or database fails.
        self.requests += 1
        limit = (
            self.limits.inventory_max_bytes
            if kind == "inventory"
            else self.limits.document_max_bytes
        )
        return acquire_one(
            self.root,
            manifest=self.manifest,
            store=self.store,
            client=self.client,
            source_url=source_url,
            kind=kind,
            max_bytes=limit,
        )
