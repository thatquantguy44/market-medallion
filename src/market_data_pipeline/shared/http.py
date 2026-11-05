"""Run-scoped, verified HTTP routing. No global opener or environment mutation."""

import base64
import copy
import logging
import os
import re
import ssl
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import (
    HTTPSHandler,
    ProxyHandler,
    Request,
    build_opener,
    proxy_bypass_environment,
)

import certifi
import yaml

from market_data_pipeline.shared.yaml import UniqueLoader


@dataclass(frozen=True)
class NetworkConfig:
    mode: str = "auto"
    preferred_route: str = "proxy"
    fallback: bool = True
    proxy_url_env: str | None = None
    no_proxy: tuple[str, ...] = ()
    use_environment_no_proxy: bool = True
    probe_timeout_seconds: int = 10
    timeout_seconds: int = 30
    ca_bundle: Path | None = None

    def __post_init__(self):
        if self.mode not in ("auto", "direct", "proxy"):
            raise ValueError("network.mode must be auto, direct or proxy")
        if self.preferred_route not in ("direct", "proxy"):
            raise ValueError("network.preferred_route must be direct or proxy")
        for key in ("fallback", "use_environment_no_proxy"):
            if type(getattr(self, key)) is not bool:
                raise ValueError(f"network.{key} must be boolean")
        for key in ("probe_timeout_seconds", "timeout_seconds"):
            if type(getattr(self, key)) is not int or not 1 <= getattr(self, key) <= 300:
                raise ValueError(f"network.{key} must be an integer in [1,300]")
        if self.proxy_url_env is not None and (
            not isinstance(self.proxy_url_env, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.proxy_url_env)
        ):
            raise ValueError("network.proxy_url_env must name an environment variable or be null")
        if (
            not isinstance(self.no_proxy, tuple)
            or len(self.no_proxy) > 100
            or any(
                not isinstance(host, str) or not re.fullmatch(r"\*|[A-Za-z0-9_.:\[\]-]+", host)
                for host in self.no_proxy
            )
        ):
            raise ValueError("network.no_proxy requires at most 100 hostname suffixes or *")
        if self.ca_bundle is not None and not isinstance(self.ca_bundle, Path):
            raise ValueError("network.ca_bundle must be a path or null")


def parse_network(value, *, base: Path) -> NetworkConfig:
    if not isinstance(value, dict) or set(value) - set(NetworkConfig.__dataclass_fields__):
        raise ValueError("unknown network settings; proxy URLs/credentials belong in environment")
    settings = dict(value)
    if "no_proxy" in settings:
        if not isinstance(settings["no_proxy"], list):
            raise ValueError("network.no_proxy must be a list")
        settings["no_proxy"] = tuple(settings["no_proxy"])
    if settings.get("ca_bundle") is not None:
        ca = settings["ca_bundle"]
        if not isinstance(ca, str) or not ca.strip() or any(ord(c) < 32 for c in ca):
            raise ValueError("network.ca_bundle must be a nonempty path or null")
        settings["ca_bundle"] = (base / Path(ca).expanduser()).resolve()
    return NetworkConfig(**settings)


def load_network_config(path: Path) -> NetworkConfig:
    """A network-only file for entrypoints without an existing runtime loader."""
    try:
        value = yaml.load(Path(path).read_text(), Loader=UniqueLoader)
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ValueError("invalid network YAML") from exc
    if not isinstance(value, dict) or set(value) != {"network"}:
        raise ValueError("network config must contain exactly a network section")
    return parse_network(value["network"], base=Path(path).resolve().parent)


def tls_context(settings: NetworkConfig) -> ssl.SSLContext:
    context = ssl.create_default_context(
        cafile=settings.ca_bundle or os.environ.get("SSL_CERT_FILE") or certifi.where()
    )
    context.set_alpn_protocols(["http/1.1"])
    return context


def _proxy_url(value):
    try:
        parts = urlsplit(value)
        if (
            parts.scheme != "http"
            or not parts.hostname
            or parts.path not in ("", "/")
            or parts.query
            or parts.fragment
            or any(c.isspace() or ord(c) < 32 for c in value)
        ):
            raise ValueError
        if parts.port == 0:  # Access also validates malformed/out-of-range ports.
            raise ValueError
        return parts
    except ValueError:
        raise ValueError("proxy environment value must be an HTTP proxy URL") from None


class _RunProxyHandler(ProxyHandler):
    """Use the snapshotted bypass rules, not urllib's process-global lookup.

    Request.set_proxy/CONNECT follow the Python 3.12 ProxyHandler contract:
    https://docs.python.org/3.12/library/urllib.request.html#urllib.request.Request.set_proxy
    """

    def __init__(self, proxies, no_proxy):
        self.no_proxy = no_proxy
        # Handle both schemes so a redirect cannot silently lose proxy routing.
        super().__init__({scheme: proxies.get(scheme, "") for scheme in ("http", "https")})

    def proxy_open(self, request, proxy, protocol):
        if proxy_bypass_environment(request.host, {"no": self.no_proxy}):
            return None
        if not proxy:
            raise URLError("selected proxy route has no proxy for the destination scheme")
        parts = _proxy_url(proxy)
        host = parts.netloc.rsplit("@", 1)[-1]
        if parts.username is not None:
            credentials = f"{unquote(parts.username)}:{unquote(parts.password or '')}"
            token = base64.b64encode(credentials.encode()).decode("ascii")
            request.add_unredirected_header("Proxy-Authorization", f"Basic {token}")
        request.set_proxy(host, "http")
        return None


def _make_opener(route, proxies, no_proxy, context):
    # An explicit empty ProxyHandler disables OS/environment proxy discovery.
    handler = _RunProxyHandler(proxies, no_proxy) if route == "proxy" else ProxyHandler({})
    return build_opener(handler, HTTPSHandler(context=context)).open


def _clone(request):
    result = copy.copy(request)
    result.headers = dict(request.headers)
    result.unredirected_hdrs = dict(request.unredirected_hdrs)
    return result


class HttpRun:
    def __init__(self, route, fallback_used, open_request, settings):
        self.route = route
        self.fallback_used = fallback_used
        self._open = open_request
        self.settings = settings

    def __call__(self, request, *, timeout=None, context=None):
        # Keep the injectable urlopen signature; this run owns timeout/TLS policy.
        try:
            return self._open(_clone(request), timeout=self.settings.timeout_seconds)
        except HTTPError:
            raise
        except (URLError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, ssl.SSLError):
                raise URLError(ssl.SSLError("verified TLS connection failed")) from None
            raise URLError(
                f"request failed on selected {self.route} route: {type(exc).__name__}"
            ) from None


def prepare_http(
    request: Request, settings=None, *, context=None, opener_factory=None, environ=None
):
    """Probe the permitted routes once; return a client that never changes route.

    HTTP errors are provider/proxy decisions, not connectivity failures. HEAD
    405/501 permits GET; other HTTP errors abort without route fallback.
    """
    settings = settings or NetworkConfig()
    env = dict(os.environ if environ is None else environ)
    context = context or tls_context(settings)
    if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
        raise ValueError("network TLS certificate and hostname verification must remain enabled")
    factory = opener_factory or _make_opener
    parts = urlsplit(request.full_url)
    if parts.scheme not in ("http", "https") or request.get_method() not in ("GET", "HEAD"):
        raise ValueError("startup routing requires an HTTP(S) GET/HEAD provider request")
    no_proxy = list(settings.no_proxy)
    if settings.use_environment_no_proxy:
        no_proxy.extend(env.get("no_proxy", env.get("NO_PROXY", "")).split(","))
    bypass = ",".join(no_proxy)
    excluded = proxy_bypass_environment(request.host, {"no": bypass})
    preferred = settings.preferred_route if settings.mode == "auto" else settings.mode
    routes = [preferred]
    if settings.mode == "auto" and settings.fallback:
        routes.append("direct" if preferred == "proxy" else "proxy")
    if excluded:
        routes = ["direct"]
    failures = []
    for route in routes:
        proxies = {}
        if route == "proxy":
            for scheme in ("http", "https"):
                value = (
                    env.get(settings.proxy_url_env, "")
                    if settings.proxy_url_env
                    else env.get(
                        f"{scheme}_proxy",
                        env.get(
                            f"{scheme.upper()}_PROXY",
                            env.get("all_proxy", env.get("ALL_PROXY", "")),
                        ),
                    )
                )
                if value:
                    _proxy_url(value)
                    proxies[scheme] = value
            if parts.scheme not in proxies:
                failures.append("proxy is not configured")
                continue
        open_request = factory(route, proxies, bypass, context)
        probe = Request(request.full_url, headers=dict(request.header_items()), method="HEAD")
        try:
            with open_request(probe, timeout=settings.probe_timeout_seconds) as response:
                status = response.code
                if not 200 <= status < 400 and status not in (405, 501):
                    raise ValueError(f"network startup rejected: HTTP {status}")
        except HTTPError as exc:
            status = exc.code
            exc.close()
            if status not in (405, 501):
                raise ValueError(f"network startup rejected: HTTP {status}") from None
        except (URLError, OSError) as exc:
            # CPython reports an HTTP CONNECT refusal as URLError(OSError(...)).
            # Keep a proxy's HTTP rejection out of connection-failure fallback.
            reason = str(getattr(exc, "reason", exc))
            refused = re.fullmatch(r"Tunnel connection failed: ([0-9]{3})(?: .*)?", reason)
            if refused:
                raise ValueError(f"network startup rejected: HTTP {refused[1]}") from None
            failures.append(f"{route}: {type(exc).__name__}")
            continue
        client = HttpRun(route, bool(failures), open_request, settings)
        logging.getLogger(__name__).info(
            "HTTP route selected: %s; startup fallback: %s", route, client.fallback_used
        )
        return client
    raise ValueError("network startup failed: " + "; ".join(failures)) from None
