"""Outbound contract: select once per run, preserve policy, never leak proxy secrets."""

import io
import ssl
from urllib.error import HTTPError, URLError
from urllib.request import Request

import pytest

from market_data_pipeline.shared.http import NetworkConfig, parse_network, prepare_http

URL = "https://provider.invalid/data"


class Response(io.BytesIO):
    code = 200
    headers = {}


def factory_with(failures, calls, contexts):
    def factory(route, proxies, no_proxy, context):
        contexts.append(context)

        def open_request(request, *, timeout):
            calls.append((route, request.get_method(), timeout))
            if route in failures:
                raise failures[route]
            return Response(b"fixture")

        return open_request

    return factory


@pytest.mark.parametrize("preferred", ["proxy", "direct"])
def test_auto_falls_back_once_then_reuses_selection_and_retests_next_run(preferred):
    calls, contexts = [], []
    other = "direct" if preferred == "proxy" else "proxy"
    settings = NetworkConfig(preferred_route=preferred, probe_timeout_seconds=2, timeout_seconds=7)
    failures = {preferred: URLError("connection refused")}
    factory = factory_with(failures, calls, contexts)
    kwargs = {"opener_factory": factory, "environ": {"HTTPS_PROXY": "http://proxy.invalid:8080"}}
    client = prepare_http(Request(URL), settings, **kwargs)
    for _ in range(3):
        with client(Request(URL)) as response:
            assert response.read() == b"fixture"
    assert calls == [(preferred, "HEAD", 2), (other, "HEAD", 2)] + [(other, "GET", 7)] * 3
    assert client.route == other and client.fallback_used
    assert all(c.check_hostname and c.verify_mode == ssl.CERT_REQUIRED for c in contexts)
    failures.clear()
    assert prepare_http(Request(URL), settings, **kwargs).route == preferred
    assert calls[-1] == (preferred, "HEAD", 2)


@pytest.mark.parametrize(
    "settings",
    [NetworkConfig(mode="direct"), NetworkConfig(mode="proxy"), NetworkConfig(fallback=False)],
)
def test_pinned_routes_never_fallback(settings):
    calls = []
    with pytest.raises(ValueError, match="network startup failed"):
        prepare_http(
            Request(URL),
            settings,
            opener_factory=factory_with(
                {"proxy": TimeoutError(), "direct": TimeoutError()}, calls, []
            ),
            environ={"HTTPS_PROXY": "http://proxy.invalid:8080"},
        )
    assert len(calls) == 1


def test_mid_run_failure_does_not_reprobe_or_switch_routes():
    failures, calls = {}, []
    client = prepare_http(
        Request(URL),
        NetworkConfig(mode="direct"),
        opener_factory=factory_with(failures, calls, []),
        environ={},
    )
    failures["direct"] = URLError("connection lost")
    with pytest.raises(URLError):
        client(Request(URL))
    assert calls == [("direct", "HEAD", 10), ("direct", "GET", 30)]


@pytest.mark.parametrize("status", [403, 407, 429, 500, 503])
def test_http_rejections_never_try_the_other_route(status):
    calls = []
    error = HTTPError(URL, status, "rejected", {}, io.BytesIO(b"error"))
    with pytest.raises(ValueError, match=f"HTTP {status}"):
        prepare_http(
            Request(URL),
            opener_factory=factory_with({"proxy": error}, calls, []),
            environ={"HTTPS_PROXY": "http://proxy.invalid"},
        )
    assert calls == [("proxy", "HEAD", 10)]
    assert error.closed


@pytest.mark.parametrize("status", [405, 501])
def test_head_unsupported_keeps_the_route(status):
    calls = []
    error = HTTPError(URL, status, "HEAD unsupported", {}, io.BytesIO())
    client = prepare_http(
        Request(URL),
        opener_factory=factory_with({"proxy": error}, calls, []),
        environ={"HTTPS_PROXY": "http://proxy.invalid"},
    )
    assert client.route == "proxy" and not client.fallback_used
    assert error.closed


def test_missing_proxy_and_bypass_select_direct_without_proxy_probe():
    for env, settings in [
        ({}, NetworkConfig()),
        ({"HTTPS_PROXY": "http://proxy.invalid", "NO_PROXY": ".invalid"}, NetworkConfig()),
        ({"HTTPS_PROXY": "http://proxy.invalid"}, NetworkConfig(no_proxy=("provider.invalid",))),
    ]:
        calls = []
        client = prepare_http(
            Request(URL), settings, environ=env, opener_factory=factory_with({}, calls, [])
        )
        assert client.route == "direct" and calls == [("direct", "HEAD", 10)]
    with pytest.raises(ValueError, match="proxy is not configured"):
        prepare_http(Request(URL), NetworkConfig(mode="proxy"), environ={})


def test_both_failures_are_bounded_and_redacted():
    calls = []
    secret = "http://user:secret@proxy.invalid:8080"
    with pytest.raises(ValueError) as error:
        prepare_http(
            Request(URL),
            environ={"HTTPS_PROXY": secret},
            opener_factory=factory_with(
                {r: URLError(secret) for r in ("direct", "proxy")}, calls, []
            ),
        )
    assert "secret" not in str(error.value)
    assert len(calls) == 2


@pytest.mark.parametrize(
    "value",
    [
        {"mode": "magic"},
        {"fallback": "true"},
        {"timeout_seconds": True},
        {"probe_timeout_seconds": 0},
        {"preferred_route": "magic"},
        {"proxy_url": "secret"},
        {"proxy_url_env": "bad name"},
        {"no_proxy": "localhost"},
        {"no_proxy": ["http://host"]},
        {"use_environment_no_proxy": 1},
        {"ca_bundle": False},
    ],
)
def test_invalid_config_is_rejected_without_network(value, tmp_path):
    with pytest.raises(ValueError):
        parse_network(value, base=tmp_path)


def test_config_resolves_ca_path_without_reading_secrets_or_files(tmp_path):
    config = parse_network(
        {"ca_bundle": "private-ca.pem", "proxy_url_env": "PRIVATE_PROXY"}, base=tmp_path
    )
    assert config.ca_bundle == tmp_path / "private-ca.pem"
    assert config.proxy_url_env == "PRIVATE_PROXY"


def test_unverified_tls_context_is_rejected_before_any_request():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with pytest.raises(ValueError, match="verification must remain enabled"):
        prepare_http(Request(URL), context=context, environ={})


def test_unused_invalid_proxy_does_not_affect_direct_mode():
    calls = []
    client = prepare_http(
        Request(URL),
        NetworkConfig(mode="direct"),
        environ={"HTTPS_PROXY": "invalid secret value"},
        opener_factory=factory_with({}, calls, []),
    )
    assert client.route == "direct" and len(calls) == 1


def test_mid_run_errors_do_not_disclose_proxy_credentials():
    failures = {}
    client = prepare_http(
        Request(URL),
        NetworkConfig(mode="direct"),
        environ={},
        opener_factory=factory_with(failures, [], []),
    )
    failures["direct"] = URLError("http://user:secret@proxy.invalid")
    with pytest.raises(URLError) as error:
        client(Request(URL))
    assert "secret" not in str(error.value)
