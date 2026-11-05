"""Exercise actual urllib routing against local fixture servers; no provider I/O."""

import base64
import os
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request

import pytest

from market_data_pipeline.shared.http import NetworkConfig, prepare_http


@contextmanager
def server(*, redirect=None, connect_status=407):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.respond()

        def do_GET(self):
            self.respond()

        def do_CONNECT(self):
            calls.append((self.command, self.path, self.headers.get("Proxy-Authorization")))
            self.send_response(connect_status)
            self.end_headers()

        def respond(self):
            calls.append((self.command, self.path, self.headers.get("Proxy-Authorization")))
            if redirect and self.command == "GET":
                self.send_response(302)
                self.send_header("Location", redirect)
            else:
                self.send_response(200)
            self.end_headers()
            if self.command == "GET" and not redirect:
                self.wfile.write(b"local fixture")

    service = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=service.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{service.server_port}", calls
    finally:
        service.shutdown()
        service.server_close()
        thread.join(timeout=2)


def test_direct_reaches_local_origin_despite_dead_environment_proxy(monkeypatch):
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    with server() as (url, calls):
        original_env = dict(os.environ)
        client = prepare_http(Request(url + "/data"), NetworkConfig(mode="direct"))
        with client(Request(url + "/data")) as response:
            assert response.read() == b"local fixture"
        assert [(m, p) for m, p, _ in calls] == [("HEAD", "/data"), ("GET", "/data")]
        assert dict(os.environ) == original_env


def test_proxy_uses_snapshotted_credentials_and_ignores_unselected_global_bypass(monkeypatch):
    monkeypatch.setenv("no_proxy", "*")
    with server() as (proxy_url, calls):
        secret_proxy = proxy_url.replace("http://", "http://fixture:pass%20word@")
        settings = NetworkConfig(
            mode="proxy", proxy_url_env="FIXTURE_PROXY", use_environment_no_proxy=False
        )
        request = Request("http://provider.invalid/data")
        client = prepare_http(request, settings, environ={"FIXTURE_PROXY": secret_proxy})
        monkeypatch.setenv("FIXTURE_PROXY", "http://127.0.0.1:1")
        with client(request) as response:
            assert response.read() == b"local fixture"
        expected = "Basic " + base64.b64encode(b"fixture:pass word").decode()
        assert calls == [
            ("HEAD", "http://provider.invalid/data", expected),
            ("GET", "http://provider.invalid/data", expected),
        ]
        assert request.host == "provider.invalid"
        assert request.get_header("Proxy-authorization") is None


def test_redirect_bypass_keeps_proxy_credentials_off_origin():
    with server() as (origin, origin_calls):
        # Use a second proxy fixture that redirects data calls to the bypass host.
        with server(redirect=origin + "/target") as (redirect_proxy, proxy_calls):
            client = prepare_http(
                Request("http://provider.invalid/data"),
                NetworkConfig(
                    mode="proxy", no_proxy=("127.0.0.1",), use_environment_no_proxy=False
                ),
                environ={"HTTP_PROXY": redirect_proxy.replace("http://", "http://user:secret@")},
            )
            with client(Request("http://provider.invalid/data")) as response:
                assert response.read() == b"local fixture"
            assert len(proxy_calls) == 2
            assert origin_calls == [("GET", "/target", None)]


@pytest.mark.parametrize("status", [403, 407])
def test_https_connect_rejection_never_falls_back_direct(status):
    with server(connect_status=status) as (proxy, calls):
        with pytest.raises(ValueError, match=f"HTTP {status}"):
            prepare_http(
                Request("https://provider.invalid/data"),
                NetworkConfig(use_environment_no_proxy=False),
                environ={"HTTPS_PROXY": proxy},
            )
        assert calls == [("CONNECT", "provider.invalid:443", None)]


def test_proxy_explicit_no_proxy_cannot_fallback_back_through_proxy():
    with server() as (origin, calls):
        client = prepare_http(
            Request(origin),
            NetworkConfig(mode="proxy", no_proxy=("127.0.0.1",)),
            environ={"HTTPS_PROXY": "http://127.0.0.1:1"},
        )
        assert client.route == "direct" and calls == [("HEAD", "/", None)]


def test_missing_redirect_scheme_proxy_cannot_silently_reach_origin_directly():
    from urllib.error import URLError

    from market_data_pipeline.shared.http import _make_opener, tls_context

    with server() as (origin, calls):
        open_request = _make_opener(
            "proxy", {"https": "http://proxy.invalid"}, "", tls_context(NetworkConfig())
        )
        with pytest.raises(URLError, match="no proxy for the destination scheme"):
            open_request(Request(origin), timeout=2)
        assert calls == []
