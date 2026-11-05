# Outbound HTTP configuration and run contract

User-requested extension, 2026-11-04. AAII and NAAIM are the two current HTTP
consumers. SEC FTD remains local-only; network settings do not enable acquisition.

## Requirements and acceptance

1. Every live run creates its own client. `mode` selects `auto`, `direct` or
   `proxy`; auto tries `preferred_route` then the other route only when `fallback`
   is true. Defaults: auto, proxy preferred, fallback enabled.
2. Before capturing data, make one bounded HEAD request to the same provider URL.
   Try each permitted route at most once. Remember the first working route for
   all data calls/retries in that run. A new run tests again. No process-global
   opener, environment mutation or persistent route cache is allowed.
3. Direct ignores proxy environment variables. Proxy URLs come from environment
   variables, optionally selected by `proxy_url_env`; credentials never belong
   in YAML, logs or receipts. Standard lower/uppercase HTTP(S)_PROXY and ALL_PROXY
   are supported; lowercase wins. NO_PROXY plus configured hostname suffixes
   force direct access, including redirects. No CIDR matching is promised.
4. Only connection failures permit startup fallback. HTTP errors stop startup
   without trying a second route; 405/501 mean HEAD is unsupported and permit
   the normal GET on that route. Proxy-authentication failures do not imply
   provider entitlement. Data-response retry/archival semantics stay provider-owned.
5. TLS verification stays enabled on both routes. Optional `ca_bundle` resolves
   relative to the YAML; otherwise use SSL_CERT_FILE or certifi. Separate bounded
   probe and request timeouts are configurable. No fallback occurs during data
   requests after startup selection.
6. CLI and Dagster pass the same settings. Local imports, health, replay and
   exports make no probe. Existing permission gates precede any probe. Injected
   transports remain usable for offline tests and own their routing.

## Configuration

```yaml
network:
  mode: auto                 # auto | direct | proxy
  preferred_route: proxy     # proxy | direct (auto only)
  fallback: true             # auto only; false pins the preferred route
  proxy_url_env: null        # e.g. MARKET_DATA_PROXY_URL, otherwise standard vars
  no_proxy: [localhost, 127.0.0.1]
  use_environment_no_proxy: true
  probe_timeout_seconds: 10
  timeout_seconds: 30
  ca_bundle: null
```

Load a trusted local `.env` in the shell; the library does not execute it.
`proxy_url_env` names an environment variable containing an HTTP proxy URL
(optionally with percent-encoded basic credentials). Proxy transport uses HTTP
CONNECT for HTTPS destinations. SOCKS and TLS-to-proxy URLs are not supported.
Use an HTTP proxy URL even when the destination is HTTPS.

AAII uses `network` in its existing runtime YAML. NAAIM uses a pipeline-owned
network-only YAML supplied through `--network-config` or Dagster's
`network_config_path`; its stopped schedule can pass `NAAIM_NETWORK_CONFIG`.
SEC FTD accepts the same section for future HTTP work without probing on import.
Absent settings use the auto defaults; no configured proxy means auto can select
direct immediately. Direct-only and proxy-only never try the other route.

## Plan and verification

Shared config/selection helper → offline routing tests → source/CLI/Dagster
integration → config examples/runbook/register/handoff → full offline regression.
Implement the provider-independent helper in `shared/http.py`, retaining
provider manifests, permission checks, retry rules and capture logic in each
pipeline. Keep pure transforms free of HTTP dependencies. No new dependency or
database migration is needed. Rollback is a source/config revert.

Tests must prove both fallback directions, pinned routes and subsequent-run
reselection, both-routes failure, missing proxies, no-proxy matching, environment
isolation, TLS verification, redacted errors, HTTP rejection behavior, actual
opener routing with local/injected fixtures, CLI/Dagster configuration forwarding,
and no-request local/permission-gate paths. Run pytest, Ruff and package build.

The implementation follows Python's documented
[opener/ProxyHandler contract](https://docs.python.org/3.12/library/urllib.request.html#urllib.request.ProxyHandler),
[Request.set_proxy](https://docs.python.org/3.12/library/urllib.request.html#urllib.request.Request.set_proxy)
and [verified TLS contexts](https://docs.python.org/3.12/library/ssl.html#ssl.create_default_context).
NAAIM remains paused; verification uses no provider requests.

## Verified checkpoint

Implemented 2026-11-04. Full offline regression: **394 passed, 2 existing opt-in
PostgreSQL skips**. Repository Ruff lint/format and sdist/wheel builds pass.
Local fixture servers exercise real direct/proxy HTTP, basic proxy credentials,
redirect bypass without forwarding proxy credentials, HTTPS CONNECT rejections
and missing-scheme protection. Injected connection failures prove both fallback
directions, one selection per invocation, retry reuse and subsequent-run retest.
TLS verification is asserted and unverified contexts are rejected. CLI/Dagster
forwarding, local no-request commands, startup health and route receipts pass.
No real provider, corporate proxy or custom corporate CA was contacted/tested.

Auth and secrets: only environment-variable names and CA paths are configured;
resolved proxy URLs are absent from logs and per-run routing metadata.
Reproducibility: source capture/first-seen semantics are unchanged, and selection
metadata contains only the route and whether startup fallback was used.
