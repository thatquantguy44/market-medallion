# NAAIM outbound network settings

NAAIM remains paused. These settings do not resume it or start its schedule.
Copy `network.example.yml` to ignored `network.local.yml`, then edit its `network`
section. See the [shared contract](../../docs/development/outbound_http.md).
Paths such as `ca_bundle` resolve relative to this configuration file.

After explicit resumption, pass this file to the existing CLI with
`--network-config config/naaim_exposure_public_delayed/network.local.yml`, or set
Dagster asset config `network_config_path` to the same path. The stopped schedule
can pass the path from `NAAIM_NETWORK_CONFIG`. The existing root and permission
reference remain required; health/export commands do not load or probe this file.

For a named proxy, use `proxy_url_env: NAAIM_PROXY_URL` and supply that environment
variable from your trusted local `.env` or worker environment. Otherwise standard
HTTP_PROXY/HTTPS_PROXY/ALL_PROXY variables apply, with lowercase preferred.
Keep URLs with credentials outside YAML. No `.env` is executed by the library.
Use `mode: direct` where proxy use is prohibited, or `mode: proxy` where direct
fallback is prohibited. In auto mode, `preferred_route` chooses which route is
tested first and `fallback: false` pins it. Explicit NO_PROXY rules always select
direct for excluded hosts, including redirects.
