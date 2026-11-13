# Optional per-pipeline Slack failure alerts

Slack alerts are available for the five implemented Dagster jobs. Each pipeline
owns an independent configuration file and stopped-by-default sensor. Alerts do
not require enabling an acquisition schedule. NAAIM acquisition remains paused.

| Pipeline / config folder | Failure sensor | Config-path environment override |
| --- | --- | --- |
| `aaii` | `aaii_slack_failure` | `AAII_ALERTS_CONFIG` |
| `naaim_exposure_public_delayed` | `naaim_exposure_public_delayed_slack_failure` | `NAAIM_EXPOSURE_PUBLIC_DELAYED_ALERTS_CONFIG` |
| `sec_fails_to_deliver` | `sec_fails_to_deliver_slack_failure` | `SEC_FAILS_TO_DELIVER_ALERTS_CONFIG` |
| `reg_sho_thresholds` | `reg_sho_thresholds_slack_failure` | `REG_SHO_THRESHOLDS_ALERTS_CONFIG` |
| `fomc_meeting_minutes` | `fomc_meeting_minutes_slack_failure` | `FOMC_MEETING_MINUTES_ALERTS_CONFIG` |

## Configure one pipeline

For example, prepare FOMC's optional alert file:

```bash
cp config/fomc_meeting_minutes/alerts.example.yml config/fomc_meeting_minutes/alerts.local.yml
```

```yaml
slack:
  enabled: true
  webhook_url_env: FOMC_MEETING_MINUTES_SLACK_WEBHOOK_URL
  dagster_url: http://127.0.0.1:3030
network:
  mode: auto
  preferred_route: proxy
  fallback: true
  probe_timeout_seconds: 5
  timeout_seconds: 10
```

Create a Slack app incoming webhook for the desired channel using
[Slack's official setup](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/).
Store the URL only in the named environment variable, for example in the ignored
`.env` loaded by `scripts/run_local_dagster.sh`. A webhook belongs to its selected
channel; YAML does not override that channel. Different pipelines can name
separate secret variables or intentionally share one webhook variable.

Both the local file and secret are optional until alerts are enabled. Missing
`alerts.local.yml` or `slack.enabled: false` means no delivery, secret resolution
or HTTP probe. A path explicitly set by the environment override must exist;
malformed, duplicate and unknown YAML keys are configuration errors. Do not put
raw webhook URLs, tokens or credentials in YAML. Only classic HTTPS incoming
webhooks on `hooks.slack.com` / `hooks.slack-gov.com` are supported.

`dagster_url` is optional; omit it or set null to suppress run links. If present,
use the URL operators can reach, including any UI path prefix, without credentials,
query or fragment. Changing `DAGSTER_PORT` does not rewrite this explicit link.
Network settings use the [shared HTTP contract](../development/outbound_http.md),
including environment-only proxies and relative CA bundle paths. Alert request
and probe timeouts must each be at most ten seconds.

## Enable the failure sensor

Load the new definitions in the existing Dagster code location. The daemon needs
the webhook environment variable (restart its process after environment changes).
Then start only the desired alert sensor in the UI, or from the checkout root:

```bash
set -a
source .env
set +a
DAGSTER_HOME="$PWD/.dagster" .venv/bin/dagster sensor start \
  fomc_meeting_minutes_slack_failure -m market_data_pipeline.orchestration.dagster_defs
```

Stop it with the corresponding `dagster sensor stop` command, or set
`slack.enabled: false` to suppress future sends. YAML is reloaded at evaluation;
there is no code-location import-time file or secret access. No sensor, service
or schedule is activated by installing these definitions or examples.

These run-failure sensors monitor their named jobs in the same code location,
including terminal failures before any asset step executes. They report failed
Dagster runs, not standalone pipeline CLI commands, archive staleness, missed
schedules, daemon outages or individual step retries that later succeed.
A running Dagster daemon is required. On its first evaluation a sensor initializes
its cursor at the latest event; older failures are not backfilled automatically.

## Delivery and recovery

Messages contain only pipeline/job names, run ID, FAILED status and an optional
run link. Run config, tags, exception text, provider data and credentials are never
forwarded. Link previews are disabled. Inspect sensitive diagnostic details in
your access-controlled Dagster instance.

Each enabled event selects a verified HTTP route using a public unauthenticated
Slack `api.test` probe, then makes one JSON POST to the secret webhook on that
client. The webhook itself is never probed. Redirects are blocked. A small HTTP
200 `ok` acknowledgement is required. HTTP failures (including 429), bad replies
and uncertain transport outcomes fail the sensor visibly without retrying a
possibly accepted POST or changing the original pipeline run result.

Inspect sensor tick errors and run reactions in Dagster when delivery fails.
Errors expose safe configuration/delivery phases and discard original exception
context. Fix the file, environment, webhook or network before the next event.
Dagster advances the run-status cursor even for a callback error; this change
adds no durable outbox or automatic redelivery. Disabled events are also consumed.
Normal ticks do not repost handled runs, but crashes after delivery or deliberate
cursor resets can duplicate messages. Slack delivery is not exactly once or
lossless. Review a missed alert manually; cursor replay should be deliberate.

Verification uses offline HTTP fixtures and ephemeral Dagster instances. No live
message has been sent, secret configured or sensor activated by this change.
[Spec 018](../../specs/spec018/README.md) records acceptance and scope.
