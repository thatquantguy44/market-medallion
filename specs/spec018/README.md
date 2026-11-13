# Spec 018 — Optional per-pipeline Slack failure alerts

**Status:** implemented and accepted locally; delivery/activation remain operator-configured.
User-selected 2026-11-12, on `feat/pipeline-slack-alerts`. Add independent Slack
alert YAML under each implemented pipeline's `config/<pipeline_name>/` directory.
Absent/default configuration disables delivery. Credentials stay environment-only.

## Contract and boundaries

- Each existing job owns one stopped-by-default Dagster run-failure sensor in its
  pipeline definitions. It monitors that named job only, including failures before
  an asset executes; it does not launch ingestion or change the failed run.
- Each pipeline has `alerts.example.yml`; an optional ignored `alerts.local.yml`
  enables Slack through `slack.enabled`. `<PIPELINE>_ALERTS_CONFIG` can select a
  different file. Missing default files mean disabled; explicit missing paths,
  malformed/duplicate/unknown fields and missing/invalid enabled secrets fail the
  sensor visibly, with safe codes. No file/secret access during definition import.
- Separate settings: `slack.enabled`, `slack.webhook_url_env`, optional
  `slack.dagster_url`, and pipeline-owned `network` using the shared HTTP contract.
  Incoming webhook destinations are selected in Slack, not overridden in YAML.
- Only HTTPS Slack/GovSlack incoming-webhook URLs without credentials/query/fragment
  are accepted. No raw webhook URL, exception text, run config, tags, provider data
  or connection configuration enters messages/logs. Payload contains pipeline/job,
  run ID, failure status and an optional escaped Dagster run link.
- Route selection uses `shared/http.py` once per enabled alert after config/secret
  gates, probing the public unauthenticated `api.test` endpoint rather than the
  secret webhook. The selected client posts JSON with verified TLS and redirects
  disabled. No HTTP, including probes, for absent/disabled settings.
- One POST per event; require HTTP 200 and bounded `ok` acknowledgement. Transport,
  HTTP/rate-limit and bad acknowledgement failures produce safe sensor errors.
  No ambiguous POST retry or exactly-once delivery claim. Dagster owns its sensor
  cursor; normal ticks do not replay prior events. Crash/reset replay can duplicate
  messages, and failed delivery requires operator review; no durable outbox is added.
- Acquisition schedules remain stopped, NAAIM paused. No live Slack message, local
  secret setup, service restart or sensor activation is part of this change.

## Plan and verification

1. Pure strict settings + safe payload/delivery helpers under `shared/`, with
   failing offline tests for gates, URL validation, HTTP failures and redaction.
2. Provider-independent Dagster sensor factory in orchestration, wired separately
   by each of the five pipeline-owned definition modules. Test actual Dagster
   failure events and tick cursor/routing, including pre-step run failures.
3. Disabled examples/ignored local paths, setup/recovery runbook, job and master
   pipeline registers and current handoff. Verify root registration, full pytest,
   isolated FOMC PostgreSQL regression, Ruff/format and wheel/source artifact build.

Commands: `.venv/bin/python -m pytest -q`, `.venv/bin/ruff check .`,
`.venv/bin/ruff format --check .`, `.venv/bin/python -m build --no-isolation`.
Use offline transport fixtures; no new dependencies, data migrations or vendored
changes. Keep provider schemas, clocks, storage and existing source retry policy.

Official API references reviewed against installed Dagster 1.12.21:
https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/
https://dagster.io/docs/guides/automate/sensors/run-status-sensors
https://dagster.io/docs/api/dagster/schedules-sensors

## Verified acceptance

Implementation: `c953c78` (settings/transport) and `b44197d` (owned sensors/configs).
Full offline regression: **1,038 passed, 80 optional skips**; all **313 FOMC checks
passed with isolated PostgreSQL enabled**. Forty-seven new behavioral cases cover
strict configuration and secret gates, default network limits, Slack/GovSlack,
redirect blocking, safe payloads/errors, bounded acknowledgements and actual
Dagster failure/success/unrelated-job/pre-step events, cursor consumption and
non-replay. Provider HTTP remains fixture-only during regression. No secret was
configured, webhook POST made, sensor enabled, service restarted, acquisition
schedule activated or operational schema migrated. Existing capture clocks,
source data and storage semantics remain unchanged.

A read-only public Slack `api.test` HEAD probe was accepted on the direct route;
webhook delivery and GovSlack remain offline-fixture verified only.
Root code-location listing exposes all five alert sensors stopped; all six
acquisition/import schedules remain stopped. Ruff lint/format, local docs links,
wheel/source build and extracted-source registration/config checks pass. The
workflow snapshot and all 61 adopted QuantSmith Git objects were verified;
external QuantSmith working-tree drift was left unchanged. Setup, error recovery
and best-effort cursor/delivery limitations are in the
[Slack runbook](../../docs/deployment/slack_alerts.md).

## Subsequent live Dagster alert verification — 2026-11-12

At the user's request, started the manual local UI/daemon, enabled only FOMC's
failure sensor and initialized its cursor before submitting the actual
`fomc_meeting_minutes_job`. Run `621d5cfc-d83e-46b2-974b-ea189c31b153` used
`live: false` and failed intentionally with `preflight_failed`, before provider
acquisition or PostgreSQL operation entry. The daemon evaluated the genuine run
failure: sensor tick 4 was SUCCESS, referenced that run and logged
`slack_alert_delivered`, which requires the webhook HTTP acknowledgement.
FOMC's ignored YAML/secret are configured locally; other sensors and all six
acquisition/import schedules remain stopped. No warehouse migration, source data
change, LaunchAgent installation or new runtime code was needed. A sanitized
local record is at ignored `data/dagster/fomc-alert-test-result.json`.

## Success alert extension — selected 2026-11-13

Add strict per-pipeline `slack.on_success` (default false) and `slack.on_failure`
(default true). Existing files retain failure-only delivery. Disabled/global or
status-specific gates resolve no webhook and make no HTTP request. Each job gains
one stopped success sensor alongside its existing failure sensor; preserve failure
sensor names/cursors. Both share safe delivery/routing/redaction and truthful
SUCCESS/FAILED message labels. FOMC's local YAML should enable both statuses;
load the definitions, initialize the new success sensor before a bounded live
1990 rerun, then verify the actual successful run and Slack acknowledgement.
Keep all acquisition schedules stopped and other pipeline sensors unchanged.
Tests must cover strict flags, defaults, both enabled/disabled status paths,
actual Dagster success ticks, unrelated jobs and cursor non-replay. Run regression,
FOMC isolated PostgreSQL checks, lint/format and build. Track work in PR #17.
