# Spec 018 — Optional per-pipeline Slack failure alerts

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
