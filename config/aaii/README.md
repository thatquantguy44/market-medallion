# AAII optional alerts

`alerts.example.yml` configures optional Slack failure alerts independently of
AAII's existing runtime YAML. Copy it to ignored `alerts.local.yml` only when
needed; see the [Slack setup/runbook](../../docs/deployment/slack_alerts.md).
Missing or disabled settings make no network request. Secrets remain in the
named environment variable; the failure sensor defaults stopped.
