"""Optional pipeline-owned Slack alert config and bounded, secret-free delivery."""

import json
import os
import re
from dataclasses import dataclass
from html import escape
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request

import yaml

from market_data_pipeline.shared.http import NetworkConfig, parse_network, prepare_http
from market_data_pipeline.shared.yaml import UniqueLoader


@dataclass(frozen=True)
class SlackAlerts:
    enabled: bool = False
    on_success: bool = False
    on_failure: bool = True
    webhook_url_env: str = "SLACK_WEBHOOK_URL"
    dagster_url: str | None = None
    network: NetworkConfig = NetworkConfig(timeout_seconds=10, probe_timeout_seconds=5)

    def __post_init__(self):
        for key in ("enabled", "on_success", "on_failure"):
            if type(getattr(self, key)) is not bool:
                raise ValueError("slack." + key + " requires a boolean")
        if not isinstance(self.webhook_url_env, str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*", self.webhook_url_env
        ):
            raise ValueError("slack.webhook_url_env requires an environment variable name")
        if self.dagster_url is not None:
            _validate_ui_url(self.dagster_url)
        if (
            not isinstance(self.network, NetworkConfig)
            or max(self.network.timeout_seconds, self.network.probe_timeout_seconds) > 10
        ):
            raise ValueError("alert network timeouts must not exceed ten seconds")


def _validate_ui_url(value):
    if not isinstance(value, str) or len(value) > 1024:
        raise ValueError("invalid Slack Dagster UI URL")
    parts = urlsplit(value)
    if (
        parts.scheme not in ("http", "https")
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or any(c.isspace() or ord(c) < 32 or c in "<>|" for c in value)
    ):
        raise ValueError("invalid Slack Dagster UI URL")
    if parts.port == 0:
        raise ValueError("invalid Slack Dagster UI port")


def load_alerts(path: Path, *, missing_ok=False) -> SlackAlerts:
    path = Path(path)
    try:
        text = path.read_text()
    except FileNotFoundError:
        if missing_ok:
            return SlackAlerts()
        raise
    if len(text) > 65536:
        raise ValueError("alert YAML exceeds size limit")
    try:
        value = yaml.load(text, Loader=UniqueLoader)
    except (yaml.YAMLError, UnicodeError):
        raise ValueError("invalid alert YAML") from None
    if not isinstance(value, dict) or set(value) - {"slack", "network"} or "slack" not in value:
        raise ValueError("alert YAML requires slack and optional network sections")
    slack = value["slack"]
    if not isinstance(slack, dict) or set(slack) - {
        "enabled",
        "on_success",
        "on_failure",
        "webhook_url_env",
        "dagster_url",
    }:
        raise ValueError("unknown Slack settings; credentials belong in environment")
    network_value = value.get("network", {})
    if not isinstance(network_value, dict):
        raise ValueError("invalid alert network settings")
    network = parse_network(
        {"timeout_seconds": 10, "probe_timeout_seconds": 5, **network_value},
        base=path.resolve().parent,
    )
    return SlackAlerts(**slack, network=network)


def send_failure(settings, **kwargs):
    return _send_status(settings, status="failure", **kwargs)


def send_success(settings, **kwargs):
    return _send_status(settings, status="success", **kwargs)


def _send_status(settings, *, status, pipeline, job, run_id, environ=None):
    """One delivery attempt; callers own safe sensor errors and cursor semantics."""
    if not settings.enabled or not getattr(settings, "on_" + status):
        return False
    env = os.environ if environ is None else environ
    webhook = env.get(settings.webhook_url_env, "")
    if not isinstance(webhook, str) or not re.fullmatch(
        r"https://hooks\.slack(?:-gov)?\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+",
        webhook,
    ):
        raise ValueError("slack_webhook_missing_or_invalid")
    label = "SUCCESS" if status == "success" else "FAILED"
    text = f"MarketMedallion {label}: {escape(pipeline)} / {escape(job)} — run {escape(run_id)}"
    if settings.dagster_url:
        text += f"\n<{settings.dagster_url.rstrip('/')}/runs/{quote(run_id, safe='')}|View run>"
    payload = {"text": text, "unfurl_links": False, "unfurl_media": False}
    domain = "slack-gov.com" if urlsplit(webhook).hostname == "hooks.slack-gov.com" else "slack.com"
    failed = False
    try:
        client = prepare_http(
            Request(f"https://{domain}/api/api.test"),
            settings.network,
            allow_redirects=False,
            environ=env,
        )
        request = Request(
            webhook,
            data=json.dumps(payload).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with client(request) as response:
            body = response.read(17)
            failed = response.code != 200 or len(body) > 16 or body.strip() != b"ok"
    except Exception as exc:
        if isinstance(exc, HTTPError):
            exc.close()
        failed = True
    if failed:
        # Outside the except block: Dagster serializes exception context as well
        # as the message; credential-bearing HTTP exceptions must be discarded.
        raise ValueError("slack_delivery_failed")
    return True
