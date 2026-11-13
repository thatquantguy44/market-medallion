"""Strict optional alert settings and secret-free Slack delivery contracts."""

import json
from urllib.error import HTTPError, URLError

import pytest

from market_data_pipeline.shared import slack_alerts as alerts

WEBHOOK = "https://hooks.slack.com/services/TTEST/BTEST/SECRET"


def config(tmp_path, text):
    path = tmp_path / "alerts.yml"
    path.write_text(text)
    return path


def test_missing_optional_config_and_disabled_alerts_never_resolve_secrets_or_probe(
    tmp_path, monkeypatch
):
    settings = alerts.load_alerts(tmp_path / "absent.yml", missing_ok=True)
    monkeypatch.setattr(alerts, "prepare_http", lambda *a, **k: pytest.fail("HTTP attempted"))
    assert not alerts.send_failure(
        settings, pipeline="fomc_meeting_minutes", job="job", run_id="id"
    )
    with pytest.raises((ValueError, OSError)):
        alerts.load_alerts(tmp_path / "explicit-missing.yml")


@pytest.mark.parametrize(
    "text",
    [
        "slack: {enabled: 'true'}",
        "slack: {enabled: 1}",
        "slack: {channel: general}",
        "slack: {webhook_url: secret}",
        "slack: {webhook_url_env: 'bad-name'}",
        "slack: {enabled: false, enabled: true}",
        "slack: [false]",
        "[]",
        "slack: [malformed",
        "slack: {}\nextra: true",
        "slack: {dagster_url: 'https://user:pass@host'}",
        "slack: {dagster_url: 'https://host/?secret=key'}",
        "slack: {dagster_url: 'https://host/|<!channel>'}",
        "network: {timeout_seconds: 11}\nslack: {}",
    ],
)
def test_invalid_alert_settings_are_rejected(tmp_path, text):
    with pytest.raises(ValueError):
        alerts.load_alerts(config(tmp_path, text))


def test_valid_network_settings_resolve_ca_relative_to_alert_file(tmp_path):
    settings = alerts.load_alerts(
        config(
            tmp_path,
            "slack: {enabled: true, webhook_url_env: FOMC_SLACK_WEBHOOK_URL, "
            "dagster_url: 'http://127.0.0.1:3030'}\nnetwork: {mode: direct, ca_bundle: ca.pem}",
        )
    )
    assert settings.enabled and settings.webhook_url_env == "FOMC_SLACK_WEBHOOK_URL"
    assert settings.network.mode == "direct" and settings.network.ca_bundle == tmp_path / "ca.pem"


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "http://hooks.slack.com/services/T/B/X",
        WEBHOOK + "?key=secret",
        "https://hooks.slack.com.evil.test/services/T/B/X",
        "https://user@hooks.slack.com/services/T/B/X",
        WEBHOOK + "#fragment",
        WEBHOOK + "\n",
        "https://127.0.0.1/services/T/B/X",
    ],
)
def test_invalid_or_missing_webhook_fails_before_any_probe(monkeypatch, value):
    monkeypatch.setattr(alerts, "prepare_http", lambda *a, **k: pytest.fail("HTTP attempted"))
    env = {} if value is None else {"SLACK_WEBHOOK_URL": value}
    with pytest.raises(ValueError):
        alerts.send_failure(
            alerts.SlackAlerts(enabled=True), pipeline="aaii", job="job", run_id="id", environ=env
        )


class Response:
    def __init__(self, code=200, body=b"ok"):
        self.code, self.body = code, body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self, limit):
        return self.body[:limit]


@pytest.mark.parametrize(
    "host,probe", [("hooks.slack.com", "slack.com"), ("hooks.slack-gov.com", "slack-gov.com")]
)
def test_delivery_probes_public_endpoint_then_posts_once_without_sensitive_run_details(
    monkeypatch, host, probe
):
    calls = []

    def prepare(request, network, **options):
        calls.append((request, network, options))

        def post(request):
            calls.append(request)
            return Response()

        return post

    monkeypatch.setattr(alerts, "prepare_http", prepare)
    settings = alerts.SlackAlerts(enabled=True, dagster_url="http://127.0.0.1:3030/prefix/")
    assert alerts.send_failure(
        settings,
        pipeline="aaii",
        job="aaii_sentiment_job",
        run_id="run <id>",
        environ={"SLACK_WEBHOOK_URL": WEBHOOK.replace("hooks.slack.com", host)},
    )
    assert calls[0][0].full_url == f"https://{probe}/api/api.test"
    assert calls[0][2]["allow_redirects"] is False
    request = calls[1]
    assert (
        request.get_method() == "POST" and request.get_header("Content-type") == "application/json"
    )
    payload = json.loads(request.data)
    assert "FAILED" in payload["text"] and "run &lt;id&gt;" in payload["text"]
    assert "/prefix/runs/run%20%3Cid%3E" in payload["text"]
    assert payload["unfurl_links"] is payload["unfurl_media"] is False
    assert "SECRET" not in str(payload) and "webhook" not in str(payload)


@pytest.mark.parametrize(
    "response",
    [
        Response(400),
        Response(429),
        Response(500),
        Response(302),
        Response(200, b"not_ok"),
        Response(200, b"ok" + b" " * 20),
        URLError("SECRET"),
        HTTPError(WEBHOOK, 403, "SECRET", {}, None),
    ],
)
def test_delivery_failures_are_safe_and_do_not_retry_ambiguous_posts(monkeypatch, response):
    posts = []

    def post(request):
        posts.append(request)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(alerts, "prepare_http", lambda *a, **k: post)
    with pytest.raises(ValueError) as error:
        alerts.send_failure(
            alerts.SlackAlerts(enabled=True),
            pipeline="aaii",
            job="job",
            run_id="id",
            environ={"SLACK_WEBHOOK_URL": WEBHOOK},
        )
    assert len(posts) == 1 and "SECRET" not in str(error.value)
