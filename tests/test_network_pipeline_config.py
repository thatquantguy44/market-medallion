"""The same run-scoped HTTP settings reach each implemented live pipeline."""

import io
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import URLError

import pytest
import yaml

from market_data_pipeline.shared.http import NetworkConfig


def test_aaii_loads_network_settings_without_resolving_proxy_secret(tmp_path):
    from market_data_pipeline.pipelines.aaii.config import load_config

    path = tmp_path / "aaii.yml"
    path.write_text("warehouse: sqlite\nnetwork:\n  mode: proxy\n  proxy_url_env: MISSING_PROXY\n")
    assert load_config(path).network.mode == "proxy"


def test_sec_local_config_accepts_network_without_changing_storage_contract(tmp_path):
    from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config

    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    path = tmp_path / "sec.yml"
    path.write_text(yaml.safe_dump(value))
    original = load_config(path).storage.contract_id
    value["network"] = {"mode": "proxy", "proxy_url_env": "UNSET_SEC_PROXY"}
    path.write_text(yaml.safe_dump(value))
    runtime = load_config(path)
    assert runtime.network.mode == "proxy"
    assert runtime.storage.contract_id == original


def test_aaii_startup_selection_is_reused_for_retries_and_recorded(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.aaii.source import run_live
    from market_data_pipeline.shared import http

    payload = Path("tests/fixtures/aaii/synthetic.xls").read_bytes()
    calls = []

    def factory(route, proxies, no_proxy, context):
        def open_request(request, *, timeout):
            calls.append((route, request.get_method()))
            if route == "proxy":
                raise URLError("proxy offline")
            response = io.BytesIO(payload)
            response.code = 200
            response.headers = {}
            if calls.count(("direct", "GET")) == 1 and request.get_method() == "GET":
                response = io.BytesIO(b"try later")
                response.code, response.headers = 503, {"Retry-After": "0"}
            return response

        return open_request

    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid")
    monkeypatch.delenv("https_proxy", raising=False)
    monkeypatch.setattr(http, "_make_opener", factory)
    result = run_live(
        Path("manifests/aaii-investor-sentiment-xls.yml"),
        tmp_path,
        start="2026-10-01",
        end="2026-10-26",
        max_attempts=3,
        network=NetworkConfig(use_environment_no_proxy=False),
        clock=lambda: datetime(2026, 10, 30, tzinfo=UTC),
        sleeper=lambda _: None,
    )
    assert result["inserted_versions"] == 3
    assert calls == [("proxy", "HEAD"), ("direct", "HEAD"), ("direct", "GET"), ("direct", "GET")]
    receipts = [json.loads(p.read_text()) for p in (tmp_path / "bronze").rglob("metadata.json")]
    assert len(receipts) == 2
    assert all(r["network_route"] == "direct" and r["network_fallback_used"] for r in receipts)


def test_naaim_permission_is_checked_before_any_probe(monkeypatch):
    from market_data_pipeline.pipelines.naaim_exposure_public_delayed import source

    def forbidden(*args, **kwargs):
        pytest.fail("permission gate must run before preflight")

    monkeypatch.setattr(source, "prepare_http", forbidden)
    with pytest.raises(ValueError, match="permission_reference"):
        source.fetch_table(permission_reference="", network=NetworkConfig())


def test_naaim_cli_forwards_pipeline_network_file(tmp_path, monkeypatch, capsys):
    from market_data_pipeline.pipelines.naaim_exposure_public_delayed import cli

    path = tmp_path / "network.yml"
    path.write_text("network:\n  mode: direct\n")
    seen = []

    def run_live(root, **kwargs):
        seen.append(kwargs["network"])
        return {"fixture": True}

    monkeypatch.setattr(cli, "run_live", run_live)
    assert (
        cli.main(
            [
                "--root",
                str(tmp_path),
                "--permission-reference",
                "fixture",
                "--network-config",
                str(path),
            ]
        )
        == 0
    )
    assert seen[0].mode == "direct"


def test_naaim_dagster_forwards_settings_and_schedule_path(tmp_path, monkeypatch):
    import dagster as dg

    from market_data_pipeline.pipelines.naaim_exposure_public_delayed import dagster_defs as defs

    path = tmp_path / "network.yml"
    path.write_text("network:\n  mode: direct\n")
    monkeypatch.setenv("NAAIM_NETWORK_CONFIG", str(path))
    monkeypatch.setenv("NAAIM_SCHEDULED_LIVE_ACCESS", "true")
    monkeypatch.setenv("NAAIM_DELAYED_ROOT", str(tmp_path))
    monkeypatch.setenv("NAAIM_PERMISSION_REFERENCE", "fixture")
    context = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 11, 4, tzinfo=UTC))
    options = (
        defs.daily_schedule.evaluate_tick(context)
        .run_requests[0]
        .run_config["ops"]["naaim_delayed_capture"]["config"]
    )
    assert options["network_config_path"] == str(path)
    assert defs.daily_schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_aaii_cli_and_dagster_forward_network_settings(tmp_path, monkeypatch, capsys):
    import dagster as dg

    from market_data_pipeline.pipelines.aaii import cli, dagster_defs, source

    path = tmp_path / "aaii.yml"
    path.write_text(f"root: {tmp_path}\nnetwork:\n  mode: direct\n")
    seen = []

    def run_live(*args, **kwargs):
        seen.append(kwargs["network"])
        return {"run_id": "fixture", "row_count": 0, "inserted_versions": 0, "snapshot": "fixture"}

    monkeypatch.setattr(source, "run_live", run_live)
    assert (
        cli.main(
            ["run", "--live", "--config", str(path), "--start", "2026-10-01", "--end", "2026-10-26"]
        )
        == 0
    )
    with dg.build_asset_context() as context:
        dagster_defs.aaii_sentiment(
            context,
            dagster_defs.AaiiConfig(
                config_path=str(path), live=True, start="2026-10-01", end="2026-10-26"
            ),
        )
    assert [config.mode for config in seen] == ["direct", "direct"]


def test_naaim_asset_uses_network_file_before_capture(tmp_path, monkeypatch):
    import dagster as dg

    from market_data_pipeline.pipelines.naaim_exposure_public_delayed import dagster_defs as defs
    from market_data_pipeline.pipelines.naaim_exposure_public_delayed.pipeline import DELAY_METADATA

    path = tmp_path / "network.yml"
    path.write_text("network:\n  mode: direct\n")
    seen = []

    def run_live(*args, **kwargs):
        seen.append(kwargs["network"])
        return {
            **DELAY_METADATA,
            "run_id": "fixture",
            "row_count": 0,
            "inserted_versions": 0,
            "latest_as_of_date": "2026-01-01",
        }

    monkeypatch.setattr(defs, "run_live", run_live)
    monkeypatch.setattr(defs, "retain_captures", lambda *a, **k: {"remove_capture_ids": []})
    with dg.build_asset_context() as context:
        defs.naaim_delayed_capture(
            context,
            defs.NaaimDelayedConfig(
                root=str(tmp_path), permission_reference="fixture", network_config_path=str(path)
            ),
        )
    assert seen[0].mode == "direct"


def test_local_checks_skip_unusable_network_config_and_do_not_probe(tmp_path, monkeypatch, capsys):
    from market_data_pipeline.pipelines.aaii import cli as aaii
    from market_data_pipeline.pipelines.naaim_exposure_public_delayed import cli as naaim
    from market_data_pipeline.pipelines.sec_fails_to_deliver import cli as sec
    from market_data_pipeline.shared import http

    def forbidden(*args, **kwargs):
        pytest.fail("local commands must not make an outbound request")

    monkeypatch.setattr(http, "_make_opener", forbidden)
    path = tmp_path / "aaii.yml"
    path.write_text(f"root: {tmp_path}\nnetwork:\n  mode: proxy\n  proxy_url_env: MISSING_PROXY\n")
    assert aaii.main(["check", "--config", str(path)]) == 1  # Empty store is unhealthy.
    assert (
        naaim.main(
            ["--root", str(tmp_path), "--check", "--network-config", str(tmp_path / "missing.yml")]
        )
        == 1
    )
    assert (
        sec.main(
            ["validate-config", "--config", "config/sec_fails_to_deliver/pipeline.example.yml"]
        )
        == 0
    )


def test_aaii_startup_failure_marks_health_without_a_data_capture(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.aaii.operations import health_report
    from market_data_pipeline.pipelines.aaii.pipeline import run
    from market_data_pipeline.pipelines.aaii.source import run_live
    from market_data_pipeline.shared import http

    manifest = Path("manifests/aaii-investor-sentiment-xls.yml")
    run(
        Path("tests/fixtures/aaii/synthetic.xls"),
        manifest,
        tmp_path,
        start="2026-10-01",
        end="2026-10-26",
        captured_at=datetime(2026, 10, 30, tzinfo=UTC),
    )
    before = list((tmp_path / "bronze").rglob("metadata.json"))

    def factory(*args):
        def fail(*args, **kwargs):
            raise URLError("connection unavailable")

        return fail

    monkeypatch.setattr(http, "_make_opener", factory)
    with pytest.raises(ValueError, match="network startup failed"):
        run_live(
            manifest,
            tmp_path,
            start="2026-10-01",
            end="2026-10-26",
            network=NetworkConfig(mode="direct"),
            clock=lambda: datetime(2026, 10, 31, tzinfo=UTC),
        )
    assert list((tmp_path / "bronze").rglob("metadata.json")) == before
    assert (
        "latest_transport_failed"
        in health_report(tmp_path, now=datetime(2026, 10, 31, tzinfo=UTC))["reasons"]
    )


def test_naaim_pipeline_uses_preflight_and_retains_sanitized_route_metadata(tmp_path, monkeypatch):
    from market_data_pipeline.pipelines.naaim_exposure_public_delayed.pipeline import run_live
    from market_data_pipeline.shared import http

    payload = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()
    calls = []

    def factory(route, proxies, no_proxy, context):
        def open_request(request, *, timeout):
            calls.append((route, request.get_method()))
            response = io.BytesIO(payload)
            response.code, response.headers = 200, {}
            return response

        return open_request

    monkeypatch.setattr(http, "_make_opener", factory)
    result = run_live(
        tmp_path, permission_reference="fixture-only", network=NetworkConfig(mode="direct")
    )
    assert result["row_count"] > 0
    assert calls == [("direct", "HEAD"), ("direct", "GET")]
    receipt = json.loads(next((tmp_path / "captures").glob("*.json")).read_text())
    assert receipt["network_route"] == "direct" and not receipt["network_fallback_used"]
