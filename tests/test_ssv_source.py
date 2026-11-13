"""SSV-AC12: admitted bounded HTTP -> raw evidence -> atomic local membership."""

import json
from dataclasses import replace
from io import BytesIO
from urllib.error import URLError

import pytest
from test_ssv_bronze import NOW, SECRET, fixture_grant
from test_ssv_parser import FIXTURES, receipt
from test_ssv_parser import manifest as manifest
from test_ssv_pipeline import gold

from market_data_pipeline.pipelines.finra_short_sale_volume import source
from market_data_pipeline.pipelines.finra_short_sale_volume.acquisition_config import (
    AcquisitionConfig,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.acquisition import Limits
from market_data_pipeline.pipelines.finra_short_sale_volume.io.acquisition_http import (
    DATA_URL,
    METADATA_URL,
    TOKEN_URL,
)

__all__ = ["manifest"]
ENV = {"FINRA_API_KEY": "invented-id", "FINRA_PASSWORD_SECRET": SECRET}


class Response(BytesIO):
    def __init__(self, body, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
        }


class Script:
    def __init__(self, manifest, responses):
        self.manifest = manifest
        self.responses = list(responses)
        self.calls = []

    def __call__(self, request, **kwargs):
        self.calls.append((request.full_url, request.get_method(), request.data))
        if request.full_url == TOKEN_URL:
            return Response(
                json.dumps(
                    dict(
                        access_token="invented-access-token", token_type="Bearer", expires_in="1800"
                    )
                ).encode()
            )
        if request.full_url == METADATA_URL:
            fields = [
                dict(name=n, type=t, **({"format": "yyyy-MM-dd"} if n == "tradeReportDate" else {}))
                for n, t in self.manifest.to_dict()["fields"].items()
            ]
            return Response(
                json.dumps(
                    dict(
                        datasetGroup="otcMarket",
                        datasetName="regShoDaily",
                        partitionFields=["tradeReportDate"],
                        fields=fields,
                    )
                ).encode()
            )
        assert request.full_url == DATA_URL
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def data_reply(payload=None, *, total=2, status=200, headers=None):
    payload = payload if payload is not None else (FIXTURES / "invented-valid.json").read_bytes()
    value = receipt(payload, total=total, status=status)["headers"]
    value["Content-Length"] = str(len(payload))
    if headers:
        value.update(headers)
    return Response(payload, status, value)


def config(root, manifest, **changes):
    value = AcquisitionConfig(
        root, manifest, "fixture", fixture_grant(), live=True, limits=Limits(page_rows=2)
    )
    return replace(value, **changes)


def run(config, script, **changes):
    args = dict(
        start="2026-11-09",
        end="2026-11-09",
        scopes=["nyse_trf"],
        overlap_days=0,
        live=True,
        transport=script,
        environ=ENV,
        clock=lambda: NOW,
        sleep=lambda _: None,
    )
    args.update(changes)
    return source.acquire(config, **args)


def test_ssv_ac12_complete_response_archives_exact_bytes_and_promotes(tmp_path, manifest):
    cfg = config(tmp_path, manifest)
    script = Script(manifest, [data_reply()])
    result = run(cfg, script)
    assert result["status"] == "COMPLETE" and result["complete_partitions"] == 1
    assert result["partitions"][0]["status"] == "APPLIED"
    assert len(gold(tmp_path, manifest)["rows"]) == 2
    assert (
        next((tmp_path / "bronze/captures").glob("*/payload.bin")).read_bytes()
        == (FIXTURES / "invented-valid.json").read_bytes()
    )
    assert [url for url, _, _ in script.calls] == [TOKEN_URL, METADATA_URL, DATA_URL]
    assert not (tmp_path / "parquet").exists()
    for path in (tmp_path / "acquisition").rglob("*.json"):
        assert SECRET not in path.read_text() and "invented-access-token" not in path.read_text()


@pytest.mark.parametrize(
    "reply,status",
    [
        (lambda: data_reply(total=20), "PARTIAL"),
        (lambda: data_reply(payload=b"[{"), "REJECTED"),
        (lambda: data_reply(payload=b"<html>login</html>"), "REJECTED"),
        (lambda: data_reply(headers={"Data-Version": "2"}), "REJECTED"),
    ],
)
def test_ssv_ac12_incomplete_or_bad_response_keeps_previous_membership(
    tmp_path, manifest, reply, status
):
    cfg = config(tmp_path, manifest)
    run(cfg, Script(manifest, [data_reply()]))
    previous = gold(tmp_path, manifest)
    result = run(cfg, Script(manifest, [reply()]))
    assert result["status"] == "INCOMPLETE" and result["partitions"][0]["status"] == status
    assert gold(tmp_path, manifest) == previous


def test_ssv_ac12_retry_after_keeps_failed_raw_capture_and_then_applies(tmp_path, manifest):
    waits = []
    cfg = config(tmp_path, manifest)
    script = Script(
        manifest,
        [
            data_reply(
                payload=b'{"message":"slow down"}', status=429, headers={"Retry-After": "3"}
            ),
            data_reply(),
        ],
    )
    result = run(cfg, script, sleep=waits.append)
    assert result["status"] == "COMPLETE" and waits == [3]
    assert len(list((tmp_path / "bronze/captures").iterdir())) == 2
    assert result["http_attempts"] == 4 and result["request_budget_used"] == 6


def test_ssv_ac12_retry_wait_budget_stops_without_early_request(tmp_path, manifest):
    cfg = config(tmp_path, manifest, limits=Limits(page_rows=2, max_wait_seconds=2))
    script = Script(
        manifest, [data_reply(payload=b"[]", status=429, headers={"Retry-After": "10"})]
    )
    result = run(cfg, script, sleep=lambda _: pytest.fail("must not retry early"))
    assert result["status"] == "INCOMPLETE" and result["error_code"] == "wait_budget"
    assert len(script.calls) == 3


def test_ssv_ac12_transport_exhaustion_has_no_fabricated_bronze(tmp_path, manifest):
    cfg = config(tmp_path, manifest)
    script = Script(manifest, [URLError(SECRET)] * 3)
    result = run(cfg, script)
    assert result["status"] == "INCOMPLETE" and result["complete_partitions"] == 0
    assert not (tmp_path / "bronze").exists()
    assert result["partitions"][0]["status"] == "TRANSPORT_FAILED"
    assert SECRET not in json.dumps(result)


@pytest.mark.parametrize("options", [dict(live=False), dict(environ={}), dict(transport=None)])
def test_ssv_ac12_execution_gates_precede_store_and_network(tmp_path, manifest, options):
    cfg = config(tmp_path / "absent", manifest)
    with pytest.raises((PermissionError, ValueError)):
        run(cfg, Script(manifest, []), **options)
    assert not cfg.root.exists()


def test_ssv_ac12_retrieval_completion_precedes_archive_with_advancing_clock(tmp_path, manifest):
    from datetime import timedelta

    point = 0

    def clock():
        nonlocal point
        value = NOW + timedelta(microseconds=point)
        point += 1
        return value

    result = run(config(tmp_path, manifest), Script(manifest, [data_reply()]), clock=clock)
    assert result["status"] == "COMPLETE"
    saved = json.loads(next((tmp_path / "bronze/captures").glob("*/receipt.json")).read_bytes())
    assert saved["retrieved_at"] < saved["archived_at"]
    assert saved["provider_published_at"] is None


def test_ssv_ac12_response_after_duration_limit_is_retained_without_promotion(tmp_path, manifest):
    point = [0]
    response = data_reply()
    read = response.read

    def late_read(size):
        point[0] = 1000
        return read(size)

    response.read = late_read
    result = run(
        config(tmp_path, manifest), Script(manifest, [response]), monotonic=lambda: point[0]
    )
    assert result["status"] == "INCOMPLETE" and result["error_code"] == "duration_budget"
    assert gold(tmp_path, manifest)["capture_id"] is None
    assert len(list((tmp_path / "bronze/captures").iterdir())) == 1


def test_ssv_ac12_live_client_selected_once_after_gates_and_reused(tmp_path, manifest, monkeypatch):
    from test_ssv_bronze import provider_grant

    selected = []
    script = Script(manifest, [data_reply()])
    script.route = "direct"
    script.fallback_used = False

    def prepare(request, network, **options):
        assert request.full_url == METADATA_URL and request.get_header("Authorization") is None
        assert options == {"allow_redirects": False}
        selected.append(network)
        return script

    monkeypatch.setattr(source, "prepare_http", prepare)
    cfg = config(tmp_path, manifest, operation="acquire", approved_use=provider_grant())
    result = run(cfg, None)
    assert result["status"] == "COMPLETE" and result["network_route"] == "direct"
    assert selected == [cfg.network] and len(script.calls) == 3


@pytest.mark.parametrize("status", [301, 302, 307, 401, 403])
def test_ssv_ac12_redirect_and_denial_never_follow_or_retry_as_success(tmp_path, manifest, status):
    script = Script(
        manifest,
        [
            data_reply(
                payload=b'{"message":"unavailable"}',
                status=status,
                headers={"Location": "https://unreviewed.invalid/token"},
            )
        ],
    )
    result = run(config(tmp_path, manifest), script)
    assert result["status"] == "INCOMPLETE" and result["complete_partitions"] == 0
    assert len(script.calls) == 3 and result["partitions"][0]["status"] == "REJECTED"


def test_ssv_ac12_credential_bearing_data_is_refused_without_persisting_secret(tmp_path, manifest):
    body = json.dumps(dict(access_token=SECRET)).encode()
    result = run(config(tmp_path, manifest), Script(manifest, [data_reply(payload=body)]))
    assert result["partitions"][0]["status"] == "UNSAFE_PAYLOAD"
    assert not (tmp_path / "bronze").exists()
    assert all(SECRET not in p.read_text() for p in tmp_path.rglob("*.json"))


def test_ssv_ac12_valid_empty_can_replace_but_truncation_preserves_membership(tmp_path, manifest):
    cfg = config(tmp_path, manifest)
    run(cfg, Script(manifest, [data_reply()]))
    previous = gold(tmp_path, manifest)
    incomplete = data_reply(headers={"Content-Length": "999999"})
    result = run(
        cfg,
        Script(
            manifest,
            [
                incomplete,
                data_reply(headers={"Content-Length": "999999"}),
                data_reply(headers={"Content-Length": "999999"}),
            ],
        ),
    )
    assert result["status"] == "INCOMPLETE" and gold(tmp_path, manifest) == previous
    result = run(cfg, Script(manifest, [data_reply(payload=b"[]", total=0)]))
    assert result["status"] == "COMPLETE" and gold(tmp_path, manifest)["rows"] == []
