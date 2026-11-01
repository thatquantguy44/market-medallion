import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.naaim_exposure_public_delayed import cli
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.data.html import parse_table
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.operations import (
    health_report,
    retain_captures,
)
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.pipeline import ingest, run_live
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.source import fetch_table


def test_parse_table_preserves_every_verified_public_metric_and_date():
    payload = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()

    observations = parse_table(payload)

    assert [(row.as_of_date, row.naaim_number, row.deviation) for row in observations] == [
        ("2025-01-08", Decimal("71.25"), Decimal("52.10")),
        ("2025-01-01", Decimal("68.50"), Decimal("49.75")),
    ]
    assert observations[0].quarter_1 == Decimal("45.00")
    assert observations[0].quarter_2 == Decimal("72.00")
    assert observations[0].quarter_3 == Decimal("100.00")
    assert observations[0].bearish == Decimal("-75")
    assert observations[0].bullish == Decimal("175")


def test_parse_table_rejects_unknown_headers_and_duplicate_dates():
    payload = Path("tests/fixtures/naaim/delayed_table.html").read_text()
    with pytest.raises(ValueError, match="headers"):
        parse_table(payload.replace("Deviation", "Standard deviation", 1).encode())
    with pytest.raises(ValueError, match="duplicate"):
        parse_table(payload.replace("01/01/2025", "01/08/2025").encode())


@pytest.mark.parametrize(
    ("fixture", "message"),
    [
        ("missing_column.html", "headers"),
        ("malformed_number.html", "NAAIM Number"),
        ("malformed_date.html", "invalid Date"),
        ("empty_table.html", "no observations"),
    ],
)
def test_parse_table_rejects_schema_and_value_failures(fixture, message):
    payload = (Path("tests/fixtures/naaim") / fixture).read_bytes()

    with pytest.raises(ValueError, match=message):
        parse_table(payload)


def test_live_fetch_requires_documented_permission_and_uses_injected_transport():
    payload = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()
    transport_arguments = {}

    with pytest.raises(ValueError, match="permission_reference"):
        fetch_table(permission_reference="", transport=lambda *_args, **_kwargs: None)

    class Response:
        code = 200
        headers = {"Content-Type": "text/html"}

        def read(self, _limit):
            return payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def transport(*_args, **kwargs):
        transport_arguments.update(kwargs)
        return Response()

    response = fetch_table(permission_reference="naaim-permission-2026-10-31", transport=transport)

    assert response.payload == payload
    assert response.source_url == "https://index.naaim.org/embeddable/table"
    assert transport_arguments["context"].check_hostname

    class ThrottledResponse(Response):
        code = 429

    with pytest.raises(ValueError, match="HTTP 429"):
        fetch_table(
            permission_reference="naaim-permission-2026-10-31",
            transport=lambda *_args, **_kwargs: ThrottledResponse(),
        )

    class ServerErrorResponse(Response):
        code = 503

    with pytest.raises(ValueError, match="HTTP 503"):
        fetch_table(
            permission_reference="naaim-permission-2026-10-31",
            transport=lambda *_args, **_kwargs: ServerErrorResponse(),
        )


def test_local_ingest_archives_the_fixture_and_publishes_attributed_delayed_latest(tmp_path):
    payload = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()

    result = ingest(
        payload,
        tmp_path,
        permission_reference="naaim-permission-2026-10-31",
        captured_at="2026-10-31T17:00:00Z",
    )

    receipt = json.loads((tmp_path / "bronze" / result["capture_id"] / "receipt.json").read_text())
    latest = json.loads((tmp_path / "gold" / "latest.json").read_text())
    assert (tmp_path / "bronze" / result["capture_id"] / "payload.html").read_bytes() == payload
    assert receipt["permission_reference"] == "naaim-permission-2026-10-31"
    assert latest == {
        "as_of_date": "2025-01-08",
        "attribution": (
            "Source: NAAIM Exposure Index® (National Association of Active Investment Managers)."
        ),
        "delayed_public_display": True,
        "metric": "naaim_number",
        "value": "71.25",
    }


def test_ingest_is_idempotent_and_retains_a_changed_historical_observation(tmp_path):
    original = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()
    changed = Path("tests/fixtures/naaim/changed_table.html").read_bytes()

    first = ingest(
        original,
        tmp_path,
        permission_reference="naaim-permission-2026-10-31",
        captured_at="2026-10-31T17:00:00Z",
    )
    same = ingest(
        original,
        tmp_path,
        permission_reference="naaim-permission-2026-10-31",
        captured_at="2026-10-31T18:00:00Z",
    )
    corrected = ingest(
        changed,
        tmp_path,
        permission_reference="naaim-permission-2026-10-31",
        captured_at="2026-10-31T19:00:00Z",
    )

    versions = json.loads((tmp_path / "silver" / "versions.json").read_text())
    assert first["inserted_versions"] == 2
    assert same["inserted_versions"] == 0
    assert corrected["inserted_versions"] == 1
    changed_values = [
        version["values"]["naaim_number"]
        for version in versions
        if version["as_of_date"] == "2025-01-08"
    ]
    assert changed_values == [
        "71.25",
        "72.25",
    ]


def test_identical_payloads_record_distinct_successful_capture_events(tmp_path):
    payload = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()

    first = ingest(
        payload,
        tmp_path,
        permission_reference="naaim-permission-2026-11-01",
        captured_at="2026-11-01T17:00:00Z",
    )
    second = ingest(
        payload,
        tmp_path,
        permission_reference="naaim-permission-2026-11-01",
        captured_at="2026-11-02T17:00:00Z",
    )

    assert first["capture_id"] == second["capture_id"]
    assert first["run_id"] != second["run_id"]
    receipts = sorted((tmp_path / "captures").glob("*.json"))
    assert len(receipts) == 2
    assert sorted(
        datetime.fromisoformat(json.loads(path.read_text())["retrieved_at"].replace("Z", "+00:00"))
        for path in receipts
    ) == [
        datetime(2026, 11, 1, 17, tzinfo=UTC),
        datetime(2026, 11, 2, 17, tzinfo=UTC),
    ]

    health = health_report(
        tmp_path,
        now=datetime(2026, 11, 10, 17, tzinfo=UTC),
        max_capture_age_days=14,
    )
    assert health["last_capture_at"] == "2026-11-02T17:00:00Z"


def test_run_live_uses_injected_transport_and_preserves_the_permission_gate(tmp_path):
    payload = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()

    class Response:
        code = 200
        headers = {"Content-Type": "text/html"}

        def read(self, _limit):
            return payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    result = run_live(
        tmp_path,
        permission_reference="naaim-permission-2026-10-31",
        captured_at="2026-10-31T17:00:00Z",
        transport=lambda *_args, **_kwargs: Response(),
    )

    assert result["row_count"] == 2


def test_local_health_report_uses_capture_freshness_not_delayed_observation_age(tmp_path):
    payload = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()

    empty = health_report(
        tmp_path,
        now=datetime(2026, 11, 5, 12, tzinfo=UTC),
        max_capture_age_days=14,
    )
    assert empty == {
        "healthy": False,
        "reasons": ["no_captures", "publication_unavailable"],
        "last_capture_at": None,
        "capture_age_days": None,
        "latest_as_of_date": None,
        "silver_versions": 0,
    }

    ingest(
        payload,
        tmp_path,
        permission_reference="user-confirmed-naaim-permission-2026-11-01",
        captured_at="2026-11-01T17:00:00Z",
    )

    healthy = health_report(
        tmp_path,
        now=datetime(2026, 11, 5, 12, tzinfo=UTC),
        max_capture_age_days=14,
    )
    assert healthy == {
        "healthy": True,
        "reasons": [],
        "last_capture_at": "2026-11-01T17:00:00Z",
        "capture_age_days": 3.7916666666666665,
        "latest_as_of_date": "2025-01-08",
        "silver_versions": 2,
    }

    stale = health_report(
        tmp_path,
        now=datetime(2026, 11, 20, 17, tzinfo=UTC),
        max_capture_age_days=14,
    )
    assert stale["healthy"] is False
    assert stale["reasons"] == ["capture_stale"]

    (tmp_path / "gold" / "latest.json").unlink()
    unpublished = health_report(
        tmp_path,
        now=datetime(2026, 11, 5, 12, tzinfo=UTC),
        max_capture_age_days=14,
    )
    assert unpublished["healthy"] is False
    assert unpublished["reasons"] == ["publication_unavailable"]


def test_cli_check_is_local_and_returns_a_health_based_exit_code(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        cli,
        "health_report",
        lambda root, *, max_capture_age_days: {
            "healthy": root == tmp_path and max_capture_age_days == 21,
            "reasons": [],
        },
    )

    assert cli.main(["--root", str(tmp_path), "--check", "--max-capture-age-days", "21"]) == 0
    assert json.loads(capsys.readouterr().out) == {"healthy": True, "reasons": []}

    monkeypatch.setattr(
        cli,
        "health_report",
        lambda *_args, **_kwargs: {"healthy": False, "reasons": ["capture_stale"]},
    )
    assert cli.main(["--root", str(tmp_path), "--check"]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "healthy": False,
        "reasons": ["capture_stale"],
    }


def test_retention_keeps_only_last_365_days_and_rebuilds_local_state(tmp_path):
    original = Path("tests/fixtures/naaim/delayed_table.html").read_bytes()
    changed = Path("tests/fixtures/naaim/changed_table.html").read_bytes()
    old = ingest(
        original,
        tmp_path,
        permission_reference="naaim-permission-2026-11-01",
        captured_at="2025-10-31T17:00:00Z",
    )
    current = ingest(
        changed,
        tmp_path,
        permission_reference="naaim-permission-2026-11-01",
        captured_at="2026-11-01T17:00:00Z",
    )

    dry = retain_captures(
        tmp_path,
        now=datetime(2026, 11, 1, 18, tzinfo=UTC),
        retention_days=365,
    )
    assert dry["applied"] is False
    assert dry["remove_run_ids"] == [old["run_id"]]
    assert dry["remove_capture_ids"] == [old["capture_id"]]

    applied = retain_captures(
        tmp_path,
        now=datetime(2026, 11, 1, 18, tzinfo=UTC),
        retention_days=365,
        apply=True,
    )
    assert applied["applied"] is True
    assert not (tmp_path / "captures" / f"{old['run_id']}.json").exists()
    assert not (tmp_path / "bronze" / old["capture_id"]).exists()
    assert not (tmp_path / "silver" / f"{old['capture_id']}.json").exists()
    assert (tmp_path / "captures" / f"{current['run_id']}.json").exists()
    versions = json.loads((tmp_path / "silver" / "versions.json").read_text())
    assert [version["capture_id"] for version in versions] == [
        current["capture_id"],
        current["capture_id"],
    ]
    latest = json.loads((tmp_path / "gold" / "latest.json").read_text())
    assert latest["value"] == "72.25"


def test_daily_schedule_is_stopped_and_requires_explicit_runtime_acknowledgement(
    monkeypatch, tmp_path
):
    import dagster as dg

    from market_data_pipeline.pipelines.naaim_exposure_public_delayed.dagster_defs import (
        daily_schedule,
    )

    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 2, 15, tzinfo=UTC)
    )
    monkeypatch.delenv("NAAIM_DELAYED_ROOT", raising=False)
    monkeypatch.delenv("NAAIM_PERMISSION_REFERENCE", raising=False)
    monkeypatch.delenv("NAAIM_SCHEDULED_LIVE_ACCESS", raising=False)
    assert daily_schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    assert daily_schedule.evaluate_tick(context).skip_message

    monkeypatch.setenv("NAAIM_DELAYED_ROOT", str(tmp_path))
    monkeypatch.setenv("NAAIM_PERMISSION_REFERENCE", "naaim-permission-2026-11-01")
    monkeypatch.setenv("NAAIM_SCHEDULED_LIVE_ACCESS", "true")
    request = daily_schedule.evaluate_tick(context).run_requests[0]
    config = request.run_config["ops"]["naaim_delayed_capture"]["config"]
    assert config == {
        "permission_reference": "naaim-permission-2026-11-01",
        "root": str(tmp_path),
    }
