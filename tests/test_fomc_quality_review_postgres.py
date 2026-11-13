"""FOMC-QAC06–07/12: portable verified review packs, no source writes or network."""

import hashlib

import pytest
from test_fomc_operation_postgres import operation_archive as operation_archive
from test_fomc_postgres import store as store

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_review import (
    make_review,
    select_cases,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality import quality_report
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality_review import (
    check_reviews,
    export_review_pack,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.operations import run_operation

__all__ = ["operation_archive", "store"]
pytestmark = pytest.mark.postgres


def test_review_pack_is_bound_to_verified_originals_and_render_evidence(
    operation_archive, tmp_path, monkeypatch
):
    store, cfg, settings, *_ = operation_archive
    run_operation(cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True)
    report = quality_report(cfg, inventory_store=store)
    plan = select_cases(report)

    def forbidden(*args, **kwargs):
        raise AssertionError("review attempted a provider request")

    monkeypatch.setattr(
        "market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition.prepare_fomc_http",
        forbidden,
    )
    output = tmp_path / "review"
    manifest = export_review_pack(cfg, report, plan, output, inventory_store=store)
    assert manifest["plan_id"] == plan["plan_id"] and len(manifest["cases"]) == 1
    entry = manifest["cases"][0]
    assert (
        hashlib.sha256((output / entry["source_path"]).read_bytes()).hexdigest()
        == plan["cases"][0]["source_sha256"]
    )
    assert "data:image/png;base64," in (output / entry["render_input_path"]).read_text()
    assert check_reviews(report, plan, [], output)["status"] == "pending_review"
    render = output / "renders" / "page.png"
    render.parent.mkdir()
    render.write_bytes(b"\x89PNG\r\n\x1a\nfixture-render")
    record = make_review(
        plan,
        plan["cases"][0]["case_id"],
        disposition="chart_or_image",
        reviewer="fixture",
        reviewed_at="2026-11-13T15:00:00Z",
        render={
            "path": "renders/page.png",
            "sha256": hashlib.sha256(render.read_bytes()).hexdigest(),
            "input_sha256": entry["render_input_sha256"],
            "tool": "fixture renderer 1",
        },
        observations=["Chart alt text matches the archived source."],
    )
    result = check_reviews(report, plan, [record], output)
    assert result["status"] == "reviewed" and not result["eligibility_changed"]
    assert result["dispositions"] == {"chart_or_image": 1}
    assert export_review_pack(cfg, report, plan, output, inventory_store=store) == manifest
    render.write_bytes(b"changed")
    with pytest.raises(ValueError):
        check_reviews(report, plan, [record], output)
    assert quality_report(cfg, inventory_store=store) == report


def test_stale_plan_bad_pack_paths_and_duplicate_reviews_are_refused(operation_archive, tmp_path):
    from copy import deepcopy

    store, cfg, settings, *_ = operation_archive
    run_operation(cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True)
    report = quality_report(cfg, inventory_store=store)
    plan = select_cases(report)
    stale = deepcopy(plan)
    stale["cases"][0]["source_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        export_review_pack(cfg, report, stale, tmp_path / "bad", inventory_store=store)
    with pytest.raises(ValueError):
        export_review_pack(cfg, report, plan, cfg.root / "text", inventory_store=store)


def test_review_cli_plan_record_and_check_are_local(
    operation_archive, tmp_path, monkeypatch, capsys
):
    import json
    from contextlib import nullcontext

    from market_data_pipeline.pipelines.fomc_meeting_minutes import cli
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality import write_report

    store, cfg, settings, *_ = operation_archive
    run_operation(cfg, settings, inventory_store=store, start_year=1990, end_year=1990, live=True)
    report = quality_report(cfg, inventory_store=store)
    report_path = tmp_path / "report.json"
    write_report(cfg, report_path, report)
    monkeypatch.setattr(cli, "load_config", lambda path: cfg)
    monkeypatch.setattr(cli, "PostgresInventoryStore", lambda settings: nullcontext(store))
    common = [
        "--config",
        "unused",
        "--operations",
        "config/fomc_meeting_minutes/operations.example.yml",
        "--report",
        str(report_path),
    ]
    plan_path = tmp_path / "plan.json"
    assert cli.main(["quality-review-plan", *common, "--output", str(plan_path)]) == 0
    capsys.readouterr()
    plan = json.loads(plan_path.read_text())
    output = tmp_path / "pack"
    assert (
        cli.main(
            ["quality-review-pack", *common, "--plan", str(plan_path), "--output", str(output)]
        )
        == 0
    )
    capsys.readouterr()

    def forbidden(*args, **kwargs):
        raise AssertionError("offline review command connected to database")

    monkeypatch.setattr(cli, "PostgresInventoryStore", forbidden)
    reviews = tmp_path / "reviews.json"
    reviews.write_text("[]")
    assert (
        cli.main(
            [
                "quality-review-check",
                *common,
                "--plan",
                str(plan_path),
                "--reviews",
                str(reviews),
                "--pack",
                str(output),
                "--output",
                str(tmp_path / "pending.json"),
            ]
        )
        == 2
    )
    assert json.loads(capsys.readouterr().out)["status"] == "pending_review"
    manifest = json.loads((output / "pack.json").read_text())
    entry = manifest["cases"][0]
    (output / "renders").mkdir()
    image = output / "renders" / "page.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\nfixture")
    observation = {
        "disposition": "unresolved",
        "reviewer": "fixture",
        "reviewed_at": "2026-11-13T15:00:00Z",
        "render": {
            "path": "renders/page.png",
            "sha256": hashlib.sha256(image.read_bytes()).hexdigest(),
            "input_sha256": entry["render_input_sha256"],
            "tool": "fixture renderer 1",
        },
        "observations": ["Image requires source review."],
    }
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(json.dumps(observation))
    record = tmp_path / "record.json"
    assert (
        cli.main(
            [
                "quality-review-record",
                *common,
                "--plan",
                str(plan_path),
                "--case-id",
                plan["cases"][0]["case_id"],
                "--observation",
                str(observation_path),
                "--output",
                str(record),
            ]
        )
        == 0
    )
    capsys.readouterr()
    reviews.write_text(json.dumps([json.loads(record.read_text())]))
    assert (
        cli.main(
            [
                "quality-review-check",
                *common,
                "--plan",
                str(plan_path),
                "--reviews",
                str(reviews),
                "--pack",
                str(output),
                "--output",
                str(tmp_path / "checked.json"),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "reviewed"
