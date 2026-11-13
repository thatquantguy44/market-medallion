"""PR016-R01/R12: explicit offline CLI, source selection and package resources."""

import json
from datetime import timedelta
from unittest.mock import patch

import pytest
import test_presidential_bundle_assembly as complete_fixture
import test_presidential_publication as baseline_fixture

from market_data_pipeline.pipelines.presidential_research import cli

NOW = baseline_fixture.NOW


def test_offline_cli_capture_parse_build_publish_health_and_replay(tmp_path, monkeypatch, capsys):
    root, capture, review = baseline_fixture.setup_fixture(tmp_path)
    config = tmp_path / "sources.toml"
    monkeypatch.setattr(cli, "_now", lambda: NOW + timedelta(hours=1))
    output = tmp_path / "baseline.json"
    controls = root / "review" / review["review_id"] / "controls.csv"
    with patch("socket.socket", side_effect=AssertionError("network forbidden")):
        assert (
            cli.main(
                [
                    "parse",
                    "--workspace",
                    str(root),
                    "--config",
                    str(config),
                    "--capture",
                    f"FF_MKT_001={capture['capture_id']}",
                ]
            )
            == 0
        )
        assert (
            cli.main(
                [
                    "build-annual",
                    "--workspace",
                    str(root),
                    "--config",
                    str(config),
                    "--capture-id",
                    capture["capture_id"],
                    "--review-id",
                    review["review_id"],
                    "--profile",
                    "fixture-cli",
                    "--controls",
                    str(controls),
                    "--output",
                    str(output),
                    "--decision-policy",
                    "window_start",
                ]
            )
            == 0
        )
        assert not (root / "gold" / "current.json").exists()
        assert json.loads(output.read_text())["title"].startswith("Fictional fixture")
        assert (
            cli.main(
                [
                    "publish-baseline",
                    "--workspace",
                    str(root),
                    "--capture-id",
                    capture["capture_id"],
                    "--review-id",
                    review["review_id"],
                    "--profile",
                    "fixture-cli",
                ]
            )
            == 0
        )
        assert cli.main(["health", "--workspace", str(root), "--expected-end-year", "1936"]) == 0
        assert (
            cli.main(
                ["replay", "--workspace", str(root), "--destination", str(tmp_path / "replay")]
            )
            == 0
        )
    assert (tmp_path / "replay" / "gold" / "current.json").is_file()
    assert "SYNTHETIC_FIXTURE" in capsys.readouterr().out
    assert (
        cli.main(
            [
                "build-annual",
                "--workspace",
                str(root),
                "--config",
                str(config),
                "--capture-id",
                capture["capture_id"],
                "--review-id",
                review["review_id"],
                "--profile",
                "fixture-cli",
                "--controls",
                str(controls),
                "--output",
                str(output),
                "--decision-policy",
                "window_start",
            ]
        )
        == 2
    )
    assert json.loads(output.read_text())["title"].startswith("Fictional fixture")


def test_complete_comparison_command_and_fetch_refusal(tmp_path, monkeypatch, capsys):
    root, ids, review = complete_fixture.fixture(tmp_path, monkeypatch)
    output = tmp_path / "bundle"
    arguments = [
        "build-public-series-comparison",
        "--workspace",
        str(root),
        "--config",
        str(tmp_path / "sources.toml"),
        "--review-id",
        review["review_id"],
        "--controls",
        str(root / "review" / review["review_id"] / "controls.csv"),
        "--profile",
        "fixture-cli",
        "--output-dir",
        str(output),
        "--decision-policy",
        "window_start",
    ]
    for source, capture_id in ids.items():
        arguments.extend(["--capture", f"{source}={capture_id}"])
    with patch("socket.socket", side_effect=AssertionError("network forbidden")):
        assert cli.main(arguments) == 0
        assert cli.main(["fetch", "--workspace", str(root)]) == 2
    assert len(list(output.glob("*-annual.json"))) == 5
    assert json.loads((output / "bundle-info.json").read_text())["inputKind"] == "SYNTHETIC_FIXTURE"
    assert "SOURCE_ACCESS_NOT_REVIEWED" in capsys.readouterr().out


def test_selection_and_classification_are_explicit(tmp_path, capsys):
    with pytest.raises(SystemExit):
        cli.main(
            [
                "import-local",
                "--workspace",
                str(tmp_path),
                "--source",
                "FEDFUNDS_001",
                "--file",
                "missing.csv",
            ]
        )
    assert cli.main(["parse", "--workspace", str(tmp_path), "--capture", "../escape=bad"]) == 2
    assert "error" in capsys.readouterr().out.lower()


def test_packaged_source_catalog_matches_reviewed_repository_catalog():
    import hashlib
    from pathlib import Path

    packaged = cli._config_default().read_bytes()
    expected = Path("config/presidential_research/sources.toml").read_bytes()
    assert hashlib.sha256(packaged).digest() == hashlib.sha256(expected).digest()


def test_controls_shape_check_is_not_approval_and_bad_policy_is_explicit(tmp_path, capsys):
    root, capture, review = baseline_fixture.setup_fixture(tmp_path)
    config = tmp_path / "sources.toml"
    controls = root / "review" / review["review_id"] / "controls.csv"
    assert (
        cli.main(["validate-controls", "--config", str(config), "--controls", str(controls)]) == 0
    )
    assert json.loads(capsys.readouterr().out)["approval_status"] == "NOT_EVALUATED"
    assert (
        cli.main(
            [
                "build-annual",
                "--workspace",
                str(root),
                "--config",
                str(config),
                "--capture-id",
                capture["capture_id"],
                "--review-id",
                review["review_id"],
                "--profile",
                "fixture-cli",
                "--controls",
                str(controls),
                "--output",
                str(tmp_path / "bad.json"),
                "--decision-policy",
                "calendar_year_start",
            ]
        )
        == 2
    )
    assert "at-origin" in capsys.readouterr().out
    assert not (tmp_path / "bad.json").exists()
