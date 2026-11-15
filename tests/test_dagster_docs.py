"""Canonical Dagster navigation and definition drift fail closed without runtime access."""

import json
import socket
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

from scripts.verify_dagster_docs import check, inventory_from_definitions

REPO = Path(__file__).resolve().parents[1]
INVENTORY = {
    "jobs": ["example_job"],
    "schedules": {"example_daily": ("example_job", "0 10 * * *", "America/New_York", "STOPPED")},
    "sensors": {"example_success": ("RUN_STATUS", "STOPPED")},
}
DOC = """# Dagster jobs and operations

<!-- dagster:jobs -->
| Job | Purpose | Config and runbook |
| --- | --- | --- |
| example_job | Invented fixture only | [Guide](guide.md#fixture) |
<!-- /dagster:jobs -->

<!-- dagster:schedules -->
| Schedule | Job | Cron | Time zone | Default |
| --- | --- | --- | --- | --- |
| example_daily | example_job | 0 10 * * * | America/New_York | STOPPED |
<!-- /dagster:schedules -->

<!-- dagster:sensors -->
| Sensor | Type | Default |
| --- | --- | --- |
| example_success | RUN_STATUS | STOPPED |
<!-- /dagster:sensors -->
"""


@pytest.fixture
def documents(tmp_path):
    for name, text in {
        "DAGSTER.md": DOC,
        "README.md": "[Dagster jobs](DAGSTER.md)\n",
        "AGENTS.md": "Maintain [Dagster jobs](DAGSTER.md) in the same change.\n",
        "guide.md": "# Fixture\n",
        "docs/deployment/dagster_jobs.md": "# Moved\n\nSee [canonical guide](../../DAGSTER.md).\n",
    }.items():
        p = tmp_path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return tmp_path


def test_canonical_fixture_passes_without_touching_files(documents):
    before = {str(p): p.read_bytes() for p in documents.rglob("*") if p.is_file()}
    assert check(documents, inventory=INVENTORY) == {"jobs": 1, "schedules": 1, "sensors": 1}
    assert {str(p): p.read_bytes() for p in documents.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("jobs", [[], ["example_job", "new_job"], ["renamed_job"]])
def test_added_removed_or_renamed_job_requires_document_update(documents, jobs):
    changed = deepcopy(INVENTORY)
    changed["jobs"] = jobs
    with pytest.raises(ValueError, match="jobs"):
        check(documents, inventory=changed)


@pytest.mark.parametrize(
    "field,value", [(0, "other_job"), (1, "15 9 * * *"), (2, "UTC"), (3, "RUNNING")]
)
def test_schedule_target_cadence_timezone_and_default_drift(documents, field, value):
    changed = deepcopy(INVENTORY)
    row = list(changed["schedules"]["example_daily"])
    row[field] = value
    changed["schedules"]["example_daily"] = tuple(row)
    with pytest.raises(ValueError, match="schedules"):
        check(documents, inventory=changed)


@pytest.mark.parametrize("kind", ["schedules", "sensors"])
@pytest.mark.parametrize("change", ["add", "remove", "rename"])
def test_automation_inventory_changes_require_document_update(documents, kind, change):
    changed = deepcopy(INVENTORY)
    name = next(iter(changed[kind]))
    row = changed[kind][name]
    if change in ("remove", "rename"):
        del changed[kind][name]
    if change in ("add", "rename"):
        changed[kind]["new_definition"] = row
    with pytest.raises(ValueError, match=kind):
        check(documents, inventory=changed)


@pytest.mark.parametrize("row", [("STANDARD", "STOPPED"), ("RUN_STATUS", "RUNNING")])
def test_sensor_type_or_default_change_requires_document_update(documents, row):
    changed = deepcopy(INVENTORY)
    changed["sensors"]["example_success"] = row
    with pytest.raises(ValueError, match="sensors"):
        check(documents, inventory=changed)


@pytest.mark.parametrize(
    "damage",
    [
        "missing_marker",
        "duplicate_marker",
        "duplicate_row",
        "bad_columns",
        "blank_description",
        "missing_runbook",
    ],
)
def test_malformed_or_ambiguous_inventory_refuses(documents, damage):
    text = DOC
    if damage == "missing_marker":
        text = text.replace("<!-- /dagster:jobs -->", "")
    elif damage == "duplicate_marker":
        text += "\n<!-- dagster:jobs -->\n"
    elif damage == "duplicate_row":
        text = text.replace(
            "<!-- /dagster:jobs -->",
            "| example_job | Duplicate | [Guide](guide.md) |\n<!-- /dagster:jobs -->",
        )
    elif damage == "bad_columns":
        text = text.replace("| example_job | Invented fixture only |", "| example_job |")
    elif damage == "missing_runbook":
        text = text.replace("[Guide](guide.md#fixture)", "TODO")
    else:
        text = text.replace("Invented fixture only", " ")
    (documents / "DAGSTER.md").write_text(text)
    with pytest.raises(ValueError):
        check(documents, inventory=INVENTORY)


@pytest.mark.parametrize("name", ["README.md", "AGENTS.md", "docs/deployment/dagster_jobs.md"])
@pytest.mark.parametrize(
    "damage",
    ["missing", "comment_only", "code_only", "bad_anchor", "inline_code_only", "tilde_code_only"],
)
def test_navigation_cannot_disappear_or_hide_in_comments_or_code(documents, name, damage):
    target = "../../DAGSTER.md" if name.startswith("docs/") else "DAGSTER.md"
    text = "[Dagster](" + target + ")"
    if damage == "missing":
        text = "No canonical link here."
    elif damage == "comment_only":
        text = "<!-- " + text + " -->"
    elif damage == "bad_anchor":
        text = "[Dagster](" + target + "#missing)"
    elif damage == "inline_code_only":
        text = "`" + text + "`"
    elif damage == "tilde_code_only":
        text = "~~~markdown\n" + text + "\n~~~"
    else:
        text = "```markdown\n" + text + "\n```"
    (documents / name).write_text(text)
    with pytest.raises(ValueError, match="navigation"):
        check(documents, inventory=INVENTORY)


@pytest.mark.parametrize("target", ["missing.md", "guide.md#missing-anchor", "../outside.md"])
def test_broken_or_escaping_local_links_refuse(documents, target):
    (documents / "DAGSTER.md").write_text(DOC.replace("guide.md#fixture", target))
    with pytest.raises(ValueError, match="link"):
        check(documents, inventory=INVENTORY)


def test_legacy_page_cannot_become_a_second_inventory(documents):
    p = documents / "docs/deployment/dagster_jobs.md"
    p.write_text(p.read_text() + "\n| Job | Purpose |\n| --- | --- |\n| stale_job | Old |\n")
    with pytest.raises(ValueError, match="pointer"):
        check(documents, inventory=INVENTORY)


def test_symlinked_canonical_document_refuses(documents, tmp_path):
    saved = tmp_path / "other.md"
    saved.write_text(DOC)
    (documents / "DAGSTER.md").unlink()
    (documents / "DAGSTER.md").symlink_to(saved)
    with pytest.raises(ValueError, match="regular"):
        check(documents, inventory=INVENTORY)


def test_actual_definitions_have_no_instance_network_or_filesystem_side_effects(
    tmp_path, monkeypatch
):
    dg = pytest.importorskip("dagster")
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "unused-home"))
    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: pytest.fail("network"))
    monkeypatch.setattr(dg.DagsterInstance, "get", lambda *a, **k: pytest.fail("instance access"))
    counts = check(REPO)
    assert set(counts) == {"jobs", "schedules", "sensors"}
    assert counts["jobs"] > 0
    assert not (tmp_path / "unused-home").exists()


def test_definition_snapshot_observes_a_changed_schedule():
    dg = pytest.importorskip("dagster")

    @dg.op
    def fixture_op():
        return None

    @dg.job
    def fixture_job():
        fixture_op()

    schedule = dg.ScheduleDefinition(
        name="fixture_daily", job=fixture_job, cron_schedule="15 9 * * *"
    )
    result = inventory_from_definitions(dg.Definitions(jobs=[fixture_job], schedules=[schedule]))
    assert result["jobs"] == ["fixture_job"]
    assert result["schedules"]["fixture_daily"] == ("fixture_job", "15 9 * * *", "UTC", "STOPPED")


def test_cli_check_and_safe_current_facts():
    pytest.importorskip("dagster")
    script = REPO / "scripts/verify_dagster_docs.py"
    result = subprocess.run([sys.executable, str(script)], cwd=REPO, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "Dagster documentation verified:" in result.stdout
    facts = subprocess.run(
        [sys.executable, str(script), "--show-current"], cwd=REPO, capture_output=True, text=True
    )
    assert facts.returncode == 0, facts.stderr
    assert (
        json.loads(facts.stdout)["schedules"]["finra_short_sale_volume_weekday_refresh"][0]
        == "finra_short_sale_volume_job"
    )


def test_cli_refuses_missing_document_with_repair_guidance(tmp_path):
    script = REPO / "scripts/verify_dagster_docs.py"
    result = subprocess.run(
        [sys.executable, str(script), "--repo", str(tmp_path)], capture_output=True, text=True
    )
    assert result.returncode == 1
    assert "DAGSTER.md" in result.stderr and "--show-current" in result.stderr
    assert result.stdout == ""


def test_ci_checks_inventory_after_install_and_before_tests():
    import yaml

    workflow = yaml.load((REPO / ".github/workflows/ci.yml").read_text(), Loader=yaml.BaseLoader)
    steps = workflow["jobs"]["verify"]["steps"]
    commands = [s.get("run", "") for s in steps]
    install = next(i for i, c in enumerate(commands) if "pip install --no-deps" in c)
    verify = commands.index("python scripts/verify_dagster_docs.py")
    tests = commands.index("python -m pytest -q")
    assert install < verify < tests
    assert workflow["permissions"] == {"contents": "read"}
