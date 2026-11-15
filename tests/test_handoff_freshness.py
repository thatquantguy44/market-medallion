"""Objective handoff freshness: Git snapshots, task state and review coverage."""

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.verify_handoff import HANDOFF, STATE, check, refresh

NOW = datetime(2026, 11, 15, 1, tzinfo=UTC)


def git(root, *args):
    return subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
            *args,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repository with spaces"
    root.mkdir()
    git(root, "init", "--quiet")
    for name, body in {
        "src/example.py": "value = 1\n",
        "specs/example/todo.md": (
            "## EX-T01 — First task\n- [x] Done\n\n## EX-T02 — Next task\n- [ ] Pending\n"
        ),
        HANDOFF: """# Master handoff

Last updated: 2026-11-14.

## Agent start here

**Next implementation: EX-T02.**

### Current work and highest priorities

Next task: EX-T02.

### Last completed work and commits

EX-T01 complete.

### Exact next slice: EX-T02

Read [tasks](../../specs/example/todo.md#ex-t02--next-task).

### Operating holds

No live requests.

### Keep this panel current

Update task and evidence.

## Historical checkpoints and retained context

Invented history.
""",
    }.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    git(root, "add", ".")
    git(root, "commit", "--quiet", "-m", "fixture base")
    refresh(
        root,
        task_file="specs/example/todo.md",
        summary="Reviewed invented fixture task and next action.",
        now=NOW,
    )
    git(root, "add", HANDOFF, STATE)
    git(root, "commit", "--quiet", "-m", "reviewed fixture")
    return root


def edit_panel(root, note="Reviewed changed source and retained the next task."):
    path = root / HANDOFF
    path.write_text(
        path.read_text().replace("### Operating holds", note + "\n\n### Operating holds")
    )


def stage_review(root):
    git(root, "add", ".")
    refresh(root, summary="Reviewed staged source, current progress and task order.", now=NOW)
    git(root, "add", HANDOFF, STATE)


def test_fresh_committed_snapshot_is_read_only_and_matches_index(repo):
    before = git(repo, "status", "--porcelain=v1")
    result = check(repo, now=NOW)
    assert result["next_task"] == "EX-T02" and result["status"] == "FRESH"
    assert check(repo, staged=True, now=NOW)["source_fingerprint"] == result["source_fingerprint"]
    assert git(repo, "status", "--porcelain=v1") == before


def test_source_change_requires_review_and_current_panel_update(repo):
    (repo / "src/example.py").write_text("value = 2\n")
    git(repo, "add", ".")
    with pytest.raises(ValueError, match="source fingerprint"):
        check(repo, staged=True, now=NOW)
    with pytest.raises(ValueError, match="current panel"):
        refresh(repo, summary="Reviewed source without updating the current panel.", now=NOW)
    edit_panel(repo)
    stage_review(repo)
    assert check(repo, staged=True, now=NOW)["status"] == "FRESH"


def test_unstaged_acknowledgement_does_not_bypass_staged_guard(repo):
    (repo / "src/example.py").write_text("value = 2\n")
    git(repo, "add", "src/example.py")
    edit_panel(repo)
    with pytest.raises(ValueError, match="stage"):
        refresh(repo, summary="Staged source but unstaged handoff must fail.", now=NOW)
    with pytest.raises(ValueError):
        check(repo, staged=True, now=NOW)


@pytest.mark.parametrize("change", ["date", "history", "whitespace"])
def test_noop_acknowledgements_do_not_cover_code_changes(repo, change):
    (repo / "src/example.py").write_text("value = 2\n")
    path = repo / HANDOFF
    text = path.read_text()
    if change == "date":
        text = text.replace("Last updated: 2026-11-14.", "Last updated: 2026-11-15.")
    elif change == "history":
        text += "\nHistorical note only.\n"
    else:
        text = text.replace("Next task: EX-T02.", "Next   task: EX-T02.")
    path.write_text(text)
    git(repo, "add", ".")
    with pytest.raises(ValueError, match="current panel"):
        refresh(repo, summary="A timestamp or history update is insufficient.", now=NOW)


@pytest.mark.parametrize("age", [timedelta(days=7, seconds=1), timedelta(minutes=-6)])
def test_old_or_future_review_fails(repo, age):
    with pytest.raises(ValueError, match="review time"):
        check(repo, now=NOW + age)


def test_completed_next_task_forces_new_pickup(repo):
    path = repo / "specs/example/todo.md"
    path.write_text(path.read_text().replace("- [ ] Pending", "- [x] Pending"))
    git(repo, "add", ".")
    with pytest.raises(ValueError):
        check(repo, staged=True, now=NOW)
    edit_panel(repo)
    git(repo, "add", HANDOFF)
    with pytest.raises(ValueError, match="unfinished task"):
        refresh(repo, summary="All tasks are complete and require a new owning task file.", now=NOW)


@pytest.mark.parametrize("damage", ["metadata", "link", "commit", "task"])
def test_invalid_evidence_refuses_without_writes(repo, damage):
    if damage == "metadata":
        path = repo / STATE
        value = json.loads(path.read_text())
        value["reviewed_commit"] = "f" * 40
        path.write_text(json.dumps(value))
    elif damage == "link":
        edit_panel(repo, "Read [missing](missing.md#nowhere).")
    elif damage == "commit":
        edit_panel(repo, "| `deadbee` | nonexistent commit |")
    else:
        path = repo / HANDOFF
        path.write_text(path.read_text().replace("EX-T02", "EX-T03"))
    git(repo, "add", ".")
    before = {name: (repo / name).read_bytes() for name in (HANDOFF, STATE)}
    with pytest.raises(ValueError):
        check(repo, staged=True, now=NOW)
    assert before == {name: (repo / name).read_bytes() for name in before}


def test_deleted_source_and_renamed_files_are_detected(repo):
    git(repo, "mv", "src/example.py", "src/renamed.py")
    with pytest.raises(ValueError, match="source fingerprint"):
        check(repo, staged=True, now=NOW)
    edit_panel(repo)
    stage_review(repo)
    assert check(repo, staged=True, now=NOW)["status"] == "FRESH"


def test_index_snapshot_wins_over_unstaged_source(repo):
    (repo / "src/example.py").write_text("value = 999\n")
    assert check(repo, staged=True, now=NOW)["status"] == "FRESH"
    # Refresh must not silently acknowledge an unstaged source change.
    assert " M src/example.py" in git(
        repo, "status", "--porcelain=v1"
    ) or "M src/example.py" in git(repo, "status", "--porcelain=v1")


def test_merge_commit_with_same_content_remains_valid(repo):
    git(repo, "checkout", "-b", "feature")
    edit_panel(repo)
    stage_review(repo)
    git(repo, "commit", "--quiet", "-m", "reviewed feature")
    git(repo, "checkout", "-")
    git(repo, "merge", "--no-ff", "feature", "-m", "merge reviewed feature")
    assert check(repo, now=NOW)["status"] == "FRESH"


def test_all_tracked_inputs_are_covered_including_vendor_entries(repo):
    path = repo / "agents/agent-skills/invented.txt"
    path.parent.mkdir(parents=True)
    path.write_text("Invented tracked entry, not a library modification.\n")
    git(repo, "add", ".")
    with pytest.raises(ValueError, match="source fingerprint"):
        check(repo, staged=True, now=NOW)


def test_shallow_checkouts_refuse_instead_of_skipping_history(repo, tmp_path):
    clone = tmp_path / "shallow"
    git(repo, "clone", "--quiet", "--depth=1", repo.as_uri(), str(clone))
    with pytest.raises(ValueError, match="full Git history"):
        check(clone, now=NOW)


def test_metadata_only_refresh_cannot_hide_unreviewed_changes(repo):
    (repo / "src/example.py").write_text("value = 2\n")
    git(repo, "add", ".")
    path = repo / STATE
    before = path.read_bytes()
    with pytest.raises(ValueError, match="current panel"):
        refresh(repo, summary="A metadata-only acknowledgement must not pass.", now=NOW)
    assert path.read_bytes() == before


def test_unstaged_metadata_and_symlinks_are_not_overwritten(repo, tmp_path):
    path = repo / STATE
    original = path.read_bytes()
    path.write_text("uncommitted draft\n")
    with pytest.raises(ValueError, match="stage"):
        refresh(repo, summary="Preserve this uncommitted metadata draft.", now=NOW)
    assert path.read_text() == "uncommitted draft\n"
    path.unlink()
    outside = tmp_path / "outside.json"
    outside.write_bytes(original)
    path.symlink_to(outside)
    with pytest.raises(ValueError, match="stage"):
        refresh(repo, summary="Do not follow a metadata symlink during refresh.", now=NOW)
    assert outside.read_bytes() == original


def test_pr_base_comparison_detects_missing_panel_update(repo):
    base = git(repo, "rev-parse", "HEAD")
    (repo / "src/example.py").write_text("value = 2\n")
    edit_panel(repo)
    stage_review(repo)
    git(repo, "commit", "--quiet", "-m", "reviewed source")
    assert check(repo, base=base, now=NOW)["status"] == "FRESH"


def test_cli_checks_committed_and_staged_views(repo):
    import sys

    from scripts import verify_handoff

    refresh(repo, summary="Actual CLI readback for the staged and committed views.")
    git(repo, "add", HANDOFF, STATE)
    git(repo, "commit", "--quiet", "-m", "CLI review")
    for extra in ([], ["--staged"]):
        result = subprocess.run(
            [sys.executable, str(verify_handoff.__file__), "--repo", str(repo), *extra],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["status"] == "FRESH"


@pytest.mark.parametrize(
    "damage,expected",
    [("link", "broken local"), ("commit", "Git history/object"), ("task", "next task")],
)
def test_refresh_cannot_acknowledge_invalid_navigation_or_task_state(repo, damage, expected):
    if damage == "link":
        edit_panel(repo, "Read [missing anchor](../../specs/example/todo.md#missing).")
    elif damage == "commit":
        edit_panel(repo, "| `deadbee` | nonexistent commit |")
    else:
        path = repo / HANDOFF
        path.write_text(path.read_text().replace("EX-T02", "EX-T03"))
    git(repo, "add", ".")
    before = {p: (repo / p).read_bytes() for p in (HANDOFF, STATE)}
    with pytest.raises(ValueError, match=expected):
        refresh(repo, summary="Invalid task state or references cannot be acknowledged.", now=NOW)
    assert before == {p: (repo / p).read_bytes() for p in before}


@pytest.mark.parametrize(
    "note", ["<!-- comment-only acknowledgement -->", "**Status verified:** just now."]
)
def test_invisible_or_timestamp_only_panel_edits_do_not_cover_source(repo, note):
    (repo / "src/example.py").write_text("value = 2\n")
    edit_panel(repo, note)
    git(repo, "add", ".")
    with pytest.raises(ValueError, match="current panel"):
        refresh(repo, summary="Invisible or time-only acknowledgements must fail.", now=NOW)


def test_task_advance_requires_matching_handoff_pickup(repo):
    path = repo / "specs/example/todo.md"
    path.write_text(
        path.read_text().replace("- [ ] Pending", "- [x] Pending")
        + "\n## EX-T03 — Following task\n- [ ] New work\n"
    )
    path = repo / HANDOFF
    path.write_text(path.read_text().replace("EX-T02", "EX-T03"))
    stage_review(repo)
    assert check(repo, staged=True, now=NOW)["next_task"] == "EX-T03"


def test_metadata_duplicates_and_short_review_summaries_refuse(repo):
    path = repo / STATE
    before = path.read_bytes()
    with pytest.raises(ValueError, match="review summary"):
        refresh(repo, summary="checked", now=NOW)
    assert path.read_bytes() == before
    path.write_text(
        path.read_text().replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1')
    )
    git(repo, "add", STATE)
    with pytest.raises(ValueError, match="duplicate"):
        check(repo, staged=True, now=NOW)


def test_ci_and_daily_workflow_keep_the_guard_enabled():
    import yaml

    from scripts import verify_handoff

    root = Path(verify_handoff.__file__).resolve().parent.parent
    workflows = [
        yaml.load((root / path).read_text(), Loader=yaml.BaseLoader)
        for path in (".github/workflows/ci.yml", ".github/workflows/handoff-freshness.yml")
    ]
    for workflow in workflows:
        steps = next(iter(workflow["jobs"].values()))["steps"]
        assert (
            next(s for s in steps if s.get("uses", "").startswith("actions/checkout@"))["with"][
                "fetch-depth"
            ]
            == "0"
        )
        assert any("scripts/verify_handoff.py" in s.get("run", "") for s in steps)
        for step in steps:
            command = step.get("run", "")
            if "scripts/verify_handoff.py" in command or "GITHUB_STEP_SUMMARY" in command:
                syntax = subprocess.run(
                    ["bash", "-n"], input=command, text=True, capture_output=True
                )
                assert syntax.returncode == 0, syntax.stderr
            if "python - <<'PY'" in command:
                code = command.split("python - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
                compile(code, "workflow inline Python", "exec")
    daily = workflows[1]
    assert {"push", "pull_request", "schedule", "workflow_dispatch"} <= set(daily["on"])
    assert daily["permissions"] == {"contents": "read", "pull-requests": "read"}
    assert any(
        "GITHUB_STEP_SUMMARY" in s.get("run", "")
        and "gh" in s.get("run", "")
        and s.get("if") == "always()"
        for s in daily["jobs"]["freshness"]["steps"]
    )


def test_initial_metadata_can_be_linked_before_it_exists(repo):
    git(repo, "rm", STATE)
    edit_panel(repo, "Review [machine evidence](handoff_state.json).")
    git(repo, "add", HANDOFF)
    refresh(
        repo,
        task_file="specs/example/todo.md",
        summary="Bootstrap linked metadata only after successful validation.",
        now=NOW,
    )
    git(repo, "add", HANDOFF, STATE)
    assert check(repo, staged=True, now=NOW)["status"] == "FRESH"


def test_first_guard_can_migrate_a_legacy_handoff_without_review_metadata(repo):
    current = (repo / HANDOFF).read_text()
    git(repo, "rm", STATE)
    (repo / HANDOFF).write_text(
        "# Old handoff\n\nLegacy checkpoint, before a current panel existed.\n"
    )
    git(repo, "add", HANDOFF)
    git(repo, "commit", "--quiet", "-m", "legacy baseline")
    base = git(repo, "rev-parse", "HEAD")
    (repo / HANDOFF).write_text(current)
    (repo / "src/example.py").write_text("value = 2\n")
    git(repo, "add", ".")
    refresh(
        repo,
        task_file="specs/example/todo.md",
        summary="Bootstrap the structured panel and review its complete staged inputs.",
        now=NOW,
    )
    git(repo, "add", HANDOFF, STATE)
    git(repo, "commit", "--quiet", "-m", "first guard")
    assert check(repo, base=base, now=NOW)["status"] == "FRESH"


def test_existing_guard_cannot_bypass_panel_coverage_via_a_broken_base(repo):
    current = (repo / HANDOFF).read_text()
    (repo / HANDOFF).write_text("# Broken handoff while metadata already exists\n")
    git(repo, "add", HANDOFF)
    git(repo, "commit", "--quiet", "-m", "invalid guarded base")
    (repo / HANDOFF).write_text(current)
    (repo / "src/example.py").write_text("value = 2\n")
    git(repo, "add", ".")
    with pytest.raises(ValueError, match="current panel"):
        refresh(
            repo, summary="An established guard cannot use the legacy migration exemption.", now=NOW
        )
