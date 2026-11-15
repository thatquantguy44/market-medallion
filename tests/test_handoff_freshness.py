"""Objective handoff freshness: Git snapshots, task state and review coverage."""

import json
import subprocess
from datetime import UTC, datetime, timedelta

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
