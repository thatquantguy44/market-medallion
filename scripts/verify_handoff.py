"""Offline freshness guard for the master handoff, using committed or staged Git blobs."""

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo

HANDOFF = "docs/handoffs/project_bootstrap.md"
STATE = "docs/handoffs/handoff_state.json"
START = "## Agent start here\n"
HISTORY = "## Historical checkpoints and retained context\n"
KEYS = {
    "schema_version",
    "reviewed_at",
    "reviewed_commit",
    "source_fingerprint",
    "document_fingerprint",
    "panel_fingerprint",
    "task_file",
    "next_task",
    "review_summary",
}


def _git(root, *args):
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        timeout=30,
    )
    if result.returncode:
        raise ValueError("Git history/object unavailable; use a full checkout and valid refs")
    return result.stdout


def _commit(root, value):
    if not isinstance(value, str) or (
        value != "HEAD" and not re.fullmatch(r"[0-9a-f]{7,64}", value)
    ):
        raise ValueError("invalid commit reference")
    return (
        _git(root, "rev-parse", "--verify", "--end-of-options", value + "^{commit}")
        .decode()
        .strip()
    )


def _path(value):
    if not isinstance(value, str) or not value or "\\" in value or not value.isprintable():
        raise ValueError("invalid repository path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ValueError("invalid repository path")
    return value


class Snapshot:
    def __init__(self, root, *, staged=False, ref="HEAD"):
        self.root, self.staged = Path(root), staged
        if _git(root, "rev-parse", "--is-shallow-repository").strip() == b"true":
            raise ValueError(
                "handoff verification requires a full Git history; unshallow the checkout"
            )
        self.commit = _commit(root, "HEAD" if staged else ref)
        self.entries = {}
        command = ("ls-files", "--stage", "-z") if staged else ("ls-tree", "-r", "-z", self.commit)
        for item in _git(root, *command).split(b"\0"):
            if not item:
                continue
            info, name = item.split(b"\t", 1)
            mode, middle, last = info.decode().split()
            if staged and last != "0":
                raise ValueError("resolve unmerged index entries before checking the handoff")
            self.entries[_path(name.decode())] = (mode, middle if staged else last)
        if len(self.entries) > 100000:
            raise ValueError("handoff inventory exceeds its file bound")

    def read(self, name, *, required=True):
        entry = self.entries.get(_path(name))
        if entry is None and not required:
            return None
        if entry is None or entry[0] not in {"100644", "100755"}:
            raise ValueError(
                "required handoff/task file is missing or not a regular tracked file: " + name
            )
        body = _git(self.root, "cat-file", "blob", entry[1])
        if len(body) > 1024 * 1024:
            raise ValueError("handoff/task document exceeds its byte bound")
        return body.decode("utf-8")

    def fingerprint(self):
        entries = [
            [name, *entry]
            for name, entry in sorted(self.entries.items())
            if name not in {HANDOFF, STATE}
        ]
        return _digest(json.dumps(entries, separators=(",", ":")))


def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def _panel(document):
    if document.count(START) != 1 or document.count(HISTORY) != 1:
        raise ValueError("master handoff must have one current panel and one history boundary")
    panel = document.split(START, 1)[1].split(HISTORY, 1)[0]
    for heading in (
        "Current work and highest priorities",
        "Last completed work and commits",
        "Keep this panel current",
    ):
        if "### " + heading not in panel:
            raise ValueError("master handoff is missing a required current-panel section")
    return panel


def _panel_fingerprint(document):
    panel = re.sub(r"<!--.*?-->", "", _panel(document), flags=re.S)
    panel = re.sub(r"(?m)^\*\*Status verified:\*\*.*$", "", panel)
    return _digest(" ".join(panel.split()))


def _tasks(snapshot, task_file):
    text = snapshot.read(_path(task_file))
    if not task_file.endswith("/todo.md"):
        raise ValueError("task_file must select a canonical todo.md")
    sections = list(re.finditer(r"(?m)^#{2,3} (.+)$", text))
    tasks = []
    for index, section in enumerate(sections):
        identity = re.match(r"([A-Z][A-Z0-9_-]*\d+)(?:\s|$)", section[1])
        if identity is None:
            continue
        end = sections[index + 1].start() if index + 1 < len(sections) else len(text)
        boxes = re.findall(r"(?m)^[-*] \[([ xX])\]", text[section.end() : end])
        if not boxes:
            raise ValueError("canonical task has no acceptance checkboxes")
        tasks.append((identity[1], all(box.lower() == "x" for box in boxes)))
    if not tasks or len({identity for identity, _ in tasks}) != len(tasks):
        raise ValueError("canonical task IDs are missing or duplicated")
    next_task = next((identity for identity, done in tasks if not done), None)
    if next_task is None:
        raise ValueError("no unfinished task; select the next approved owning task file")
    return next_task


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate handoff metadata key")
        value[key] = item
    return value


def _metadata(text):
    value = json.loads(text, object_pairs_hook=_object)
    if (
        not isinstance(value, dict)
        or set(value) != KEYS
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
    ):
        raise ValueError("invalid handoff metadata schema")
    for key in ("source_fingerprint", "document_fingerprint", "panel_fingerprint"):
        if not isinstance(value[key], str) or not re.fullmatch(r"[0-9a-f]{64}", value[key]):
            raise ValueError("invalid handoff metadata fingerprint")
    summary = value["review_summary"]
    if (
        not isinstance(summary, str)
        or not 20 <= len(summary.strip()) <= 2000
        or not summary.isprintable()
    ):
        raise ValueError("provide a meaningful one-line review summary (20..2000 characters)")
    return value


def _slug(heading):
    heading = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", heading).lower().replace("`", "")
    return re.sub(
        r"\s", "-", "".join(c for c in heading if c.isalnum() or c.isspace() or c in "-_")
    )


def _references(snapshot, document):
    panel = _panel(document)
    for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", panel):
        parts = urlsplit(target)
        if parts.scheme or parts.netloc:
            continue
        path = unquote(parts.path)
        # Normalize relative links without reading through filesystem symlinks.
        components = list(PurePosixPath(HANDOFF).parent.parts) if path else []
        for item in PurePosixPath(path or HANDOFF).parts:
            if item == "..":
                if not components:
                    raise ValueError("handoff link escapes repository")
                components.pop()
            elif item != ".":
                components.append(item)
        name = "/".join(components)
        exists = (
            name == STATE
            or name in snapshot.entries
            or any(p.startswith(name + "/") for p in snapshot.entries)
        )
        if not exists:
            raise ValueError("handoff has a broken local link: " + target)
        if parts.fragment:
            text = snapshot.read(name)
            headings = re.findall(r"(?m)^#{1,6}\s+(.+?)\s*#*$", text)
            if unquote(parts.fragment) not in {_slug(h) for h in headings}:
                raise ValueError("handoff has a broken local anchor: " + target)
    for reference in re.findall(r"(?m)^\| (?:\[)?`([0-9a-f]{7,64})`", panel):
        _commit(snapshot.root, reference)


def _base(snapshot, base):
    if base:
        return Snapshot(snapshot.root, ref=base)
    if snapshot.staged:
        return Snapshot(snapshot.root)
    try:
        parent = (
            _git(snapshot.root, "rev-parse", "--verify", snapshot.commit + "^").decode().strip()
        )
    except ValueError:
        return None
    return Snapshot(snapshot.root, ref=parent)


def _validate(snapshot, metadata, document, *, now, base=None):
    value = _metadata(json.dumps(metadata))
    try:
        reviewed = datetime.fromisoformat(value["reviewed_at"])
        if reviewed.tzinfo is None or reviewed.utcoffset() != timedelta(0):
            raise ValueError
        age = now - reviewed
        if age > timedelta(days=7) or age < timedelta(minutes=-5):
            raise ValueError
    except (ValueError, TypeError):
        raise ValueError(
            "handoff review time is expired or in the future; review and refresh it"
        ) from None
    reviewed_commit = _commit(snapshot.root, value["reviewed_commit"])
    _git(snapshot.root, "merge-base", "--is-ancestor", reviewed_commit, snapshot.commit)
    if value["source_fingerprint"] != snapshot.fingerprint():
        raise ValueError("handoff source fingerprint differs; review the changes and refresh it")
    if value["document_fingerprint"] != _digest(document) or value[
        "panel_fingerprint"
    ] != _panel_fingerprint(document):
        raise ValueError("handoff document differs from its review; stage it and refresh")
    expected_task = _tasks(snapshot, value["task_file"])
    if (
        value["next_task"] != expected_task
        or not re.search(
            r"(?m)^\*\*Next implementation:.*\b" + re.escape(expected_task) + r"\b", document
        )
        or "### Exact next slice: " + expected_task not in document
    ):
        raise ValueError("handoff next task disagrees with the first unfinished canonical task")
    previous = _base(snapshot, base)
    if previous is not None:
        old_document = previous.read(HANDOFF, required=False)
        if old_document is not None and previous.fingerprint() != snapshot.fingerprint():
            try:
                previous_panel = _panel_fingerprint(old_document)
            except ValueError:
                # Only the first migration may start from a legacy unguarded panel.
                if previous.read(STATE, required=False) is not None:
                    raise
            else:
                if previous_panel == _panel_fingerprint(document):
                    raise ValueError(
                        "source changes require a meaningful update to the current panel"
                    )
    _references(snapshot, document)
    return dict(
        status="FRESH",
        next_task=expected_task,
        reviewed_at=value["reviewed_at"],
        checked_commit=snapshot.commit,
        view="index" if snapshot.staged else "commit",
        source_fingerprint=value["source_fingerprint"],
        review_summary=value["review_summary"],
    )


def check(root, *, staged=False, ref="HEAD", base=None, now=None):
    snapshot = Snapshot(root, staged=staged, ref=ref)
    return _validate(
        snapshot,
        _metadata(snapshot.read(STATE)),
        snapshot.read(HANDOFF),
        now=now or datetime.now(UTC),
        base=base,
    )


def refresh(root, *, summary, task_file=None, now=None):
    root, now = Path(root), now or datetime.now(UTC)
    snapshot = Snapshot(root, staged=True)
    document = snapshot.read(HANDOFF)
    prior = snapshot.read(STATE, required=False)
    for name, staged in ((HANDOFF, document), (STATE, prior)):
        path = root / name
        if (
            path.is_symlink()
            or (staged is None and path.exists())
            or (staged is not None and path.read_text() != staged)
        ):
            raise ValueError(
                "stage the handoff and metadata edits before refreshing; "
                "unstaged edits are preserved"
            )
    task_file = task_file or (_metadata(prior)["task_file"] if prior is not None else None)
    if task_file is None:
        raise ValueError("initial refresh requires --task-file")
    document = re.sub(
        r"(?m)^Last updated: .*?$",
        "Last updated: " + now.astimezone(ZoneInfo("America/New_York")).date().isoformat() + ".",
        document,
        count=1,
    )
    value = dict(
        schema_version=1,
        reviewed_at=now.astimezone(UTC).isoformat(),
        reviewed_commit=snapshot.commit,
        task_file=_path(task_file),
        next_task=_tasks(snapshot, task_file),
        source_fingerprint=snapshot.fingerprint(),
        document_fingerprint=_digest(document),
        panel_fingerprint=_panel_fingerprint(document),
        review_summary=summary,
    )
    _validate(snapshot, value, document, now=now)
    (root / HANDOFF).write_text(document)
    (root / STATE).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--staged", action="store_true", help="check exactly the Git index")
    parser.add_argument("--ref", default="HEAD", help="committed snapshot to check (default HEAD)")
    parser.add_argument("--base", help="comparison commit; CI supplies the PR base")
    parser.add_argument(
        "--refresh", action="store_true", help="review the staged snapshot and write metadata"
    )
    parser.add_argument("--summary", help="one-line description of the actual handoff review")
    parser.add_argument("--task-file", help="initial/explicit owning todo.md selection")
    args = parser.parse_args(argv)
    try:
        if args.refresh:
            if not args.summary or args.staged or args.ref != "HEAD" or args.base:
                raise ValueError("--refresh requires --summary and uses the staged snapshot")
            result = refresh(args.repo, summary=args.summary, task_file=args.task_file)
        else:
            result = check(args.repo, staged=args.staged, ref=args.ref, base=args.base)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        UnicodeError,
        subprocess.SubprocessError,
    ) as error:
        print("Handoff freshness failed: " + str(error), file=sys.stderr)
        print(
            "Update the current panel, stage changes, run --refresh --summary, "
            "then stage both handoff files.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
