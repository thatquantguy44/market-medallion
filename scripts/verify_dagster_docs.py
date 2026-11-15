"""Check the canonical Dagster guide against root-composed definitions, offline."""

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

DOCUMENT = "DAGSTER.md"
LEGACY = "docs/deployment/dagster_jobs.md"
HEADERS = {
    "jobs": ("Job", "Purpose", "Config and runbook"),
    "schedules": ("Schedule", "Job", "Cron", "Time zone", "Default"),
    "sensors": ("Sensor", "Type", "Default"),
}


def inventory_from_definitions(definitions):
    """Inspect definitions only; never open a Dagster instance or evaluate a tick."""
    repo = definitions.get_repository_def()
    return {
        "jobs": sorted(j.name for j in repo.get_all_jobs() if not j.name.startswith("__ASSET_JOB")),
        "schedules": {
            s.name: (
                s.job_name,
                s.cron_schedule
                if isinstance(s.cron_schedule, str)
                else " ; ".join(s.cron_schedule),
                s.execution_timezone or "UTC",
                s.default_status.value,
            )
            for s in sorted(repo.schedule_defs, key=lambda s: s.name)
        },
        "sensors": {
            s.name: (s.sensor_type.value, s.default_status.value)
            for s in sorted(repo.sensor_defs, key=lambda s: s.name)
        },
    }


def current_inventory():
    try:
        from market_data_pipeline.orchestration.dagster_defs import defs
    except ImportError:
        raise ValueError(
            "Install the project's pinned Dagster environment and editable package before checking"
        ) from None
    return inventory_from_definitions(defs)


def _read(root, name):
    path = root / name
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise ValueError("required documentation must be a regular repository file: " + name)
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("documentation exceeds its byte bound: " + name)
    return path.read_text(encoding="utf-8")


def _table(document, kind):
    start, end = "<!-- dagster:" + kind + " -->", "<!-- /dagster:" + kind + " -->"
    if document.count(start) != 1 or document.count(end) != 1:
        raise ValueError("canonical " + kind + " markers are missing or duplicated")
    before, body = document.split(start)
    if end in before:
        raise ValueError("canonical inventory markers are out of order")
    body = body.split(end)[0]
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    if len(lines) < 2:
        raise ValueError("canonical " + kind + " table is missing")

    def cells(line):
        if not line.startswith("|") or not line.endswith("|"):
            raise ValueError("malformed canonical " + kind + " table row")
        result = tuple(cell.strip().strip("`") for cell in line[1:-1].split("|"))
        if len(result) != len(HEADERS[kind]) or any(not cell for cell in result):
            raise ValueError("canonical " + kind + " table has missing/extra/blank columns")
        return result

    if cells(lines[0]) != HEADERS[kind] or any(
        not re.fullmatch(r":?-{3,}:?", cell) for cell in cells(lines[1])
    ):
        raise ValueError("canonical " + kind + " table headers differ from the contract")
    rows = {}
    for line in lines[2:]:
        name, *values = cells(line)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) or name in rows:
            raise ValueError("canonical " + kind + " identity is invalid or duplicated")
        rows[name] = tuple(values)
    return rows


def _visible(document):
    lines, fence = [], None
    for line in document.splitlines():
        match = re.match(r"^\s*(`{3,}|~{3,})", line)
        if match:
            token = match[1]
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            continue
        if fence is None:
            lines.append(line)
    text = re.sub(r"<!--.*?-->", "", "\n".join(lines), flags=re.S)
    return re.sub(r"`[^`\n]*`", "", text)


def _links(document):
    return re.findall(r"\[[^\]\n]+\]\(([^)\n]+)\)", _visible(document))


def _local_target(root, owner, target):
    parts = urlsplit(target)
    if parts.scheme or parts.netloc:
        return None
    path = (root / owner).parent / unquote(parts.path) if parts.path else root / owner
    path = path.resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError("broken or escaping local documentation link in " + owner)
    if parts.fragment:
        text = path.read_text(encoding="utf-8")
        headings = re.findall(r"(?m)^#{1,6}\s+(.+?)\s*#*$", text)
        slugs = {
            re.sub(
                r"\s",
                "-",
                "".join(
                    c
                    for c in heading.lower().replace("`", "")
                    if c.isalnum() or c.isspace() or c in "-_"
                ),
            )
            for heading in headings
        }
        if unquote(parts.fragment) not in slugs:
            raise ValueError("broken local documentation link anchor in " + owner)
    return path


def _compare(kind, actual, expected):
    if actual == expected:
        return
    missing, obsolete = sorted(set(expected) - set(actual)), sorted(set(actual) - set(expected))
    changed = sorted(k for k in set(actual) & set(expected) if actual[k] != expected[k])
    raise ValueError(
        "canonical "
        + kind
        + " differ from definitions; missing="
        + str(missing)
        + ", obsolete="
        + str(obsolete)
        + ", changed="
        + str(changed)
    )


def check(root, *, inventory=None):
    """Check working-tree documentation; CI checks the committed checkout."""
    root = Path(root).resolve()
    document = _read(root, DOCUMENT)
    inventory = current_inventory() if inventory is None else inventory
    jobs = _table(document, "jobs")
    _compare("jobs", dict.fromkeys(jobs), dict.fromkeys(inventory["jobs"]))
    for name, (_, references) in jobs.items():
        if not any(_local_target(root, DOCUMENT, target) for target in _links(references)):
            raise ValueError("canonical jobs need a local config/runbook link: " + name)
    for kind in ("schedules", "sensors"):
        _compare(kind, _table(document, kind), inventory[kind])
    for target in _links(document):
        _local_target(root, DOCUMENT, target)
    for name in ("README.md", "AGENTS.md", LEGACY):
        text = _read(root, name)
        targets = _links(text)
        linked = [
            t
            for t in targets
            if (
                not urlsplit(t).scheme
                and not urlsplit(t).netloc
                and ((root / name).parent / unquote(urlsplit(t).path)).resolve() == root / DOCUMENT
            )
        ]
        if not linked:
            raise ValueError("canonical Dagster navigation missing from " + name)
        try:
            for target in linked:
                _local_target(root, name, target)
        except ValueError:
            raise ValueError(
                "canonical Dagster navigation has an invalid anchor in " + name
            ) from None
        if name == LEGACY and re.search(r"(?im)^\s*\|\s*(Job|Pipeline|Schedule|Sensor)\b", text):
            raise ValueError("legacy Dagster page must remain a pointer, not a second inventory")
    return {"jobs": len(jobs), **{k: len(inventory[k]) for k in ("schedules", "sensors")}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--show-current", action="store_true", help="Print safe current definition facts"
    )
    args = parser.parse_args(argv)
    try:
        if args.show_current:
            print(json.dumps(current_inventory(), sort_keys=True, indent=2))
        else:
            counts = check(args.repo)
            print(
                "Dagster documentation verified: "
                + ", ".join(f"{v} {k}" for k, v in counts.items())
            )
        return 0
    except (ValueError, OSError, UnicodeError) as error:
        print("Dagster documentation check failed: " + str(error), file=sys.stderr)
        print(
            "Update DAGSTER.md and README/AGENTS links in the same change; see --show-current.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
