"""OTC-T07 offline CLI acceptance; optionally compare an isolated installed wheel.

Uses invented fixtures and temporary stores only. Child CLI network calls fail.
"""

import argparse
import base64
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
FIXTURES = REPO / "tests/fixtures/finra_otc_transparency"
MODULE = "market_data_pipeline.pipelines.finra_otc_transparency"
RUNNER = (
    "import runpy,socket; "
    "deny=lambda *a,**k: (_ for _ in ()).throw(RuntimeError('network forbidden')); "
    "socket.socket.connect=deny; socket.socket.connect_ex=deny; socket.getaddrinfo=deny; "
    f"runpy.run_module('{MODULE}',run_name='__main__')"
)


def state(root):
    return {
        str(p.relative_to(root)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    }


def tables(root):
    with sqlite3.connect(root / "otc.sqlite3") as db:
        return {
            t: db.execute("SELECT * FROM " + t + " ORDER BY 1,2").fetchall()
            for t in ("otc_attempts", "otc_publications", "otc_rows")
        }


def verify(python, wheel_python, work):
    comparisons, profiles, roots = [], [], []

    def cli(args, code=0, remember=False, interpreter=python):
        result = subprocess.run(
            [str(interpreter), "-I", "-c", RUNNER, *map(str, args)],
            cwd=work,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == code, (args[0], result.returncode, result.stderr)
        value = json.loads(result.stdout if result.stdout else result.stderr)
        if remember:
            comparisons.append((args, code, value))
        return value

    for product in ("ATS_WEEKLY_DETAIL", "NON_ATS_WEEKLY_DETAIL"):
        for tier in ("T1", "T2", "OTCE"):
            label = product + "-" + tier
            root = work / label
            roots.append(root)
            base = ["--invented-fixture", "--root", root]
            scope = ["--partition", f"2026-10-19:{product}:{tier}"]
            name = (
                "ats_t1_weekly.txt" if product == "ATS_WEEKLY_DETAIL" else "non_ats_t1_invented.txt"
            )
            raw = (FIXTURES / name).read_bytes().replace(b"\nT1|", b"\n" + tier.encode() + b"|")
            short = b"\n".join(raw.splitlines()[:-1]) + b"\n"
            events, prior = [], None
            steps = [
                ("initial", raw, 3, 3),
                ("repeat", raw, 3, 3),
                ("correction", raw.replace(b"12345678901234567890", b"12345678901234567891"), 3, 4),
                ("reversion", raw, 3, 5),
                ("unverified", short, None, 5),
                ("removal", short, 2, 6),
                ("empty", raw.splitlines()[0] + b"\n", 0, 8),
                ("revival", raw, 3, 11),
            ]
            for step, payload, count, versions in steps:
                source = work / (label + "-" + step + ".txt")
                source.write_bytes(payload)
                args = [
                    "run",
                    *base,
                    "--manifest",
                    REPO / "manifests/finra-otc-transparency.yml",
                    "--input",
                    source,
                    "--week",
                    "2026-10-19",
                    "--product",
                    product,
                    "--tier",
                    tier,
                ]
                if count is not None:
                    args += ["--expected-rows", count]
                code = 2 if count is None else 0
                event = cli(args, code)
                assert event["status"] == ("UNVERIFIED" if count is None else "PUBLISHED")
                events.append((event, code))
                cutoff = [
                    "--as-of",
                    event["captured_at"],
                    "--as-of-sequence",
                    event["capture_sequence"],
                ]
                before = state(root)
                panel = cli(["inspect", *base, *scope, *cutoff], code, True)
                history = cli(["history", *base, *scope, *cutoff], remember=True)
                assert state(root) == before
                assert len(history["versions"]) == versions
                assert len(panel["rows"]) == (3 if count is None else count)
                assert all(r["provider_published_at"] is None for r in panel["rows"])
                if step == "repeat":
                    assert {r["first_seen_at"] for r in panel["rows"]} == {
                        events[0][0]["captured_at"]
                    }
                if step == "correction":
                    assert any(r["weekly_shares"] == "12345678901234567891" for r in panel["rows"])
                if step in ("reversion", "revival"):
                    assert any(r["weekly_shares"] == "12345678901234567890" for r in panel["rows"])
                if step == "unverified":
                    assert panel["rows"] == prior["rows"]
                prior = panel
            assert sum(v["state"] == "REMOVED" for v in history["versions"]) == 3
            assert all(v["content"] is None for v in history["versions"] if v["state"] == "REMOVED")
            before = state(root)
            cli(["inspect", *base, *scope], remember=True)
            cli(["history", *base, *scope], remember=True)
            for event, code in events:
                assert (
                    cli(["ingest-capture", *base, "--capture-id", event["capture_id"]], code, True)
                    == event
                )
            assert state(root) == before
            profiles.append(
                {
                    "product": product,
                    "tier": tier,
                    "captures": 8,
                    "snapshots": 7,
                    "versions": 11,
                    "removals": 3,
                    "current_rows": 3,
                }
            )

    root = work / "legacy"
    root.mkdir()
    roots.append(root)
    fixture = json.loads((FIXTURES / "warehouse_v1.json").read_text())
    for name, data in fixture["files_base64"].items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(data))
    with sqlite3.connect(root / "otc.sqlite3") as db:
        db.executescript(fixture["sql_dump"])
        db.execute("PRAGMA application_id=" + str(fixture["application_id"]))
        db.execute("PRAGMA user_version=" + str(fixture["user_version"]))
    base = ["--invented-fixture", "--root", root]
    scope = ["--partition", "2026-10-19:ATS_WEEKLY_DETAIL:T1"]
    original, before = tables(root), state(root)
    assert len(cli(["inspect", *base, *scope], 2)["rows"]) == 3
    deferred = fixture["attempts"][1]["capture_id"]
    cli(["ingest-capture", *base, "--capture-id", deferred], 1)
    assert state(root) == before
    assert cli(["migrate-warehouse", *base])["seeded_snapshots"] == 1
    assert tables(root) == original
    assert {k: v for k, v in before.items() if k != "otc.sqlite3"} == {
        k: v for k, v in state(root).items() if k != "otc.sqlite3"
    }
    cli(["ingest-capture", *base, "--capture-id", deferred])
    assert tables(root) == original
    before = state(root)
    assert cli(["migrate-warehouse", *base], remember=True)["status"] == "ALREADY_CURRENT"
    panel = cli(["inspect", *base, *scope], 2, True)
    history = cli(["history", *base, *scope], remember=True)
    assert any(r["weekly_shares"] == "9007199254740994" for r in panel["rows"])
    assert len(history["versions"]) == 4 and state(root) == before

    if wheel_python:
        origin = subprocess.check_output(
            [str(wheel_python), "-I", "-c", f"import {MODULE} as m; print(m.__file__)"],
            cwd=work,
            text=True,
        ).strip()
        assert not Path(origin).resolve().is_relative_to(REPO), "wheel interpreter imports source"
        before = {str(r): state(r) for r in roots}
        for args, code, expected in comparisons:
            assert cli(args, code, interpreter=wheel_python) == expected, args[0]
        assert {str(r): state(r) for r in roots} == before
    return {
        "origin": "INVENTED_FIXTURE",
        "source_conformance": "NOT_CHECKED",
        "profiles": profiles,
        "legacy_versions": 4,
        "legacy_tables_preserved": True,
        "read_retry_files_unchanged": sum(len(state(r)) for r in roots),
        "wheel_comparisons": len(comparisons) if wheel_python else 0,
        "network_connections": "DENIED_IN_CLI_CHILDREN",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--wheel-python", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="market-medallion-t07-") as temporary:
        result = verify(
            args.python.absolute(),
            args.wheel_python and args.wheel_python.absolute(),
            Path(temporary),
        )
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
