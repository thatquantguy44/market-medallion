"""FOMC-QAC06/07: immutable portable review inputs and actual render-hash checks."""

import hashlib
import json
from collections import Counter
from pathlib import Path, PurePosixPath

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality import build_report
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_preview import (
    PROFILE,
    render_html,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.quality_review import (
    validate_plan,
    validate_review,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
    PostgresDependencyStore,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.quality import (
    read_quality_inputs,
    source_snapshot,
    write_quality_artifact,
    write_quality_bytes,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest


def _sha(body):
    return hashlib.sha256(body).hexdigest()


def export_review_pack(cfg, report, plan, output, *, inventory_store):
    validate_plan(plan, report)
    output = Path(output)
    manifest = load_manifest(cfg.manifest)
    dependencies = PostgresDependencyStore(PostgresBronzeStore(inventory_store))
    entries = []
    with source_snapshot(inventory_store):
        args = read_quality_inputs(cfg, inventory_store, report=report)
        if build_report(*args) != report:
            raise ValueError("FOMC review report no longer matches pinned originals")
        extractions = args[3]
        for case in plan["cases"]:
            capture_id = case["capture_ids"][0]
            receipt, payload = dependencies._verified(capture_id, manifest, cfg.root)
            if (
                receipt["sha256"] != case["source_sha256"]
                or receipt["source_url"] != case["source_url"]
            ):
                raise ValueError("FOMC review original source identity mismatch")
            source_path = (
                "sources/"
                + case["source_sha256"]
                + (".pdf" if case["format"] == "pdf" else ".html")
            )
            write_quality_bytes(cfg, output / source_path, payload)
            assets = {}
            asset_evidence = []
            if case["format"] == "html":
                ids = inventory_store.connection.execute(
                    inventory_store._query(
                        "SELECT check_id FROM {schema}.bronze_dependency_checks "
                        "WHERE parent_capture_id=%s"
                    ),
                    (capture_id,),
                ).fetchall()
                for (identity,) in ids:
                    for child in dependencies.read_check(identity)["dependencies"]:
                        asset, body = dependencies._verified(
                            child["child_capture_id"], manifest, cfg.root
                        )
                        if asset["kind"] == "asset":
                            if (
                                asset["source_url"] in assets
                                and assets[asset["source_url"]] != body
                            ):
                                raise ValueError("FOMC review ambiguous archived image revisions")
                            assets[asset["source_url"]] = body
                            asset_evidence.append(
                                {k: asset[k] for k in ("capture_id", "source_url", "sha256")}
                            )
                preview = render_html(payload, case["source_url"], manifest, assets)
                render_path = "previews/" + case["case_id"] + ".html"
                write_quality_bytes(cfg, output / render_path, preview)
                input_sha = _sha(preview)
            else:
                render_path = source_path
                input_sha = case["source_sha256"]
            extraction = extractions[case["extraction_id"]]
            text = (
                extraction["units"][case["unit_ordinal"]]["text"]
                if case["unit_ordinal"] is not None
                else ""
            )
            text_path = "units/" + case["case_id"] + ".txt"
            write_quality_bytes(cfg, output / text_path, text.encode())
            entries.append(
                {
                    "case_id": case["case_id"],
                    "source_path": source_path,
                    "source_sha256": case["source_sha256"],
                    "capture_id": capture_id,
                    "render_input_path": render_path,
                    "render_input_sha256": input_sha,
                    "preview_profile": PROFILE if case["format"] == "html" else None,
                    "page": case["page"],
                    "unit_text_path": text_path,
                    "unit_text_sha256": _sha(text.encode()),
                    "assets": sorted(
                        asset_evidence, key=lambda a: (a["source_url"], a["capture_id"])
                    ),
                }
            )
    value = {
        "schema_id": "fomc-quality-review-pack/1",
        "report_id": report["report_id"],
        "plan_id": plan["plan_id"],
        "cases": entries,
        "request_performed": False,
    }
    value["pack_id"] = digest(value)
    # Publish the complete manifest only after every immutable input file exists.
    write_quality_artifact(cfg, output / "pack.json", value)
    return value


def _read(root, relative, expected):
    path = PurePosixPath(relative)
    if (
        not path.parts
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in relative
        or ":" in relative
    ):
        raise ValueError("FOMC review unsafe evidence path")
    base = Path(root).resolve()
    target = (base / relative).resolve()
    if not target.is_relative_to(base):
        raise ValueError("FOMC review evidence escaped output root")
    body = target.read_bytes()
    if _sha(body) != expected:
        raise ValueError("FOMC review evidence hash mismatch")
    return body


def check_reviews(report, plan, records, output):
    validate_plan(plan, report)
    try:
        pack = json.loads((Path(output) / "pack.json").read_bytes())
        body = dict(pack)
        identity = body.pop("pack_id")
        if (
            identity != digest(body)
            or pack["schema_id"] != "fomc-quality-review-pack/1"
            or pack["report_id"] != report["report_id"]
            or pack["plan_id"] != plan["plan_id"]
            or pack["request_performed"] is not False
        ):
            raise ValueError
        cases = {c["case_id"]: c for c in plan["cases"]}
        entries = {e["case_id"]: e for e in pack["cases"]}
        if len(entries) != len(pack["cases"]) or set(entries) != set(cases):
            raise ValueError
        for key, entry in entries.items():
            case = cases[key]
            if (
                entry["source_sha256"] != case["source_sha256"]
                or entry["capture_id"] not in case["capture_ids"]
                or entry["page"] != case["page"]
            ):
                raise ValueError
            _read(output, entry["source_path"], case["source_sha256"])
            _read(output, entry["render_input_path"], entry["render_input_sha256"])
            text = _read(output, entry["unit_text_path"], entry["unit_text_sha256"])
            if case["unit_text_sha256"] is not None and _sha(text) != case["unit_text_sha256"]:
                raise ValueError
        if not isinstance(records, list) or len(records) > len(cases):
            raise ValueError
        seen = set()
        for record in records:
            validate_review(record, plan)
            key = record["case"]["case_id"]
            if (
                key in seen
                or record["render"]["input_sha256"] != entries[key]["render_input_sha256"]
            ):
                raise ValueError
            seen.add(key)
            image = _read(output, record["render"]["path"], record["render"]["sha256"])
            if not image.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ValueError
    except (KeyError, TypeError, ValueError, OSError):
        raise ValueError("FOMC review pack or review evidence is invalid") from None
    value = {
        "schema_id": "fomc-quality-review-check/1",
        "report_id": report["report_id"],
        "plan_id": plan["plan_id"],
        "pack_id": pack["pack_id"],
        "status": "reviewed" if seen == set(cases) else "pending_review",
        "reviewed_cases": len(seen),
        "review_ids": sorted(r["review_id"] for r in records),
        "pending_case_ids": sorted(set(cases) - seen),
        "dispositions": dict(sorted(Counter(r["disposition"] for r in records).items())),
        "confirmed_native_defects": [
            r["case"]["case_id"] for r in records if r["disposition"] == "confirmed_native_defect"
        ],
        "unresolved_cases": [
            r["case"]["case_id"]
            for r in records
            if r["disposition"] in {"unresolved", "ocr_candidate"}
        ],
        "eligibility_changed": False,
        "whole_corpus_quality_certified": False,
        "request_performed": False,
    }
    value["check_id"] = digest(value)
    return value
