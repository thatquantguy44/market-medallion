"""Publish one review-bound baseline as an immutable local generation.

Only explicit local captures are read; publication never fetches provider data.
"""

import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

from .assemble import build_annual_dataset, write_monthly_returns
from .canonical import canonical_sha256
from .config import load_config
from .dataset import MAX_DATASET_BYTES, parse_dataset_bytes
from .provenance import canonical_json as donor_canonical_json
from .review import load_review_version
from .storage import _read_bounded, _safe_child, load_capture
from .warehouse import _normalize

_ID = re.compile(r"^[0-9a-f]{64}$")
_PROFILE = re.compile(r"^[a-z][a-z0-9-]{0,79}$")
_DATA = "crsp-market-proxy-annual.json"
_MANIFEST = "crsp-market-proxy-annual.manifest.json"


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _bytes(value):
    return (
        json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode()


def _tree_digest():
    directory = Path(__file__).parent
    # Scope is explicit so an unknown repository revision is never implied.
    names = (
        "__init__.py",
        "assemble.py",
        "canonical.py",
        "config.py",
        "dataset.py",
        "parsers.py",
        "pipeline.py",
        "provenance.py",
        "publication.py",
        "quality.py",
        "review.py",
        "review_workbook.py",
        "storage.py",
        "warehouse.py",
    )
    files = {name: _sha((directory / name).read_bytes()) for name in names}
    files["shared/files.py"] = _sha((directory.parent.parent / "shared" / "files.py").read_bytes())
    return canonical_sha256(files)


def _generation(root, snapshot_id):
    if not isinstance(snapshot_id, str) or not _ID.fullmatch(snapshot_id):
        raise ValueError("Invalid snapshot identity.")
    return _safe_child(_safe_child(_safe_child(Path(root), "gold"), "snapshots"), snapshot_id)


def _read_generation(root, snapshot_id, expected_catalog=None):
    try:
        directory = _generation(root, snapshot_id)
        catalog_bytes = _read_bounded(_safe_child(directory, "catalog.json"), 1024 * 1024)
        if expected_catalog is not None and _sha(catalog_bytes) != expected_catalog:
            raise ValueError("Catalog digest mismatch.")
        catalog = json.loads(catalog_bytes)
        if (
            catalog["schemaVersion"] != "presidential-research-bundle/1"
            or catalog["snapshotId"] != snapshot_id
        ):
            raise ValueError("Catalog identity mismatch.")
        semantic = catalog["semanticInventory"]
        if canonical_sha256(semantic) != snapshot_id:
            raise ValueError("Snapshot identity mismatch.")
        if any(
            catalog[key] != semantic[key]
            for key in (
                "profileId",
                "decisionPolicy",
                "producer",
                "inputInventory",
                "datasets",
                "artifacts",
                "inputKind",
                "sources",
                "quality",
                "limitations",
            )
        ):
            raise ValueError("Catalog content mismatch.")
        entries = catalog["datasets"]
        if not isinstance(entries, list) or len(entries) != 1:
            raise ValueError("Invalid dataset inventory.")
        entry = entries[0]
        if entry["path"] != f"datasets/{_DATA}" or entry["manifestPath"] != f"datasets/{_MANIFEST}":
            raise ValueError("Invalid dataset paths.")
        dataset_dir = _safe_child(directory, "datasets")
        payload = _read_bounded(_safe_child(dataset_dir, _DATA), MAX_DATASET_BYTES)
        manifest = _read_bounded(_safe_child(dataset_dir, _MANIFEST), 1024 * 1024)
        if (
            _sha(payload) != entry["sha256"]
            or len(payload) != entry["bytes"]
            or _sha(manifest) != entry["manifestSha256"]
            or len(manifest) != entry["manifestBytes"]
        ):
            raise ValueError("Dataset digest mismatch.")
        dataset = parse_dataset_bytes(payload)
        if canonical_sha256(dataset) != entry["canonicalSha256"]:
            raise ValueError("Canonical dataset digest mismatch.")
        sidecar = json.loads(manifest)
        if sidecar["dataset_sha256"] != _sha(donor_canonical_json(dataset).encode()):
            raise ValueError("Donor dataset digest mismatch.")
        return {
            "catalog": catalog,
            "dataset": dataset,
            "manifest": sidecar,
            "catalogSha256": _sha(catalog_bytes),
        }
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Publication integrity check failed.") from exc


def read_current(root):
    root = Path(root)
    try:
        pointer = json.loads(
            _read_bounded(_safe_child(_safe_child(root, "gold"), "current.json"), 65536)
        )
        if (
            set(pointer) != {"schemaVersion", "snapshotId", "catalogPath", "catalogSha256"}
            or pointer["schemaVersion"] != "presidential-research-pointer/1"
        ):
            raise ValueError("Invalid pointer.")
        snapshot_id = pointer["snapshotId"]
        if (
            not isinstance(snapshot_id, str)
            or not _ID.fullmatch(snapshot_id)
            or pointer["catalogPath"] != f"gold/snapshots/{snapshot_id}/catalog.json"
            or not isinstance(pointer["catalogSha256"], str)
            or not _ID.fullmatch(pointer["catalogSha256"])
        ):
            raise ValueError("Invalid pointer fields.")
        return _read_generation(root, snapshot_id, pointer["catalogSha256"])
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Publication integrity check failed.") from exc


def _sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(path, data):
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def publish_baseline(root, capture_id, review_id, *, profile_id, created_at: datetime, fault=None):
    """Validate explicit archived inputs and publish one baseline generation."""
    if not isinstance(profile_id, str) or not _PROFILE.fullmatch(profile_id):
        raise ValueError("Invalid profile.")
    capture, raw, config_bytes = load_capture(root, "FF_MKT_001", capture_id)
    review, controls, control_manifest = load_review_version(root, review_id)
    fixture = capture["input_kind"] == "SYNTHETIC_FIXTURE"
    if fixture != (review["evidence_kind"] == "SYNTHETIC_FIXTURE"):
        raise ValueError("Capture and review evidence kinds do not match.")
    if (fixture and not profile_id.startswith("fixture-")) or (
        not fixture and not profile_id.startswith("local-")
    ):
        raise ValueError("profile is not admitted for these input kinds.")
    if capture["config_sha256"] != review["config_sha256"]:
        raise ValueError("Capture and review configuration digests differ.")
    if capture["parser_id"] != "fama_french_monthly_zip":
        raise ValueError("Unsupported baseline parser.")
    with tempfile.TemporaryDirectory(prefix="presidential-build-") as tmp:
        work = Path(tmp)
        (work / "sources.toml").write_bytes(config_bytes)
        config = load_config(work / "sources.toml")
        if config.source("FF_MKT_001").parser != capture["parser_id"]:
            raise ValueError("Archived parser differs from configuration.")
        rows = [
            {key: row[key] for key in ("date", "market_return_decimal", "source_id")}
            for _, _, row in _normalize(capture["parser_id"], raw, "FF_MKT_001")
        ]
        market_path = work / "monthly.csv"
        controls_path = work / "controls.csv"
        write_monthly_returns(market_path, rows)
        controls_path.write_bytes(controls)
        (work / "controls.manifest.json").write_bytes(_bytes(control_manifest))
        dataset, manifest = build_annual_dataset(
            config,
            market_path,
            controls_path,
            "window_start",
            additional_inputs={
                "raw_capture_sha256": capture["sha256"],
                "raw_capture_id": capture_id,
                "review_id": review_id,
                "review_receipt_controls_sha256": review["controls_sha256"],
            },
        )
    if fixture:
        dataset["title"] = "Fictional fixture — " + dataset["title"]
        for source in dataset["sources"]:
            source.update(
                title="Fictional fixture: " + source["id"],
                locator=None,
                note="Invented test inputs; no real source observations.",
            )
        manifest["dataset_kind"] = "SYNTHETIC_FIXTURE"
        manifest["construction"] = "Fictional inputs used to exercise the reconstruction method."
        manifest["reason"] = "Invented test data; this is not historical research."
    manifest["input_kind"] = capture["input_kind"]
    manifest["dataset_sha256"] = _sha(donor_canonical_json(dataset).encode())
    data_bytes, manifest_bytes = _bytes(dataset), _bytes(manifest)
    parse_dataset_bytes(data_bytes)
    producer = {
        "repository": "https://github.com/thatquantguy44/market-medallion",
        "commit": None,
        "dirty": None,
        "sourceTreeSha256": _tree_digest(),
        "sourceTreeScope": "named presidential_research modules and shared/files.py",
        "methodVersion": "presidential-donor/0.1.0",
    }
    inputs = {
        "captureId": capture_id,
        "rawSha256": capture["sha256"],
        "configSha256": capture["config_sha256"],
        "captureFirstSeenAt": capture["first_seen_at"],
        "reviewId": review_id,
        "reviewWorkbookSha256": review["workbook_sha256"],
        "reviewControlsSha256": review["controls_sha256"],
        "reviewAdmittedAt": review["admitted_at"],
    }
    entry = {
        "selectionKey": "crsp-market-proxy-annual",
        "datasetId": dataset["id"],
        "title": dataset["title"],
        "construction": manifest["construction"],
        "sampleId": f"{config.annual_start_year}-{config.annual_end_year}",
        "firstYear": config.annual_start_year,
        "lastYear": config.annual_end_year,
        "windowCount": len(dataset["windows"]),
        "path": f"datasets/{_DATA}",
        "manifestPath": f"datasets/{_MANIFEST}",
        "sha256": _sha(data_bytes),
        "bytes": len(data_bytes),
        "canonicalSha256": canonical_sha256(dataset),
        "manifestSha256": _sha(manifest_bytes),
        "manifestBytes": len(manifest_bytes),
    }
    semantic = {
        "profileId": profile_id,
        "decisionPolicy": "window_start",
        "producer": producer,
        "inputInventory": inputs,
        "inputKind": capture["input_kind"],
        "datasets": [entry],
        "artifacts": [],
        "sources": dataset["sources"],
        "quality": {"status": "PASSED", "requiredDatasets": 1, "publishedDatasets": 1},
        "limitations": [
            "One local baseline only; no independent source authenticity "
            "or full replay certification.",
            "Fictional fixture; no historical observations."
            if fixture
            else "Local capture provenance is declared and has not been independently verified.",
        ],
    }
    snapshot_id = canonical_sha256(semantic)
    catalog = {
        "schemaVersion": "presidential-research-bundle/1",
        "snapshotId": snapshot_id,
        "createdAt": utc_timestamp(created_at),
        **semantic,
        "semanticInventory": semantic,
    }
    root = Path(root)
    with writer_lock(root):
        gold = _safe_child(root, "gold")
        gold.mkdir(mode=0o700, exist_ok=True)
        snapshots = _safe_child(gold, "snapshots")
        snapshots.mkdir(mode=0o700, exist_ok=True)
        audit = _safe_child(root, "audit")
        audit.mkdir(mode=0o700, exist_ok=True)
        journal = _safe_child(audit, "publications")
        journal.mkdir(mode=0o700, exist_ok=True)
        final = _generation(root, snapshot_id)
        staging = Path(tempfile.mkdtemp(prefix=".stage-", dir=snapshots))
        try:
            (staging / "datasets").mkdir(mode=0o700)
            _write(staging / "datasets" / _DATA, data_bytes)
            _write(staging / "datasets" / _MANIFEST, manifest_bytes)
            _write(staging / "catalog.json", _bytes(catalog))
            _sync_dir(staging / "datasets")
            _sync_dir(staging)
            if fault:
                fault("after_stage")
            journal_path = _safe_child(journal, snapshot_id + ".json")
            atomic_json(journal_path, {"snapshotId": snapshot_id, "status": "PENDING"})
            if fault:
                fault("after_journal")
            if final.exists():
                existing = _read_generation(root, snapshot_id)
                catalog_sha = existing["catalogSha256"]
            else:
                os.rename(staging, final)
                _sync_dir(snapshots)
                catalog_sha = _read_generation(root, snapshot_id)["catalogSha256"]
            if fault:
                fault("after_rename")
            atomic_json(
                _safe_child(gold, "current.json"),
                {
                    "schemaVersion": "presidential-research-pointer/1",
                    "snapshotId": snapshot_id,
                    "catalogPath": f"gold/snapshots/{snapshot_id}/catalog.json",
                    "catalogSha256": catalog_sha,
                },
            )
            if fault:
                fault("after_pointer")
            atomic_json(journal_path, {"snapshotId": snapshot_id, "status": "PUBLISHED"})
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    return {"snapshotId": snapshot_id, "catalogSha256": catalog_sha}


def recover_publications(root):
    """Complete only a verified pointer's interrupted journal transition."""
    root = Path(root)
    with writer_lock(root):
        try:
            current = read_current(root)
        except ValueError:
            return
        snapshot_id = current["catalog"]["snapshotId"]
        journal = _safe_child(
            _safe_child(_safe_child(root, "audit"), "publications"), snapshot_id + ".json"
        )
        if journal.exists():
            state = json.loads(_read_bounded(journal, 65536))
            if state == {"snapshotId": snapshot_id, "status": "PENDING"}:
                atomic_json(journal, {"snapshotId": snapshot_id, "status": "PUBLISHED"})
