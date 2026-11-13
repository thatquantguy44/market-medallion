"""Bounded, allowlisted local backup and verified offline restore."""

import hashlib
import json
import os
import re
import stat
import tempfile
from datetime import datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile

from market_data_pipeline.shared.files import utc_timestamp

from .publication import _ARTIFACTS, _DATASETS, _sync_dir, _write, read_current
from .replay import replay_selected_snapshot
from .storage import _read_bounded

_MAX_FILE = 128 * 1024 * 1024
_MAX_TOTAL = 512 * 1024 * 1024
_MAX_MANIFEST = 5 * 1024 * 1024
_MAX_FILES = 20_000
_ID = re.compile(r"^[0-9a-f]{32}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_SOURCE = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")


def _allowed(name):
    parts = name.split("/")
    if any(part in ("", ".", "..") for part in parts):
        return False
    if len(parts) == 2 and parts[0] == "manual-evidence":
        return bool(_HASH.fullmatch(parts[1]))
    if len(parts) == 4 and parts[0] == "bronze":
        return bool(
            _SOURCE.fullmatch(parts[1])
            and _ID.fullmatch(parts[2])
            and parts[3] in {"payload", "config.toml", "receipt.json", "parse.json"}
        )
    if len(parts) == 3 and parts[0] == "review":
        return bool(
            _ID.fullmatch(parts[1])
            and parts[2]
            in {
                "review.xlsx",
                "sources.toml",
                "controls.csv",
                "controls.manifest.json",
                "receipt.json",
            }
        )
    if name in {
        "gold/current.json",
        "silver/presidential.sqlite3",
        "audit/capture-sequence.json",
        "audit/replay-receipt.json",
    }:
        return True
    if len(parts) == 3 and parts[:2] == ["audit", "publications"]:
        return bool(parts[2].endswith(".json") and _HASH.fullmatch(parts[2][:-5]))
    if len(parts) >= 4 and parts[:2] == ["gold", "snapshots"] and _HASH.fullmatch(parts[2]):
        if len(parts) == 4:
            return parts[3] == "catalog.json"
        if len(parts) == 5 and parts[3] == "datasets":
            return parts[4] in {
                *_DATASETS,
                *(name.removesuffix(".json") + ".manifest.json" for name in _DATASETS),
            }
        if len(parts) == 5 and parts[3] == "evidence":
            return parts[4] in _ARTIFACTS
    return False


def _pairs(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("Duplicate backup manifest field.")
        output[key] = value
    return output


def backup_selected_snapshot(source_root, destination, *, backed_up_at: datetime):
    """Back up only replay-verified pipeline state; never include unrelated files."""
    target = Path(destination)
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    parent = target.parent.resolve(strict=True)
    target = parent / target.name
    created = utc_timestamp(backed_up_at)
    with tempfile.TemporaryDirectory(prefix="pres-backup-") as temporary:
        verified = Path(temporary) / "verified"
        result = replay_selected_snapshot(source_root, verified, replayed_at=backed_up_at)
        staging = None
        try:
            with tempfile.NamedTemporaryFile(
                prefix=".pres-backup-", dir=parent, delete=False
            ) as handle:
                staging = Path(handle.name)
                records, total = [], 0
                with ZipFile(handle, "w", ZIP_DEFLATED) as archive:
                    for file in sorted(verified.rglob("*")):
                        if file.is_symlink():
                            raise ValueError("Backup inventory contains a symlink.")
                        if not file.is_file() or file.name == ".writer.lock":
                            continue
                        name = file.relative_to(verified).as_posix()
                        if not _allowed(name):
                            raise ValueError("Backup contains an unsupported file.")
                        content = _read_bounded(file, _MAX_FILE)
                        total += len(content)
                        if len(records) >= _MAX_FILES or total > _MAX_TOTAL:
                            raise ValueError("Backup exceeds size limits.")
                        archive.writestr(name, content)
                        records.append(
                            {
                                "path": name,
                                "bytes": len(content),
                                "sha256": hashlib.sha256(content).hexdigest(),
                            }
                        )
                    manifest = {
                        "schemaVersion": "presidential-backup/1",
                        "createdAt": created,
                        "snapshotId": result["snapshotId"],
                        "catalogSha256": result["catalogSha256"],
                        "files": records,
                    }
                    manifest_bytes = json.dumps(
                        manifest, sort_keys=True, separators=(",", ":")
                    ).encode()
                    if len(manifest_bytes) > _MAX_MANIFEST:
                        raise ValueError("Backup manifest exceeds size limit.")
                    archive.writestr("backup-manifest.json", manifest_bytes)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(staging, target)
            _sync_dir(parent)
        finally:
            if staging is not None:
                staging.unlink(missing_ok=True)
    return {
        "snapshotId": result["snapshotId"],
        "catalogSha256": result["catalogSha256"],
        "path": str(target),
    }


def restore_backup(archive_path, destination, *, restored_at: datetime):
    """Verify every allowlisted byte, then rebuild into an absent destination."""
    archive_path, target = Path(archive_path), Path(destination)
    if target.exists() or target.is_symlink():
        raise ValueError("Restore destination must be absent.")
    if (
        archive_path.is_symlink()
        or not archive_path.is_file()
        or archive_path.stat().st_size > _MAX_TOTAL
    ):
        raise ValueError("Backup archive is unavailable or exceeds size limits.")
    restored = utc_timestamp(restored_at)
    with tempfile.TemporaryDirectory(prefix="pres-restore-") as temporary:
        source = Path(temporary) / "verified-input"
        source.mkdir(mode=0o700)
        try:
            with ZipFile(archive_path) as archive:
                infos = archive.infolist()
                names = [info.filename for info in infos]
                if len(names) != len(set(names)):
                    raise ValueError("Backup contains duplicate entries.")
                if len(names) > _MAX_FILES + 1 or "backup-manifest.json" not in names:
                    raise ValueError("Invalid backup inventory.")
                for info in infos:
                    mode = info.external_attr >> 16
                    if (
                        info.is_dir()
                        or stat.S_ISLNK(mode)
                        or (info.filename != "backup-manifest.json" and not _allowed(info.filename))
                    ):
                        raise ValueError("Backup contains an invalid path.")
                    if info.file_size > (
                        _MAX_MANIFEST if info.filename == "backup-manifest.json" else _MAX_FILE
                    ):
                        raise ValueError("Backup member exceeds size limit.")
                if sum(info.file_size for info in infos) > _MAX_TOTAL:
                    raise ValueError("Backup exceeds size limits.")
                manifest = json.loads(
                    archive.read("backup-manifest.json"), object_pairs_hook=_pairs
                )
                if (
                    not isinstance(manifest, dict)
                    or set(manifest)
                    != {"schemaVersion", "createdAt", "snapshotId", "catalogSha256", "files"}
                    or manifest["schemaVersion"] != "presidential-backup/1"
                    or not isinstance(manifest["files"], list)
                    or not isinstance(manifest["snapshotId"], str)
                    or not _HASH.fullmatch(manifest["snapshotId"])
                    or not isinstance(manifest["catalogSha256"], str)
                    or not _HASH.fullmatch(manifest["catalogSha256"])
                ):
                    raise ValueError("Invalid backup manifest.")
                try:
                    created = utc_timestamp(
                        datetime.fromisoformat(manifest["createdAt"].replace("Z", "+00:00"))
                    )
                    if created != manifest["createdAt"] or restored < created:
                        raise ValueError("Invalid backup time.")
                except (AttributeError, TypeError, ValueError) as exc:
                    raise ValueError("Invalid backup time.") from exc
                records = manifest["files"]
                paths = [item.get("path") for item in records if isinstance(item, dict)]
                if (
                    len(paths) != len(records)
                    or any(not isinstance(path, str) for path in paths)
                    or len(paths) != len(set(paths))
                    or set(names) != {*paths, "backup-manifest.json"}
                ):
                    raise ValueError("Backup inventory does not match manifest.")
                for record in records:
                    if (
                        set(record) != {"path", "bytes", "sha256"}
                        or not isinstance(record["path"], str)
                        or not _allowed(record["path"])
                        or type(record["bytes"]) is not int
                        or not 0 <= record["bytes"] <= _MAX_FILE
                        or not isinstance(record["sha256"], str)
                        or not _HASH.fullmatch(record["sha256"])
                    ):
                        raise ValueError("Invalid backup member.")
                    with archive.open(record["path"]) as stream:
                        data = stream.read(_MAX_FILE + 1)
                    if (
                        len(data) != record["bytes"]
                        or hashlib.sha256(data).hexdigest() != record["sha256"]
                    ):
                        raise ValueError("Backup member digest mismatch.")
                    target_file = source.joinpath(*record["path"].split("/"))
                    target_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    _write(target_file, data)
        except (BadZipFile, UnicodeError, json.JSONDecodeError, KeyError) as exc:
            raise ValueError("Invalid backup archive.") from exc
        current = read_current(source)
        if (
            current["catalog"]["snapshotId"] != manifest["snapshotId"]
            or current["catalogSha256"] != manifest["catalogSha256"]
        ):
            raise ValueError("Backup snapshot identity mismatch.")
        return replay_selected_snapshot(source, destination, replayed_at=restored_at)
