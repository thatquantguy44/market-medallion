"""PR016-R13: bounded, verified backup/restore of selected local state."""

from datetime import timedelta
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import test_presidential_bundle_assembly as fixtures

from market_data_pipeline.pipelines.presidential_research.backup import (
    backup_selected_snapshot,
    restore_backup,
)
from market_data_pipeline.pipelines.presidential_research.publication import (
    publish_full_bundle,
    read_current,
)
from market_data_pipeline.pipelines.presidential_research.warehouse import observed_as_of

NOW = fixtures.NOW


def prepared(tmp_path, monkeypatch):
    root, ids, review = fixtures.fixture(tmp_path, monkeypatch)
    published = publish_full_bundle(
        root, ids, review["review_id"], profile_id="fixture-complete", created_at=NOW
    )
    return root, published


def test_backup_restores_verified_state_without_unrelated_files(tmp_path, monkeypatch):
    root, published = prepared(tmp_path, monkeypatch)
    (root / ".env").write_text("do not include")
    archive = tmp_path / "research.zip"
    backup_selected_snapshot(root, archive, backed_up_at=NOW + timedelta(hours=1))
    with ZipFile(archive) as backup:
        assert not any(
            ".env" in name or name.startswith("/") or ".." in name for name in backup.namelist()
        )
    destination = tmp_path / "restored"
    restored = restore_backup(archive, destination, restored_at=NOW + timedelta(hours=2))
    assert restored["snapshotId"] == published["snapshotId"]
    assert read_current(destination)["catalogSha256"] == read_current(root)["catalogSha256"]
    assert observed_as_of(destination, NOW) == observed_as_of(root, NOW)
    assert not (destination / ".env").exists()
    with pytest.raises(FileExistsError):
        backup_selected_snapshot(root, archive, backed_up_at=NOW + timedelta(hours=1))


def test_backup_corruption_traversal_and_existing_destinations_fail_closed(tmp_path, monkeypatch):
    root, _ = prepared(tmp_path, monkeypatch)
    archive = tmp_path / "research.zip"
    backup_selected_snapshot(root, archive, backed_up_at=NOW + timedelta(hours=1))
    with ZipFile(archive) as backup:
        entries = {name: backup.read(name) for name in backup.namelist()}
    payload = next(name for name in entries if name.endswith("/payload"))
    entries[payload] += b"changed"
    corrupted = tmp_path / "corrupted.zip"
    with ZipFile(corrupted, "w", ZIP_DEFLATED) as output:
        for name, data in entries.items():
            output.writestr(name, data)
    with pytest.raises(ValueError, match="digest"):
        restore_backup(corrupted, tmp_path / "bad", restored_at=NOW + timedelta(hours=2))
    assert not (tmp_path / "bad").exists()
    malicious = tmp_path / "traversal.zip"
    with ZipFile(malicious, "w") as output:
        output.writestr("../escape", b"bad")
        output.writestr("backup-manifest.json", b"{}")
    with pytest.raises(ValueError):
        restore_backup(malicious, tmp_path / "malicious", restored_at=NOW + timedelta(hours=2))
    assert not (tmp_path / "escape").exists()
    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "keep").write_text("keep")
    with pytest.raises(ValueError, match="destination"):
        restore_backup(archive, existing, restored_at=NOW + timedelta(hours=2))
    assert (existing / "keep").read_text() == "keep"


def test_duplicate_archive_entries_and_early_restore_are_rejected(tmp_path, monkeypatch):
    root, _ = prepared(tmp_path, monkeypatch)
    archive = tmp_path / "research.zip"
    backup_selected_snapshot(root, archive, backed_up_at=NOW + timedelta(hours=1))
    with pytest.raises(ValueError, match="time"):
        restore_backup(archive, tmp_path / "early", restored_at=NOW)
    with ZipFile(archive) as source:
        data = [(name, source.read(name)) for name in source.namelist()]
    duplicate = tmp_path / "duplicate.zip"
    with ZipFile(duplicate, "w", ZIP_DEFLATED) as output:
        for name, payload in data:
            output.writestr(name, payload)
        with pytest.warns(UserWarning):
            output.writestr(data[0][0], data[0][1])
    with pytest.raises(ValueError, match="duplicate"):
        restore_backup(duplicate, tmp_path / "dup", restored_at=NOW + timedelta(hours=2))


def test_untrusted_manifest_types_and_size_limits_fail_cleanly(tmp_path, monkeypatch):
    import json

    from market_data_pipeline.pipelines.presidential_research import backup as backup_module

    root, _ = prepared(tmp_path, monkeypatch)
    archive = tmp_path / "research.zip"
    backup_selected_snapshot(root, archive, backed_up_at=NOW + timedelta(hours=1))
    with ZipFile(archive) as original:
        entries = {name: original.read(name) for name in original.namelist()}
    manifest = json.loads(entries["backup-manifest.json"])
    manifest["files"][0]["path"] = {"bad": "path"}
    entries["backup-manifest.json"] = json.dumps(manifest).encode()
    malformed = tmp_path / "malformed.zip"
    with ZipFile(malformed, "w", ZIP_DEFLATED) as output:
        for name, payload in entries.items():
            output.writestr(name, payload)
    with pytest.raises(ValueError, match="inventory"):
        restore_backup(malformed, tmp_path / "invalid", restored_at=NOW + timedelta(hours=2))
    monkeypatch.setattr(backup_module, "_MAX_TOTAL", 1)
    with pytest.raises(ValueError, match="size"):
        restore_backup(archive, tmp_path / "oversized", restored_at=NOW + timedelta(hours=2))
    assert not (tmp_path / "invalid").exists()
    assert not (tmp_path / "oversized").exists()


def test_content_pinned_evidence_survives_replay_backup_and_restore(tmp_path, monkeypatch):
    import hashlib

    from market_data_pipeline.pipelines.presidential_research.frozen import (
        evidence_inventory,
        import_manual_evidence,
    )
    from market_data_pipeline.pipelines.presidential_research.replay import replay_selected_snapshot

    root, _ = prepared(tmp_path, monkeypatch)
    source = tmp_path / "review-evidence"
    source.write_bytes(b"fictional review evidence")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    import_manual_evidence(root, source, digest)
    replay = tmp_path / "replayed"
    replay_selected_snapshot(root, replay, replayed_at=NOW + timedelta(hours=1))
    assert evidence_inventory(replay)[digest] == source.read_bytes()
    archive = tmp_path / "evidence.zip"
    backup_selected_snapshot(root, archive, backed_up_at=NOW + timedelta(hours=1))
    restored = tmp_path / "restored-evidence"
    restore_backup(archive, restored, restored_at=NOW + timedelta(hours=2))
    assert evidence_inventory(restored)[digest] == source.read_bytes()
    (root / "manual-evidence" / digest).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="digest"):
        backup_selected_snapshot(
            root, tmp_path / "tampered.zip", backed_up_at=NOW + timedelta(hours=1)
        )
