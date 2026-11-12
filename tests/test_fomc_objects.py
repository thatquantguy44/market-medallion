"""T03: content-addressed originals retain all capture receipts and URL lineage."""

import os
from pathlib import Path

import pytest

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import capture, read_capture
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import migrate_local_objects
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import load_manifest


class Store:
    def save_capture(self, receipt, payload):
        pass


def make(root, url, payload=b"same original"):
    from datetime import UTC, datetime

    manifest = load_manifest(Path("manifests/fomc-meeting-minutes.yml"))
    return capture(
        root,
        payload,
        manifest=manifest,
        store=Store(),
        source_url=url,
        kind="document",
        captured_at=datetime.now(UTC),
        status=200,
        headers={},
        max_bytes=100,
    )


def test_identical_urls_and_bundled_bytes_share_one_object_but_keep_receipts(tmp_path):
    base = "https://www.federalreserve.gov/monetarypolicy/"
    captures = [make(tmp_path, base + name) for name in ("one.pdf", "two.pdf", "one.pdf")]
    paths = [tmp_path / "captures" / r["capture_id"] / "payload.bin" for r in captures]
    assert len({p.stat().st_ino for p in paths}) == 1
    assert len(list((tmp_path / "objects" / "sha256").glob("*.bin"))) == 1
    assert len({r["capture_id"] for r in captures}) == 3
    assert all(read_capture(tmp_path, r["capture_id"])[0] == r for r in captures)


def test_explicit_local_migration_relinks_legacy_bytes_without_changing_evidence(tmp_path):
    import shutil

    receipt = make(tmp_path, "https://www.federalreserve.gov/monetarypolicy/one.pdf")
    path = tmp_path / "captures" / receipt["capture_id"] / "payload.bin"
    # Recreate the earlier independent-file layout without altering its receipt/marker.
    independent = path.with_suffix(".independent")
    shutil.copyfile(path, independent)
    os.replace(independent, path)
    before = read_capture(tmp_path, receipt["capture_id"])
    result = migrate_local_objects(tmp_path)
    assert result["captures_verified"] == 1
    assert read_capture(tmp_path, receipt["capture_id"]) == before
    obj = tmp_path / "objects" / "sha256" / (receipt["sha256"] + ".bin")
    assert path.stat().st_ino == obj.stat().st_ino
    assert migrate_local_objects(tmp_path) == result


def test_corrupt_existing_object_is_not_overwritten(tmp_path):
    receipt = make(tmp_path, "https://www.federalreserve.gov/monetarypolicy/one.pdf")
    obj = tmp_path / "objects" / "sha256" / (receipt["sha256"] + ".bin")
    obj.write_bytes(b"corruption")
    with pytest.raises(ValueError, match="object"):
        make(tmp_path, "https://www.federalreserve.gov/monetarypolicy/two.pdf")
