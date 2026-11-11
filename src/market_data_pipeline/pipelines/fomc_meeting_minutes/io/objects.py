"""T03: immutable content-addressed originals and explicit legacy-file relinking."""

import hashlib
import os
import uuid
from pathlib import Path

LOCAL_CONTRACT = b'{"schema_id":"fomc-local-objects/1"}'


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def make_directory(path):
    """Create private directories and persist each new parent link."""
    path = Path(path)
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o700, exist_ok=True)
        _sync_directory(directory.parent)


def store_payload(root, payload):
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import _write

    directory = Path(root) / "objects" / "sha256"
    make_directory(directory)
    marker = directory.parent / "format.json"
    try:
        _write(marker, LOCAL_CONTRACT)
    except FileExistsError:
        if marker.read_bytes() != LOCAL_CONTRACT:
            raise ValueError("FOMC local object-store identity mismatch") from None
    digest = hashlib.sha256(payload).hexdigest()
    target = directory / (digest + ".bin")
    temporary = directory / (".staged-" + uuid.uuid4().hex)
    try:
        _write(temporary, payload)
        try:
            os.link(temporary, target)
            _sync_directory(directory)
        except FileExistsError:
            if target.read_bytes() != payload:
                raise ValueError(
                    "FOMC content-addressed object hash collision or corruption"
                ) from None
    finally:
        temporary.unlink(missing_ok=True)
    return target


def migrate_local_objects(root, *, max_captures=10000):
    """Explicitly replace independent legacy payloads with byte-identical hard links.

    Receipts, capture IDs, clocks, URLs and commit markers are not rewritten.
    Each replacement is atomic; an interrupted migration is safe to retry.
    """
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import read_capture

    if type(max_captures) is not int or max_captures < 1:
        raise ValueError("FOMC local migration requires a positive capture bound")
    paths = sorted((Path(root) / "captures").iterdir())
    if len(paths) > max_captures:
        raise ValueError("FOMC local migration exceeds its capture bound")
    # Verify the whole bounded batch before changing any legacy payload path.
    for directory in paths:
        read_capture(root, directory.name)
    digests = set()
    for directory in paths:
        receipt, payload = read_capture(root, directory.name)
        target = store_payload(root, payload)
        temporary = directory / (".relink-" + uuid.uuid4().hex)
        try:
            os.link(target, temporary)
            os.replace(temporary, directory / "payload.bin")
            _sync_directory(directory)
        finally:
            temporary.unlink(missing_ok=True)
        digests.add(receipt["sha256"])
    return {
        "schema_id": "fomc-local-object-migration/1",
        "captures_verified": len(paths),
        "unique_payloads": len(digests),
    }
