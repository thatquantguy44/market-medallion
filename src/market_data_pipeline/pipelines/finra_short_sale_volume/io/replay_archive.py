"""SSV-AC11: bounded verified Bronze inventory and byte-preserving replay copy."""

import re
from pathlib import Path

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import (
    digest,
    encoded,
    settings,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

MAX_PROFILES = 16


def _paths(directory, maximum):
    result = []
    for path in directory.iterdir():
        result.append(path)
        if len(result) > maximum:
            raise ValueError("FINRA replay archive inventory budget exceeded")
        if path.is_symlink():
            raise ValueError("FINRA replay archive entry is unsafe")
    return result


def _profile(value, max_bytes, max_rows):
    try:
        expected = settings(value["max_bytes"], value["max_rows"])
        if value != expected or value["max_bytes"] > max_bytes or value["max_rows"] > max_rows:
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError("FINRA replay parser profile is unsupported or exceeds budget") from None
    return value


def inventory(
    root, manifest, *, approved_use, operation, max_captures, max_bytes, max_rows, max_total_bytes
):
    base = bronze._base(Path(root))
    captures = _paths(base / "captures", max_captures)
    outcomes = _paths(base / "outcomes", max_captures)
    slots = _paths(base / "sequences", bronze.MAX_RESERVATIONS)
    if not captures:
        raise ValueError("FINRA replay requires at least one terminal capture")
    files = [base / "format.json"]
    for directory in captures:
        bronze._identity(directory.name)
        if not directory.is_dir() or {p.name for p in _paths(directory, 2)} != {
            "payload.bin",
            "receipt.json",
        }:
            raise ValueError("FINRA replay capture inventory is incomplete")
        files.extend([directory / "payload.bin", directory / "receipt.json"])
    outcome_files = {}
    for directory in outcomes:
        bronze._identity(directory.name)
        if not directory.is_dir():
            raise ValueError("FINRA replay outcome inventory is unsafe")
        entries = _paths(directory, MAX_PROFILES)
        if not entries or any(not re.fullmatch(r"[0-9a-f]{64}\.json", p.name) for p in entries):
            raise ValueError("FINRA replay outcome inventory is invalid")
        outcome_files[directory.name] = entries
        files.extend(entries)
    for slot in slots:
        if not re.fullmatch(r"[0-9]{20}\.json", slot.name):
            raise ValueError("FINRA replay sequence inventory is invalid")
        files.append(slot)
    size = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise ValueError("FINRA replay archive file is missing or unsafe")
        size += path.stat().st_size
        if size > max_total_bytes:
            raise ValueError("FINRA replay total byte budget exceeded")
        bound = max_bytes if path.name == "payload.bin" else bronze.MAX_RECEIPT_BYTES
        if path.stat().st_size > bound:
            raise ValueError("FINRA replay input byte budget exceeded")
    reservations = {}
    for slot in slots:
        number = int(slot.stem)
        value = bronze._json(slot)
        if (
            type(value.get("capture_sequence")) is not int
            or not 1 <= number < 2**63
            or value
            != dict(
                schema_id=bronze.CONTRACT,
                capture_sequence=number,
                capture_id=bronze._identity(value.get("capture_id")),
            )
        ):
            raise ValueError("FINRA replay sequence identity mismatch")
        reservations[number] = value
    selected = []
    for directory in captures:
        saved, _ = bronze.read_capture(
            root,
            directory.name,
            manifest,
            approved_use=approved_use,
            operation=operation,
            max_bytes=max_bytes,
        )
        entries = outcome_files.get(directory.name)
        if entries is None:
            raise ValueError("FINRA replay capture has no original parse outcome")
        profiles = {}
        for path in entries:
            outcome = bronze._json(path)
            body = dict(outcome)
            checksum = body.pop("outcome_sha256", None)
            profile = _profile(body.get("profile"), max_bytes, max_rows)
            if (
                checksum != digest(body)
                or body.get("schema_id") != "finra-ssv-parse-outcome/1"
                or body.get("profile_sha256") != digest(profile)
                or path.stem != digest(profile)
                or any(
                    body.get(k) != saved[k]
                    for k in ("capture_id", "run_id", "receipt_sha256", "payload_sha256")
                )
            ):
                raise ValueError("FINRA replay parse outcome identity mismatch")
            profiles[path.stem] = outcome
        selected.append(dict(receipt=saved, outcomes=profiles))
    if set(outcome_files) != {r["receipt"]["capture_id"] for r in selected}:
        raise ValueError("FINRA replay has orphan parse outcomes")
    selected.sort(key=lambda r: r["receipt"]["capture_sequence"])
    last = None
    for entry in selected:
        saved = entry["receipt"]
        point = (saved["capture_sequence"], saved["archived_at"])
        if last is not None and (point[0] <= last[0] or point[1] < last[1]):
            raise ValueError("FINRA replay capture ordering is ambiguous or regressed")
        last = point
    return dict(captures=selected, reservations=reservations, bytes=size)


def copy(source, destination, manifest, inventory, *, approved_use, operation, max_bytes):
    """Copy original receipts/slots verbatim; never call the capture allocator."""
    base = bronze._base(destination, create=True)
    for number, value in inventory["reservations"].items():
        bronze._put(base / "sequences" / f"{number:020d}.json", encoded(value))
    for entry in inventory["captures"]:
        saved = entry["receipt"]
        capture_id = saved["capture_id"]
        original = Path(source) / "bronze/captures" / capture_id
        bronze._publish_directory(
            base / "captures",
            capture_id,
            {
                "receipt.json": bronze._read(original / "receipt.json", bronze.MAX_RECEIPT_BYTES),
                "payload.bin": bronze._read(original / "payload.bin", max_bytes),
            },
        )
        verified, _ = bronze.read_capture(
            destination,
            capture_id,
            manifest,
            approved_use=approved_use,
            operation=operation,
            max_bytes=max_bytes,
        )
        if verified != saved:
            raise ValueError("FINRA replay copied capture differs from its original")
        bronze._publish_directory(
            base / "outcomes",
            capture_id,
            {key + ".json": encoded(value) for key, value in entry["outcomes"].items()},
        )
