"""Local Bronze, Silver and research-only Gold flow for permitted NAAIM captures."""

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.naaim_exposure_public_delayed.data.html import (
    Exposure,
    parse_table,
)
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.source import (
    TABLE_URL,
    fetch_table,
)
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

ATTRIBUTION = "Source: NAAIM Exposure Index® (National Association of Active Investment Managers)."
DELAY_METADATA = {
    "display_name": "NAAIM Exposure Index — 3-month-delayed public data",
    "delayed_public_display": True,
    "provider_delay_months": 3,
    "delay_notice": (
        "NAAIM delays this public feed by three months. The observation date is as_of_date; "
        "a recent capture does not mean current-week data. Internal research only."
    ),
}


def _atomic_bytes(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _serialize(observation: Exposure) -> dict[str, str]:
    return {
        "as_of_date": observation.as_of_date,
        "naaim_number": str(observation.naaim_number),
        "bearish": str(observation.bearish),
        "quarter_1": str(observation.quarter_1),
        "quarter_2": str(observation.quarter_2),
        "quarter_3": str(observation.quarter_3),
        "bullish": str(observation.bullish),
        "deviation": str(observation.deviation),
    }


def _versions(path: Path) -> list[dict]:
    if not path.exists():
        return []
    value = json.loads(path.read_text())
    if not isinstance(value, list):
        raise ValueError("NAAIM Silver versions must be a list")
    return value


def _append_versions(
    versions: list[dict], observations: list[Exposure], capture_id: str, captured_at: str
) -> int:
    inserted = 0
    for observation in observations:
        values = _serialize(observation)
        previous = next(
            (
                version
                for version in reversed(versions)
                if version["as_of_date"] == observation.as_of_date
            ),
            None,
        )
        if previous is not None and previous["values"] == values:
            continue
        versions.append(
            {
                "as_of_date": observation.as_of_date,
                "capture_id": capture_id,
                "first_seen_at": captured_at,
                "values": values,
            }
        )
        inserted += 1
    return inserted


def ingest(
    payload: bytes,
    root: Path,
    *,
    permission_reference: str,
    captured_at: str,
) -> dict[str, str | int]:
    """Archive a permitted capture and publish a local, delayed research snapshot."""
    if not isinstance(permission_reference, str) or not permission_reference.strip():
        raise ValueError("permission_reference is required for a NAAIM capture")
    try:
        captured = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError("captured_at must be an ISO-8601 timestamp") from exc
    captured_utc = utc_timestamp(captured)
    observations = parse_table(payload)
    capture_id = hashlib.sha256(payload).hexdigest()
    run_id = uuid.uuid4().hex
    root = Path(root)
    with writer_lock(root):
        bronze = root / "bronze" / capture_id
        bronze.mkdir(parents=True, exist_ok=True)
        raw_path = bronze / "payload.html"
        if raw_path.exists():
            if raw_path.read_bytes() != payload:
                raise ValueError("capture checksum collision")
        else:
            _atomic_bytes(raw_path, payload)
        receipt = bronze / "receipt.json"
        if not receipt.exists():
            atomic_json(
                receipt,
                {
                    "capture_id": capture_id,
                    "dataset": "naaim-exposure-index-public-delayed",
                    "delayed_public_display": True,
                    "permission_reference": permission_reference,
                    "retrieved_at": captured_utc,
                    "sha256": capture_id,
                    "source_url": TABLE_URL,
                },
            )
        captures = root / "captures"
        captures.mkdir(parents=True, exist_ok=True)
        atomic_json(
            captures / f"{run_id}.json",
            {
                "capture_id": capture_id,
                "dataset": "naaim-exposure-index-public-delayed",
                "delayed_public_display": True,
                "permission_reference": permission_reference,
                "retrieved_at": captured_utc,
                "run_id": run_id,
                "source_url": TABLE_URL,
            },
        )
        silver = root / "silver"
        silver.mkdir(parents=True, exist_ok=True)
        snapshot = {
            "capture_id": capture_id,
            "observations": [_serialize(row) for row in observations],
        }
        atomic_json(silver / f"{capture_id}.json", snapshot)
        versions_path = silver / "versions.json"
        versions = _versions(versions_path)
        inserted_versions = _append_versions(versions, observations, capture_id, captured_utc)
        atomic_json(versions_path, versions)
        latest = max(observations, key=lambda row: row.as_of_date)
        gold = root / "gold"
        gold.mkdir(parents=True, exist_ok=True)
        atomic_json(
            gold / "latest.json",
            {
                "as_of_date": latest.as_of_date,
                "attribution": ATTRIBUTION,
                **DELAY_METADATA,
                "metric": "naaim_number",
                "value": str(latest.naaim_number),
            },
        )
    return {
        **DELAY_METADATA,
        "latest_as_of_date": latest.as_of_date,
        "capture_id": capture_id,
        "inserted_versions": inserted_versions,
        "row_count": len(observations),
        "run_id": run_id,
    }


def run_live(
    root: Path,
    *,
    permission_reference: str,
    transport=None,
    captured_at: str | None = None,
) -> dict[str, str | int]:
    """Perform one explicitly permission-gated table fetch and local ingestion."""
    response = fetch_table(permission_reference=permission_reference, transport=transport)
    return ingest(
        response.payload,
        root,
        permission_reference=permission_reference,
        captured_at=captured_at or utc_timestamp(datetime.now(UTC)),
    )
