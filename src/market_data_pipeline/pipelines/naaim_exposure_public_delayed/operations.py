"""Read-only operational checks for the delayed NAAIM research slice."""

import json
import re
import shutil
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from market_data_pipeline.pipelines.naaim_exposure_public_delayed.data.html import Exposure
from market_data_pipeline.pipelines.naaim_exposure_public_delayed.pipeline import (
    ATTRIBUTION,
    DELAY_METADATA,
    _append_versions,
)
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock


def _read_json(path: Path):
    return json.loads(path.read_text())


def _capture_events(root: Path) -> list[dict]:
    receipt_paths = sorted((root / "captures").glob("*.json"))
    if not receipt_paths:
        receipt_paths = sorted((root / "bronze").glob("*/receipt.json"))
    events = []
    for receipt_path in receipt_paths:
        try:
            receipt = _read_json(receipt_path)
            capture_id = receipt["capture_id"]
            if not isinstance(capture_id, str) or not re.fullmatch(r"[0-9a-f]{64}", capture_id):
                raise ValueError("invalid capture_id")
            captured_at = datetime.fromisoformat(receipt["retrieved_at"].replace("Z", "+00:00"))
            if captured_at.tzinfo is None or captured_at.utcoffset() is None:
                raise ValueError("retrieved_at must include a timezone")
            run_id = receipt.get("run_id", receipt_path.stem)
            if not isinstance(run_id, str) or not re.fullmatch(r"[0-9a-f]{32}|receipt", run_id):
                raise ValueError("invalid run_id")
            events.append(
                {
                    "capture_id": capture_id,
                    "captured_at": captured_at.astimezone(UTC),
                    "path": receipt_path,
                    "run_id": run_id,
                }
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid NAAIM capture receipt: {receipt_path}") from exc
    return events


def _snapshot_observations(path: Path, capture_id: str) -> list[Exposure]:
    try:
        snapshot = _read_json(path)
        if snapshot["capture_id"] != capture_id or not isinstance(snapshot["observations"], list):
            raise ValueError("invalid snapshot")
        return [
            Exposure(
                as_of_date=row["as_of_date"],
                naaim_number=Decimal(row["naaim_number"]),
                bearish=Decimal(row["bearish"]),
                quarter_1=Decimal(row["quarter_1"]),
                quarter_2=Decimal(row["quarter_2"]),
                quarter_3=Decimal(row["quarter_3"]),
                bullish=Decimal(row["bullish"]),
                deviation=Decimal(row["deviation"]),
            )
            for row in snapshot["observations"]
        ]
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid NAAIM Silver snapshot: {path}") from exc


def retain_captures(root: Path, *, now=None, retention_days=365, apply=False) -> dict:
    """Dry-run-first retention of NAAIM provider data by capture time.

    Active capture events are replayed from their immutable Silver snapshots so
    the retained version history and delayed latest output cannot reference an
    expired provider payload.
    """
    now = now or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("retention cutoff requires a timezone-aware timestamp")
    if type(retention_days) is not int or retention_days < 1:
        raise ValueError("retention_days must be positive")
    if type(apply) is not bool:
        raise ValueError("apply must be boolean")

    root = Path(root)
    cutoff = now.astimezone(UTC).timestamp() - retention_days * 86400
    with writer_lock(root):
        events = _capture_events(root)
        active = [event for event in events if event["captured_at"].timestamp() >= cutoff]
        expired = [event for event in events if event not in active]
        active.sort(key=lambda event: (event["captured_at"], event["run_id"]))
        active_capture_ids = {event["capture_id"] for event in active}
        expired_capture_ids = sorted(
            {
                event["capture_id"]
                for event in expired
                if event["capture_id"] not in active_capture_ids
            }
        )

        versions = []
        for event in active:
            observations = _snapshot_observations(
                root / "silver" / f"{event['capture_id']}.json", event["capture_id"]
            )
            _append_versions(
                versions,
                observations,
                event["capture_id"],
                utc_timestamp(event["captured_at"]),
            )

        if apply:
            for event in expired:
                event["path"].unlink()
            for capture_id in expired_capture_ids:
                bronze = root / "bronze" / capture_id
                snapshot = root / "silver" / f"{capture_id}.json"
                if bronze.exists():
                    shutil.rmtree(bronze)
                snapshot.unlink(missing_ok=True)
            atomic_json(root / "silver" / "versions.json", versions)
            latest_path = root / "gold" / "latest.json"
            if versions:
                latest = max(versions, key=lambda version: version["as_of_date"])
                atomic_json(
                    latest_path,
                    {
                        "as_of_date": latest["as_of_date"],
                        "attribution": ATTRIBUTION,
                        **DELAY_METADATA,
                        "metric": "naaim_number",
                        "value": latest["values"]["naaim_number"],
                    },
                )
            else:
                latest_path.unlink(missing_ok=True)
            log = root / "operations" / "retention"
            log.mkdir(parents=True, exist_ok=True)
            atomic_json(
                log / f"{uuid.uuid4().hex}.json",
                {
                    "at": utc_timestamp(now),
                    "expired_capture_ids": expired_capture_ids,
                    "expired_run_ids": [event["run_id"] for event in expired],
                    "retention_days": retention_days,
                },
            )
    return {
        "applied": apply,
        "remove_capture_ids": expired_capture_ids,
        "remove_run_ids": [event["run_id"] for event in expired],
        "retention_days": retention_days,
    }


def health_report(root: Path, *, now=None, max_capture_age_days=14) -> dict:
    """Report local capture health without making a provider request.

    The underlying display is intentionally delayed, so health uses the newest
    successful local capture time rather than the latest provider observation.
    """
    now = now or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("health cutoff requires a timezone-aware timestamp")
    if type(max_capture_age_days) is not int or max_capture_age_days < 1:
        raise ValueError("max_capture_age_days must be positive")

    root = Path(root)
    reasons = []
    captures = []
    try:
        events = _capture_events(root)
    except ValueError:
        events = []
        reasons.append("invalid_capture_receipt")
    for event in events:
        try:
            captures.append(event["captured_at"])
        except KeyError:
            reasons.append("invalid_capture_receipt")

    latest_capture = max(captures, default=None)
    if latest_capture is None:
        reasons.append("no_captures")
        capture_age = None
    else:
        capture_age = (now.astimezone(UTC) - latest_capture).total_seconds() / 86400
        if capture_age > max_capture_age_days:
            reasons.append("capture_stale")

    if latest_capture is None:
        version_count = 0
    else:
        versions_path = root / "silver" / "versions.json"
        try:
            versions = _read_json(versions_path)
            if not isinstance(versions, list):
                raise ValueError("versions must be a list")
            version_count = len(versions)
        except (OSError, ValueError, json.JSONDecodeError):
            version_count = 0
            reasons.append("silver_versions_unavailable")

    latest_as_of_date = None
    try:
        published = _read_json(root / "gold" / "latest.json")
        if (
            published["attribution"] != ATTRIBUTION
            or published["delayed_public_display"] is not True
            or not isinstance(published["as_of_date"], str)
        ):
            raise ValueError("invalid delayed research output")
        latest_as_of_date = published["as_of_date"]
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        reasons.append("publication_unavailable")

    return {
        **DELAY_METADATA,
        "healthy": not reasons,
        "reasons": reasons,
        "last_capture_at": latest_capture.isoformat().replace("+00:00", "Z")
        if latest_capture
        else None,
        "capture_age_days": capture_age,
        "latest_as_of_date": latest_as_of_date,
        "silver_versions": version_count,
    }
