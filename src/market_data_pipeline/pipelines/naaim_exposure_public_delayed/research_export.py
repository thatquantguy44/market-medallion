"""Publish a narrow, attributed NAAIM research artifact for a local consumer.

This module never contacts NAAIM.  It reads only the pipeline-owned Silver and
Gold files produced by an already permitted capture, selects the current
observed version per as-of date, and writes an atomic exchange file.  The
artifact is deliberately not a warehouse, an API, or a claim of point-in-time
knowledge.
"""

import json
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from market_data_pipeline.pipelines.naaim_exposure_public_delayed.pipeline import (
    ATTRIBUTION,
    DELAY_METADATA,
)
from market_data_pipeline.shared.files import atomic_json, utc_timestamp, writer_lock

SCHEMA_VERSION = "naaim-exposure-research/1"
DATASET = "naaim-exposure-index-public-delayed"
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_CAPTURE_ID = re.compile(r"[0-9a-f]{64}\Z")


def _iso_utc(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must be a UTC timestamp")
    return utc_timestamp(parsed)


def _date(value: object) -> str:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        raise ValueError("Silver as_of_date must be an ISO calendar date")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Silver as_of_date must be an ISO calendar date") from exc
    return value


def _decimal_text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Silver naaim_number must be decimal text")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("Silver naaim_number must be decimal text") from exc
    if not number.is_finite():
        raise ValueError("Silver naaim_number must be finite")
    return value


def _current_versions(root: Path) -> list[dict[str, str]]:
    try:
        versions = json.loads((root / "silver" / "versions.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("NAAIM Silver versions are unavailable") from exc
    if not isinstance(versions, list) or not versions:
        raise ValueError("NAAIM Silver versions must be a non-empty list")

    current: dict[str, dict[str, str]] = {}
    for version in versions:
        if not isinstance(version, dict):
            raise ValueError("NAAIM Silver version must be an object")
        as_of_date = _date(version.get("as_of_date"))
        capture_id = version.get("capture_id")
        if not isinstance(capture_id, str) or not _CAPTURE_ID.fullmatch(capture_id):
            raise ValueError("NAAIM Silver capture_id is invalid")
        values = version.get("values")
        if not isinstance(values, dict):
            raise ValueError("NAAIM Silver values are invalid")
        # Last occurrence is the current observed version.  The source pipeline
        # appends corrections and retention replay rebuilds that same ordering.
        current[as_of_date] = {
            "as_of_date": as_of_date,
            "capture_id": capture_id,
            "first_seen_at": _iso_utc(version.get("first_seen_at"), field="first_seen_at"),
            "value": _decimal_text(values.get("naaim_number")),
        }
    return [current[key] for key in sorted(current)]


def _verify_gold(root: Path, observations: list[dict[str, str]]) -> None:
    try:
        gold = json.loads((root / "gold" / "latest.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("NAAIM Gold latest output is unavailable") from exc
    latest = observations[-1]
    if not isinstance(gold, dict) or gold.get("attribution") != ATTRIBUTION:
        raise ValueError("NAAIM Gold attribution is invalid")
    if any(gold.get(key) != value for key, value in DELAY_METADATA.items()):
        raise ValueError("NAAIM Gold delay labels are invalid")
    if (
        gold.get("metric") != "naaim_number"
        or gold.get("as_of_date") != latest["as_of_date"]
        or gold.get("value") != latest["value"]
    ):
        raise ValueError("NAAIM Gold does not match current Silver output")


def export_research_artifact(
    root: Path,
    destination: Path,
    *,
    research_use_reference: str,
    generated_at: str | None = None,
) -> dict[str, int | str]:
    """Write a current-vintage, delayed NAAIM artifact for an approved report.

    ``research_use_reference`` is a nonsecret owner/provider authorization
    reference.  It makes the distribution boundary explicit without storing
    credentials or copying any raw provider payload into the consumer artifact.
    """
    if not isinstance(research_use_reference, str) or not research_use_reference.strip():
        raise ValueError("research_use_reference is required for a NAAIM research export")
    if generated_at is None:
        generated_at = utc_timestamp(datetime.now(UTC))
    exported_at = _iso_utc(generated_at, field="generated_at")
    root = Path(root)
    destination = Path(destination)
    with writer_lock(root):
        observations = _current_versions(root)
        _verify_gold(root, observations)
        artifact = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": exported_at,
            "provenance": {
                "source": "NAAIM Exposure Index®",
                "dataset": DATASET,
                "attribution": ATTRIBUTION,
                "authorized_research_use_reference": research_use_reference.strip(),
                "knowledge_basis": "CURRENT_VINTAGE",
                **DELAY_METADATA,
            },
            "observations": observations,
        }
        destination.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(destination, artifact)
    return {
        "schema_version": SCHEMA_VERSION,
        "destination": str(destination),
        "observation_count": len(observations),
        "latest_as_of_date": observations[-1]["as_of_date"],
    }
