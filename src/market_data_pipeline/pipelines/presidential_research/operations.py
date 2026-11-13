"""Offline operational status for the local presidential research store."""

import csv
import fcntl
import io
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from market_data_pipeline.shared.files import utc_timestamp

from .publication import read_current
from .review import load_review_version
from .storage import _safe_child, load_capture
from .warehouse import _database

_SOURCE = re.compile(r"^[A-Z][A-Z0-9_]{0,79}$")
_ID = re.compile(r"^[0-9a-f]{32}$")


def research_health(root, *, now: datetime, stale_after: timedelta, expected_end_year=None):
    """Report capture, publication, coverage and review status without network calls."""
    now_string = utc_timestamp(now)
    if not isinstance(stale_after, timedelta) or stale_after.total_seconds() < 0:
        raise ValueError("Invalid capture freshness threshold.")
    if expected_end_year is not None and (
        type(expected_end_year) is not int or not 1 <= expected_end_year <= 9999
    ):
        raise ValueError("Invalid requested observation coverage year.")
    root = Path(root)
    result = {
        "checkedAt": now_string,
        "snapshot": {"status": "UNAVAILABLE", "snapshotId": None},
        "captureInventoryStatus": "UNAVAILABLE",
        "captures": {},
        "coverage": {
            "status": "UNAVAILABLE",
            "latestYear": None,
            "expectedEndYear": expected_end_year,
        },
        "review": {"status": "UNAVAILABLE"},
    }
    if not root.is_dir() or root.is_symlink():
        return result
    lock = _safe_child(root, ".writer.lock")
    if not lock.is_file():
        return result
    with lock.open("rb") as handle:
        fcntl.flock(handle, fcntl.LOCK_SH)
        try:
            try:
                connection = _database(root, create=False)
                try:
                    outcomes = dict(connection.execute("SELECT capture_id, status FROM captures"))
                finally:
                    connection.close()
                latest = {}
                bronze = _safe_child(root, "bronze")
                for source in bronze.iterdir():
                    if (
                        not _SOURCE.fullmatch(source.name)
                        or source.is_symlink()
                        or not source.is_dir()
                    ):
                        raise ValueError("Invalid capture inventory.")
                    for directory in source.iterdir():
                        if (
                            not _ID.fullmatch(directory.name)
                            or directory.is_symlink()
                            or not directory.is_dir()
                        ):
                            raise ValueError("Invalid capture inventory.")
                        receipt, _, _ = load_capture(root, source.name, directory.name)
                        prior = latest.get(source.name)
                        if prior is None or receipt["capture_sequence"] > prior["capture_sequence"]:
                            latest[source.name] = receipt
                for source_id, receipt in sorted(latest.items()):
                    seen = datetime.fromisoformat(receipt["first_seen_at"].replace("Z", "+00:00"))
                    seconds = (now - seen).total_seconds()
                    freshness = (
                        "CLOCK_AHEAD"
                        if seconds < 0
                        else "STALE"
                        if seconds > stale_after.total_seconds()
                        else "FRESH"
                    )
                    result["captures"][source_id] = {
                        "freshness": freshness,
                        "latestOutcome": outcomes.get(receipt["capture_id"], "NOT_PROCESSED"),
                        "firstSeenAt": receipt["first_seen_at"],
                        "captureSequence": receipt["capture_sequence"],
                    }
                result["captureInventoryStatus"] = "VALID"
            except (OSError, ValueError, sqlite3.Error):
                result["captureInventoryStatus"] = "INTEGRITY_FAILURE"
            try:
                current = read_current(root)
            except ValueError:
                gold = _safe_child(root, "gold")
                if gold.exists():
                    result["snapshot"]["status"] = "INTEGRITY_FAILURE"
                return result
            catalog = current["catalog"]
            result["snapshot"] = {"status": "VALID", "snapshotId": catalog["snapshotId"]}
            latest_year = catalog["datasets"][0]["lastYear"]
            result["coverage"] = {
                "status": "NOT_ASSESSED"
                if expected_end_year is None
                else "SATISFIED"
                if latest_year >= expected_end_year
                else "INSUFFICIENT",
                "latestYear": latest_year,
                "expectedEndYear": expected_end_year,
            }
            try:
                _, controls_bytes, _ = load_review_version(
                    root, catalog["inputInventory"]["reviewId"]
                )
                controls = list(csv.DictReader(io.StringIO(controls_bytes.decode("utf-8"))))
                covered = True
                for entry in current["datasets"]:
                    dataset = entry["dataset"]
                    if dataset["controls"] != controls:
                        raise ValueError("Review controls differ from the selected dataset.")
                    for window in dataset["windows"]:
                        if not any(
                            row["startDate"] <= window["decisionDate"] < row["endDate"]
                            for row in controls
                        ):
                            covered = False
                result["review"] = {"status": "VALID" if covered else "INSUFFICIENT_COVERAGE"}
            except (OSError, ValueError, UnicodeError, KeyError):
                result["review"] = {"status": "INTEGRITY_FAILURE"}
            return result
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
