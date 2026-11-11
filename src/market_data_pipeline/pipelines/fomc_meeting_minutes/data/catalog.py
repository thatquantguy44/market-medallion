"""FOMC-AC04/05: bounded catalogs, observed content changes and capture cutoffs."""

from datetime import datetime
from uuid import UUID

from market_data_pipeline.pipelines.fomc_meeting_minutes.config import validate_start_year
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest, validate
from market_data_pipeline.shared.files import utc_timestamp


def capture_time(value):
    try:
        return utc_timestamp(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("FOMC catalog requires actual timezone-aware capture clocks") from None


def build_catalog(inventory, records, *, inventory_run_id, start_year=1990, as_of=None):
    """No I/O. Source versions use raw changes; derivatives remain independent.

    as_of is explicitly a document-capture cutoff applied to the supplied metadata
    snapshot, not a claim to reconstruct historical publication/inventory knowledge.
    """
    validate_start_year(start_year)
    run_id = str(UUID(inventory_run_id))
    cutoff = utc_timestamp(as_of) if as_of is not None else None
    meetings = {m["meeting_id"]: m for m in inventory["meetings"]}
    allowed = {
        key
        for key, m in meetings.items()
        if m["committee_body"] == "fomc"
        and all(int(d[:4]) >= start_year for d in m["meeting_dates"])
    }
    selected, seen = [], set()
    for record in records:
        receipt, extraction = record["receipt"], validate(record["extraction"])
        ids = record["meeting_ids"]
        if not ids or set(ids) - allowed:
            raise ValueError("FOMC catalog meeting association outside selected scope")
        if (
            not receipt["accepted"]
            or receipt["kind"] != "document"
            or receipt["sha256"] != extraction["source_sha256"]
            or receipt["source_url"] != extraction["source_url"]
        ):
            raise ValueError("FOMC catalog source/extraction mismatch")
        identity = (receipt["capture_id"], extraction["extraction_id"])
        if identity in seen:
            raise ValueError("FOMC catalog duplicate capture/extraction binding")
        seen.add(identity)
        timestamp = capture_time(receipt["captured_at"])
        if cutoff is None or timestamp <= cutoff:
            selected.append((timestamp, receipt["capture_id"], extraction["extraction_id"], record))
    representations, bindings = {}, []
    for timestamp, capture_id, extraction_id, record in sorted(selected, key=lambda r: r[:3]):
        receipt, extraction = record["receipt"], record["extraction"]
        url = receipt["source_url"]
        rep = representations.setdefault(
            url, {"source_url": url, "meeting_ids": [], "versions": []}
        )
        rep["meeting_ids"] = sorted(set(rep["meeting_ids"]) | set(record["meeting_ids"]))
        versions = rep["versions"]
        if not versions or versions[-1]["source_sha256"] != receipt["sha256"]:
            ordinal = len(versions) + 1
            versions.append(
                {
                    "version_id": digest([url, ordinal, receipt["sha256"]]),
                    "ordinal": ordinal,
                    "source_sha256": receipt["sha256"],
                    "first_seen_at": timestamp,
                    "capture_ids": [],
                    "extraction_ids": [],
                    "extractions": [],
                }
            )
        version = versions[-1]
        version["capture_ids"] = sorted(set(version["capture_ids"]) | {capture_id})
        if extraction_id not in version["extraction_ids"]:
            version["extraction_ids"].append(extraction_id)
            version["extractions"].append(
                {
                    k: extraction[k]
                    for k in [
                        "extraction_id",
                        "profile_sha256",
                        "status",
                        "text_sha256",
                        "page_count",
                        "issues",
                    ]
                }
            )
        bindings.append(
            {
                "capture_id": capture_id,
                "extraction_id": extraction_id,
                "meeting_ids": sorted(set(record["meeting_ids"])),
            }
        )
    gaps = []
    for doc in inventory["documents"]:
        if doc["source_url"] not in representations:
            gaps.append(
                {
                    "document_id": doc["document_id"],
                    "source_url": doc["source_url"],
                    "outcome": "unacquired",
                }
            )
    for entry in inventory["entries"]:
        if entry["meeting_id"] in allowed and entry["minutes_status"] != "listed":
            gaps.append(
                {
                    "meeting_id": entry["meeting_id"],
                    "source_page_url": entry["source_page_url"],
                    "outcome": entry["minutes_status"],
                }
            )
    for rep in representations.values():
        for extraction in rep["versions"][-1]["extractions"]:
            if extraction["status"] != "native_extracted":
                gaps.append(
                    {
                        "source_url": rep["source_url"],
                        "outcome": extraction["status"],
                        "extraction_id": extraction["extraction_id"],
                    }
                )
    value = {
        "schema_id": "fomc-text-catalog/1",
        "inventory_run_id": run_id,
        "inventory": inventory,
        "scope_start_year": start_year,
        "as_of": cutoff,
        "cutoff_kind": "document_capture_time_with_supplied_inventory_snapshot",
        "full_history_complete": False,
        "capture_ids": sorted({r[1] for r in selected}),
        "bindings": bindings,
        "representations": [representations[k] for k in sorted(representations)],
        "gaps": sorted(gaps, key=digest),
    }
    value["catalog_id"] = digest(value)
    return value


def validate_catalog(value):
    try:
        data = dict(value)
        identity = data.pop("catalog_id")
        if identity != digest(data) or value["schema_id"] != "fomc-text-catalog/1":
            raise ValueError
    except (ValueError, TypeError, KeyError):
        raise ValueError("FOMC catalog integrity mismatch") from None
    return value
