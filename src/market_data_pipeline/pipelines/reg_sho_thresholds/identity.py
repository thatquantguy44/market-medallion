"""Dated, knowledge-aware mappings that never replace provider-native facts."""

import json
from datetime import date, datetime
from pathlib import Path


def load_mappings(path: Path):
    try:
        value = json.loads(Path(path).read_text())
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid threshold identity mapping artifact") from exc
    if not isinstance(value, dict) or set(value) != {"schema_version", "mappings"}:
        raise ValueError("identity mapping requires exactly schema_version and mappings")
    if value["schema_version"] != "security-identity-mapping/1" or not isinstance(
        value["mappings"], list
    ):
        raise ValueError("unsupported identity mapping artifact")
    result = []
    required = {
        "namespace",
        "raw_identifier",
        "target_security_id",
        "effective_from",
        "effective_to",
        "mapping_version",
        "evidence",
        "first_seen_at",
        "status",
    }
    for item in value["mappings"]:
        if not isinstance(item, dict) or set(item) != required:
            raise ValueError("identity mapping fields differ from the contract")
        try:
            start = date.fromisoformat(item["effective_from"])
            end = date.fromisoformat(item["effective_to"]) if item["effective_to"] else None
            seen = datetime.fromisoformat(item["first_seen_at"].replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid identity mapping date/clock") from exc
        if (
            item["status"] not in ("RESOLVED", "AMBIGUOUS", "UNRESOLVED")
            or seen.utcoffset() is None
            or (end is not None and end < start)
            or any(
                not isinstance(item[key], str) or not item[key]
                for key in ("namespace", "raw_identifier", "mapping_version", "evidence")
            )
            or (
                item["status"] == "RESOLVED"
                and (
                    not isinstance(item["target_security_id"], str)
                    or not item["target_security_id"]
                )
            )
            or (item["status"] != "RESOLVED" and item["target_security_id"] is not None)
        ):
            raise ValueError("invalid identity mapping semantics")
        result.append({**item, "effective_from": start, "effective_to": end, "first_seen_at": seen})
    return result


def resolve_identity(mappings, *, namespace, raw_identifier, economic_date, knowledge_cutoff):
    if type(economic_date) is not date or not isinstance(knowledge_cutoff, datetime):
        raise ValueError("identity resolution requires economic date and knowledge cutoff")
    if knowledge_cutoff.utcoffset() is None:
        raise ValueError("identity knowledge cutoff requires a timezone")
    candidates = [
        item
        for item in mappings
        if item["namespace"] == namespace
        and item["raw_identifier"] == raw_identifier
        and item["effective_from"] <= economic_date
        and (item["effective_to"] is None or economic_date <= item["effective_to"])
        and item["first_seen_at"] <= knowledge_cutoff
    ]
    resolved = [item for item in candidates if item["status"] == "RESOLVED"]
    if len(resolved) == 1 and len(candidates) == 1:
        return {
            "match_status": "RESOLVED",
            "target_security_id": resolved[0]["target_security_id"],
            "mapping_version": resolved[0]["mapping_version"],
        }
    return {
        "match_status": "AMBIGUOUS" if len(candidates) > 1 else "UNRESOLVED",
        "target_security_id": None,
        "mapping_version": None,
    }
