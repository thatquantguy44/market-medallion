"""FOMC-AC07/12: complete selected chunk generations pinned to one text catalog."""

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.catalog import validate_catalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest


def versions(base):
    validate_catalog(base)
    result = {}
    for representation in base["representations"]:
        for version in representation["versions"]:
            identity = version["version_id"]
            if identity in result:
                raise ValueError("FOMC duplicate observed document version")
            result[identity] = (representation, version)
    return result


def build_generation(base, sets):
    expected = versions(base)
    for value in sets:
        body = dict(value)
        identity = body.pop("set_id")
        if identity != digest(body) or value["profile_sha256"] != digest(value["profile"]):
            raise ValueError("FOMC chunk set integrity mismatch")
    if not sets or len({s["document_version_id"] for s in sets}) != len(sets):
        raise ValueError("FOMC generation requires one selected extraction per document version")
    by_version = {s["document_version_id"]: s for s in sets}
    if set(by_version) != set(expected):
        raise ValueError("FOMC generation is missing or has unexpected document versions")
    if len({s["profile_sha256"] for s in sets}) != 1:
        raise ValueError("FOMC generation requires one chunker profile")
    selections = []
    for identity in sorted(expected):
        representation, version = expected[identity]
        value = by_version[identity]
        if (
            value["extraction_id"] not in version["extraction_ids"]
            or value["source_url"] != representation["source_url"]
            or value["source_sha256"] != version["source_sha256"]
        ):
            raise ValueError("FOMC chunk input does not match its pinned source version")
        selections.append(
            {
                "document_version_id": identity,
                "extraction_id": value["extraction_id"],
                "set_id": value["set_id"],
                "source_url": representation["source_url"],
                "source_sha256": version["source_sha256"],
                "observed_version_ordinal": version["ordinal"],
                "first_seen_at": version["first_seen_at"],
                "capture_ids": list(version["capture_ids"]),
                "meeting_ids": list(representation["meeting_ids"]),
                "current": identity == representation["versions"][-1]["version_id"],
            }
        )
    rows = [r for s in sets for r in s["chunks"]]
    units = [u for s in sets for u in s["coverage"]]
    coverage = {
        "sets": len(sets),
        "chunks": len(rows),
        "eligible_chunks": sum(r["eligible"] for r in rows),
        "review_chunks": sum(not r["eligible"] for r in rows),
        "source_unit_characters": sum(u["characters"] for u in units),
        "covered_characters": sum(u["covered_characters"] for u in units),
        "eligible_characters": sum(u["eligible_characters"] for u in units),
        "review_characters": sum(u["review_characters"] for u in units),
        "separator_characters": sum(s["separator_characters"] for s in sets),
        "empty_units": sum(u["characters"] == 0 for u in units),
        "complete_selected_unit_coverage": all(
            u["covered_characters"] == u["characters"] for u in units
        ),
    }
    value = {
        "schema_id": "fomc-chunk-catalog/1",
        "text_catalog_id": base["catalog_id"],
        "inventory_run_id": base["inventory_run_id"],
        "scope_start_year": base["scope_start_year"],
        "source_capture_cutoff": base["as_of"],
        "cutoff_kind": base["cutoff_kind"],
        "profile": sets[0]["profile"],
        "profile_sha256": sets[0]["profile_sha256"],
        "selections": selections,
        "coverage": coverage,
        "upstream_gaps": [dict(g) for g in base["gaps"]],
        "full_history_complete": False,
        "text_or_ocr_accuracy_certified": False,
    }
    value["catalog_id"] = digest(value)
    return value


def validate_generation(value):
    try:
        data = dict(value)
        identity = data.pop("catalog_id")
        if identity != digest(data) or value["schema_id"] != "fomc-chunk-catalog/1":
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError("FOMC chunk generation integrity mismatch") from None
    return value
