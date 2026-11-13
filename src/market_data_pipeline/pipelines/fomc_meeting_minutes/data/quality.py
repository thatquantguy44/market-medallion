"""FOMC-QAC01–03: pure, generation-pinned extraction-quality diagnostics."""

from collections import Counter

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunk_catalog import (
    build_generation,
    validate_generation,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.chunks import validate as validate_set
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest

SCHEMA = "fomc-quality-report/1"
PROFILE = "selected-version-unit-issues/1"
ISSUES = frozenset(
    {
        "missing_native_text",
        "image_content",
        "replacement_character",
        "invalid_text_encoding",
        "byte_limit",
        "encrypted_pdf",
        "empty_pdf",
        "page_limit",
        "invalid_pdf",
        "content_stream_limit",
        "page_extraction_failed",
        "unsupported_html",
    }
)


def _counts(rows):
    return {
        "selected_versions": len({r["document_version_id"] for r in rows}),
        "units": len(
            {
                (r["document_version_id"], r["unit_ordinal"])
                for r in rows
                if r["unit_ordinal"] is not None
            }
        ),
        "chunks": len({c for r in rows for c in r["chunk_ids"]}),
        "source_unit_characters": sum(r["end"] - r["start"] for r in rows),
    }


def _group(rows, key, *, multiple=False):
    grouped = {}
    for row in rows:
        for label in row[key] if multiple else [row[key]]:
            grouped.setdefault(str(label), []).append(row)
    return {label: _counts(values) for label, values in sorted(grouped.items())}


def build_report(inventory, text_catalog, generation, extractions, sets):
    """Report verified selected source versions, including historical reversions.

    Inputs come from owned verified readers. Distinct source versions remain
    distinct denominators even when a reversion reuses an extraction ID.
    """
    validate_generation(generation)
    if build_generation(text_catalog, sets) != generation:
        raise ValueError("FOMC quality generation does not match selected inputs")
    meetings = {m["meeting_id"]: m for m in inventory["meetings"]}
    documents = {d["document_id"]: d for d in inventory["documents"]}
    by_set = {s["set_id"]: s for s in sets}
    units, sources = [], []
    for selected in generation["selections"]:
        value = by_set[selected["set_id"]]
        extraction = extractions[value["extraction_id"]]
        validate_set(value, extraction)
        if set(extraction["issues"]) - ISSUES:
            raise ValueError("FOMC quality unknown extraction issue code")
        if any(m not in meetings for m in selected["meeting_ids"]):
            raise ValueError("FOMC quality unknown meeting association")
        dates = sorted({d for m in selected["meeting_ids"] for d in meetings[m]["meeting_dates"]})
        if not dates:
            raise ValueError("FOMC quality missing meeting dates")
        context = {
            **{
                k: selected[k]
                for k in (
                    "document_version_id",
                    "extraction_id",
                    "source_url",
                    "source_sha256",
                    "capture_ids",
                    "meeting_ids",
                    "first_seen_at",
                    "current",
                )
            },
            "profile_sha256": extraction["profile_sha256"],
            "format": "pdf" if selected["source_url"].lower().endswith(".pdf") else "html",
            "meeting_dates": dates,
            "years": sorted({int(d[:4]) for d in dates}),
            "status": extraction["status"],
            "release_claims": sorted(
                [
                    {
                        k: entry[k]
                        for k in ("meeting_id", "source_page_url", "provider_release_date")
                    }
                    for entry in inventory["entries"]
                    if entry["document_id"] in documents
                    and documents[entry["document_id"]]["source_url"] == selected["source_url"]
                    and entry["meeting_id"] in selected["meeting_ids"]
                ],
                key=lambda claim: (
                    claim["meeting_id"],
                    claim["source_page_url"],
                    claim["provider_release_date"] or "",
                ),
            ),
        }
        sources.append(
            {**context, "issues": extraction["issues"], "unit_count": len(extraction["units"])}
        )
        coverage = value["coverage"]
        if not coverage:
            # A document-level missing-text outcome must survive the absence of units/chunks.
            coverage = [
                {
                    "unit_ordinal": None,
                    "page": None,
                    "heading": "",
                    "start": 0,
                    "end": 0,
                    "issues": extraction["issues"],
                }
            ]
        for coverage_unit in coverage:
            ordinal = coverage_unit["unit_ordinal"]
            original_unit = extraction["units"][ordinal] if ordinal is not None else None
            issues = coverage_unit["issues"]
            if set(issues) - ISSUES:
                raise ValueError("FOMC quality unknown unit issue code")
            rows = [r for r in value["chunks"] if r["unit_ordinal"] == ordinal]
            unit = {
                **context,
                **{
                    k: coverage_unit[k] for k in ("unit_ordinal", "page", "heading", "start", "end")
                },
                "issues": list(issues),
                "chunk_ids": sorted(r["chunk_id"] for r in rows),
                "assets": list(original_unit["assets"]) if original_unit else [],
                "unit_text_sha256": original_unit["text_sha256"] if original_unit else None,
            }
            unit["case_id"] = digest(
                [selected["document_version_id"], selected["extraction_id"], ordinal]
            )
            units.append(unit)
    units.sort(
        key=lambda r: (
            r["document_version_id"],
            -1 if r["unit_ordinal"] is None else r["unit_ordinal"],
        )
    )
    sources.sort(key=lambda r: r["document_version_id"])
    findings = [{**r, "issue_code": issue} for r in units for issue in r["issues"]]
    coverage = generation["coverage"]
    result = {
        "schema_id": SCHEMA,
        "profile": PROFILE,
        "inventory_run_id": generation["inventory_run_id"],
        "text_catalog_id": text_catalog["catalog_id"],
        "chunk_catalog_id": generation["catalog_id"],
        "scope_start_year": generation["scope_start_year"],
        "source_capture_cutoff": generation["source_capture_cutoff"],
        "totals": {
            **_counts(units),
            "selected_versions": len(sources),
            "representations": len({r["source_url"] for r in sources}),
            "extractions": len({r["extraction_id"] for r in sources}),
            "review_versions": sum(r["status"] == "review_required" for r in sources),
            "representations_without_units": sum(r["unit_count"] == 0 for r in sources),
            **{
                k: coverage[k]
                for k in (
                    "eligible_chunks",
                    "review_chunks",
                    "empty_units",
                    "eligible_characters",
                    "review_characters",
                    "separator_characters",
                )
            },
            "findings": len(findings),
        },
        "status_counts": dict(sorted(Counter(s["status"] for s in sources).items())),
        "groups": {
            "year": _group(units, "years", multiple=True),
            "format": _group(units, "format"),
            "profile": _group(units, "profile_sha256"),
            "issue": _group(findings, "issue_code"),
        },
        "units": units,
        "sources": sources,
        "findings": findings,
        "upstream_gaps": generation["upstream_gaps"],
        "group_memberships_may_overlap": True,
        "request_performed": False,
        "text_or_ocr_accuracy_certified": False,
    }
    if (
        result["totals"]["chunks"] != coverage["chunks"]
        or result["totals"]["source_unit_characters"] != coverage["source_unit_characters"]
    ):
        raise ValueError("FOMC quality coverage does not reconcile")
    result["report_id"] = digest(result)
    return result


def validate_report(value):
    try:
        body = dict(value)
        identity = body.pop("report_id")
        if (
            identity != digest(body)
            or value["schema_id"] != SCHEMA
            or value["profile"] != PROFILE
            or value["request_performed"] is not False
            or value["text_or_ocr_accuracy_certified"] is not False
            or any(set(u["issues"]) - ISSUES for u in value["units"])
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise ValueError("FOMC quality report integrity mismatch") from None
    return value
