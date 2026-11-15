"""SSV-AC01/14: narrow rights-admitted interchange from verified T10 publication."""

import json
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume import publication
from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import _day
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.data.gold import (
    RATIO_METHOD,
    decimal_text,
    matched_metrics,
    ratio_change,
    share_ratios,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.quantities import (
    quantity_from_text,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    COMPONENT as COMPONENT_FIELDS,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.snapshot import (
    DEFAULT_BYTES,
    DEFAULT_ROWS,
    FACT,
    FIELDS,
    encode_decimal,
    limits,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.versions import TRANSFORM
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
    _admit,
    _clock,
    _clock_text,
    _identity,
    _read,
    _sync,
    _write,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import require_approved_use
from market_data_pipeline.pipelines.finra_short_sale_volume.registry import select_scopes
from market_data_pipeline.pipelines.finra_short_sale_volume.research import METHOD

CONTRACT = "finra-short-sale-volume-research/1"
PURPOSE = "personal_noncommercial_internal_research"
SOURCE = dict(
    owner="FINRA",
    dataset="finra-short-sale-volume",
    schema_id="finra-reg-sho-api/1",
    url="https://developer.finra.org/docs#reg-sho-daily-short-sale-volume",
)
REQUEST = ("trade_date", "facility_ids", "as_of", "as_of_sequence", "earliest_trade_date")
COVERAGE = (
    "status",
    "method",
    "quantity_unit",
    "ratio_method",
    "ratio_unit",
    "knowledge_basis",
    "clock_basis",
    "population_basis",
    "evidence_known_at",
    "prior_trade_date",
    "calendar_day_gap",
    "gap_basis",
    "missing_facilities",
    "current_partitions",
    "prior_partitions",
)
PARTITION = (
    "facility_id",
    "capture_id",
    "capture_sequence",
    "archived_at",
    "receipt_sha256",
    "status",
)
ROW = tuple(f[0] for f in FIELDS["research"]) + (
    "null_reason",
    "prior_null_reason",
    "missing_row_facilities",
)
COMPONENT = tuple(f[0] for f in COMPONENT_FIELDS)
VERSION = tuple(f[0] for f in FACT) + (
    "payload_sha256",
    "receipt_sha256",
    "manifest_sha256",
    "transform",
    "ratio_method",
    "ratio_unit",
    "reporting_facility_code",
    "market_raw",
)
NUMBERS = (
    "short_volume",
    "short_exempt_volume",
    "total_volume",
    "short_share",
    "short_exempt_share",
)


def _pick(value, keys):
    return {k: value[k] for k in keys}


def _exact(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError("FINRA interchange contains unknown or missing fields")


def _rights(output_use):
    if output_use not in (PURPOSE, "invented_fixture"):
        raise PermissionError("FINRA output use is not admitted")
    return dict(
        purpose=output_use,
        data_kind="INVENTED_FIXTURE" if output_use == "invented_fixture" else "PROVIDER",
        attribution="FINRA",
        redistribution="NOT_ADMITTED",
        terms_url="https://developer.finra.org/specific-terms-equity-data",
    )


def _gate(manifest, approved_use, operation, output_use):
    rights = _rights(output_use)
    _admit(manifest, approved_use, operation, {"fixture", "transform"})
    if operation == "fixture":
        if output_use != "invented_fixture":
            raise PermissionError("FINRA fixture output must retain its invented label")
    else:
        require_approved_use(approved_use, operation="internal_export")
        if output_use != PURPOSE:
            raise PermissionError("FINRA provider data cannot be exported as invented data")
    return rights


def _project(selected, rights):
    coverage = _pick(selected["research"], COVERAGE)
    for key in ("current_partitions", "prior_partitions"):
        coverage[key] = [_pick(p, PARTITION) for p in coverage[key]]
    rows = [_pick(row, ROW) for row in selected["tables"]["research"]]
    for row in rows:
        for key in ("components", "prior_components"):
            row[key] = [_pick(c, COMPONENT) for c in row[key]]
    return dict(
        contract=CONTRACT,
        source=SOURCE.copy(),
        rights=rights,
        publication=_pick(selected, ("generation_id", "manifest_sha256", "state")),
        request=_pick(selected["request"], REQUEST),
        coverage=coverage,
        rows=rows,
        versions=[_pick(v, VERSION) for v in selected["tables"]["silver"]],
    )


def validate(value, *, expected_scopes, output_use, max_rows=DEFAULT_ROWS, max_bytes=DEFAULT_BYTES):
    """Consumer boundary: fail closed on contract, population, units and lineage drift."""
    try:
        return _validate(
            value,
            expected_scopes=expected_scopes,
            output_use=output_use,
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
    except (KeyError, TypeError, AttributeError, InvalidOperation, RecursionError):
        raise ValueError("invalid FINRA interchange structure") from None


def _validate(value, *, expected_scopes, output_use, max_rows, max_bytes):
    limits(max_rows, max_bytes)
    facilities = [f.id for f in select_scopes(expected_scopes)]
    rights = _rights(output_use)
    _exact(
        value,
        ("contract", "source", "rights", "publication", "request", "coverage", "rows", "versions"),
    )
    if value["contract"] != CONTRACT or value["source"] != SOURCE or value["rights"] != rights:
        raise ValueError("FINRA interchange version, source or rights mismatch")
    request, coverage = value["request"], value["coverage"]
    _exact(request, REQUEST)
    _exact(coverage, COVERAGE)
    if request["facility_ids"] != facilities or coverage["missing_facilities"] != []:
        raise ValueError("FINRA interchange scope mismatch")
    day = _day(request["trade_date"])
    if _day(request["earliest_trade_date"]) > day:
        raise ValueError("invalid FINRA interchange date bounds")
    if request["as_of"] is not None:
        _time(request["as_of"])
    sequence = request["as_of_sequence"]
    if sequence is not None and (
        request["as_of"] is None or type(sequence) is not int or not 1 <= sequence < 2**63
    ):
        raise ValueError("invalid FINRA interchange cutoff")
    if any(
        coverage[k] != v
        for k, v in dict(
            method=METHOD,
            quantity_unit="shares",
            ratio_method=RATIO_METHOD,
            ratio_unit="1",
            knowledge_basis="CURRENT_VINTAGE",
            clock_basis="LOCAL_CAPTURE",
            population_basis="LATEST_COMPLETE_PARTITION_PER_FACILITY_AT_CUTOFF",
            gap_basis="CALENDAR_DAYS",
        ).items()
    ):
        raise ValueError("FINRA interchange units or method mismatch")
    _exact(value["publication"], ("generation_id", "manifest_sha256", "state"))
    _identity(value["publication"]["generation_id"])
    if value["publication"]["state"] != "PUBLISHED":
        raise ValueError("FINRA interchange requires a completed publication")
    _hash(value["publication"]["manifest_sha256"])
    for key in ("rows", "versions"):
        if not isinstance(value[key], list) or len(value[key]) > max_rows:
            raise ValueError("FINRA interchange row budget exceeded")
    if len(value["rows"]) + len(value["versions"]) > max_rows or len(encoded(value)) > max_bytes:
        raise ValueError("FINRA interchange row/byte budget exceeded")
    if coverage["status"] != ("COMPLETE" if value["rows"] else "VALID_EMPTY"):
        raise ValueError("FINRA interchange coverage mismatch")
    clocks = []
    partitions = {}
    prior_day = coverage["prior_trade_date"]
    if prior_day is not None:
        gap = (day - _day(prior_day)).days
        if (
            gap < 1
            or type(coverage["calendar_day_gap"]) is not int
            or coverage["calendar_day_gap"] != gap
            or _day(prior_day) < _day(request["earliest_trade_date"])
        ):
            raise ValueError("FINRA interchange prior date mismatch")
    elif coverage["calendar_day_gap"] is not None:
        raise ValueError("FINRA interchange prior gap mismatch")
    for key in ("current_partitions", "prior_partitions"):
        entries = coverage[key]
        expected = facilities if key == "current_partitions" or prior_day is not None else []
        if not isinstance(entries, list) or [p.get("facility_id") for p in entries] != expected:
            raise ValueError("FINRA interchange partition scope mismatch")
        partitions[key] = {}
        for p in entries:
            _exact(p, PARTITION)
            if p["status"] not in ("COMPLETE", "VALID_EMPTY"):
                raise ValueError("FINRA interchange partition incomplete")
            _identity(p["capture_id"])
            _clock_text(p["archived_at"])
            if type(p["capture_sequence"]) is not int or not 1 <= p["capture_sequence"] < 2**63:
                raise ValueError("invalid FINRA interchange capture sequence")
            if request["as_of"] is not None and (_time(p["archived_at"]), p["capture_sequence"]) > (
                _time(request["as_of"]),
                sequence or 2**63 - 1,
            ):
                raise ValueError("FINRA interchange partition exceeds cutoff")
            for h in ("receipt_sha256",):
                _hash(p[h])
            clocks.append(p["archived_at"])
            partitions[key][p["facility_id"]] = p
    if coverage["evidence_known_at"] != max(clocks, default=None):
        raise ValueError("FINRA interchange knowledge clock mismatch")
    versions = {}
    for v in value["versions"]:
        _exact(v, VERSION)
        if (
            v["facility_id"] not in facilities
            or not request["earliest_trade_date"] <= v["trade_date"] <= request["trade_date"]
        ):
            raise ValueError("FINRA interchange version outside requested scope")
        if v["version_id"] in versions:
            raise ValueError("duplicate FINRA interchange version")
        for h in ("version_id", "payload_sha256", "receipt_sha256", "manifest_sha256"):
            _hash(v[h])
        _identity(v["capture_id"])
        _clock_text(v["first_seen_at"])
        if v["retrieved_at"] is not None:
            _time(v["retrieved_at"])
        if v["provider_published_at"] is not None:
            _time(v["provider_published_at"])
        if request["as_of"] is not None and (_time(v["first_seen_at"]), v["capture_sequence"]) > (
            _time(request["as_of"]),
            sequence or 2**63 - 1,
        ):
            raise ValueError("FINRA interchange version exceeds cutoff")
        _numbers(v)
        _day(v["trade_date"])
        if (
            type(v["removed"]) is not bool
            or type(v["capture_sequence"]) is not int
            or not 1 <= v["capture_sequence"] < 2**63
            or v["ratio_method"] != RATIO_METHOD
            or v["ratio_unit"] != "1"
            or v["transform"] != TRANSFORM
        ):
            raise ValueError("invalid FINRA interchange version metadata")
        facility = next(f for f in select_scopes(expected_scopes) if f.id == v["facility_id"])
        if (v["reporting_facility_code"], v["market_raw"]) != (
            facility.reporting_facility_code,
            facility.market_code,
        ):
            raise ValueError("invalid FINRA interchange facility identity")
        _symbol(v["symbol_raw"])
        if v["removed"]:
            if any(v[n] is not None for n in (*NUMBERS, "source_row")):
                raise ValueError("invalid FINRA interchange removal")
        else:
            _source_row(v["source_row"])
            ratios = share_ratios(*(quantity_from_text(v[n]) for n in NUMBERS[:3]))
            if (v["short_share"], v["short_exempt_share"]) != (
                decimal_text(ratios.short_share),
                decimal_text(ratios.short_exempt_share),
            ):
                raise ValueError("invalid FINRA interchange source ratios")
        versions[v["version_id"]] = v
    selected_versions = _selected_versions(versions, partitions, request["trade_date"], prior_day)
    symbols = set()
    for row in value["rows"]:
        _exact(row, ROW)
        _symbol(row["symbol_raw"])
        if (
            row["facility_ids"] != facilities
            or row["trade_date"] != request["trade_date"]
            or row["prior_trade_date"] != prior_day
            or row["evidence_known_at"] != coverage["evidence_known_at"]
            or row["calendar_day_gap"] != coverage["calendar_day_gap"]
            or row["symbol_raw"] in symbols
        ):
            raise ValueError("FINRA interchange row scope/clock mismatch")
        symbols.add(row["symbol_raw"])
        _numbers(row)
        for n in (
            "prior_short_share",
            "prior_short_exempt_share",
            "short_share_change",
            "short_exempt_share_change",
        ):
            encode_decimal(row[n])
        for key, pkey in (
            ("components", "current_partitions"),
            ("prior_components", "prior_partitions"),
        ):
            components = row[key]
            if not isinstance(components, list) or [
                c.get("facility_id") for c in components
            ] != list(partitions[pkey]):
                raise ValueError("FINRA interchange component scope mismatch")
            for c in components:
                _exact(c, COMPONENT)
                p = partitions[pkey][c["facility_id"]]
                if c["capture_id"] != p["capture_id"]:
                    raise ValueError("FINRA interchange capture lineage mismatch")
                expected = selected_versions.get(
                    (
                        request["trade_date"] if key == "components" else prior_day,
                        c["facility_id"],
                        row["symbol_raw"],
                    )
                )
                if c["status"] == "REPORTED":
                    if expected is None or expected["version_id"] != c["version_id"]:
                        raise ValueError(
                            "FINRA interchange does not select the latest observed version"
                        )
                    v = versions.get(c["version_id"])
                    if (
                        v is None
                        or v["removed"]
                        or v["facility_id"] != c["facility_id"]
                        or v["symbol_raw"] != row["symbol_raw"]
                        or v["capture_id"] != c["source_capture_id"]
                        or v["trade_date"]
                        != (request["trade_date"] if key == "components" else prior_day)
                    ):
                        raise ValueError("FINRA interchange version lineage mismatch")
                    _source_row(c["source_row"])
                    if p["status"] != "COMPLETE" or (v["first_seen_at"], v["capture_sequence"]) > (
                        p["archived_at"],
                        p["capture_sequence"],
                    ):
                        raise ValueError("FINRA interchange version exceeds observation knowledge")
                    if v["capture_id"] == p["capture_id"] and (
                        v["first_seen_at"] != p["archived_at"]
                        or v["capture_sequence"] != p["capture_sequence"]
                        or v["receipt_sha256"] != p["receipt_sha256"]
                        or v["source_row"] != c["source_row"]
                    ):
                        raise ValueError("FINRA interchange origin capture metadata mismatch")
                    if any(c[n] != v[n] for n in NUMBERS[:3]):
                        raise ValueError("FINRA interchange quantity lineage mismatch")
                elif c["status"] != "NO_REPORTED_ROW" or any(
                    c[k] is not None
                    for k in ("version_id", "source_capture_id", "source_row", *NUMBERS[:3])
                ):
                    raise ValueError("FINRA interchange missing row mismatch")
                if (
                    c["status"] == "NO_REPORTED_ROW"
                    and expected is not None
                    and not expected["removed"]
                ):
                    raise ValueError("FINRA interchange absence contradicts observed membership")
        _metrics(row)
    expected_symbols = {
        symbol
        for (trade_date, _, symbol), v in selected_versions.items()
        if trade_date == request["trade_date"] and not v["removed"]
    }
    if symbols != expected_symbols:
        raise ValueError("FINRA interchange symbol population differs from observed membership")
    return value


def _selected_versions(versions, partitions, current_day, prior_day):
    """Reconstruct membership at each capture from retained value/removal history."""
    captures = {
        (trade_date, facility): p
        for key, trade_date in (
            ("current_partitions", current_day),
            ("prior_partitions", prior_day),
        )
        for facility, p in partitions[key].items()
    }
    selected = {}
    for v in versions.values():
        p = captures.get((v["trade_date"], v["facility_id"]))
        clock = (v["first_seen_at"], v["capture_sequence"])
        if p is None or clock > (p["archived_at"], p["capture_sequence"]):
            continue
        key = (v["trade_date"], v["facility_id"], v["symbol_raw"])
        old = selected.get(key)
        old_clock = (old["first_seen_at"], old["capture_sequence"]) if old else None
        if old_clock == clock:
            raise ValueError("FINRA interchange contains conflicting version chronology")
        if old_clock is None or clock > old_clock:
            selected[key] = v
    populated = {(d, f) for (d, f, _), v in selected.items() if not v["removed"]}
    if any((key in populated) != (p["status"] == "COMPLETE") for key, p in captures.items()):
        raise ValueError("FINRA interchange partition coverage contradicts observed membership")
    return selected


def _symbol(value):
    if not isinstance(value, str) or not value or not value.isprintable() or value.strip() != value:
        raise ValueError("invalid FINRA interchange raw symbol")


def _source_row(value):
    if type(value) is not int or not 1 <= value < 2**63:
        raise ValueError("invalid FINRA interchange source row")


def _aggregate(components):
    if not components:
        return dict(
            status="NO_PRIOR_COMPLETE_DATE",
            null_reason="NO_PRIOR_COMPLETE_DATE",
            **{n: None for n in NUMBERS},
        )
    absent = [c["facility_id"] for c in components if c["status"] == "NO_REPORTED_ROW"]
    if absent:
        return dict(
            status="NO_REPORTED_ROW", null_reason="NO_REPORTED_ROW", **{n: None for n in NUMBERS}
        )
    return dict(
        status="AVAILABLE",
        **matched_metrics(
            [tuple(quantity_from_text(c[n]) for n in NUMBERS[:3]) for c in components]
        ),
    )


def _metrics(row):
    current, prior = _aggregate(row["components"]), _aggregate(row["prior_components"])
    if any(row[n] != current[n] for n in (*NUMBERS, "status", "null_reason")):
        raise ValueError("FINRA interchange aggregate mismatch")
    missing = [c["facility_id"] for c in row["components"] if c["status"] == "NO_REPORTED_ROW"]
    if (
        row["missing_row_facilities"] != missing
        or row["prior_status"] != prior["status"]
        or row["prior_null_reason"] != prior["null_reason"]
    ):
        raise ValueError("FINRA interchange missingness mismatch")
    for n in NUMBERS[3:]:
        change = decimal_text(
            ratio_change(
                Decimal(current[n]) if current[n] is not None else None,
                Decimal(prior[n]) if prior[n] is not None else None,
            )
        )
        if row["prior_" + n] != prior[n] or row[n + "_change"] != change:
            raise ValueError("FINRA interchange prior metric mismatch")


def _time(value):
    if not isinstance(value, str):
        raise ValueError("invalid FINRA interchange UTC clock")
    return _clock(datetime.fromisoformat(value))


def _hash(value):
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError("invalid FINRA interchange hash")


def _numbers(row):
    for name in NUMBERS:
        encode_decimal(row[name])


def read(path, *, expected_scopes, output_use, max_rows=DEFAULT_ROWS, max_bytes=DEFAULT_BYTES):
    _rights(output_use)
    limits(max_rows, max_bytes)
    body = _read(Path(path), max_bytes)
    value = json.loads(body)
    if encoded(value) != body:
        raise ValueError("FINRA interchange is not canonical JSON")
    return validate(
        value,
        expected_scopes=expected_scopes,
        output_use=output_use,
        max_rows=max_rows,
        max_bytes=max_bytes,
    )


def export(
    root,
    manifest,
    *,
    approved_use,
    operation,
    destination,
    output_use,
    scopes,
    expected_generation_id=None,
    max_rows=DEFAULT_ROWS,
    max_bytes=DEFAULT_BYTES,
):
    rights = _gate(manifest, approved_use, operation, output_use)
    limits(max_rows, max_bytes)
    if expected_generation_id is not None:
        _identity(expected_generation_id)
    facilities = [f.id for f in select_scopes(scopes)]
    selected = publication.read(
        root,
        manifest,
        approved_use=approved_use,
        operation=operation,
        max_rows=max_rows,
        max_bytes=max_bytes,
    )
    if selected["state"] != "PUBLISHED" or selected["recovery_required"]:
        raise ValueError("recover FINRA publication before research export")
    if expected_generation_id is not None and selected["generation_id"] != expected_generation_id:
        raise ValueError("FINRA expected publication generation is no longer current")
    if selected["request"]["facility_ids"] != facilities:
        raise ValueError("FINRA export scope does not match publication")
    value = _project(selected, rights)
    validate(
        value, expected_scopes=scopes, output_use=output_use, max_rows=max_rows, max_bytes=max_bytes
    )
    path = Path(destination).absolute()
    if (
        any(p.is_symlink() for p in (path, *path.parents))
        or not path.parent.is_dir()
        or path.resolve().is_relative_to(Path(root).resolve())
    ):
        raise ValueError("unsafe FINRA research export destination")
    body = encoded(value)
    stage = path.parent / (".ssv-export-" + uuid4().hex)
    try:
        _write(stage, body)
        read(
            stage,
            expected_scopes=scopes,
            output_use=output_use,
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
        try:
            os.link(stage, path)
        except FileExistsError:
            if _read(path, max_bytes) != body:
                raise ValueError("FINRA export destination contains different bytes") from None
        _sync(path.parent)
    finally:
        stage.unlink(missing_ok=True)
    return dict(
        contract=CONTRACT,
        generation_id=selected["generation_id"],
        rows=len(value["rows"]),
        versions=len(value["versions"]),
        bytes=len(body),
        rights=rights,
    )
