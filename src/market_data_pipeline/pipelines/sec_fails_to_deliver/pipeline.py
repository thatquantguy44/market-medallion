"""Bounded local SEC FTD capture -> typed SQLite facts -> database Gold."""

import hashlib
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from market_data_pipeline.pipelines.sec_fails_to_deliver.bronze import archive_capture, load_capture
from market_data_pipeline.pipelines.sec_fails_to_deliver.completeness import parse_evidence
from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.data import parse_archive
from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import (
    SCHEMA_VERSION,
    parse_manifest,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.outcomes import make_outcome, write_outcome
from market_data_pipeline.pipelines.sec_fails_to_deliver.publication import (
    publish_new,
    recover_publications,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import IDENTITY
from market_data_pipeline.pipelines.sec_fails_to_deliver.versions import knowledge_cutoff
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import (
    is_database_error,
    warehouse_for,
)
from market_data_pipeline.shared.files import utc_timestamp


def _runtime(path):
    return load_config(Path(path))


def _fact(row, receipt, captured_at):
    values = {
        "settlement_date": row.settlement_date.isoformat(),
        "cusip": row.cusip,
        "symbol_raw": row.symbol_raw,
        "description_raw": row.description_raw,
        "quantity_fails_text": row.quantity_fails_raw,
        "prior_close_price_text": row.prior_close_price_raw,
        "price_missing_reason": row.price_missing_reason,
    }
    content_hash = hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
    version_id = hashlib.sha256(
        f"{receipt['run_id']}:{row.settlement_date}:{row.cusip}:{content_hash}".encode()
    ).hexdigest()
    values.update(settlement_date=row.settlement_date)
    return {
        **IDENTITY,
        **values,
        "quantity_fails": Decimal(row.quantity_fails),
        "prior_close_price": row.prior_close_price,
        "price_basis": "PROVIDER_PREVIOUS_CLOSE",
        "row_state": "REPORTED",
        "source_row_number": row.source_row_number,
        "partition_id": receipt["partition_id"],
        "raw_member": receipt["member_name"],
        "version_id": version_id,
        "content_hash": content_hash,
        "raw_sha256": receipt["raw_sha256"],
        "member_sha256": receipt["member_sha256"],
        "run_id": receipt["run_id"],
        "capture_sequence": receipt["capture_sequence"],
        "first_seen_at": captured_at,
        "retrieved_at": captured_at,
        "provider_published_at": None,
        "source_updated_at": None,
        "parser_version": SCHEMA_VERSION,
    }


def coverage_record(manifest, evidence, parsed, audit, outcome):
    accepted = audit["status"] == "SUCCEEDED"
    return {
        **IDENTITY,
        "partition_id": f"{manifest.period_start}_{manifest.period_end}",
        "period_start": manifest.period_start,
        "period_end": manifest.period_end,
        "coverage_state": outcome,
        "expected_date_count": len(evidence.expected_dates) if evidence is not None else None,
        "observed_date_count": len(parsed.observed_dates) if parsed else 0,
        "received_row_count": len(parsed.observations) if parsed else 0,
        "trailer_record_count": len(parsed.observations) if parsed else None,
        "trailer_quantity_text": str(sum(r.quantity_fails for r in parsed.observations))
        if parsed
        else None,
        "validation_state": "MATCHED"
        if accepted
        else "NOT_CHECKED"
        if outcome == "UNAVAILABLE"
        else "REJECTED",
        "version_id": hashlib.sha256(f"coverage:{audit['run_id']}".encode()).hexdigest(),
        "run_id": audit["run_id"],
        "capture_sequence": audit["capture_sequence"],
        "first_seen_at": audit["captured_at"],
        "raw_sha256": audit["raw_sha256"],
    }


def run(config_path: Path, partition: str, *, clock=None, acquired=None) -> dict:
    """One configured local partition; every bounded attempt gets durable lineage."""
    config = _runtime(config_path)
    if config.databricks.enabled:
        raise ValueError("SEC FTD Delta publication is not implemented yet (T21)")
    if partition not in config.partitions:
        raise ValueError("partition is not present in the SEC FTD config")
    selected = config.partitions[partition]
    if acquired is not None and acquired.metadata.get("partition") != partition:
        raise ValueError("acquired payload does not match the selected partition")
    with selected.manifest.open("rb") as stream:
        manifest_payload = stream.read(65537)
    manifest = parse_manifest(manifest_payload)
    evidence_payload = None
    evidence = None
    if selected.coverage_evidence is not None:
        with selected.coverage_evidence.open("rb") as stream:
            evidence_payload = stream.read(65537)
        evidence = parse_evidence(evidence_payload, manifest)
    now = clock or (lambda: datetime.now(UTC))
    captured_at = datetime.fromisoformat(utc_timestamp(now()))
    run_id = uuid.uuid4().hex
    with warehouse_for(config) as warehouse:
        recover_publications(config.root, warehouse)
        with warehouse.transaction():
            sequence = warehouse.next_capture_sequence()
            started = {
                **IDENTITY,
                "run_id": run_id,
                "capture_sequence": sequence,
                "captured_at": captured_at,
                "completed_at": None,
                "status": "STARTED",
                "row_count": 0,
                "raw_sha256": None,
                "error_code": None,
            }
            warehouse.record_run(started)
        parsed = None
        receipt = None
        stage = "capture"
        facts = []
        error_code = None
        status = "SUCCEEDED"
        coverage_state = "PARTIAL"
        try:
            receipt, payload = archive_capture(
                config.root,
                run_id=run_id,
                sequence=sequence,
                captured_at=captured_at,
                manifest_payload=manifest_payload,
                settings=config.storage,
                input_path=acquired.path if acquired is not None else selected.input,
                clock=now,
                coverage_evidence=evidence_payload,
                acquisition=acquired.metadata if acquired is not None else None,
            )
            captured_at = datetime.fromisoformat(receipt["captured_at"])
            started["captured_at"] = captured_at
            stage = "integrity"
            stored_receipt, stored_payload, stored_manifest, stored_settings = load_capture(
                config.root / "bronze" / run_id
            )
            if (
                stored_receipt != receipt
                or stored_payload != payload
                or stored_manifest != manifest_payload
                or stored_settings != config.storage
            ):
                raise ValueError("capture artifacts differ from the acquired input")
            if receipt["source_state"] == "UNAVAILABLE":
                status, error_code, coverage_state = "FAILED", "SOURCE_UNAVAILABLE", "UNAVAILABLE"
            elif receipt["source_state"] == "OVERSIZED":
                status, error_code, coverage_state = "REJECTED", "INPUT_TOO_LARGE", "FAILED"
            else:
                stage = "parse"
                parsed = parse_archive(payload, manifest)
                if evidence is not None:
                    stage = "coverage"
                    evidence.validate(
                        raw_sha256=parsed.raw_sha256, observed_dates=parsed.observed_dates
                    )
                    coverage_state = "COMPLETE"
                receipt["member_sha256"] = parsed.member_sha256
                facts = [_fact(row, receipt, captured_at) for row in parsed.observations]
        except (ValueError, OSError) as exc:
            status, coverage_state = "REJECTED", "FAILED"
            if isinstance(exc, OSError):
                status, error_code = "FAILED", "BRONZE_WRITE_FAILED"
            elif stage == "parse":
                error_code = (
                    "EMPTY_PARTITION_UNSUPPORTED"
                    if str(exc).startswith("empty file")
                    else "INVALID_ARCHIVE"
                )
            elif stage == "coverage":
                error_code = "COVERAGE_EVIDENCE_INVALID"
            else:
                error_code = "CAPTURE_INTEGRITY_FAILED"
        raw_hash = receipt["raw_sha256"] if receipt else None

        completed_at = datetime.fromisoformat(utc_timestamp(now()))
        audit = {
            **started,
            "completed_at": completed_at,
            "status": status,
            "row_count": len(facts) if status == "SUCCEEDED" else 0,
            "raw_sha256": raw_hash,
            "error_code": error_code,
        }
        try:
            inserted = warehouse.promote_partition(
                facts if status == "SUCCEEDED" else [],
                coverage_record(manifest, evidence, parsed, audit, coverage_state),
                audit,
                evidence=evidence,
                capture={
                    "raw_member": receipt["member_name"],
                    "member_sha256": parsed.member_sha256,
                    "parser_version": SCHEMA_VERSION,
                }
                if parsed is not None and receipt is not None
                else None,
            )
        except Exception as exc:
            if not isinstance(exc, ValueError) and not is_database_error(exc):
                raise
            status, coverage_state = "REJECTED", "FAILED"
            error_code = (
                str(exc) if str(exc) == "OVERLAPPING_PARTITION" else "STORAGE_VALIDATION_FAILED"
            )
            if is_database_error(exc):
                status, error_code = "FAILED", "WAREHOUSE_WRITE_FAILED"
            audit.update(status=status, row_count=0, error_code=error_code)
            warehouse.promote_partition(
                [], coverage_record(manifest, evidence, parsed, audit, coverage_state), audit
            )
            inserted = 0
        if receipt is not None and error_code not in (
            "CAPTURE_INTEGRITY_FAILED",
            "BRONZE_WRITE_FAILED",
        ):
            directory = config.root / "bronze" / run_id
            write_outcome(
                directory,
                make_outcome(
                    directory,
                    config.storage,
                    audit,
                    coverage_record(manifest, evidence, parsed, audit, coverage_state),
                    warehouse.fact_fingerprint(run_id),
                ),
            )
        publication = publish_new(config.root, warehouse)
        return {
            "pipeline": "sec-fails-to-deliver",
            "run_id": run_id,
            "capture_sequence": sequence,
            "status": status,
            "row_count": len(facts) if status == "SUCCEEDED" else 0,
            "inserted_versions": inserted,
            "coverage_state": coverage_state,
            "source_coverage": "NOT_CERTIFIED",
            "raw_sha256": raw_hash,
            "error_code": error_code,
            "snapshot": publication["snapshot"],
            "snapshot_id": publication["snapshot_id"],
            "transport": receipt["transport"] if receipt is not None else None,
        }


def acquire_and_run(config_path: Path, partition: str, *, transport=None, clock=None) -> dict:
    """Acquire one exact configured URL, then archive and promote it in one operator action."""
    from market_data_pipeline.pipelines.sec_fails_to_deliver.acquisition import acquire_partition

    acquired = acquire_partition(config_path, partition, transport=transport, clock=clock)
    return run(config_path, partition, clock=clock, acquired=acquired)


def gold(
    config_path: Path,
    *,
    limit: int,
    as_of: datetime | None = None,
    as_of_sequence: int | None = None,
    include_withdrawn: bool = False,
) -> dict:
    clock = knowledge_cutoff(as_of, as_of_sequence)
    as_of = datetime.fromisoformat(clock) if clock is not None else None
    config = _runtime(config_path)
    if not (config.root / ".warehouse.json").is_file() or (
        config.warehouse == "sqlite" and not (config.root / "warehouse.sqlite3").is_file()
    ):
        raise ValueError("SEC FTD warehouse does not exist")
    with warehouse_for(config) as warehouse:
        cutoff = {"as_of": as_of, "as_of_sequence": as_of_sequence}
        rows = warehouse.select_facts(limit=limit, include_withdrawn=include_withdrawn, **cutoff)
        count = warehouse.count_facts(include_withdrawn=include_withdrawn, **cutoff)
        return {
            "pipeline": "sec-fails-to-deliver",
            "knowledge_mode": "OBSERVED_AS_OF" if as_of is not None else "CURRENT_VINTAGE",
            "as_of": as_of,
            "as_of_sequence": as_of_sequence,
            "source_coverage": "NOT_CERTIFIED",
            "row_count": count,
            "rows": rows,
            "coverage": warehouse.select_coverage(**cutoff),
        }
