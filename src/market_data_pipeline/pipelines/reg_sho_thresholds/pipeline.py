"""Versioned local Nasdaq/NYSE Reg SHO threshold-list state."""

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.reg_sho_thresholds.config import load_config
from market_data_pipeline.pipelines.reg_sho_thresholds.manifest import load_manifest
from market_data_pipeline.pipelines.reg_sho_thresholds.parser import parse_list
from market_data_pipeline.pipelines.reg_sho_thresholds.warehouse import warehouse_for
from market_data_pipeline.shared.files import utc_timestamp


def _write_once(path: Path, payload: bytes):
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _hash(value) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def run(config_path: Path, partition: str, *, clock=None, acquired=None):
    config = load_config(Path(config_path))
    if partition not in config.partitions:
        raise ValueError("partition is not present in the threshold config")
    selected = config.partitions[partition]
    if acquired is not None and acquired.metadata.get("partition") != partition:
        raise ValueError("acquired threshold payload differs from selected partition")
    manifest_payload = selected.manifest.read_bytes()
    manifest = load_manifest(selected.manifest)
    if manifest.provider != selected.provider or selected.market not in manifest.allowed_markets:
        raise ValueError("threshold partition differs from its provider manifest")
    now = clock or (lambda: datetime.now(UTC))
    started_at = datetime.fromisoformat(utc_timestamp(now()))
    run_id = uuid.uuid4().hex
    input_path = acquired.path if acquired is not None else selected.input
    with warehouse_for(config) as warehouse:
        with warehouse.transaction():
            sequence = warehouse.next_capture_sequence()
            started = {
                "dataset": "reg-sho-thresholds",
                "provider": selected.provider,
                "market": selected.market,
                "run_id": run_id,
                "capture_sequence": sequence,
                "captured_at": started_at,
                "completed_at": None,
                "status": "STARTED",
                "row_count": 0,
                "raw_sha256": None,
                "error_code": None,
            }
            warehouse.record_run(started)
        maximum = manifest.max_bytes
        try:
            if Path(input_path).is_symlink() or not Path(input_path).is_file():
                raise OSError("threshold input is unavailable")
            with Path(input_path).open("rb") as stream:
                payload = stream.read(maximum + 1)
            if len(payload) > maximum:
                raise ValueError("threshold payload exceeds its byte limit")
            raw_hash = hashlib.sha256(payload).hexdigest()
            parsed = parse_list(
                payload, manifest, list_date=selected.list_date, market=selected.market
            )
            status, error_code = "SUCCEEDED", None
        except OSError:
            payload, raw_hash, parsed = None, None, None
            status, error_code = "FAILED", "SOURCE_UNAVAILABLE"
        except ValueError:
            raw_hash = hashlib.sha256(payload).hexdigest()
            parsed = None
            status, error_code = "REJECTED", "INVALID_LIST"
        captured_at = datetime.fromisoformat(utc_timestamp(now()))
        bronze = config.root / "bronze" / run_id
        bronze.mkdir(parents=True, exist_ok=False)
        _write_once(bronze / "manifest.yml", manifest_payload)
        if payload is not None:
            _write_once(bronze / "payload.bin", payload)
        receipt = {
            "receipt_version": "reg-sho-capture/1",
            "run_id": run_id,
            "capture_sequence": sequence,
            "partition": partition,
            "provider": selected.provider,
            "market": selected.market,
            "list_date": selected.list_date.isoformat(),
            "transport": "https" if acquired is not None else "local-file",
            "source_url": acquired.metadata["source_url"] if acquired is not None else None,
            "origin_retrieved_at": (
                acquired.metadata["retrieved_at"] if acquired is not None else None
            ),
            "acquisition": acquired.metadata if acquired is not None else None,
            "captured_at": utc_timestamp(captured_at),
            "source_state": "AVAILABLE" if payload is not None else "UNAVAILABLE",
            "raw_sha256": raw_hash,
            "captured_bytes": len(payload) if payload is not None else 0,
            "status": status,
            "error_code": error_code,
        }
        _write_once(
            bronze / "receipt.json",
            (json.dumps(receipt, sort_keys=True, indent=2) + "\n").encode(),
        )
        members = []
        partition_id = f"{selected.provider}:{selected.market}:{selected.list_date.isoformat()}"
        if parsed is not None:
            for row in parsed.rows:
                values = {
                    "security_name_raw": row.security_name_raw,
                    "flags_raw_json": row.flags_raw_json,
                    "row_state": "REPORTED",
                }
                content_hash = _hash(values)
                members.append(
                    {
                        "dataset": "reg-sho-thresholds",
                        "provider": selected.provider,
                        "market": selected.market,
                        "list_date": selected.list_date,
                        "raw_symbol": row.raw_symbol,
                        "rule_family": "REG_SHO_203",
                        "source_issue_id": None,
                        "security_name_raw": row.security_name_raw,
                        "flags_raw_json": row.flags_raw_json,
                        "row_state": "REPORTED",
                        "source_row_number": row.source_row_number,
                        "partition_id": partition_id,
                        "version_id": _hash(
                            {"run_id": run_id, "symbol": row.raw_symbol, "content": content_hash}
                        ),
                        "content_hash": content_hash,
                        "raw_sha256": raw_hash,
                        "run_id": run_id,
                        "capture_sequence": sequence,
                        "first_seen_at": captured_at,
                        "retrieved_at": captured_at,
                        "provider_published_at": None,
                    }
                )
        if parsed is not None:
            coverage_state = "COMPLETE" if parsed.rows else "COMPLETE_EMPTY"
            validation_state = "VALIDATED"
        elif status == "FAILED":
            coverage_state, validation_state = "UNAVAILABLE", "NOT_CHECKED"
        else:
            coverage_state, validation_state = "FAILED", "REJECTED"
        coverage = {
            "dataset": "reg-sho-thresholds",
            "provider": selected.provider,
            "market": selected.market,
            "list_date": selected.list_date,
            "requested_date": selected.list_date,
            "source_date": parsed.list_date if parsed is not None else None,
            "source_created_at_raw": parsed.source_created_at_raw if parsed is not None else None,
            "source_created_at": None,
            "calendar_version": "provider-list-date/1",
            "partition_id": partition_id,
            "coverage_state": coverage_state,
            "validation_state": validation_state,
            "expected_row_count": None,
            "received_row_count": len(members),
            "version_id": _hash({"coverage": run_id}),
            "raw_sha256": raw_hash,
            "run_id": run_id,
            "capture_sequence": sequence,
            "first_seen_at": captured_at,
        }
        audit = {
            **started,
            "captured_at": captured_at,
            "completed_at": datetime.fromisoformat(utc_timestamp(now())),
            "status": status,
            "row_count": len(members) if status == "SUCCEEDED" else 0,
            "raw_sha256": raw_hash,
            "error_code": error_code,
        }
        inserted = warehouse.promote_partition(members, coverage, audit)
        return {
            "pipeline": "reg-sho-thresholds",
            "provider": selected.provider,
            "market": selected.market,
            "list_date": selected.list_date.isoformat(),
            "run_id": run_id,
            "capture_sequence": sequence,
            "status": status,
            "coverage_state": coverage_state,
            "row_count": audit["row_count"],
            "inserted_versions": inserted,
            "raw_sha256": raw_hash,
            "error_code": error_code,
            "transport": receipt["transport"],
        }


def acquire_and_run(config_path: Path, partition: str, *, transport=None, clock=None):
    from market_data_pipeline.pipelines.reg_sho_thresholds.acquisition import acquire_partition

    acquired = acquire_partition(config_path, partition, transport=transport, clock=clock)
    return run(config_path, partition, clock=clock, acquired=acquired)


def gold(config_path: Path, *, as_of=None, as_of_sequence=None, include_withdrawn=False):
    config = load_config(Path(config_path))
    if not (config.root / ".warehouse.json").is_file():
        raise ValueError("threshold warehouse does not exist")
    with warehouse_for(config, read_only=True) as warehouse:
        rows = warehouse.select_membership(
            as_of=as_of,
            as_of_sequence=as_of_sequence,
            include_withdrawn=include_withdrawn,
        )
        return {
            "pipeline": "reg-sho-thresholds",
            "knowledge_mode": "OBSERVED_AS_OF" if as_of is not None else "CURRENT_VINTAGE",
            "rows": rows,
            "row_count": len(rows),
            "coverage": warehouse.select_coverage(as_of=as_of, as_of_sequence=as_of_sequence),
        }
