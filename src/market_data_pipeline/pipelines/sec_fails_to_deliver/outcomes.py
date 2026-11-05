"""Immutable admission evidence for deterministic SEC FTD replay."""

import hashlib
import json
import re
from datetime import datetime

from market_data_pipeline.pipelines.sec_fails_to_deliver.bronze import (
    _read_bounded,
    _sync,
    _write_once,
)
from market_data_pipeline.pipelines.sec_fails_to_deliver.storage import storage_schemas


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate outcome key")
        result[key] = value
    return result


def validate_lineage(receipt, audit, coverage):
    clock = datetime.fromisoformat(receipt["captured_at"])
    if audit["status"] not in ("SUCCEEDED", "REJECTED", "FAILED"):
        raise ValueError("replay requires an explicit supported terminal outcome")
    if (
        audit["run_id"] != receipt["run_id"]
        or audit["capture_sequence"] != receipt["capture_sequence"]
        or audit["captured_at"] != clock
        or audit["raw_sha256"] != receipt["raw_sha256"]
        or coverage["run_id"] != audit["run_id"]
        or coverage["capture_sequence"] != audit["capture_sequence"]
        or coverage["first_seen_at"] != clock
        or coverage["raw_sha256"] != audit["raw_sha256"]
        or coverage["partition_id"] != receipt["partition_id"]
        or f"{coverage['period_start']}_{coverage['period_end']}" != receipt["partition_id"]
        or coverage["version_id"]
        != hashlib.sha256(f"coverage:{audit['run_id']}".encode()).hexdigest()
    ):
        raise ValueError("original outcome disagrees with its capture lineage")
    accepted = audit["status"] == "SUCCEEDED"
    if accepted:
        if (
            audit["error_code"] is not None
            or coverage["validation_state"] != "MATCHED"
            or coverage["coverage_state"] not in ("PARTIAL", "COMPLETE")
            or audit["row_count"] != coverage["received_row_count"]
        ):
            raise ValueError("accepted outcome contradicts its original coverage")
    elif (
        audit["row_count"] != 0
        or coverage["coverage_state"] not in ("FAILED", "UNAVAILABLE")
        or coverage["validation_state"] not in ("REJECTED", "NOT_CHECKED")
    ):
        raise ValueError("rejected/failed outcome cannot admit observations")


def make_outcome(directory, settings, audit, coverage, fingerprint):
    schemas = storage_schemas(settings)
    value = {
        "outcome_version": "sec-ftd-outcome/1",
        "receipt_sha256": hashlib.sha256(
            _read_bounded(directory / "receipt.json", 65536)
        ).hexdigest(),
        "storage_contract_id": settings.contract_id,
        "record_schemas": {name: schema.version for name, schema in schemas.items()},
        "audit": schemas["audit"].encode(audit, "sqlite"),
        "coverage": schemas["coverage"].encode(coverage, "sqlite"),
        "facts": fingerprint,
    }
    return {**value, "sha256": hashlib.sha256(_canonical(value)).hexdigest()}


def load_outcome(directory, receipt, settings):
    try:
        value = json.loads(
            _read_bounded(directory / "outcome.json", 65536), object_pairs_hook=_unique
        )
        if not isinstance(value, dict) or set(value) != {
            "outcome_version",
            "receipt_sha256",
            "storage_contract_id",
            "record_schemas",
            "audit",
            "coverage",
            "facts",
            "sha256",
        }:
            raise ValueError("unsupported outcome fields")
        data = {key: item for key, item in value.items() if key != "sha256"}
        schemas = storage_schemas(settings)
        if (
            value["outcome_version"] != "sec-ftd-outcome/1"
            or value["sha256"] != hashlib.sha256(_canonical(data)).hexdigest()
            or value["receipt_sha256"]
            != hashlib.sha256(_read_bounded(directory / "receipt.json", 65536)).hexdigest()
            or value["storage_contract_id"] != settings.contract_id
            or value["record_schemas"] != {name: schema.version for name, schema in schemas.items()}
        ):
            raise ValueError("outcome integrity/schema verification failed")
        facts = value["facts"]
        if (
            not isinstance(facts, dict)
            or set(facts) != {"count", "sha256"}
            or type(facts["count"]) is not int
            or not 0 <= facts["count"] < 2**63
            or not isinstance(facts["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", facts["sha256"])
        ):
            raise ValueError("invalid outcome fact fingerprint")
        audit = schemas["audit"].decode(value["audit"], "sqlite")
        coverage = schemas["coverage"].decode(value["coverage"], "sqlite")
        validate_lineage(receipt, audit, coverage)
        if audit["status"] != "SUCCEEDED" and facts["count"] != 0:
            raise ValueError("rejected outcome cannot contain fact versions")
        return value, audit, coverage
    except (KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid SEC FTD outcome receipt") from exc


def write_outcome(directory, value):
    _write_once(directory / "outcome.json", _canonical(value) + b"\n")
    _sync(directory)
