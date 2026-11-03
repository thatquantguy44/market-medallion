"""Independent threshold types, without assuming an exchange's physical format."""

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Protocol

from market_data_pipeline.shared.storage_types import Column, RecordSchema


def _columns(kind: str, names: str, *, nullable=False) -> tuple[Column, ...]:
    return tuple(Column(name, kind, nullable) for name in names.split())


def _unique_flags(pairs):
    result = {}
    for key, value in pairs:
        if key in result or not isinstance(value, str):
            raise ValueError("threshold raw flags require unique text tokens")
        result[key] = value
    return result


def _validate(name: str, row: dict) -> None:
    if row["dataset"] != "reg-sho-thresholds":
        raise ValueError("threshold storage dataset differs from the contract")
    if name != "publications" and (
        row["provider"] not in ("nasdaq", "nyse") or not row["market"].strip()
    ):
        raise ValueError("threshold records require independent provider/market scope")
    for key, value in row.items():
        if value is None:
            continue
        if key.endswith("sha256") or key == "content_hash":
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise ValueError("threshold lineage requires SHA-256 hex digests")
        if key.endswith("_count") and value < 0:
            raise ValueError("threshold metadata counts cannot be negative")
    if "capture_sequence" in row and row["capture_sequence"] < 1:
        raise ValueError("threshold capture sequence must be positive")
    if name == "membership":
        if not row["raw_symbol"].strip() or not row["rule_family"].strip():
            raise ValueError("threshold membership requires native symbol and rule family")
        attributes = ("source_issue_id", "security_name_raw", "flags_raw_json", "source_row_number")
        if row["row_state"] == "WITHDRAWN":
            if any(row[key] is not None for key in attributes):
                raise ValueError("threshold withdrawal cannot manufacture membership attributes")
            return
        if (
            row["row_state"] != "REPORTED"
            or row["source_row_number"] is None
            or row["source_row_number"] < 1
        ):
            raise ValueError("reported threshold member requires source-row lineage")
        if row["flags_raw_json"] is not None:
            try:
                flags = json.loads(row["flags_raw_json"], object_pairs_hook=_unique_flags)
                if not isinstance(flags, dict):
                    raise ValueError("flags must be an object")
            except ValueError as exc:
                raise ValueError("invalid threshold raw flag tokens") from exc
    elif name == "coverage":
        state = row["coverage_state"]
        if state not in (
            "COMPLETE",
            "COMPLETE_EMPTY",
            "NOT_PUBLISHED",
            "UNAVAILABLE",
            "PARTIAL",
            "FAILED",
        ):
            raise ValueError("invalid threshold coverage state")
        if row["validation_state"] not in ("VALIDATED", "NOT_CHECKED", "REJECTED"):
            raise ValueError("invalid threshold validation state")
        if state in ("COMPLETE", "COMPLETE_EMPTY"):
            if (
                row["validation_state"] != "VALIDATED"
                or not row["requested_date"] == row["list_date"] == row["source_date"]
                or (
                    row["expected_row_count"] is not None
                    and row["expected_row_count"] != row["received_row_count"]
                )
                or (state == "COMPLETE_EMPTY") != (row["received_row_count"] == 0)
            ):
                raise ValueError(
                    "complete threshold coverage requires consistent date/count validation"
                )
    elif name == "audit":
        if row["status"] not in ("STARTED", "SUCCEEDED", "PARTIAL", "REJECTED", "FAILED"):
            raise ValueError("invalid threshold audit status")
        if (row["status"] == "STARTED") != (row["completed_at"] is None):
            raise ValueError("threshold audit status and completion clock disagree")
        if row["completed_at"] is not None and row["completed_at"] < row["captured_at"]:
            raise ValueError("threshold completion precedes capture")
    elif name == "publications":
        if row["status"] not in ("STAGED", "COMMITTED", "PUBLISHED", "ABANDONED"):
            raise ValueError("invalid threshold publication status")
        if row["status"] == "PUBLISHED" and row["published_at"] is None:
            raise ValueError("published threshold snapshot requires a publication clock")
        if row["published_at"] is not None and row["published_at"] < row["generated_at"]:
            raise ValueError("threshold publication precedes generation")
        path = row["snapshot_path"]
        if "\\" in path or any(part in ("", ".", "..") for part in path.split("/")):
            raise ValueError("threshold snapshot path must be relative and traversal-free")


def storage_schemas(*, schema_version="reg-sho-storage/1") -> dict[str, RecordSchema]:
    """Pin normalized fields only; T08/T10 independently admit physical inputs."""
    if schema_version != "reg-sho-storage/1":
        raise ValueError("unsupported threshold storage schema version")
    contract_id = hashlib.sha256(f"{schema_version}:UTC:source-native".encode()).hexdigest()
    scope = _columns("string", "provider market")
    fields = {
        "membership": (
            *scope,
            *_columns("date", "list_date"),
            *_columns(
                "string",
                "raw_symbol rule_family row_state partition_id version_id "
                "content_hash raw_sha256 run_id",
            ),
            *_columns("string", "source_issue_id security_name_raw flags_raw_json", nullable=True),
            *_columns("int64", "source_row_number", nullable=True),
            *_columns("int64", "capture_sequence"),
            *_columns("timestamp", "first_seen_at retrieved_at"),
            *_columns("timestamp", "provider_published_at", nullable=True),
        ),
        "coverage": (
            *scope,
            *_columns("date", "list_date requested_date"),
            *_columns("date", "source_date", nullable=True),
            *_columns("string", "source_created_at_raw", nullable=True),
            *_columns("timestamp", "source_created_at", nullable=True),
            *_columns(
                "string",
                "calendar_version partition_id coverage_state validation_state "
                "version_id raw_sha256 run_id",
            ),
            *_columns("int64", "expected_row_count", nullable=True),
            *_columns("int64", "received_row_count capture_sequence"),
            *_columns("timestamp", "first_seen_at"),
        ),
        "audit": (
            *scope,
            *_columns("string", "run_id status"),
            *_columns("int64", "capture_sequence row_count"),
            *_columns("timestamp", "captured_at"),
            *_columns("timestamp", "completed_at", nullable=True),
            *_columns("string", "raw_sha256 error_code", nullable=True),
        ),
        "publications": (
            *_columns("string", "snapshot_id run_id status manifest_sha256 snapshot_path"),
            *_columns("timestamp", "generated_at"),
            *_columns("timestamp", "published_at", nullable=True),
        ),
    }
    return {
        name: RecordSchema(
            f"reg-sho-{name}/1",
            contract_id,
            (*_columns("string", "dataset"), *columns),
            lambda row, name=name: _validate(name, row),
        )
        for name, columns in fields.items()
    }


class Warehouse(Protocol):
    """Source-owned boundary for T09/T20; no concrete adapter is supplied here."""

    def transaction(self) -> AbstractContextManager[None]: ...
    def next_capture_sequence(self) -> int: ...
    def record_run(self, row: Mapping[str, object]) -> None: ...
    def promote_partition(
        self,
        membership: Sequence[Mapping[str, object]],
        coverage: Mapping[str, object],
        audit: Mapping[str, object],
    ) -> int: ...
    def select_membership(self, *, as_of: datetime | None = None) -> Sequence[dict]: ...
    def select_coverage(self, *, as_of: datetime | None = None) -> Sequence[dict]: ...
    def select_runs(self) -> Sequence[dict]: ...
    def record_publication(self, row: Mapping[str, object]) -> None: ...
    def pending_publications(self) -> Sequence[dict]: ...
