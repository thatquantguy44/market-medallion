"""Versioned SEC FTD record types and warehouse boundary, without persistence."""

import re
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from market_data_pipeline.pipelines.sec_fails_to_deliver.completeness import CompletenessEvidence
from market_data_pipeline.pipelines.sec_fails_to_deliver.config import StorageConfig
from market_data_pipeline.shared.storage_types import Column, RecordSchema

IDENTITY = {
    "source": "sec",
    "dataset": "sec-fails-to-deliver",
    "reporting_regime": "semi-monthly-2009+",
}


def _columns(kind: str, names: str, *, nullable=False) -> tuple[Column, ...]:
    return tuple(Column(name, kind, nullable) for name in names.split())


def _validate(name: str, row: dict) -> None:
    if any(row[key] != value for key, value in IDENTITY.items()):
        raise ValueError("SEC FTD storage identity differs from the contract")
    for key, value in row.items():
        if value is None:
            continue
        if key.endswith("sha256") or key == "content_hash":
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise ValueError("SEC FTD lineage requires SHA-256 hex digests")
        if key.endswith("_count") or key == "row_count":
            if value < 0:
                raise ValueError("SEC FTD metadata counts cannot be negative")
    if "capture_sequence" in row and row["capture_sequence"] < 1:
        raise ValueError("SEC FTD capture sequence must be positive")
    if name == "facts":
        if not re.fullmatch(r"[A-Z0-9*@#]{9}", row["cusip"]):
            raise ValueError("invalid SEC FTD storage CUSIP")
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*\.txt", row["raw_member"])
            or ".." in row["raw_member"]
        ):
            raise ValueError("invalid SEC FTD raw member")
        if row["price_basis"] != "PROVIDER_PREVIOUS_CLOSE":
            raise ValueError("invalid SEC FTD price basis")
        observation_fields = (
            "symbol_raw",
            "description_raw",
            "quantity_fails",
            "quantity_fails_text",
            "prior_close_price",
            "prior_close_price_text",
            "price_missing_reason",
            "source_row_number",
        )
        if row["row_state"] == "WITHDRAWN":
            if any(row[key] is not None for key in observation_fields):
                raise ValueError("withdrawal must not manufacture an observation")
            return
        if row["row_state"] != "REPORTED":
            raise ValueError("invalid SEC FTD row state")
        token = row["quantity_fails_text"]
        if not isinstance(token, str) or not re.fullmatch(r"[0-9]+", token):
            raise ValueError("invalid SEC FTD exact quantity text")
        if row["quantity_fails"] != Decimal(token):
            raise ValueError("SEC FTD quantity projection differs from exact text")
        if row["source_row_number"] is None or row["source_row_number"] < 2:
            raise ValueError("SEC FTD observation requires its source row")
        token = row["prior_close_price_text"]
        if token == ".":
            if (
                row["prior_close_price"] is not None
                or row["price_missing_reason"] != "SOURCE_PRICE_UNAVAILABLE"
            ):
                raise ValueError("SEC FTD missing price requires the source null reason")
        elif (
            not isinstance(token, str)
            or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", token)
            or row["prior_close_price"] != Decimal(token)
            or row["price_missing_reason"] is not None
        ):
            raise ValueError("SEC FTD price projection differs from exact text")
    elif name == "coverage":
        if row["period_start"] > row["period_end"]:
            raise ValueError("invalid SEC FTD coverage bounds")
        if row["coverage_state"] not in (
            "COMPLETE",
            "COMPLETE_EMPTY",
            "NOT_PUBLISHED",
            "UNAVAILABLE",
            "PARTIAL",
            "FAILED",
        ):
            raise ValueError("invalid SEC FTD coverage state")
        if row["validation_state"] not in ("MATCHED", "NOT_CHECKED", "REJECTED"):
            raise ValueError("invalid SEC FTD validation state")
        if row["coverage_state"] in ("COMPLETE", "COMPLETE_EMPTY") and (
            row["validation_state"] != "MATCHED"
            or row["trailer_record_count"] != row["received_row_count"]
            or row["trailer_quantity_text"] is None
            or row["raw_sha256"] is None
        ):
            raise ValueError("complete SEC FTD coverage requires matching trailer evidence")
        token = row["trailer_quantity_text"]
        if token is not None and not re.fullmatch(r"[0-9]+", token):
            raise ValueError("invalid SEC FTD trailer quantity text")
        if row["coverage_state"] == "COMPLETE_EMPTY" and row["received_row_count"] != 0:
            raise ValueError("complete-empty coverage cannot contain rows")
    elif name == "audit":
        if row["status"] not in ("STARTED", "SUCCEEDED", "PARTIAL", "REJECTED", "FAILED"):
            raise ValueError("invalid SEC FTD audit status")
        if (row["status"] == "STARTED") != (row["completed_at"] is None):
            raise ValueError("SEC FTD audit status and completion clock disagree")
        if row["completed_at"] is not None and row["completed_at"] < row["captured_at"]:
            raise ValueError("SEC FTD completion precedes capture")
    elif name == "publications":
        if row["status"] not in ("STAGED", "COMMITTED", "PUBLISHED", "ABANDONED"):
            raise ValueError("invalid SEC FTD publication status")
        if row["status"] == "PUBLISHED" and row["published_at"] is None:
            raise ValueError("published SEC FTD snapshot requires a publication clock")
        if row["published_at"] is not None and row["published_at"] < row["generated_at"]:
            raise ValueError("SEC FTD publication precedes generation")
        path = row["snapshot_path"]
        if "\\" in path or any(part in ("", ".", "..") for part in path.split("/")):
            raise ValueError("SEC FTD snapshot path must be relative and traversal-free")


def storage_schemas(settings: StorageConfig) -> dict[str, RecordSchema]:
    """Field names/semantics are fixed; numeric projection bounds are configured."""
    identity = _columns("string", "source dataset reporting_regime")
    fields = {
        "facts": (
            *_columns("date", "settlement_date"),
            *_columns("string", "cusip"),
            *_columns(
                "string",
                "symbol_raw description_raw quantity_fails_text "
                "prior_close_price_text price_missing_reason",
                nullable=True,
            ),
            Column("quantity_fails", "decimal", True, settings.quantity_precision, 0),
            Column(
                "prior_close_price", "decimal", True, settings.price_precision, settings.price_scale
            ),
            *_columns(
                "string",
                "price_basis row_state partition_id raw_member version_id content_hash "
                "raw_sha256 member_sha256 run_id parser_version",
            ),
            *_columns("int64", "source_row_number", nullable=True),
            *_columns("int64", "capture_sequence"),
            *_columns("timestamp", "first_seen_at retrieved_at"),
            *_columns("timestamp", "provider_published_at source_updated_at", nullable=True),
        ),
        "coverage": (
            *_columns(
                "string",
                "partition_id coverage_state validation_state version_id run_id",
            ),
            *_columns("string", "raw_sha256", nullable=True),
            *_columns("date", "period_start period_end"),
            *_columns("int64", "expected_date_count trailer_record_count", nullable=True),
            *_columns("int64", "observed_date_count received_row_count capture_sequence"),
            *_columns("string", "trailer_quantity_text", nullable=True),
            *_columns("timestamp", "first_seen_at"),
        ),
        "audit": (
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
            f"sec-ftd-{name}/{'2' if name == 'coverage' else '1'}",
            settings.contract_id,
            (*identity, *columns),
            lambda row, name=name: _validate(name, row),
        )
        for name, columns in fields.items()
    }


class Warehouse(Protocol):
    """T04/T19 adapters must implement this boundary under one root contract.

    Calls accept/return normalized schema records. Promotion atomically commits
    facts, coverage and audit. Selection orders equal clocks by capture_sequence.
    """

    def transaction(self) -> AbstractContextManager[None]: ...
    def next_capture_sequence(self) -> int: ...
    def start_replay_run(self, row: Mapping[str, object]) -> None: ...
    def fact_fingerprint(self, run_id: str) -> Mapping[str, object]: ...
    def record_run(self, row: Mapping[str, object]) -> None: ...
    def promote_partition(
        self,
        facts: Sequence[Mapping[str, object]],
        coverage: Mapping[str, object],
        audit: Mapping[str, object],
        *,
        evidence: CompletenessEvidence | None = None,
        capture: Mapping[str, object] | None = None,
    ) -> int: ...
    def select_facts(
        self,
        *,
        as_of: datetime | None = None,
        as_of_sequence: int | None = None,
        include_withdrawn: bool = False,
        limit: int | None = None,
    ) -> Sequence[dict]: ...
    def count_facts(
        self,
        *,
        as_of: datetime | None = None,
        as_of_sequence: int | None = None,
        include_withdrawn: bool = False,
    ) -> int: ...
    def select_coverage(
        self, *, as_of: datetime | None = None, as_of_sequence: int | None = None
    ) -> Sequence[dict]: ...
    def select_runs(self) -> Sequence[dict]: ...
    def record_publication(self, row: Mapping[str, object]) -> None: ...
    def pending_publications(self) -> Sequence[dict]: ...
