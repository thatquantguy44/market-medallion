"""Explicit reviewed assertions for one complete archive, not global coverage."""

import json
import re
from dataclasses import dataclass
from datetime import date

from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import Manifest


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate completeness evidence key")
        result[key] = value
    return result


@dataclass(frozen=True)
class CompletenessEvidence:
    period_start: date
    period_end: date
    member_name: str
    raw_sha256: str
    expected_dates: tuple[date, ...]
    source_reference: str
    calendar_reference: str
    review_reference: str

    def __post_init__(self):
        if (
            type(self.period_start) is not date
            or type(self.period_end) is not date
            or self.period_start > self.period_end
        ):
            raise ValueError("invalid completeness partition bounds")
        if not isinstance(self.raw_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.raw_sha256
        ):
            raise ValueError("completeness evidence requires an exact archive SHA-256")
        if (
            not isinstance(self.member_name, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*\.txt", self.member_name)
            or ".." in self.member_name
        ):
            raise ValueError("completeness evidence requires a safe explicit member")
        if (
            not isinstance(self.expected_dates, tuple)
            or not 1 <= len(self.expected_dates) <= 31
            or any(
                type(d) is not date or not self.period_start <= d <= self.period_end
                for d in self.expected_dates
            )
            or tuple(sorted(set(self.expected_dates))) != self.expected_dates
        ):
            raise ValueError(
                "expected settlement dates must be nonempty, sorted, unique and in bounds"
            )
        for value in (self.source_reference, self.calendar_reference, self.review_reference):
            if (
                not isinstance(value, str)
                or not value.strip()
                or len(value) > 2048
                or any(ord(c) < 32 for c in value)
            ):
                raise ValueError(
                    "completeness evidence requires source, calendar and review references"
                )

    def validate(self, *, raw_sha256: str, observed_dates: tuple[date, ...]) -> None:
        if raw_sha256 != self.raw_sha256 or observed_dates != self.expected_dates:
            raise ValueError(
                "archive hash or observed settlement dates contradict completeness evidence"
            )


def parse_evidence(payload: bytes, manifest: Manifest) -> CompletenessEvidence:
    if not isinstance(payload, bytes) or len(payload) > 65536:
        raise ValueError("completeness evidence must be JSON within 64 KiB")
    try:
        value = json.loads(payload, object_pairs_hook=_unique)
        fields = {
            "schema_version",
            "source",
            "dataset",
            "reporting_regime",
            "period_start",
            "period_end",
            "member_name",
            "raw_sha256",
            "expected_settlement_dates",
            "source_reference",
            "calendar_reference",
            "review_reference",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("completeness evidence fields differ from the supported contract")
        constants = {
            "schema_version": "sec-ftd-completeness/1",
            "source": "sec",
            "dataset": "sec-fails-to-deliver",
            "reporting_regime": "semi-monthly-2009+",
            "period_start": manifest.period_start.isoformat(),
            "period_end": manifest.period_end.isoformat(),
            "member_name": manifest.member_name,
        }
        if any(value[key] != expected for key, expected in constants.items()):
            raise ValueError("completeness evidence does not identify the selected SEC partition")
        dates = value["expected_settlement_dates"]
        if not isinstance(dates, list) or any(
            not isinstance(d, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", d)
            for d in dates
        ):
            raise ValueError("expected settlement dates require ISO date labels")
        return CompletenessEvidence(
            manifest.period_start,
            manifest.period_end,
            value["member_name"],
            value["raw_sha256"],
            tuple(date.fromisoformat(d) for d in dates),
            value["source_reference"],
            value["calendar_reference"],
            value["review_reference"],
        )
    except (TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid completeness evidence JSON") from exc
