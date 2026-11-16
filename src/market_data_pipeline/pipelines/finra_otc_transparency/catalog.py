"""OTC-T02: six disjoint weekly detail populations, not current source coverage.

Historical field/summary evidence: specs/spec013/source_review.md.
No API tier alias or live schema conformance is inferred from a file example.
"""

import re
from dataclasses import dataclass
from datetime import date

CATALOG_VERSION = "finra-otc-weekly-catalog/1"
ATS_FIXTURE_SCHEMA = "finra-otc-ats-weekly-pipe-fixture/1"
MAX_PARTITIONS = 312  # Local request-planning ceiling, not a provider limit.


@dataclass(frozen=True)
class ReportProfile:
    product: str
    tier: str
    scope_id: str
    schema_profile: str
    summary_code: str
    expected_delay_weeks: int
    parser_status: str
    conformance: str = "NOT_CHECKED"
    reviewed_on: str = "2026-11-15"
    evidence: str = "specs/spec013/source_review.md"

    def to_dict(self) -> dict:
        return {
            "product": self.product,
            "tier": self.tier,
            "scope_id": self.scope_id,
            "schema_profile": self.schema_profile,
            "summary_code": self.summary_code,
            "expected_delay_weeks": self.expected_delay_weeks,
            "parser_status": self.parser_status,
            "conformance": self.conformance,
            "reviewed_on": self.reviewed_on,
            "evidence": self.evidence,
        }


PROFILES = tuple(
    ReportProfile(
        product=product,
        tier=tier,
        scope_id=f"finra-otc:weekly:{product}:{tier}",
        schema_profile=(
            ATS_FIXTURE_SCHEMA
            if (product, tier) == ("ATS_WEEKLY_DETAIL", "T1")
            else f"finra-otc-{label}-weekly-pipe-candidate/1"
        ),
        summary_code=summary,
        expected_delay_weeks=2 if tier == "T1" else 4,
        parser_status=(
            "INVENTED_FIXTURE_ONLY"
            if (product, tier) == ("ATS_WEEKLY_DETAIL", "T1")
            else "NOT_IMPLEMENTED"
        ),
    )
    for product, label, summary in (
        ("ATS_WEEKLY_DETAIL", "ats", "ATS_W_SMBL_FIRM"),
        ("NON_ATS_WEEKLY_DETAIL", "non-ats", "OTC_W_SMBL_FIRM"),
    )
    for tier in ("T1", "T2", "OTCE")
)


def profile_for(product: object, tier: object) -> ReportProfile:
    for profile in PROFILES:
        if product == profile.product and tier == profile.tier:
            return profile
    raise ValueError("unknown OTC weekly detail product/tier; aliases and summaries are excluded")


def iso_date(value: object, *, field: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError(f"OTC {field} must be a literal ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"OTC {field} must be a valid ISO date") from None


@dataclass(frozen=True)
class Partition:
    reporting_week_start: date
    product: str
    tier: str

    def __post_init__(self):
        if type(self.reporting_week_start) is not date or self.reporting_week_start.weekday() != 0:
            raise ValueError("OTC reporting week must be a Monday date")
        profile_for(self.product, self.tier)

    @property
    def scope_id(self) -> str:
        return profile_for(self.product, self.tier).scope_id


def select_partitions(
    values: object, *, max_partitions: int = MAX_PARTITIONS
) -> tuple[Partition, ...]:
    """Enumerate only an explicit bounded list; never expand an implicit 'all'.

    Each entry declares one reporting Monday, product and tier. Duplicate keys
    overlap exactly the same population and are rejected rather than summed.
    No earliest/latest availability or release-calendar conformance is claimed.
    """
    if type(max_partitions) is not int or not 1 <= max_partitions <= MAX_PARTITIONS:
        raise ValueError("OTC partition budget must be an integer in 1..312")
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= max_partitions:
        raise ValueError("OTC partitions require a nonempty finite list within the budget")
    result, seen = [], set()
    for value in values:
        if not isinstance(value, dict) or set(value) != {"reporting_week_start", "product", "tier"}:
            raise ValueError("OTC partition fields must be reporting_week_start, product and tier")
        partition = Partition(
            iso_date(value["reporting_week_start"], field="reporting_week_start"),
            value["product"],
            value["tier"],
        )
        if partition in seen:
            raise ValueError("duplicate OTC population in requested partitions")
        seen.add(partition)
        result.append(partition)
    return tuple(result)
