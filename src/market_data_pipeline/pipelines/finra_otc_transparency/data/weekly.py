"""OTC-T03/T06: strict invented ATS and non-ATS pipe profiles; not current live conformance.

Header evidence: FINRA file API v0.4, p.10 (2019-01-16):
https://www.finra.org/sites/default/files/OTC-Transparency-Data-File-Download-API-v04.pdf
Only file syntax is established. Completeness, identity reconciliation, original
publication and capture/first-seen clocks belong to later tasks.
"""

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from datetime import date

from market_data_pipeline.pipelines.finra_otc_transparency.catalog import (
    ATS_FIXTURE_SCHEMA,
    NON_ATS_FIXTURE_SCHEMA,
    Partition,
    iso_date,
    profile_for,
)
from market_data_pipeline.pipelines.finra_otc_transparency.data.identity import (
    ParticipantIdentity,
    participant_identity,
    require_unique_participants,
)

ATS_HEADER = (
    "tierIdentifier",
    "issueSymbolIdentifier",
    "issueName",
    "marketParticipantName",
    "MPID",
    "totalWeeklyShareQuantity",
    "lastUpdateDate",
    "totalWeeklyTradeCount",
)


# Deliberately invented extension, NOT the historical/current FINRA non-ATS header.
# Explicit attribution prevents guessing a group from the participant display name.
NON_ATS_HEADER = ATS_HEADER + ("firmCRDNumber", "attribution")
FIXTURE_HEADERS = {ATS_FIXTURE_SCHEMA: ATS_HEADER, NON_ATS_FIXTURE_SCHEMA: NON_ATS_HEADER}


@dataclass(frozen=True)
class WeeklyRow:
    partition: Partition
    tier_raw: str
    symbol_raw: str
    issue_name_raw: str
    identity: ParticipantIdentity
    weekly_shares: int
    weekly_trades: int
    weekly_shares_text: str
    weekly_trades_text: str
    provider_update_date: date
    provider_initial_publication_date: date | None = None

    @property
    def participant_name_raw(self) -> str | None:
        return self.identity.participant_name_raw

    @property
    def mpid(self) -> str | None:
        return self.identity.mpid

    @property
    def firm_crd(self) -> str | None:
        return self.identity.firm_crd


@dataclass(frozen=True)
class ParsedWeekly:
    partition: Partition
    schema_profile: str
    payload_sha256: str
    rows: tuple[WeeklyRow, ...]

    @property
    def outcome(self) -> str:
        return "PARSED" if self.rows else "PARSED_EMPTY"

    @property
    def completeness(self) -> str:
        return "UNVERIFIED"

    @property
    def context_source(self) -> str:
        return "EXPLICIT_SELECTION"


def _text(value: str, field: str) -> str:
    if not value.strip() or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError(f"OTC {field} is missing or contains control characters")
    return value


def _count(value: str, field: str) -> int:
    # A local fixture-profile ceiling, not a claim about FINRA's current schema.
    if not re.fullmatch(r"[0-9]{1,38}", value):
        raise ValueError(f"OTC {field} must be a nonnegative integer token of at most 38 digits")
    return int(value)


def parse_weekly(
    payload: bytes,
    *,
    partition: Partition,
    schema_profile: str,
    max_bytes: int = 2_000_000,
    max_rows: int = 10_000,
) -> ParsedWeekly:
    """Parse one explicit fixture profile atomically, without file/network I/O.

    The selected file supplies no reporting week or product column. Callers must
    retain external selection provenance; a caller-supplied label alone does not
    prove real-data rights, source scope, file/page completeness or availability.
    """
    if not isinstance(partition, Partition):
        raise ValueError("OTC parser requires an explicit validated partition")
    profile = profile_for(partition.product, partition.tier)
    if schema_profile != profile.schema_profile or schema_profile not in FIXTURE_HEADERS:
        raise ValueError("OTC parser requires the selected invented weekly profile")
    header = FIXTURE_HEADERS[schema_profile]
    if any(type(limit) is not int or limit <= 0 for limit in (max_bytes, max_rows)):
        raise ValueError("OTC parser limits must be positive finite integers")
    if not isinstance(payload, bytes) or not payload or len(payload) > max_bytes:
        raise ValueError("OTC parser requires nonempty bytes within the configured byte budget")
    try:
        text = payload.decode("utf-8")
    except UnicodeError:
        raise ValueError("OTC fixture profile requires strict UTF-8 without a BOM") from None
    rows = []
    try:
        reader = csv.reader(
            io.StringIO(text, newline=""), delimiter="|", quotechar='"', strict=True
        )
        if tuple(next(reader, ())) != header:
            raise ValueError("OTC header differs from the pinned invented pipe schema")
        for cells in reader:
            if len(rows) >= max_rows:
                raise ValueError("OTC payload exceeds the configured row budget")
            if len(cells) != len(header):
                raise ValueError("OTC record must have exactly the pinned columns")
            tier, symbol, issue, participant, mpid, shares, update, trades = cells[:8]
            crd, attribution = cells[8:] if len(header) == 10 else (None, "REPORTED")
            if tier != partition.tier:
                raise ValueError("OTC row tier differs from the selected population")
            symbol = _text(symbol, "symbol")
            issue = _text(issue, "issue name")
            participant = _text(participant, "participant name")
            mpid = _text(mpid, "MPID") if schema_profile == ATS_FIXTURE_SCHEMA else (mpid or None)
            if any(c.isspace() for token in (symbol, mpid or "") for c in token):
                raise ValueError("OTC symbol/MPID tokens cannot contain whitespace")
            identity = participant_identity(
                partition=partition,
                schema_profile=schema_profile,
                route="invented_fixture",
                participant_name_raw=participant,
                mpid=mpid,
                firm_crd=crd or None,
                attribution=attribution,
            )
            rows.append(
                WeeklyRow(
                    partition=partition,
                    tier_raw=tier,
                    symbol_raw=symbol,
                    issue_name_raw=issue,
                    identity=identity,
                    weekly_shares=_count(shares, "weekly shares"),
                    weekly_trades=_count(trades, "weekly trades"),
                    weekly_shares_text=shares,
                    weekly_trades_text=trades,
                    provider_update_date=iso_date(update, field="provider_update_date"),
                )
            )
    except csv.Error:
        raise ValueError("malformed OTC delimited record") from None
    require_unique_participants((row.symbol_raw, row.identity) for row in rows)
    return ParsedWeekly(partition, schema_profile, hashlib.sha256(payload).hexdigest(), tuple(rows))
