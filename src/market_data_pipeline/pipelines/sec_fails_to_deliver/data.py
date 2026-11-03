"""Pure parsers for the locally reconciled SEC FTD six-column/trailer profile.

No network, extraction to disk, persistence or complete-source-coverage
assertion occurs in this module. Conformance evidence is limited to local samples.
SEC field semantics: https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data
ZIP handling: https://docs.python.org/3.12/library/zipfile.html#zipfile-objects
"""

import hashlib
import io
import re
import stat
import zipfile
import zlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import BinaryIO

from market_data_pipeline.pipelines.sec_fails_to_deliver.manifest import HEADER, Manifest


@dataclass(frozen=True)
class Observation:
    settlement_date: date
    cusip: str
    symbol_raw: str | None
    quantity_fails: int
    description_raw: str | None
    prior_close_price: Decimal | None
    price_missing_reason: str | None
    source_row_number: int
    quantity_fails_raw: str
    prior_close_price_raw: str


@dataclass(frozen=True)
class ParsedArchive:
    member_name: str
    raw_sha256: str
    member_sha256: str
    observations: tuple[Observation, ...]
    observed_dates: tuple[date, ...]


def _observation(fields: list[str], row_number: int, manifest: Manifest) -> Observation:
    if len(fields) != len(HEADER):
        raise ValueError(f"row {row_number}: invalid column count")
    raw_date, cusip, symbol, quantity, description, raw_price = fields
    if not re.fullmatch(r"[0-9]{8}", raw_date):
        raise ValueError(f"row {row_number}: invalid settlement date")
    try:
        settled = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:]))
    except ValueError as exc:
        raise ValueError(f"row {row_number}: invalid settlement date") from exc
    if not manifest.period_start <= settled <= manifest.period_end:
        raise ValueError(f"row {row_number}: date outside requested partition")
    if not re.fullmatch(r"[A-Z0-9*@#]{9}", cusip):
        raise ValueError(f"row {row_number}: invalid CUSIP")
    if not re.fullmatch(r"[0-9]+", quantity):
        raise ValueError(f"row {row_number}: invalid quantity")
    try:
        count = int(quantity)
    except ValueError as exc:
        raise ValueError(f"row {row_number}: quantity exceeds integer parsing limit") from exc
    if raw_price == ".":
        price, reason = None, "SOURCE_PRICE_UNAVAILABLE"
    elif re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", raw_price):
        price, reason = Decimal(raw_price), None
    else:
        raise ValueError(f"row {row_number}: invalid price")
    return Observation(
        settlement_date=settled,
        cusip=cusip,
        symbol_raw=symbol or None,
        quantity_fails=count,
        description_raw=description or None,
        prior_close_price=price,
        price_missing_reason=reason,
        source_row_number=row_number,
        quantity_fails_raw=quantity,
        prior_close_price_raw=raw_price,
    )


def _parse_stream(stream: BinaryIO, manifest: Manifest) -> tuple[tuple[Observation, ...], str]:
    observations: list[Observation] = []
    keys: set[tuple[date, str]] = set()
    digest = hashlib.sha256()
    total_bytes = 0
    line_number = 0
    trailer_state = 0
    total_quantity = 0
    while raw := stream.readline(manifest.max_record_bytes + 1):
        total_bytes += len(raw)
        line_number += 1
        if len(raw) > manifest.max_record_bytes or total_bytes > manifest.max_expanded_bytes:
            raise ValueError("record or expanded-byte resource limit exceeded")
        digest.update(raw)
        if not raw.endswith(b"\n"):
            raise ValueError(f"row {line_number}: unterminated record")
        line = raw[:-1]
        if line.endswith(b"\r"):
            line = line[:-1]
        try:
            text = line.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError(f"row {line_number}: expected ASCII encoding") from exc
        if any(ord(char) < 32 or ord(char) == 127 for char in text):
            raise ValueError(f"row {line_number}: unexpected control character")
        fields = text.split("|")
        if line_number == 1:
            if tuple(fields) != HEADER:
                raise ValueError("SEC FTD header differs from the source profile")
            continue
        if trailer_state == 2:
            raise ValueError("unexpected record after SEC FTD trailers")
        if trailer_state == 1:
            if text != f"Trailer total quantity of shares {total_quantity}":
                raise ValueError("missing, malformed or mismatched SEC FTD quantity trailer")
            trailer_state = 2
            continue
        if text.startswith("Trailer"):
            if text != f"Trailer record count {len(observations)}":
                raise ValueError("malformed or mismatched SEC FTD record-count trailer")
            trailer_state = 1
            continue
        if len(observations) >= manifest.max_rows:
            raise ValueError("row resource limit exceeded")
        observation = _observation(fields, line_number, manifest)
        key = (observation.settlement_date, observation.cusip)
        if key in keys:
            raise ValueError(f"row {line_number}: duplicate settlement-date/CUSIP key")
        keys.add(key)
        observations.append(observation)
        total_quantity += observation.quantity_fails
    if not observations:
        raise ValueError("empty file cannot establish a complete SEC FTD partition")
    if trailer_state != 2:
        raise ValueError("missing SEC FTD trailers")
    return tuple(observations), digest.hexdigest()


def parse_pipe(payload: bytes, manifest: Manifest) -> tuple[Observation, ...]:
    """Parse one bounded source-profile file; validate both terminal totals."""
    if not isinstance(payload, bytes):
        raise ValueError("pipe payload must be bytes")
    if len(payload) > manifest.max_expanded_bytes:
        raise ValueError("expanded-byte resource limit exceeded")
    return _parse_stream(io.BytesIO(payload), manifest)[0]


def parse_archive(payload: bytes, manifest: Manifest) -> ParsedArchive:
    """Read exactly one explicit regular member without extracting files.

    Physical ZIP success does not certify the provider's population/date
    coverage. Decompressed data is streamed under actual byte and row limits.
    """
    if not isinstance(payload, bytes):
        raise ValueError("archive payload must be bytes")
    if len(payload) > manifest.max_archive_bytes:
        raise ValueError("archive-byte resource limit exceeded")
    if not payload.startswith(b"PK\x03\x04"):
        raise ValueError("invalid ZIP archive signature")
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            members = archive.infolist()
            if len(members) != 1:
                raise ValueError("archive requires exactly one unique member")
            member = members[0]
            mode = stat.S_IFMT(member.external_attr >> 16)
            if (
                member.filename != manifest.member_name
                or member.orig_filename != manifest.member_name
                or member.is_dir()
                or mode not in (0, stat.S_IFREG)
            ):
                raise ValueError("archive member must be the expected regular .txt file")
            if member.header_offset != 0 or len(payload) < 30:
                raise ValueError("invalid archive member header offset")
            local_flags = int.from_bytes(payload[6:8], "little")
            if local_flags != member.flag_bits:
                raise ValueError("archive local and directory flags disagree")
            if member.flag_bits & (1 | 0x20 | 0x40) or member.compress_type not in (
                zipfile.ZIP_STORED,
                zipfile.ZIP_DEFLATED,
            ):
                raise ValueError("unsupported archive encryption or compression")
            if member.file_size > manifest.max_expanded_bytes:
                raise ValueError("expanded-byte resource limit exceeded")
            with archive.open(member) as stream:
                observations, member_hash = _parse_stream(stream, manifest)
    except (
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
        EOFError,
        OSError,
        zlib.error,
        NotImplementedError,
        RuntimeError,
    ) as exc:
        raise ValueError("invalid or corrupt ZIP archive") from exc
    return ParsedArchive(
        member_name=member.filename,
        raw_sha256=hashlib.sha256(payload).hexdigest(),
        member_sha256=member_hash,
        observations=observations,
        observed_dates=tuple(sorted({row.settlement_date for row in observations})),
    )
