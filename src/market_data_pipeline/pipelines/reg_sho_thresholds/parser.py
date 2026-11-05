"""Independent Nasdaq and NYSE threshold-list envelope parsers."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date

from market_data_pipeline.pipelines.reg_sho_thresholds.manifest import Manifest


@dataclass(frozen=True)
class MembershipRow:
    raw_symbol: str
    security_name_raw: str
    market_category: str
    flags_raw_json: str
    source_row_number: int


@dataclass(frozen=True)
class ParsedList:
    provider: str
    market: str
    list_date: date
    source_created_at_raw: str
    rows: tuple[MembershipRow, ...]
    raw_sha256: str


def _text(value: str, field: str, maximum: int) -> str:
    controls = any(ord(char) < 32 or ord(char) == 127 for char in value)
    if not value or len(value) > maximum or controls:
        raise ValueError(f"invalid threshold {field}")
    return value


def parse_list(payload: bytes, manifest: Manifest, *, list_date: date, market: str) -> ParsedList:
    if not isinstance(payload, bytes) or len(payload) > manifest.max_bytes:
        raise ValueError("threshold payload exceeds its byte limit")
    if market not in manifest.allowed_markets:
        raise ValueError("threshold market is not admitted by the provider manifest")
    try:
        text = payload.decode(manifest.encoding)
    except UnicodeDecodeError as exc:
        raise ValueError("threshold payload encoding differs from its manifest") from exc
    if "\x00" in text or "\r" in text.replace("\r\n", ""):
        raise ValueError("threshold payload contains unsupported control bytes")
    lines = text.splitlines()
    if len(lines) < 2 or lines[0].split("|") != list(manifest.header):
        raise ValueError("threshold header differs from its provider manifest")
    trailer = lines[-1]
    if not re.fullmatch(r"[0-9]{14}", trailer) or trailer[:8] != list_date.strftime("%Y%m%d"):
        raise ValueError("threshold creation trailer does not match the selected list date")
    data = lines[1:-1]
    if len(data) > manifest.max_rows:
        raise ValueError("threshold payload exceeds its row limit")
    rows = []
    keys = set()
    for number, line in enumerate(data, 2):
        if len(line.encode(manifest.encoding)) > manifest.max_record_bytes:
            raise ValueError("threshold record exceeds its byte limit")
        fields = line.split("|")
        if len(fields) != 6:
            raise ValueError("threshold record does not have six fields")
        symbol = _text(fields[0], "symbol", 64)
        name = _text(fields[1], "security name", 1024)
        category = _text(fields[2], "market category", 64)
        if symbol in keys:
            raise ValueError("duplicate threshold symbol in one provider list")
        keys.add(symbol)
        if manifest.provider == "nasdaq":
            if market != "NASDAQ" or category not in ("Q", "G", "S"):
                raise ValueError("invalid Nasdaq market category")
            if fields[3] != "Y" or fields[4] not in ("Y", "N") or fields[5] != "":
                raise ValueError("invalid Nasdaq threshold flags")
            flags = {
                "market_category": category,
                "reg_sho_threshold_flag": fields[3],
                "rule_3210": fields[4],
                "filler": fields[5],
            }
        else:
            if category != market:
                raise ValueError("NYSE response market differs from the selected market")
            if fields[3] != "Y" or fields[4:] != ["", ""]:
                raise ValueError("invalid NYSE threshold flags")
            flags = {
                "market_category": category,
                "reg_sho_threshold_flag": fields[3],
                "filler_1": fields[4],
                "filler_2": fields[5],
            }
        rows.append(
            MembershipRow(
                symbol,
                name,
                category,
                json.dumps(flags, sort_keys=True, separators=(",", ":")),
                number,
            )
        )
    return ParsedList(
        manifest.provider,
        market,
        list_date,
        trailer,
        tuple(rows),
        hashlib.sha256(payload).hexdigest(),
    )
