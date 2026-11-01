"""Strict parser for NAAIM's public delayed table, using only the standard library."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser

HEADERS = (
    "Date",
    "NAAIM Number",
    "Bearish",
    "Quarter 1",
    "Quarter 2",
    "Quarter 3",
    "Bullish",
    "Deviation",
)


@dataclass(frozen=True)
class Exposure:
    as_of_date: str
    naaim_number: Decimal
    bearish: Decimal
    quarter_1: Decimal
    quarter_2: Decimal
    quarter_3: Decimal
    bullish: Decimal
    deviation: Decimal


class _TableParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.in_table = False
        self.in_head = False
        self.in_body = False
        self.in_cell = False
        self.headers: list[str] = []
        self.rows: list[list[str]] = []
        self.cell: list[str] = []
        self.row: list[str] = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "table" and "table-naaim" in attributes.get("class", "").split():
            if self.in_table:
                raise ValueError("multiple NAAIM tables are not supported")
            self.in_table = True
        elif self.in_table and tag == "thead":
            self.in_head = True
        elif self.in_table and tag == "tbody":
            self.in_body = True
        elif self.in_table and tag == "tr" and self.in_body:
            self.row = []
        elif self.in_table and tag in {"th", "td"}:
            self.in_cell = True
            self.cell = []

    def handle_data(self, data):
        if self.in_cell:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if not self.in_table:
            return
        if tag in {"th", "td"} and self.in_cell:
            value = " ".join("".join(self.cell).split())
            if tag == "th" and self.in_head:
                self.headers.append(value)
            elif tag == "td" and self.in_body:
                self.row.append(value)
            self.in_cell = False
        elif tag == "tr" and self.in_body and self.row:
            self.rows.append(self.row)
            self.row = []
        elif tag == "thead":
            self.in_head = False
        elif tag == "tbody":
            self.in_body = False
        elif tag == "table":
            self.in_table = False


def _decimal(value: str, field: str) -> Decimal:
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{field} must be a decimal") from exc
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    return number


def parse_table(payload: bytes) -> list[Exposure]:
    """Parse a complete permitted public table; reject all schema drift."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("NAAIM table must be UTF-8 HTML") from exc
    parser = _TableParser()
    parser.feed(text)
    parser.close()
    if tuple(parser.headers) != HEADERS:
        raise ValueError(f"NAAIM table headers must be {list(HEADERS)}")
    if not parser.rows:
        raise ValueError("NAAIM table has no observations")
    observations = []
    seen_dates = set()
    for row_number, row in enumerate(parser.rows, start=1):
        if len(row) != len(HEADERS):
            raise ValueError(
                f"NAAIM row {row_number} has {len(row)} cells; expected {len(HEADERS)}"
            )
        try:
            as_of_date = datetime.strptime(row[0], "%m/%d/%Y").date().isoformat()
        except ValueError as exc:
            raise ValueError(f"NAAIM row {row_number} has an invalid Date") from exc
        if as_of_date in seen_dates:
            raise ValueError(f"NAAIM table has duplicate date {as_of_date}")
        seen_dates.add(as_of_date)
        observations.append(
            Exposure(
                as_of_date=as_of_date,
                naaim_number=_decimal(row[1], "NAAIM Number"),
                bearish=_decimal(row[2], "Bearish"),
                quarter_1=_decimal(row[3], "Quarter 1"),
                quarter_2=_decimal(row[4], "Quarter 2"),
                quarter_3=_decimal(row[5], "Quarter 3"),
                bullish=_decimal(row[6], "Bullish"),
                deviation=_decimal(row[7], "Deviation"),
            )
        )
    return observations
