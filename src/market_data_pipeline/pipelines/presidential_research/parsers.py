from __future__ import annotations

import calendar
import csv
import io
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from zipfile import ZipFile


class ParseError(ValueError):
    pass


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def parse_fama_french_monthly_zip(payload: bytes, source_id: str) -> list[dict[str, str]]:
    """Return monthly CRSP-market proxy records from Mkt-RF + RF, both percent."""
    try:
        with ZipFile(io.BytesIO(payload)) as archive:
            candidates = [
                name for name in archive.namelist() if name.lower().endswith((".csv", ".txt"))
            ]
            if len(candidates) != 1:
                raise ParseError("Fama/French archive must contain exactly one CSV or text table")
            text = archive.read(candidates[0]).decode("utf-8-sig")
    except OSError as exc:
        raise ParseError(f"Invalid Fama/French ZIP: {exc}") from exc
    header_index = next(
        (
            index
            for index, line in enumerate(text.splitlines())
            if "Mkt-RF" in line and "RF" in line
        ),
        None,
    )
    if header_index is None:
        raise ParseError("Fama/French monthly factor header with Mkt-RF and RF was not found")
    rows: list[dict[str, str]] = []
    reader = csv.DictReader(io.StringIO("\n".join(text.splitlines()[header_index:])))
    for record in reader:
        raw_month = (record.get("") or record.get("Date") or "").strip()
        if not re.fullmatch(r"\d{6}", raw_month):
            continue
        try:
            market_excess = Decimal((record.get("Mkt-RF") or "").strip())
            risk_free = Decimal((record.get("RF") or "").strip())
        except InvalidOperation as exc:
            raise ParseError(f"Invalid Fama/French numeric value at {raw_month}") from exc
        if not market_excess.is_finite() or not risk_free.is_finite():
            raise ParseError(f"Non-finite Fama/French numeric value at {raw_month}")
        year, month = int(raw_month[:4]), int(raw_month[4:])
        if not 1 <= month <= 12:
            raise ParseError(f"Invalid Fama/French month: {raw_month}")
        market_return = (market_excess + risk_free) / Decimal("100")
        rows.append(
            {
                "date": _month_end(year, month).isoformat(),
                "market_return_decimal": format(market_return.normalize(), "f"),
                "source_id": source_id,
            }
        )
    if not rows:
        raise ParseError("Fama/French file contained no monthly observations")
    return rows


def parse_fred_csv(payload: bytes, source_id: str) -> list[dict[str, str]]:
    text = payload.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "observation_date" not in reader.fieldnames:
        raise ParseError("FRED CSV must contain observation_date")
    value_columns = [field for field in reader.fieldnames if field != "observation_date"]
    if len(value_columns) != 1:
        raise ParseError("FRED CSV must contain exactly one value column")
    value_column = value_columns[0]
    rows: list[dict[str, str]] = []
    for record in reader:
        observed = (record.get("observation_date") or "").strip()
        raw_value = (record.get(value_column) or "").strip()
        if not observed or raw_value in {"", "."}:
            continue
        try:
            date.fromisoformat(observed)
            value = Decimal(raw_value)
        except InvalidOperation as exc:
            raise ParseError(f"Invalid FRED observation {observed!r}: {raw_value!r}") from exc
        if not value.is_finite():
            raise ParseError(f"Non-finite FRED observation {observed!r}")
        rows.append(
            {
                "date": observed,
                "policy_rate_percent": format(value.normalize(), "f"),
                "source_id": source_id,
            }
        )
    if not rows:
        raise ParseError("FRED CSV contained no usable observations")
    return rows


class _Tables(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table is not None:
            self.tables.append(self._table)
            self._table = None


_CONGRESS = re.compile(r"^(\d+)(?:st|nd|rd|th)\s*\((\d{4})[–-](\d{4})\)")
_WIKI_CONGRESS = re.compile(r"^(\d+)(?:st|nd|rd|th)$")
_WIKI_YEARS = re.compile(r"^(\d{4})[–-](\d{4})$")


def _party(text: str) -> str:
    lowered = text.lower()
    if "democrat" in lowered:
        return "D"
    if "republican" in lowered:
        return "R"
    if "tie" in lowered or "evenly divided" in lowered:
        return "TIED"
    return "UNKNOWN"


def parse_house_party_government_html(payload: bytes, source_id: str) -> list[dict[str, str]]:
    parser = _Tables()
    parser.feed(payload.decode("utf-8", errors="replace"))
    table = next(
        (
            candidate
            for candidate in parser.tables
            if candidate
            and any("Congress" in cell for cell in candidate[0])
            and any("Presidency" in cell for cell in candidate[0])
        ),
        None,
    )
    if table is None:
        raise ParseError("House party-government table was not found")
    rows: list[dict[str, str]] = []
    for row in table[1:]:
        if len(row) < 4:
            continue
        match = _CONGRESS.match(row[0])
        if not match:
            continue
        president = row[3]
        name_match = re.search(r"\(([^)]+)\)", president)
        rows.append(
            {
                "congress": match.group(1),
                "start_year": match.group(2),
                "end_year": match.group(3),
                "house_party": _party(row[1]),
                "senate_party": _party(row[2]),
                "president_party": _party(president),
                "president_label": name_match.group(1).strip() if name_match else president,
                "source_id": source_id,
                "review_status": "CANDIDATE_REQUIRES_DATED_REVIEW",
            }
        )
    if not rows:
        raise ParseError("House party-government table contained no Congress rows")
    return rows


def _wiki_text(value: str) -> str:
    """Remove rendered citation markers while preserving published counts."""
    return re.sub(r"\[\d+\]", "", value).strip()


def parse_wikipedia_congress_party_divisions_html(
    payload: bytes, source_id: str
) -> list[dict[str, str]]:
    """Extract 1930s-present Congress count cross-checks from a raw snapshot.

    This adapter intentionally preserves count strings such as ``50/49`` and
    ``56–58`` instead of deciding which party controlled a chamber on a date.
    It is a secondary-source cross-check, never a controls.csv generator.
    """
    parser = _Tables()
    parser.feed(payload.decode("utf-8", errors="replace"))
    table = next(
        (
            candidate
            for candidate in parser.tables
            if candidate
            and candidate[0]
            and candidate[0][0] == "Congress"
            and any(row and _WIKI_CONGRESS.fullmatch(row[0]) for row in candidate)
        ),
        None,
    )
    if table is None:
        raise ParseError("Wikipedia Congress party-divisions table was not found")
    rows: list[dict[str, str]] = []
    for row in table:
        # President and trifecta cells use HTML row-spans. In the flattened
        # snapshot they are absent on repeated rows, whereas the twelve party
        # count cells remain present.
        if len(row) < 12:
            continue
        congress = _WIKI_CONGRESS.fullmatch(row[0])
        years = _WIKI_YEARS.fullmatch(row[1])
        if congress is None or years is None:
            continue
        # The modern Democratic/Republican layout begins with the 73rd
        # Congress. The annual study begins with the 74th; retain all modern
        # rows so the normalized source remains useful for boundary review.
        if int(congress.group(1)) < 73:
            continue
        values = [_wiki_text(value) for value in row]
        values.extend([""] * (14 - len(values)))
        variation_hint = any("/" in value or "–" in value for value in values[2:12])
        president_label, trifecta_label = values[12], values[13]
        # A row that shares its President cell with its predecessor can still
        # have a distinct Trifecta cell.  Do not shift that label into the
        # president field after HTML row-span flattening.
        if len(row) == 13 and president_label.startswith(("Yes", "No")):
            president_label, trifecta_label = "", president_label
        rows.append(
            {
                "congress": congress.group(1),
                "start_year": years.group(1),
                "end_year": years.group(2),
                "senate_total": values[2],
                "senate_democrat_count": values[3],
                "senate_republican_count": values[4],
                "senate_other_count": values[5],
                "senate_vacancies": values[6],
                "house_total": values[7],
                "house_democrat_count": values[8],
                "house_republican_count": values[9],
                "house_other_count": values[10],
                "house_vacancies": values[11],
                "president_label": president_label,
                "trifecta_label": trifecta_label,
                "within_congress_variation_hint": "TRUE" if variation_hint else "FALSE",
                "source_id": source_id,
                "review_status": "SECONDARY_CROSSCHECK_ONLY",
            }
        )
    if not rows:
        raise ParseError("Wikipedia table contained no modern Congress count rows")
    return rows


def parse_shiller_xls(payload: bytes, source_id: str) -> list[dict[str, str]]:
    """Parse the optional legacy Shiller workbook without guessing its schema."""
    try:
        import xlrd  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ParseError(
            "Shiller parsing needs the optional dependency: python -m pip install -e '.[shiller]'"
        ) from exc
    try:
        workbook = xlrd.open_workbook(file_contents=payload)
        # The official workbook's first sheet is a disclaimer; the historical
        # observations live in its explicitly named ``Data`` worksheet.
        sheet = workbook.sheet_by_name("Data")
    except Exception as exc:  # xlrd exposes multiple workbook exceptions
        raise ParseError(f"Could not open Shiller workbook: {exc}") from exc
    header_row = next(
        (
            index
            for index in range(sheet.nrows)
            if str(sheet.cell_value(index, 0)).strip() == "Date"
        ),
        None,
    )
    if header_row is None:
        raise ParseError("Shiller workbook Date header was not found")
    headers = [str(sheet.cell_value(header_row, column)).strip() for column in range(sheet.ncols)]
    required = {"Date", "P", "D", "CAPE"}
    missing = required - set(headers)
    if missing:
        raise ParseError(
            f"Shiller workbook missing expected column(s): {', '.join(sorted(missing))}"
        )
    locations = {name: headers.index(name) for name in required}
    rows: list[dict[str, str]] = []
    for index in range(header_row + 1, sheet.nrows):
        raw_date = str(sheet.cell_value(index, locations["Date"])).strip()
        # xlrd preserves October as a numeric value such as ``1871.1`` rather
        # than ``1871.10``.  Accept one or two month digits and normalize it.
        match = re.fullmatch(r"(\d{4})\.(\d{1,2})", raw_date)
        if not match:
            continue
        year = int(match.group(1))
        # The source formats dates as YYYY.MM. Excel drops the trailing zero
        # from October (``1871.10`` becomes ``1871.1``); January through
        # September retain their leading zero as two digits.
        month_text = match.group(2)
        month = int(month_text) * 10 if len(month_text) == 1 else int(month_text)
        if not 1 <= month <= 12:
            raise ParseError(f"Invalid Shiller month at {raw_date}")
        try:
            price = Decimal(str(sheet.cell_value(index, locations["P"])))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ParseError(f"Invalid Shiller row at {raw_date}") from exc
        raw_dividend = str(sheet.cell_value(index, locations["D"])).strip()
        try:
            dividend = "" if raw_dividend in {"", "NA"} else Decimal(raw_dividend)
        except InvalidOperation as exc:
            raise ParseError(f"Invalid Shiller dividend at {raw_date}") from exc
        raw_cape = str(sheet.cell_value(index, locations["CAPE"])).strip()
        try:
            cape = "" if raw_cape in {"", "NA"} else Decimal(raw_cape)
        except InvalidOperation as exc:
            raise ParseError(f"Invalid Shiller CAPE at {raw_date}") from exc
        if (
            not price.is_finite()
            or (dividend != "" and not dividend.is_finite())
            or (cape != "" and not cape.is_finite())
        ):
            raise ParseError(f"Non-finite Shiller value at {raw_date}")
        rows.append(
            {
                "date": _month_end(year, month).isoformat(),
                "price": format(price.normalize(), "f"),
                # Keep the published trailing dividend field.  Any conversion to a
                # monthly return is a separately documented reconstruction step.
                "dividend": "" if dividend == "" else format(dividend.normalize(), "f"),
                # The source's early observations explicitly report CAPE as NA.
                # Preserve missingness rather than converting it to zero or
                # discarding a usable price observation.
                "cape": "" if cape == "" else format(cape.normalize(), "f"),
                "source_id": source_id,
            }
        )
    if not rows:
        raise ParseError("Shiller workbook contained no monthly price/CAPE rows")
    return rows


PARSERS = {
    "fama_french_monthly_zip": parse_fama_french_monthly_zip,
    "fred_csv": parse_fred_csv,
    "house_party_government_html": parse_house_party_government_html,
    "wikipedia_congress_party_divisions_html": parse_wikipedia_congress_party_divisions_html,
    "shiller_xls": parse_shiller_xls,
}


def parse_payload(parser_name: str, payload: bytes, source_id: str) -> list[dict[str, str]]:
    try:
        parser = PARSERS[parser_name]
    except KeyError as exc:
        raise ParseError(f"No parser registered for {parser_name}") from exc
    return parser(payload, source_id)


def output_name(parser_name: str) -> str:
    return {
        "fama_french_monthly_zip": "market_returns_monthly.csv",
        "fred_csv": "policy_rates_monthly.csv",
        "house_party_government_html": "political_candidates.csv",
        "wikipedia_congress_party_divisions_html": "wikipedia_congress_crosscheck.csv",
        "shiller_xls": "shiller_monthly.csv",
    }[parser_name]


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ParseError(f"No rows to write to {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
