from __future__ import annotations

import csv
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from posixpath import normpath
from urllib.parse import urlsplit
from zipfile import BadZipFile, ZipFile

from .config import PipelineConfig
from .provenance import sha256_file, write_json
from .quality import check_controls


class ReviewWorkbookError(ValueError):
    """Raised when a review workbook cannot be safely promoted."""


MAX_WORKBOOK_BYTES = 50 * 1024 * 1024
MAX_WORKBOOK_MEMBER_BYTES = 10 * 1024 * 1024
MAX_WORKBOOK_EXPANDED_BYTES = 50 * 1024 * 1024
MAX_WORKBOOK_MEMBERS = 2000
MAX_REVIEW_ROWS = 100_000
MAX_REVIEW_COLUMNS = 256

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_OFFICE_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_EXCEL_EPOCH = date(1899, 12, 30)
_CONTROL_FIELDS = {
    "Approved interval ID": "id",
    "Start date": "startDate",
    "End date": "endDate",
    "President ID": "presidentId",
    "President name": "presidentName",
    "President party": "presidentParty",
    "House control": "houseControl",
    "Senate control": "senateControl",
    "Controlling source ID": "sourceId",
}


def _column_number(reference: str) -> int:
    letters = "".join(character for character in reference if character.isalpha())
    result = 0
    for character in letters:
        result = result * 26 + ord(character.upper()) - ord("A") + 1
    return result


def _excel_date(value: str) -> str:
    if "T" in value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            return value
    try:
        serial = float(value)
    except ValueError:
        return value
    if not serial.is_integer() or serial < 1:
        return value
    try:
        return (_EXCEL_EPOCH + timedelta(days=int(serial))).isoformat()
    except OverflowError:
        return value


def _xml(archive: ZipFile, member: str) -> ET.Element:
    info = archive.getinfo(member)
    if info.file_size > MAX_WORKBOOK_MEMBER_BYTES:
        raise ReviewWorkbookError("Workbook member exceeds size limit")
    with archive.open(info) as source:
        payload = source.read(MAX_WORKBOOK_MEMBER_BYTES + 1)
    if len(payload) > MAX_WORKBOOK_MEMBER_BYTES:
        raise ReviewWorkbookError("Workbook member exceeds size limit")
    if b"<!DOCTYPE" in payload.upper() or b"<!ENTITY" in payload.upper():
        raise ReviewWorkbookError("Workbook XML declarations are not allowed")
    return ET.fromstring(payload)


def _shared_strings(archive: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = _xml(archive, "xl/sharedStrings.xml")
    return ["".join(item.itertext()) for item in root.findall(f"{{{_MAIN_NS}}}si")]


def _worksheet_path(archive: ZipFile, sheet_name: str) -> str:
    workbook = _xml(archive, "xl/workbook.xml")
    relation_id = None
    for sheet in workbook.findall(f".//{{{_MAIN_NS}}}sheet"):
        if sheet.attrib.get("name") == sheet_name:
            relation_id = sheet.attrib.get(f"{{{_OFFICE_REL_NS}}}id")
            break
    if relation_id is None:
        raise ReviewWorkbookError(f"Workbook lacks required sheet: {sheet_name}")
    relations = _xml(archive, "xl/_rels/workbook.xml.rels")
    for relation in relations.findall(f"{{{_PACKAGE_REL_NS}}}Relationship"):
        if relation.attrib.get("Id") == relation_id:
            target = relation.attrib.get("Target", "")
            resolved = (
                normpath(target.lstrip("/")) if target.startswith("/") else normpath(f"xl/{target}")
            )
            if not resolved.startswith("xl/") or "\\" in resolved:
                raise ReviewWorkbookError("Workbook worksheet path is invalid")
            return resolved
    raise ReviewWorkbookError(f"Workbook has no relationship target for sheet: {sheet_name}")


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(cell.itertext()).strip()
    value = cell.findtext(f"{{{_MAIN_NS}}}v", default="")
    if cell_type == "s":
        try:
            return shared_strings[int(value)]
        except (IndexError, ValueError) as exc:
            raise ReviewWorkbookError("Workbook has an invalid shared-string reference") from exc
    return value.strip()


def _review_rows(workbook_path: Path) -> list[dict[str, str]]:
    try:
        if workbook_path.stat().st_size > MAX_WORKBOOK_BYTES:
            raise ReviewWorkbookError("Workbook exceeds size limit")
        with ZipFile(workbook_path) as archive:
            members = archive.infolist()
            if (
                len(members) > MAX_WORKBOOK_MEMBERS
                or len({item.filename for item in members}) != len(members)
                or sum(item.file_size for item in members) > MAX_WORKBOOK_EXPANDED_BYTES
                or any(item.file_size > MAX_WORKBOOK_MEMBER_BYTES for item in members)
            ):
                raise ReviewWorkbookError("Workbook expansion exceeds size limit")
            shared_strings = _shared_strings(archive)
            worksheet = _xml(archive, _worksheet_path(archive, "Intervals to review"))
    except (ET.ParseError, OSError, KeyError, BadZipFile) as exc:
        raise ReviewWorkbookError(f"Cannot read review workbook: {workbook_path}") from exc
    rows: dict[int, dict[int, str]] = {}
    for row in worksheet.findall(f".//{{{_MAIN_NS}}}row"):
        if len(rows) >= MAX_REVIEW_ROWS:
            raise ReviewWorkbookError("Workbook exceeds review row limit")
        try:
            row_number = int(row.attrib["r"])
        except (KeyError, ValueError) as exc:
            raise ReviewWorkbookError("Workbook has an invalid review row") from exc
        if row_number < 1 or row_number in rows:
            raise ReviewWorkbookError("Workbook has duplicate or invalid review rows")
        cells: dict[int, str] = {}
        for cell in row.findall(f"{{{_MAIN_NS}}}c"):
            reference = cell.attrib.get("r", "")
            if reference:
                column = _column_number(reference)
                if column < 1 or column > MAX_REVIEW_COLUMNS or column in cells:
                    raise ReviewWorkbookError("Workbook has duplicate or invalid review cells")
                cells[column] = _cell_value(cell, shared_strings)
        rows[row_number] = cells
    headers = rows.get(6)
    if not headers:
        raise ReviewWorkbookError("Review workbook has no header row at row 6")
    by_column = {column: value for column, value in headers.items() if value}
    if len(set(by_column.values())) != len(by_column):
        raise ReviewWorkbookError("Review workbook contains duplicate headers")
    expected = set(_CONTROL_FIELDS) | {
        "Review status",
        "Candidate ID",
        "Primary source locator",
        "Evidence note",
        "Reviewer",
        "Reviewed at",
    }
    missing = sorted(expected - set(by_column.values()))
    if missing:
        raise ReviewWorkbookError(
            f"Review workbook is missing required header(s): {', '.join(missing)}"
        )
    output: list[dict[str, str]] = []
    for row_number in sorted(number for number in rows if number >= 7):
        output.append(
            {header: rows[row_number].get(column, "") for column, header in by_column.items()}
        )
    return output


def export_approved_controls(
    config: PipelineConfig, workbook_path: Path, output_path: Path
) -> dict[str, object]:
    """Promote only APPROVED workbook rows to a canonical controls.csv file."""
    review_rows = _review_rows(workbook_path)
    approved = [row for row in review_rows if row.get("Review status") == "APPROVED"]
    if not approved:
        raise ReviewWorkbookError("Review workbook contains no APPROVED intervals")
    review_required = ("Primary source locator", "Evidence note", "Reviewer", "Reviewed at")
    incomplete = [
        row.get("Approved interval ID", f"review row {index}")
        for index, row in enumerate(approved, 7)
        if any(not row.get(field, "").strip() for field in review_required)
    ]
    if incomplete:
        raise ReviewWorkbookError(
            "APPROVED controls lack required review evidence: " + ", ".join(incomplete[:5])
        )
    for row in approved:
        locators = re.findall(r"https?://[^\s;,)]+", row["Primary source locator"])
        if not locators or any(not urlsplit(url).hostname for url in locators):
            raise ReviewWorkbookError("APPROVED controls require a source locator")
    try:
        for row in approved:
            date.fromisoformat(_excel_date(row["Reviewed at"]))
    except (ValueError, OverflowError) as exc:
        raise ReviewWorkbookError("APPROVED controls have invalid reviewed dates") from exc
    reviewed_controls = [
        (
            {
                destination: _excel_date(row.get(source, ""))
                if destination in {"startDate", "endDate"}
                else row.get(source, "")
                for source, destination in _CONTROL_FIELDS.items()
            },
            row,
        )
        for row in approved
    ]
    controls = [control for control, _ in reviewed_controls]
    issues = check_controls(controls)
    known_source_ids = {source.id for source in config.sources}
    unknown = sorted(
        {
            control["sourceId"]
            for control in controls
            if control.get("sourceId") not in known_source_ids
        }
    )
    if unknown:
        raise ReviewWorkbookError(
            f"APPROVED controls cite unknown source ID(s): {', '.join(unknown)}"
        )
    ineligible = sorted(
        {
            control["sourceId"]
            for control in controls
            if control.get("sourceId") not in config.manual_control_source_ids
        }
    )
    if ineligible:
        raise ReviewWorkbookError(
            "APPROVED controls must cite manual reviewed political-control evidence source ID(s): "
            + ", ".join(ineligible)
        )
    errors = [issue for issue in issues if issue.severity == "ERROR"]
    if errors:
        message = "; ".join(f"{issue.check_id}: {issue.message}" for issue in errors[:5])
        raise ReviewWorkbookError(f"APPROVED controls failed validation: {message}")
    reviewed_controls.sort(
        key=lambda item: (item[0]["startDate"], item[0]["endDate"], item[0]["id"])
    )
    controls = [control for control, _ in reviewed_controls]
    fields = [
        "id",
        "startDate",
        "endDate",
        "presidentId",
        "presidentName",
        "presidentParty",
        "houseControl",
        "senateControl",
        "sourceId",
    ]
    if (
        output_path.exists()
        or output_path.is_symlink()
        or output_path.with_suffix(".manifest.json").exists()
        or output_path.with_suffix(".manifest.json").is_symlink()
    ):
        raise ReviewWorkbookError("Review output already exists")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(controls)
    manifest_path = output_path.with_suffix(".manifest.json")
    manifest = {
        "schema_version": 1,
        "artifact_kind": "APPROVED_POLITICAL_CONTROLS",
        "review_workbook": str(workbook_path),
        "review_workbook_sha256": sha256_file(workbook_path),
        "config_sha256": sha256_file(config.path),
        "controls_sha256": sha256_file(output_path),
        "approved_count": len(controls),
        "reviewed_records": [
            {
                "id": control["id"],
                "candidate_id": row.get("Candidate ID", ""),
                "primary_source_locator": row.get("Primary source locator", ""),
                "evidence_note": row.get("Evidence note", ""),
                "reviewer": row.get("Reviewer", ""),
                "reviewed_at": _excel_date(row.get("Reviewed at", "")),
            }
            for control, row in reviewed_controls
        ],
    }
    write_json(manifest_path, manifest)
    return {"controls": len(controls), "output": output_path, "manifest": manifest_path}
