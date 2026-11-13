# ruff: noqa: E501
from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.provenance import read_json
from market_data_pipeline.pipelines.presidential_research.review_workbook import (
    ReviewWorkbookError,
    export_approved_controls,
)


def _cell(column: str, row: int, value: str) -> str:
    return f'<c r="{column}{row}" t="inlineStr"><is><t>{escape(value)}</t></is></c>'


def _workbook_fixture(
    path: Path, review_status: str, evidence_note: str = "Official evidence note"
) -> None:
    headers = [
        "Candidate ID",
        "Approved interval ID",
        "Start date",
        "End date",
        "President ID",
        "President name",
        "President party",
        "House control",
        "Senate control",
        "Controlling source ID",
        "Primary source locator",
        "Evidence note",
        "Reviewer",
        "Reviewed at",
        "Review status",
    ]
    values = [
        "house-congress-74",
        "control-74-1935-01-03",
        "1935-01-03T00:00:00.000Z",
        "1937-01-03T00:00:00.000Z",
        "franklin-d-roosevelt",
        "Franklin D. Roosevelt",
        "D",
        "D",
        "D",
        "CTRL_074_A",
        "https://example.test/official-record",
        evidence_note,
        "Research reviewer",
        "2026-11-01",
        review_status,
    ]
    columns = [chr(ord("A") + index) for index in range(len(headers))]
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
        f'<row r="6">{"".join(_cell(column, 6, value) for column, value in zip(columns, headers, strict=True))}</row>'
        f'<row r="7">{"".join(_cell(column, 7, value) for column, value in zip(columns, values, strict=True))}</row>'
        "</sheetData></worksheet>"
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Intervals to review" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    relations_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", relations_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)


class ReviewWorkbookTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(__file__).resolve().parents[1]
        self.config = load_config(root / "config" / "presidential_research" / "sources.toml")

    def test_exports_only_approved_review_rows_with_replay_manifest(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            workbook, controls = directory / "review.xlsx", directory / "controls.csv"
            _workbook_fixture(workbook, "APPROVED")
            result = export_approved_controls(self.config, workbook, controls)
            with controls.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            manifest = read_json(controls.with_suffix(".manifest.json"), {})
        self.assertEqual(result["controls"], 1)
        self.assertEqual(rows[0]["startDate"], "1935-01-03")
        self.assertEqual(rows[0]["sourceId"], "CTRL_074_A")
        self.assertEqual(manifest["approved_count"], 1)
        self.assertEqual(manifest["reviewed_records"][0]["candidate_id"], "house-congress-74")
        self.assertEqual(
            manifest["reviewed_records"][0]["primary_source_locator"],
            "https://example.test/official-record",
        )

    def test_rejects_workbook_without_approved_rows(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            workbook, controls = directory / "review.xlsx", directory / "controls.csv"
            _workbook_fixture(workbook, "IN REVIEW")
            with self.assertRaisesRegex(ReviewWorkbookError, "no APPROVED intervals"):
                export_approved_controls(self.config, workbook, controls)

    def test_rejects_approved_row_without_required_evidence(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            workbook, controls = directory / "review.xlsx", directory / "controls.csv"
            _workbook_fixture(workbook, "APPROVED", evidence_note="")
            with self.assertRaisesRegex(ReviewWorkbookError, "lack required review evidence"):
                export_approved_controls(self.config, workbook, controls)
