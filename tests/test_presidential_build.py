# ruff: noqa: E501
from __future__ import annotations

import csv
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from market_data_pipeline.pipelines.presidential_research.assemble import (
    BuildError,
    build_annual_dataset,
    build_inherited_fundamentals,
    build_presidential_cycle_analysis,
    build_public_series_comparison,
    derive_shiller_monthly_total_return,
    write_inherited_conditions_ledger,
)
from market_data_pipeline.pipelines.presidential_research.config import ConfigError, load_config
from market_data_pipeline.pipelines.presidential_research.quality import check_monthly_returns


def month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date.fromordinal(date(year, month + 1, 1).toordinal() - 1)


class BuildTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1]
        self.config = load_config(self.root / "config" / "presidential_research" / "sources.toml")

    def write_market(self, path: Path, source_id: str = "FF_MKT_001") -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=["date", "market_return_decimal", "source_id"]
            )
            writer.writeheader()
            for year in range(1935, 2024):
                for month in range(1, 13):
                    writer.writerow(
                        {
                            "date": month_end(year, month).isoformat(),
                            "market_return_decimal": "0.001",
                            "source_id": source_id,
                        }
                    )

    def write_shiller_context(self, path: Path) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=["date", "price", "dividend", "cape", "source_id"]
            )
            writer.writeheader()
            for year in range(1934, 2024):
                for month in range(1, 13):
                    writer.writerow(
                        {
                            "date": month_end(year, month).isoformat(),
                            "price": str(100 + (year - 1934) * 12 + month),
                            "dividend": "3",
                            "cape": "20",
                            "source_id": "SHILLER_001",
                        }
                    )

    def write_policy_rates(self, path: Path) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["date", "policy_rate_percent", "source_id"])
            writer.writeheader()
            for year in range(1954, 2024):
                for month in range(1, 13):
                    writer.writerow(
                        {
                            "date": f"{year:04d}-{month:02d}-01",
                            "policy_rate_percent": "2.5",
                            "source_id": "FEDFUNDS_001",
                        }
                    )

    def write_controls(
        self, path: Path, overlapping: bool = False, source_id: str = "CTRL_074_A"
    ) -> None:
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
        rows = [
            {
                "id": "reviewed-1",
                "startDate": "1935-01-01",
                "endDate": "2024-01-01",
                "presidentId": "reviewed-president",
                "presidentName": "Reviewed record",
                "presidentParty": "D",
                "houseControl": "D",
                "senateControl": "D",
                "sourceId": source_id,
            }
        ]
        if overlapping:
            rows.append({**rows[0], "id": "overlap", "startDate": "1940-01-01"})
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def test_builds_user_supplied_proxy_with_explicit_decision_policy(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            market, controls = directory / "market.csv", directory / "controls.csv"
            self.write_market(market)
            self.write_controls(controls)
            dataset, manifest = build_annual_dataset(self.config, market, controls, "window_start")
        self.assertEqual(dataset["kind"], "USER_SUPPLIED")
        self.assertEqual(dataset["returnBasis"], "TOTAL_RETURN")
        self.assertEqual(len(dataset["windows"]), 88)
        self.assertEqual(dataset["windows"][0]["decisionDate"], "1935-12-31")
        self.assertFalse(manifest["exact_replication"])
        self.assertEqual(manifest["decision_policy"], "window_start")

    def test_blocks_overlapping_controls_before_writing_research_data(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            market, controls = directory / "market.csv", directory / "controls.csv"
            self.write_market(market)
            self.write_controls(controls, overlapping=True)
            with self.assertRaisesRegex(BuildError, "controls.overlap"):
                build_annual_dataset(self.config, market, controls, "calendar_year_start")

    def test_requires_the_source_declared_by_the_reconstruction(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            market, controls = directory / "market.csv", directory / "controls.csv"
            self.write_market(market, source_id="SHILLER_001")
            self.write_controls(controls)
            with self.assertRaisesRegex(BuildError, "requires market source FF_MKT_001"):
                build_annual_dataset(self.config, market, controls, "window_start")

    def test_derives_shiller_monthly_returns_with_explicit_dividend_convention(self) -> None:
        derived = derive_shiller_monthly_total_return(
            [
                {
                    "date": "1935-01-31",
                    "price": "100",
                    "dividend": "12",
                    "source_id": "SHILLER_001",
                },
                {
                    "date": "1935-02-28",
                    "price": "102",
                    "dividend": "12",
                    "source_id": "SHILLER_001",
                },
            ]
        )
        self.assertEqual(
            derived,
            [{"date": "1935-02-28", "market_return_decimal": "0.03", "source_id": "SHILLER_001"}],
        )

    def test_builds_aligned_public_series_comparison(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            market, shiller, controls = (
                directory / "market.csv",
                directory / "shiller_monthly.csv",
                directory / "controls.csv",
            )
            self.write_market(market)
            self.write_shiller_context(shiller)
            self.write_controls(controls)
            primary, primary_manifest, robustness, robustness_manifest, derived, comparison = (
                build_public_series_comparison(
                    self.config, market, shiller, controls, "window_start"
                )
            )
        self.assertEqual(primary_manifest["reconstruction"], "crsp_market_proxy")
        self.assertEqual(
            robustness_manifest["reconstruction"], "shiller_sp_composite_approximation"
        )
        self.assertEqual(primary["id"], "annual-crsp-market-proxy-1936-2023")
        self.assertEqual(robustness["id"], "annual-shiller-sp-composite-approximation-1936-2023")
        self.assertEqual(len(derived), 1068)
        self.assertEqual(comparison["summary"]["observations"], 88)
        self.assertEqual(len(comparison["annual_rows"]), 88)

    def test_comparison_stops_at_last_complete_shiller_december(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            market, shiller, controls = (
                directory / "market.csv",
                directory / "shiller_monthly.csv",
                directory / "controls.csv",
            )
            self.write_market(market)
            self.write_shiller_context(shiller)
            with shiller.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            for row in rows:
                if row["date"] >= "2023-07-31":
                    row["dividend"] = ""
            with shiller.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=["date", "price", "dividend", "cape", "source_id"]
                )
                writer.writeheader()
                writer.writerows(rows)
            self.write_controls(controls)
            _, _, robustness, _, _, comparison = build_public_series_comparison(
                self.config, market, shiller, controls, "window_start"
            )
            common_primary, common_manifest = build_annual_dataset(
                replace(
                    self.config, annual_end_year=int(comparison["overlap"]["last_annual_year"])
                ),
                market,
                controls,
                "window_start",
            )
        self.assertEqual(len(robustness["windows"]), 87)
        self.assertEqual(comparison["overlap"]["last_annual_year"], 2022)
        self.assertEqual(common_primary["id"], "annual-crsp-market-proxy-1936-2022")
        self.assertEqual(len(common_primary["windows"]), len(robustness["windows"]))
        self.assertEqual(common_manifest["reconstruction"], "crsp_market_proxy")
        self.assertEqual(
            comparison["robustness_source_completeness"]["last_complete_dividend_december"],
            "2022-12-31",
        )
        self.assertEqual(
            comparison["robustness_source_completeness"]["excluded_incomplete_dividend_months"],
            [
                "2023-07-31",
                "2023-08-31",
                "2023-09-30",
                "2023-10-31",
                "2023-11-30",
                "2023-12-31",
            ],
        )

    def test_builds_contiguous_frozen_inherited_conditions(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            shiller, policy = (
                directory / "shiller_monthly.csv",
                directory / "policy_rates_monthly.csv",
            )
            self.write_shiller_context(shiller)
            self.write_policy_rates(policy)
            fundamentals, audit = build_inherited_fundamentals(
                replace(self.config, annual_end_year=2022), shiller, policy, 2022
            )
        self.assertEqual(len(fundamentals), 68)
        self.assertEqual(audit["first_annual_year"], 1955)
        self.assertEqual(audit["last_annual_year"], 2022)
        self.assertEqual(
            fundamentals[0],
            {
                "date": "1954-10-31",
                "availableDate": "1954-11-30",
                "valuation": 20.0,
                "valuationDefinition": "Shiller workbook CAPE field",
                "policyRatePct": 2.5,
                "sourceId": "INHERITED_CAPE_FEDFUNDS_001",
            },
        )

    def test_writes_a_reconciling_inherited_conditions_ledger(self) -> None:
        comparison = {
            "annual_rows": [
                {
                    "year": 1955,
                    "decisionDate": "1954-12-31",
                    "presidentParty": "D",
                    "houseControl": "D",
                    "senateControl": "D",
                    "crspMarketProxyReturnDecimal": 0.2,
                    "shillerSpCompositeApproximationReturnDecimal": 0.19,
                    "differenceDecimal": -0.01,
                },
                {
                    "year": 1956,
                    "decisionDate": "1955-12-31",
                    "presidentParty": "R",
                    "houseControl": "D",
                    "senateControl": "R",
                    "crspMarketProxyReturnDecimal": 0.1,
                    "shillerSpCompositeApproximationReturnDecimal": 0.11,
                    "differenceDecimal": 0.01,
                },
            ],
        }
        audit = {
            "observations": 2,
            "rows": [
                {
                    "year": 1955,
                    "fundamental": {"valuation": 10.0, "policyRatePct": 1.0},
                    "valuation_observation_date": "1954-10-31",
                    "valuation_assumed_available_date": "1954-11-30",
                    "policy_observation_date": "1954-10-01",
                    "policy_assumed_available_date": "1954-11-01",
                },
                {
                    "year": 1956,
                    "fundamental": {"valuation": 11.0, "policyRatePct": 2.0},
                    "valuation_observation_date": "1955-10-31",
                    "valuation_assumed_available_date": "1955-11-30",
                    "policy_observation_date": "1955-10-01",
                    "policy_assumed_available_date": "1955-11-01",
                },
            ],
        }
        with TemporaryDirectory() as temporary:
            output = Path(temporary)
            write_inherited_conditions_ledger(output, comparison, audit)
            with (output / "inherited-conditions-ledger.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["politicalConfiguration"], "D president · D Congress")
        self.assertEqual(rows[1]["politicalConfiguration"], "R president · Split Congress")
        self.assertEqual(rows[1]["policyRatePct"], "2.0")

    def test_builds_end_of_year_presidential_cycle_rows_and_transition_flags(self) -> None:
        controls = [
            {
                "id": "fdr",
                "startDate": "1935-01-03",
                "endDate": "1945-04-12",
                "presidentId": "franklin-d-roosevelt",
                "presidentName": "Franklin D. Roosevelt",
                "presidentParty": "D",
                "houseControl": "D",
                "senateControl": "D",
                "sourceId": "CTRL_074_A",
            },
            {
                "id": "truman",
                "startDate": "1945-04-12",
                "endDate": "1947-01-03",
                "presidentId": "harry-s-truman",
                "presidentName": "Harry S. Truman",
                "presidentParty": "D",
                "houseControl": "D",
                "senateControl": "D",
                "sourceId": "CTRL_GOV_001",
            },
        ]
        comparison = {
            "annual_rows": [
                {
                    "year": 1936,
                    "decisionDate": "1935-12-31",
                    "crspMarketProxyReturnDecimal": 0.1,
                    "shillerSpCompositeApproximationReturnDecimal": 0.2,
                },
                {
                    "year": 1937,
                    "decisionDate": "1936-12-31",
                    "crspMarketProxyReturnDecimal": 0.11,
                    "shillerSpCompositeApproximationReturnDecimal": 0.21,
                },
                {
                    "year": 1945,
                    "decisionDate": "1944-12-31",
                    "crspMarketProxyReturnDecimal": 0.12,
                    "shillerSpCompositeApproximationReturnDecimal": 0.22,
                },
                {
                    "year": 1946,
                    "decisionDate": "1945-12-31",
                    "crspMarketProxyReturnDecimal": 0.13,
                    "shillerSpCompositeApproximationReturnDecimal": 0.23,
                },
            ],
        }
        ledger, summary = build_presidential_cycle_analysis(comparison, controls)
        self.assertEqual([row["year"] for row in ledger], [1937, 1945, 1946])
        self.assertEqual(
            ledger[0],
            {
                "year": 1937,
                "termStartYear": 1937,
                "termCycleYear": 1,
                "inaugurationTransition": True,
                "presidentChangedWithinWindow": False,
                "endOfYearPresidentId": "franklin-d-roosevelt",
                "endOfYearPresidentName": "Franklin D. Roosevelt",
                "endOfYearPresidentParty": "D",
                "crspMarketProxyReturnDecimal": 0.11,
                "shillerSpCompositeApproximationReturnDecimal": 0.21,
            },
        )
        self.assertTrue(ledger[1]["inaugurationTransition"])
        self.assertTrue(ledger[1]["presidentChangedWithinWindow"])
        self.assertEqual(ledger[1]["endOfYearPresidentId"], "harry-s-truman")
        self.assertEqual(ledger[2]["termCycleYear"], 2)
        self.assertFalse(ledger[2]["inaugurationTransition"])
        self.assertEqual(summary["observations"], 3)
        self.assertEqual(summary["excludedPre1937Years"], [1936])
        crsp_cycle_one = next(
            item for item in summary["series"][0]["byPartyAndCycle"] if item["termCycleYear"] == 1
        )
        self.assertEqual(crsp_cycle_one["best"]["year"], 1945)
        self.assertEqual(crsp_cycle_one["worst"]["year"], 1937)

    def test_registers_disabled_manual_control_evidence_without_fetching_it(self) -> None:
        source = self.config.source("CTRL_074_A")
        self.assertFalse(source.enabled)
        self.assertEqual(source.parser, "manual_reviewed_control_evidence")
        self.assertEqual(source.captured_at, "2026-10-31T17:54:33Z")
        self.assertEqual(
            source.captured_sha256,
            "dd79cf46562091e216dccc7dfbf1ffb4fecf6561a94e8c930bd4d04c0b42dce3",
        )
        self.assertIn(
            "https://www.senate.gov/about/parties-leadership/party-division.htm", source.locators
        )
        self.assertEqual(source.captures[0].sha256, source.captured_sha256)
        self.assertEqual(len(source.captures), len(source.locators))
        general_bundle = self.config.source("CTRL_GOV_001")
        self.assertEqual(len(general_bundle.captures), 3)
        self.assertEqual(
            {capture.sha256 for capture in general_bundle.captures},
            {
                "7712424eb2ac8a7ca83a02e494d14a795253bccc0c6f9275c73e31822d692b55",
                "2be84a8b5cc5c71ba08ee3bf499712e6b5131a9ef8a215ac83c9c3b5625cf9f0",
                "65400bbe9f162a2b1b7d05df39b928f3ba4c4b6a0af1d7bbe71496232edfe17f",
            },
        )

    def test_exports_registered_manual_evidence_in_source_catalog(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            market, controls = directory / "market.csv", directory / "controls.csv"
            self.write_market(market)
            self.write_controls(controls, source_id="CTRL_074_A")
            dataset, _ = build_annual_dataset(self.config, market, controls, "window_start")
        bundle = next(record for record in dataset["sources"] if record["id"] == "CTRL_074_A")
        self.assertEqual(set(bundle), {"id", "title", "locator", "note"})
        self.assertIn(
            "https://www.senate.gov/about/parties-leadership/party-division.htm", bundle["note"]
        )
        self.assertIn(
            "SHA-256 dd79cf46562091e216dccc7dfbf1ffb4fecf6561a94e8c930bd4d04c0b42dce3",
            bundle["note"],
        )
        self.assertIn("evidence captures:", bundle["note"])

    def test_rejects_duplicate_calendar_month_with_different_dates(self) -> None:
        issues = check_monthly_returns(
            [
                {"date": "1936-01-30", "market_return_decimal": "0.01", "source_id": "FF_MKT_001"},
                {"date": "1936-01-31", "market_return_decimal": "0.02", "source_id": "FF_MKT_001"},
            ]
        )
        self.assertEqual({issue.check_id for issue in issues}, {"market.valid", "market.unique"})

    def test_rejects_manual_evidence_bundle_with_unpinned_locator(self) -> None:
        with TemporaryDirectory() as temporary:
            config_path = Path(temporary) / "sources.toml"
            config_path.write_text(
                """[pipeline]
schema_version = 1
project_id = "test"
currency = "USD"
annual_start_year = 1936
annual_end_year = 2023

[[source]]
id = "CTRL_TEST"
title = "Test evidence"
url = "https://example.test/evidence"
locators = ["https://example.test/evidence"]
parser = "manual_reviewed_control_evidence"
authority = "Test authority"
role = "manual_reviewed_political_control_evidence"
enabled = false
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ConfigError, "without a content-pinned capture"):
                load_config(config_path)
