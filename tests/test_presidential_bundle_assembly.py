"""PR016-R05/R09/R10: assemble a complete, explicitly fictional bundle."""

import io
import sys
from datetime import UTC, datetime
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import test_presidential_review_workbook as review_fixtures

from market_data_pipeline.pipelines.presidential_research.bundle_assembly import build_full_bundle
from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture
from market_data_pipeline.pipelines.presidential_research.review import promote_review_workbook
from market_data_pipeline.pipelines.presidential_research.warehouse import ingest_capture

NOW = datetime(2026, 11, 13, tzinfo=UTC)


def fixture(tmp_path, monkeypatch):
    config_path = tmp_path / "sources.toml"
    template = open("config/presidential_research/sources.toml", encoding="utf-8").read()
    config_path.write_text(template.replace("annual_end_year = 2023", "annual_end_year = 1938"))
    config = load_config(config_path)
    rows = [["Date", "P", "D", "CAPE"]]
    for year in range(1934, 1938):
        for month in range(1, 13):
            rows.append([f"{year}.{month:02}", 100 + (year - 1934) * 12 + month, 3, 20])

    class Sheet:
        nrows = len(rows)
        ncols = 4

        def cell_value(self, row, column):
            return rows[row][column]

    monkeypatch.setitem(
        sys.modules,
        "xlrd",
        SimpleNamespace(open_workbook=lambda **_: SimpleNamespace(sheet_by_name=lambda _: Sheet())),
    )
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        factors = [",Mkt-RF,SMB,HML,RF"]
        factors.extend(
            f"{year}{month:02},0.10,0,0,0.05"
            for year in range(1935, 1939)
            for month in range(1, 13)
        )
        archive.writestr("factors.csv", "\n".join(factors))
    policy = b"observation_date,FEDFUNDS\n1935-10-01,2.50\n1936-10-01,3.00\n"
    payloads = {
        "FF_MKT_001": buffer.getvalue(),
        "SHILLER_001": b"fictional workbook",
        "FEDFUNDS_001": policy,
    }
    root = tmp_path / "store"
    ids = {}
    for source, payload in payloads.items():
        receipt, status = import_local_capture(
            root, config, source, payload, input_kind="SYNTHETIC_FIXTURE", admitted_at=NOW
        )
        assert status["status"] == "PARSED"
        assert ingest_capture(root, source, receipt["capture_id"])["status"] == "PARSED"
        ids[source] = receipt["capture_id"]
    workbook = tmp_path / "review.xlsx"
    review_fixtures._workbook_fixture(workbook, "APPROVED")
    with ZipFile(workbook) as source:
        files = {name: source.read(name) for name in source.namelist()}
    files["xl/worksheets/sheet1.xml"] = files["xl/worksheets/sheet1.xml"].replace(
        b"1937-01-03", b"1940-01-03"
    )
    with ZipFile(workbook, "w", ZIP_DEFLATED) as destination:
        for name, payload in files.items():
            destination.writestr(name, payload)
    review = promote_review_workbook(
        root, config, workbook, admitted_at=NOW, evidence_kind="SYNTHETIC_FIXTURE"
    )
    return root, ids, review


def test_five_dataset_and_evidence_counts_are_explicitly_fictional(tmp_path, monkeypatch):
    root, ids, review = fixture(tmp_path, monkeypatch)
    result = build_full_bundle(root, ids, review["review_id"], profile_id="fixture-complete")
    assert result["input_kind"] == "SYNTHETIC_FIXTURE"
    assert set(result["fact_inventory"]) == {"FF_MKT_001", "SHILLER_001", "FEDFUNDS_001"}
    assert all(len(item["versions_sha256"]) == 64 for item in result["fact_inventory"].values())
    datasets = result["datasets"]
    assert {key: len(value[0]["windows"]) for key, value in datasets.items()} == {
        "crsp-market-proxy-annual.json": 3,
        "crsp-market-proxy-common-sample-annual.json": 2,
        "shiller-sp-composite-annual.json": 2,
        "crsp-market-proxy-inherited-conditions-annual.json": 2,
        "shiller-sp-composite-inherited-conditions-annual.json": 2,
    }
    assert all(dataset["title"].startswith("Fictional fixture") for dataset, _ in datasets.values())
    assert all(
        source["locator"] is None
        for dataset, _ in datasets.values()
        for source in dataset["sources"]
    )
    import json
    from decimal import Decimal, localcontext

    with localcontext() as context:
        context.prec = 50
        expected_market = Decimal("1.0015") ** 12 - 1
        expected_shiller = Decimal(1)
        for price in range(125, 137):
            expected_shiller *= (Decimal(price) + Decimal("0.25")) / Decimal(price - 1)
        expected_shiller -= 1
    first = result["comparison"]["annual_rows"][0]
    assert first["year"] == 1936
    assert first["crspMarketProxyReturnDecimal"] == pytest.approx(float(expected_market), abs=1e-12)
    assert first["shillerSpCompositeApproximationReturnDecimal"] == pytest.approx(
        float(expected_shiller), abs=1e-12
    )
    assert (
        json.loads(result["artifacts"]["public-series-comparison.json"])["annual_rows"]
        == result["comparison"]["annual_rows"]
    )
    assert result["comparison"]["summary"]["observations"] == 2
    assert result["cycle_summary"]["observations"] == 1
    assert result["cycle_summary"]["excludedPre1937Years"] == [1936]
    assert len(result["artifacts"]) == 7
    assert result["artifacts"]["presidential-cycle-ledger.csv"]


def test_frozen_label_missing_capture_and_unadmitted_version_block_build(tmp_path, monkeypatch):
    root, ids, review = fixture(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="Frozen review"):
        build_full_bundle(
            root, ids, review["review_id"], profile_id="public-reconstruction-2026-11-03"
        )
    with pytest.raises(ValueError, match="source"):
        build_full_bundle(
            root,
            {"FF_MKT_001": ids["FF_MKT_001"]},
            review["review_id"],
            profile_id="fixture-complete",
        )
    (root / "bronze" / "FEDFUNDS_001" / ids["FEDFUNDS_001"] / "payload").write_bytes(b"changed")
    with pytest.raises(ValueError, match="digest"):
        build_full_bundle(root, ids, review["review_id"], profile_id="fixture-complete")
