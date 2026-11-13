"""PR016-R04/N01: observed fact versions retain clocks, tokens and reversion."""

from datetime import UTC, datetime, timedelta

import pytest

from market_data_pipeline.pipelines.presidential_research.config import load_config
from market_data_pipeline.pipelines.presidential_research.pipeline import import_local_capture
from market_data_pipeline.pipelines.presidential_research.warehouse import (
    export_parquet,
    ingest_capture,
    observed_as_of,
)

NOW = datetime(2026, 11, 13, 12, tzinfo=UTC)
CONFIG = load_config("config/presidential_research/sources.toml")


def capture(root, value, when=NOW):
    payload = f"observation_date,FEDFUNDS\n1970-01-01,{value}\n".encode()
    receipt, _ = import_local_capture(
        root, CONFIG, "FEDFUNDS_001", payload, input_kind="SYNTHETIC_FIXTURE", admitted_at=when
    )
    return receipt


def test_revisions_reversions_and_equal_clock_order_survive_reopen(tmp_path):
    a = capture(tmp_path, "8.980")
    b = capture(tmp_path, "8.980")
    c = capture(tmp_path, "9.010")
    d = capture(tmp_path, "8.980")
    results = [
        ingest_capture(tmp_path, "FEDFUNDS_001", item["capture_id"]) for item in (a, b, c, d)
    ]
    assert [item["new_versions"] for item in results] == [1, 0, 1, 1]
    assert ingest_capture(tmp_path, "FEDFUNDS_001", b["capture_id"])["new_versions"] == 0
    one = observed_as_of(tmp_path, NOW, capture_sequence=a["capture_sequence"])
    three = observed_as_of(tmp_path, NOW, capture_sequence=c["capture_sequence"])
    last = observed_as_of(tmp_path, NOW)
    assert [rows[0]["values"]["policy_rate_percent"] for rows in (one, three, last)] == [
        "8.98",
        "9.01",
        "8.98",
    ]
    assert one[0]["values"]["source_token"] == "8.980"
    assert len({one[0]["version_id"], three[0]["version_id"], last[0]["version_id"]}) == 3
    assert observed_as_of(tmp_path, NOW - timedelta(microseconds=1)) == []
    assert last[0]["input_kind"] == "SYNTHETIC_FIXTURE"
    assert last[0]["raw_sha256"] == d["sha256"]


def test_missing_values_are_versions_not_zero_or_disappearing(tmp_path):
    first = capture(tmp_path, "8.980")
    ingest_capture(tmp_path, "FEDFUNDS_001", first["capture_id"])
    payload = b"observation_date,FEDFUNDS\n1970-01-01,.\n1970-02-01,3.50\n"
    second, _ = import_local_capture(
        tmp_path, CONFIG, "FEDFUNDS_001", payload, input_kind="SYNTHETIC_FIXTURE", admitted_at=NOW
    )
    result = ingest_capture(tmp_path, "FEDFUNDS_001", second["capture_id"])
    values = observed_as_of(tmp_path, NOW)
    assert result["missing_count"] == 1
    assert values[0]["values"]["policy_rate_percent"] is None
    assert values[0]["values"]["source_token"] == "."
    assert values[1]["values"]["policy_rate_percent"] == "3.5"


def test_out_of_order_and_tampered_capture_are_rejected(tmp_path):
    first = capture(tmp_path, "1")
    second = capture(tmp_path, "2")
    ingest_capture(tmp_path, "FEDFUNDS_001", second["capture_id"])
    with pytest.raises(ValueError, match="order"):
        ingest_capture(tmp_path, "FEDFUNDS_001", first["capture_id"])
    third = capture(tmp_path, "3")
    (tmp_path / "bronze" / "FEDFUNDS_001" / third["capture_id"] / "payload").write_bytes(
        b"tampered"
    )
    with pytest.raises(ValueError, match="digest"):
        ingest_capture(tmp_path, "FEDFUNDS_001", third["capture_id"])


def test_typed_parquet_round_trip_and_refuse_overwrite(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    receipt = capture(tmp_path, "8.980")
    ingest_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])
    output = tmp_path / "fixture.parquet"
    export_parquet(tmp_path, output, NOW)
    table = pq.read_table(output)
    assert table.schema.field("observation_date").type == pa.date32()
    assert table.schema.field("numeric_value").type == pa.decimal128(38, 18)
    assert table.schema.field("first_seen_at").type == pa.timestamp("us", tz="UTC")
    assert str(table.to_pylist()[0]["numeric_value"]) == "8.980000000000000000"
    assert table.to_pylist()[0]["source_token"] == "8.980"
    with pytest.raises(FileExistsError):
        export_parquet(tmp_path, output, NOW)


def test_factor_tokens_and_review_only_candidates_remain_distinct(tmp_path):
    import io
    from zipfile import ZipFile

    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("factors.csv", ",Mkt-RF,SMB,HML,RF\n193601,2.00,0,0,0.10\n")
    factor, _ = import_local_capture(
        tmp_path,
        CONFIG,
        "FF_MKT_001",
        buffer.getvalue(),
        input_kind="SYNTHETIC_FIXTURE",
        admitted_at=NOW,
    )
    assert ingest_capture(tmp_path, "FF_MKT_001", factor["capture_id"])["status"] == "PARSED"
    html = b"""<table><tr><th>Congress</th><th>House</th><th>Senate</th><th>Presidency</th></tr>
    <tr><td>74th (1935-1937)</td><td>Democrats</td><td>Tied</td>
    <td>Democrat (Fictional)</td></tr></table>"""
    political, _ = import_local_capture(
        tmp_path, CONFIG, "HOUSE_GOV_001", html, input_kind="SYNTHETIC_FIXTURE", admitted_at=NOW
    )
    assert ingest_capture(tmp_path, "HOUSE_GOV_001", political["capture_id"])["status"] == "PARSED"
    facts = observed_as_of(tmp_path, NOW)
    market, candidate = facts
    assert market["values"]["market_return_decimal"] == "0.021"
    assert market["values"]["market_excess_token"] == "2.00"
    assert market["values"]["risk_free_token"] == "0.10"
    assert candidate["fact_type"] == "POLITICAL_CANDIDATE"
    assert candidate["values"]["review_status"] == "CANDIDATE_REQUIRES_DATED_REVIEW"
    assert candidate["values"]["senate_party"] == "TIED"


def test_unrepresentable_and_malformed_values_record_rejection_without_new_fact(tmp_path):
    first = capture(tmp_path, "3.25")
    ingest_capture(tmp_path, "FEDFUNDS_001", first["capture_id"])
    precision = capture(tmp_path, "0.1234567890123456789")
    assert ingest_capture(tmp_path, "FEDFUNDS_001", precision["capture_id"])["status"] == "REJECTED"
    malformed = capture(tmp_path, "NaN")
    failed = ingest_capture(tmp_path, "FEDFUNDS_001", malformed["capture_id"])
    assert failed["status"] == "REJECTED"
    assert ingest_capture(tmp_path, "FEDFUNDS_001", malformed["capture_id"])["reused"] is True
    assert observed_as_of(tmp_path, NOW)[0]["values"]["policy_rate_percent"] == "3.25"


def test_unrelated_sqlite_database_is_not_adopted(tmp_path):
    import sqlite3

    silver = tmp_path / "silver"
    silver.mkdir()
    with sqlite3.connect(silver / "presidential.sqlite3") as db:
        db.execute("CREATE TABLE unrelated (value TEXT)")
    receipt = capture(tmp_path, "1")
    with pytest.raises(ValueError, match="identity"):
        ingest_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])
    with sqlite3.connect(silver / "presidential.sqlite3") as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [
            ("unrelated",)
        ]


def test_tampered_fact_or_raw_capture_cannot_be_queried(tmp_path):
    import sqlite3

    receipt = capture(tmp_path, "5.00")
    ingest_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])
    with sqlite3.connect(tmp_path / "silver" / "presidential.sqlite3") as db:
        original = db.execute("SELECT values_json FROM facts").fetchone()[0]
        db.execute("UPDATE facts SET values_json=?", (original.replace("5.00", "6.00"),))
    with pytest.raises(ValueError, match="integrity"):
        observed_as_of(tmp_path, NOW)
    with sqlite3.connect(tmp_path / "silver" / "presidential.sqlite3") as db:
        db.execute("UPDATE facts SET values_json=?", (original,))
    (tmp_path / "bronze" / "FEDFUNDS_001" / receipt["capture_id"] / "payload").write_bytes(
        b"changed"
    )
    with pytest.raises(ValueError, match="integrity"):
        observed_as_of(tmp_path, NOW)


def test_capture_admission_clock_must_not_move_backwards(tmp_path):
    first = capture(tmp_path, "1", NOW)
    earlier = capture(tmp_path, "2", NOW - timedelta(seconds=1))
    ingest_capture(tmp_path, "FEDFUNDS_001", first["capture_id"])
    with pytest.raises(ValueError, match="order"):
        ingest_capture(tmp_path, "FEDFUNDS_001", earlier["capture_id"])


def test_donor_decimal_rounding_never_becomes_a_silent_fact(tmp_path):
    receipt = capture(tmp_path, "12345678901234567890.123456789012345678")
    result = ingest_capture(tmp_path, "FEDFUNDS_001", receipt["capture_id"])
    assert result["status"] == "REJECTED"
    assert observed_as_of(tmp_path, NOW) == []


def test_shiller_decoded_binary_values_preserve_missingness_and_typed_nulls(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace

    import pyarrow.parquet as pq

    class Sheet:
        values = [["Date", "P", "D", "CAPE"], ["1871.01", 4.44, "", "NA"]]
        nrows = 2
        ncols = 4

        def cell_value(self, row, column):
            return self.values[row][column]

    workbook = SimpleNamespace(sheet_by_name=lambda name: Sheet())
    monkeypatch.setitem(sys.modules, "xlrd", SimpleNamespace(open_workbook=lambda **_: workbook))
    receipt, _ = import_local_capture(
        tmp_path,
        CONFIG,
        "SHILLER_001",
        b"fictional-workbook",
        input_kind="SYNTHETIC_FIXTURE",
        admitted_at=NOW,
    )
    assert ingest_capture(tmp_path, "SHILLER_001", receipt["capture_id"])["status"] == "PARSED"
    fact = observed_as_of(tmp_path, NOW)[0]
    assert fact["values"]["source_value_origin"] == "XLS_BINARY_DECODED"
    assert fact["values"]["dividend"] == fact["values"]["cape"] == ""
    output = tmp_path / "shiller.parquet"
    export_parquet(tmp_path, output, NOW)
    row = pq.read_table(output).to_pylist()[0]
    assert str(row["price"]) == "4.440000000000000000"
    assert row["dividend"] is None and row["cape"] is None


def test_selected_capture_requires_matching_admitted_facts(tmp_path):
    import sqlite3

    from market_data_pipeline.pipelines.presidential_research.warehouse import (
        admitted_capture_facts,
    )

    first = capture(tmp_path, "1.00")
    with pytest.raises(ValueError, match="unavailable"):
        admitted_capture_facts(tmp_path, "FEDFUNDS_001", first["capture_id"])
    ingest_capture(tmp_path, "FEDFUNDS_001", first["capture_id"])
    _, facts = admitted_capture_facts(tmp_path, "FEDFUNDS_001", first["capture_id"])
    assert facts[0]["values"]["source_token"] == "1.00"
    second = capture(tmp_path, "1.0")
    ingest_capture(tmp_path, "FEDFUNDS_001", second["capture_id"])
    _, duplicate = admitted_capture_facts(tmp_path, "FEDFUNDS_001", second["capture_id"])
    assert duplicate[0]["version_id"] == facts[0]["version_id"]
    assert duplicate[0]["values"]["source_token"] == "1.0"
    with sqlite3.connect(tmp_path / "silver" / "presidential.sqlite3") as db:
        db.execute("DELETE FROM capture_facts WHERE capture_id=?", (second["capture_id"],))
    with pytest.raises(ValueError, match="inventory"):
        admitted_capture_facts(tmp_path, "FEDFUNDS_001", second["capture_id"])
