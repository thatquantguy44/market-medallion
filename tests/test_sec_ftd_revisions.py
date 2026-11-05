"""S11-A05: corrections, reversions and cutoffs use actual capture knowledge."""

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from market_data_pipeline.pipelines.sec_fails_to_deliver.config import load_config
from market_data_pipeline.pipelines.sec_fails_to_deliver.pipeline import gold, run
from market_data_pipeline.pipelines.sec_fails_to_deliver.warehouse import SQLiteWarehouse

FIXTURE = Path("tests/fixtures/sec_fails_to_deliver/modern.txt").read_bytes()
CLOCK = datetime(2026, 11, 3, 10, tzinfo=UTC)


def bundle(payload):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("cnsfails202610a.txt", payload)
    return stream.getvalue()


def setup(tmp_path):
    value = yaml.safe_load(Path("config/sec_fails_to_deliver/pipeline.example.yml").read_text())
    source = tmp_path / "input.zip"
    value.update(
        warehouse="sqlite",
        root=str(tmp_path / "root"),
        partitions={
            "fixture": {
                "input": str(source),
                "manifest": str(Path("manifests/sec-fails-to-deliver.yml").resolve()),
            }
        },
    )
    path = tmp_path / "pipeline.yml"
    path.write_text(yaml.safe_dump(value))
    return path, source


def capture(path, source, payload, second):
    source.write_bytes(bundle(payload))
    return run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=second))


def first_price(path, **cutoff):
    return gold(path, limit=20, **cutoff)["rows"][0]["prior_close_price"]


def test_correction_reversion_and_unchanged_capture_keep_distinct_true_first_seen(tmp_path):
    path, source = setup(tmp_path)
    a = capture(path, source, FIXTURE, 0)
    b = capture(path, source, FIXTURE.replace(b"12.3400", b"12.3500"), 1)
    unchanged = capture(path, source, FIXTURE.replace(b"12.3400", b"12.3500"), 2)
    reverted = capture(path, source, FIXTURE, 3)
    assert [
        a["inserted_versions"],
        b["inserted_versions"],
        unchanged["inserted_versions"],
        reverted["inserted_versions"],
    ] == [2, 1, 0, 1]
    assert all(r["status"] == "SUCCEEDED" for r in (a, b, unchanged, reverted))
    assert gold(path, limit=20, as_of=CLOCK - timedelta(microseconds=1))["rows"] == []
    assert first_price(path, as_of=CLOCK) == Decimal("12.34")
    assert first_price(path, as_of=CLOCK + timedelta(seconds=1)) == Decimal("12.35")
    assert first_price(path, as_of=CLOCK + timedelta(seconds=2)) == Decimal("12.35")
    assert first_price(path, as_of=CLOCK + timedelta(seconds=3)) == Decimal("12.34")
    config = load_config(path)
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        rows = warehouse.connection.execute(
            "SELECT version_id,content_hash,first_seen_at,capture_sequence "
            "FROM sec_ftd_facts WHERE cusip='000000001' ORDER BY capture_sequence"
        ).fetchall()
        assert len(rows) == 3 and len({r[0] for r in rows}) == 3
        assert rows[0][1] == rows[2][1] and rows[0][0] != rows[2][0]
        assert [r[3] for r in rows] == [1, 2, 4]
        assert warehouse.select_facts(as_of=CLOCK + timedelta(seconds=2))[0][
            "first_seen_at"
        ] == CLOCK + timedelta(seconds=1)
        assert len(warehouse.select_runs()) == 4


def test_equal_clock_cutoffs_use_capture_sequence_and_coverage_from_that_cutoff(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    capture(path, source, FIXTURE.replace(b"12.3400", b"12.3500"), 0)
    capture(path, source, FIXTURE, 0)
    assert first_price(path, as_of=CLOCK, as_of_sequence=1) == Decimal("12.34")
    assert first_price(path, as_of=CLOCK, as_of_sequence=2) == Decimal("12.35")
    assert first_price(path, as_of=CLOCK, as_of_sequence=3) == Decimal("12.34")
    assert gold(path, limit=20, as_of=CLOCK, as_of_sequence=0)["rows"] == []
    selected = gold(path, limit=20, as_of=CLOCK, as_of_sequence=2)
    assert selected["coverage"][0]["capture_sequence"] == 2
    assert selected["knowledge_mode"] == "OBSERVED_AS_OF"
    assert first_price(path, as_of=CLOCK + timedelta(seconds=1), as_of_sequence=0) == Decimal(
        "12.34"
    )


def test_partial_replacement_cannot_withdraw_an_omitted_observation(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    one = (
        FIXTURE.splitlines(keepends=True)[0]
        + FIXTURE.splitlines(keepends=True)[1]
        + b"Trailer record count 1\nTrailer total quantity of shares 125\n"
    )
    result = capture(path, source, one, 1)
    assert result["status"] == "SUCCEEDED" and result["inserted_versions"] == 0
    assert gold(path, limit=20)["row_count"] == 2
    assert gold(path, limit=20)["coverage"][0]["received_row_count"] == 1


@pytest.mark.parametrize(
    "cutoff",
    [
        {"as_of_sequence": 1},
        {"as_of": datetime(2026, 11, 3)},
        {"as_of": CLOCK, "as_of_sequence": -1},
        {"as_of": CLOCK, "as_of_sequence": True},
    ],
)
def test_invalid_or_ambiguous_cutoffs_fail(tmp_path, cutoff):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    with pytest.raises(ValueError):
        gold(path, limit=20, **cutoff)


def evidence(path, raw, *, dates=None, hash_override=None):
    from test_sec_ftd_completeness import evidence_value

    proof = path.parent / "completeness.json"
    value = evidence_value(hash_override or hashlib.sha256(raw).hexdigest())
    if dates is not None:
        value["expected_settlement_dates"] = dates
    proof.write_text(json.dumps(value))
    config = yaml.safe_load(path.read_text())
    config["partitions"]["fixture"]["coverage_evidence"] = str(proof)
    path.write_text(yaml.safe_dump(config))
    return proof


def baseline_three():
    rows = FIXTURE.splitlines(keepends=True)
    return (
        b"".join(rows[:3]) + b"20261001|000000003|FIX.C|5|INVENTED C|1.25\nTrailer record count 3\n"
        b"Trailer total quantity of shares 1180591620717411303554\n"
    )


def without_first():
    rows = baseline_three().splitlines(keepends=True)
    return (
        rows[0]
        + rows[2]
        + rows[3]
        + b"Trailer record count 2\nTrailer total quantity of shares 1180591620717411303429\n"
    )


def test_complete_replacement_withdraws_once_and_reappearance_is_a_new_version(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, baseline_three(), 0)
    raw = bundle(without_first())
    source.write_bytes(raw)
    proof = evidence(path, raw)
    removed = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))
    assert removed["status"] == "SUCCEEDED" and removed["coverage_state"] == "COMPLETE"
    assert removed["inserted_versions"] == 1
    current = gold(path, limit=20)
    assert current["row_count"] == 2
    before = gold(path, limit=20, as_of=CLOCK)
    assert before["row_count"] == 3 and before["coverage"][0]["coverage_state"] == "PARTIAL"
    states = gold(path, limit=20, include_withdrawn=True)["rows"]
    withdrawn = next(r for r in states if r["cusip"] == "000000001")
    assert withdrawn["row_state"] == "WITHDRAWN" and withdrawn["quantity_fails"] is None
    assert withdrawn["source_row_number"] is None and withdrawn["run_id"] == removed["run_id"]
    config = load_config(path)
    capture_dir = config.root / "bronze" / removed["run_id"]
    assert (capture_dir / "coverage-evidence.json").read_bytes() == proof.read_bytes()
    assert (
        json.loads((capture_dir / "receipt.json").read_text())["receipt_version"]
        == "sec-ftd-capture/2"
    )
    unchanged = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=2))
    assert unchanged["inserted_versions"] == 0
    raw = bundle(baseline_three())
    source.write_bytes(raw)
    evidence(path, raw)
    returned = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=3))
    assert returned["inserted_versions"] == 1
    assert gold(path, limit=20)["row_count"] == 3
    assert gold(path, limit=20, as_of=CLOCK + timedelta(seconds=1))["row_count"] == 2


@pytest.mark.parametrize("bad", ["hash", "dates"])
def test_contradictory_completeness_evidence_cannot_remove_or_update_facts(tmp_path, bad):
    path, source = setup(tmp_path)
    capture(path, source, baseline_three(), 0)
    raw = bundle(without_first())
    source.write_bytes(raw)
    evidence(
        path,
        raw,
        hash_override="b" * 64 if bad == "hash" else None,
        dates=["2026-10-01"] if bad == "dates" else None,
    )
    result = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))
    assert result["status"] == "REJECTED" and result["error_code"] == "COVERAGE_EVIDENCE_INVALID"
    assert gold(path, limit=20)["row_count"] == 3


def test_coverage_only_upgrade_preserves_fact_versions_and_earlier_coverage(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    raw = bundle(FIXTURE)
    source.write_bytes(raw)
    evidence(path, raw)
    result = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))
    assert result["inserted_versions"] == 0 and result["coverage_state"] == "COMPLETE"
    assert gold(path, limit=20, as_of=CLOCK)["coverage"][0]["coverage_state"] == "PARTIAL"
    assert gold(path, limit=20)["coverage"][0]["expected_date_count"] == 2
    assert gold(path, limit=20)["rows"][0]["first_seen_at"] == CLOCK


def test_withdrawal_insert_failure_rolls_back_corrections_and_complete_coverage(tmp_path):
    path, source = setup(tmp_path)
    capture(path, source, baseline_three(), 0)
    config = load_config(path)
    with SQLiteWarehouse(config.root, config.storage) as warehouse:
        warehouse.connection.execute(
            "CREATE TRIGGER fail_withdrawal BEFORE INSERT ON sec_ftd_facts "
            "WHEN NEW.row_state='WITHDRAWN' BEGIN SELECT RAISE(ABORT,'fixture failure'); END"
        )
    raw = bundle(without_first().replace(b"|1.25", b"|1.30"))
    source.write_bytes(raw)
    evidence(path, raw)
    result = run(path, "fixture", clock=lambda: CLOCK + timedelta(seconds=1))
    assert result["error_code"] == "WAREHOUSE_WRITE_FAILED"
    current = gold(path, limit=20)
    assert current["row_count"] == 3
    assert next(r for r in current["rows"] if r["cusip"] == "000000003")[
        "prior_close_price"
    ] == Decimal("1.25")
    assert current["coverage"][0]["coverage_state"] == "FAILED"


def test_gold_cli_supports_aware_timestamp_sequence_and_explicit_withdrawn_states(tmp_path, capsys):
    from market_data_pipeline.pipelines.sec_fails_to_deliver.cli import main

    path, source = setup(tmp_path)
    capture(path, source, FIXTURE, 0)
    capture(path, source, FIXTURE.replace(b"12.3400", b"12.3500"), 0)
    assert (
        main(
            [
                "gold",
                "--config",
                str(path),
                "--limit",
                "20",
                "--as-of",
                CLOCK.isoformat(),
                "--as-of-sequence",
                "1",
                "--include-withdrawn",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["rows"][0]["prior_close_price_text"] == "12.3400"
    assert result["knowledge_mode"] == "OBSERVED_AS_OF"


@pytest.mark.parametrize(
    "timestamp", ["2026-11-03", "2026-11-03T10:00:00", "2026-11-03T10:00:00.1234567Z"]
)
def test_cli_refuses_date_only_naive_or_overprecise_cutoffs(timestamp):
    from market_data_pipeline.pipelines.sec_fails_to_deliver.cli import main

    with pytest.raises(SystemExit) as exc:
        main(["gold", "--config", "unused.yml", "--limit", "20", "--as-of", timestamp])
    assert exc.value.code == 2
