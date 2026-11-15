"""SSV-AC07/08/10/11/14: native query, frozen export and replay equivalence."""

import hashlib
import json
from datetime import datetime

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_gold_query import load as load
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume import (
    publication,
    replay,
    research,
    research_export,
)

__all__ = ["load", "manifest"]
FACILITIES = ["nasdaq_carteret_trf", "nasdaq_chicago_trf", "nyse_trf"]
OPTIONS = dict(approved_use=fixture_grant(), operation="fixture")
INITIAL = [("SYN.A", "40.1250", "5.00", "100.25"), ("000SYN.B", "-0.00", "0E-2", "0")]


def inventory(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize(
    "scenario",
    [
        "unchanged_reorder",
        "correction_reversion",
        "removal_missing",
        "cutoff_time",
        "cutoff_sequence",
        "empty_population",
        "exact_exponents",
    ],
)
def test_ssv_ac14_native_frozen_export_replay_equivalence(tmp_path, manifest, load, scenario):
    def add(day, facility, values):
        return load(
            day,
            facility=facility,
            values=values,
            **({"clock": NOW} if scenario == "cutoff_sequence" else {}),
        )

    values = [] if scenario == "empty_population" else INITIAL
    if scenario == "exact_exponents":
        values = [("SYN.A", "4.01250E-100", "5.00E-101", "1.0025E-99"), INITIAL[1]]
    prior = [] if scenario == "empty_population" else [("SYN.A", "20", "1.00", "100")]
    for f in FACILITIES:
        add("2026-11-06", f, prior)
    latest = [add("2026-11-09", f, values) for f in FACILITIES]
    extra = {}
    if scenario == "unchanged_reorder":
        for f in FACILITIES:
            add("2026-11-09", f, list(reversed(values)))
    elif scenario == "correction_reversion":
        add("2026-11-09", "nyse_trf", [("SYN.A", "70", "5", "100.25"), INITIAL[1]])
        add("2026-11-09", "nyse_trf", values)
    elif scenario == "removal_missing":
        add("2026-11-09", "nasdaq_carteret_trf", values[:1])
        add("2026-11-09", "nasdaq_chicago_trf", [])
    elif scenario in ("cutoff_time", "cutoff_sequence"):
        extra["as_of"] = datetime.fromisoformat(latest[-1]["archived_at"])
        if scenario == "cutoff_sequence":
            extra["as_of_sequence"] = latest[-1]["capture_sequence"]
        add("2026-11-09", "nyse_trf", [("SYN.A", "70", "5", "100.25"), INITIAL[1]])
    request = dict(trade_date="2026-11-09", scopes=["listed_trfs"], lookback_days=5, **extra)
    native = research.research_gold(tmp_path, manifest, **OPTIONS, **request)
    first = publication.publish(tmp_path, manifest, **OPTIONS, **request)
    before = inventory(tmp_path)
    output = tmp_path.parent / (tmp_path.name + "-source.json")
    research_export.export(
        tmp_path,
        manifest,
        **OPTIONS,
        destination=output,
        output_use="invented_fixture",
        scopes=["listed_trfs"],
        expected_generation_id=first["generation_id"],
    )
    artifact = research_export.read(
        output, expected_scopes=["listed_trfs"], output_use="invented_fixture"
    )
    dimensions = {
        k: native[k]
        for k in (
            "trade_date",
            "facility_ids",
            "prior_trade_date",
            "calendar_day_gap",
            "evidence_known_at",
        )
    }
    assert artifact["rows"] == [row | dimensions for row in native["rows"]]
    if scenario == "unchanged_reorder":
        component = artifact["rows"][0]["components"][0]
        version = next(
            v for v in artifact["versions"] if v["version_id"] == component["version_id"]
        )
        assert component["source_row"] != version["source_row"]
        assert component["capture_id"] != component["source_capture_id"]
    if scenario in ("cutoff_time", "cutoff_sequence"):
        assert all(row["short_volume"] != "70" for row in artifact["versions"])
    if scenario == "removal_missing":
        assert any(v["removed"] for v in artifact["versions"])
        assert all(r["short_volume"] is None for r in artifact["rows"])
    destination = tmp_path.parent / (tmp_path.name + "-replay")
    rebuilt = replay.replay(tmp_path, destination, manifest, **OPTIONS)
    assert rebuilt["status"] == "SUCCEEDED"
    second = publication.publish(destination, manifest, **OPTIONS, **request)
    copy = tmp_path.parent / (tmp_path.name + "-replay.json")
    research_export.export(
        destination,
        manifest,
        **OPTIONS,
        destination=copy,
        output_use="invented_fixture",
        scopes=["listed_trfs"],
        expected_generation_id=second["generation_id"],
    )
    replayed = research_export.read(
        copy, expected_scopes=["listed_trfs"], output_use="invented_fixture"
    )
    assert artifact["publication"] != replayed["publication"]
    assert {k: v for k, v in artifact.items() if k != "publication"} == {
        k: v for k, v in replayed.items() if k != "publication"
    }
    assert inventory(tmp_path) == before


def test_ssv_ac14_consumer_rejects_prior_date_outside_lookback(tmp_path, manifest, load):
    load("2026-11-06", values=[])
    load(values=[])
    publication.publish(tmp_path, manifest, **OPTIONS, trade_date="2026-11-09", scopes=["nyse_trf"])
    path = tmp_path.parent / (tmp_path.name + "-out.json")
    research_export.export(
        tmp_path,
        manifest,
        **OPTIONS,
        destination=path,
        scopes=["nyse_trf"],
        output_use="invented_fixture",
    )
    value = json.loads(path.read_bytes())
    value["request"]["earliest_trade_date"] = "2026-11-07"
    with pytest.raises(ValueError):
        research_export.validate(value, expected_scopes=["nyse_trf"], output_use="invented_fixture")
