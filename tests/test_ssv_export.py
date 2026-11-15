"""SSV-AC01/14: admitted frozen exports and independent consumer readback."""

import json
import socket
from copy import deepcopy

import pytest
from test_ssv_bronze import fixture_grant
from test_ssv_cli import config_file, invoke
from test_ssv_gold_query import load as load
from test_ssv_parser import FIXTURES
from test_ssv_parser import manifest as manifest
from test_ssv_publication import publish

from market_data_pipeline.pipelines.finra_short_sale_volume import research_export as export

__all__ = ["load", "manifest"]
OPTIONS = dict(
    approved_use=fixture_grant(),
    operation="fixture",
    output_use="invented_fixture",
    scopes=["nyse_trf"],
)


def write(root, manifest, destination, **changes):
    return export.export(root, manifest, destination=destination, **(OPTIONS | changes))


def consume(path, **changes):
    return export.read(
        path, **(dict(expected_scopes=["nyse_trf"], output_use="invented_fixture") | changes)
    )


def test_ssv_ac14_frozen_publication_exact_text_lineage_and_no_network(
    tmp_path, manifest, load, monkeypatch
):
    load("2026-11-06", values=[("SYN.A", "20.00", "0", "100")])
    first = load(values=[("SYN.A", "40.1250", "5.00", "100.25")])
    selected = publish(tmp_path, manifest)
    load(values=[("SYN.A", "70", "0", "100")])
    before = {
        str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
    }
    monkeypatch.setattr(socket, "create_connection", lambda *a, **k: pytest.fail("network"))
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    summary = write(tmp_path, manifest, path)
    value = consume(path)
    assert summary["generation_id"] == selected["generation_id"]
    assert value["rows"][0]["short_volume"] == "40.1250"
    version = next(v for v in value["versions"] if v["capture_id"] == first["capture_id"])
    assert version["first_seen_at"] == first["archived_at"]
    assert version["provider_published_at"] is None
    assert version["short_exempt_volume"] == "5.00"
    assert value["coverage"]["knowledge_basis"] == "CURRENT_VINTAGE"
    assert value["publication"]["manifest_sha256"] == selected["manifest_sha256"]
    assert {
        str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()
    } == before
    text = path.read_text()
    for forbidden in (
        str(tmp_path),
        "permission_reference",
        "run_id",
        "approved_use",
        "attempt_id",
    ):
        assert forbidden not in text
    assert write(tmp_path, manifest, path) == summary


@pytest.mark.parametrize(
    "changes",
    [
        {"approved_use": None},
        {"output_use": "public_distribution"},
        {"output_use": "personal_noncommercial_internal_research"},
        {"operation": "internal_export"},
        {"approved_use": {**fixture_grant(), "operations": ["fixture", "internal_export"]}},
    ],
)
def test_ssv_ac01_ac14_disallowed_use_before_io(tmp_path, manifest, monkeypatch, changes):
    monkeypatch.setattr(export.publication, "read", lambda *a, **k: pytest.fail("source read"))
    with pytest.raises((ValueError, PermissionError)):
        write(tmp_path / "absent", manifest, tmp_path / "out.json", **changes)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("mode", ["empty", "zero", "missing", "removed", "huge"])
def test_ssv_ac14_empty_null_missing_removal_and_wide_quantities(tmp_path, manifest, load, mode):
    scopes = ["nyse_trf"]
    if mode == "empty":
        load(values=[])
    elif mode == "zero":
        load(values=[("SYN", "-0.00", "0E-2", "0")])
    elif mode == "removed":
        load()
        load(values=[])
    elif mode == "huge":
        load(values=[("SYN", "1" + "0" * 4095, "0", "1" + "0" * 4095)])
    else:
        load()
        load(facility="nasdaq_carteret_trf", values=[])
        scopes += ["nasdaq_carteret_trf"]
    publication = export.publication.publish(
        tmp_path,
        manifest,
        approved_use=fixture_grant(),
        operation="fixture",
        trade_date="2026-11-09",
        scopes=scopes,
    )
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    write(tmp_path, manifest, path, scopes=scopes)
    value = consume(path, expected_scopes=scopes)
    assert value["publication"]["generation_id"] == publication["generation_id"]
    if mode in ("empty", "removed"):
        assert value["rows"] == [] and value["coverage"]["status"] == "VALID_EMPTY"
        if mode == "removed":
            assert any(v["removed"] for v in value["versions"])
    elif mode == "zero":
        assert value["rows"][0]["short_share"] is None
    elif mode == "missing":
        assert value["rows"][0]["status"] == "NO_REPORTED_ROW"
        assert value["rows"][0]["total_volume"] is None
    else:
        assert len(value["rows"][0]["short_volume"]) == 4096


@pytest.mark.parametrize(
    "scopes", [["nasdaq_chicago_trf"], ["listed_trfs"], ["nyse_trf", "nyse_trf"]]
)
def test_ssv_ac14_expected_scope_must_match(tmp_path, manifest, load, scopes):
    load()
    publish(tmp_path, manifest)
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    with pytest.raises(ValueError):
        write(tmp_path, manifest, path, scopes=scopes)
    assert not path.exists()


@pytest.mark.parametrize("damage", ["files", "pending"])
def test_ssv_ac14_corrupt_or_pending_publication_refuses(tmp_path, manifest, load, damage):
    load()
    published = publish(tmp_path, manifest)
    if damage == "files":
        (tmp_path / "parquet/snapshots" / published["generation_id"] / "gold.parquet").write_bytes(
            b"bad"
        )
    else:
        load(values=[("SYN", "50", "0", "100")])

        def fault(at):
            if at == "committed":
                raise OSError("invented")

        with pytest.raises(OSError):
            publish(tmp_path, manifest, _fault=fault)
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    with pytest.raises(ValueError):
        write(tmp_path, manifest, path)
    assert not path.exists()


def test_ssv_ac14_destination_never_overwrites_source_conflicts_or_symlinks(
    tmp_path, manifest, load
):
    load()
    publish(tmp_path, manifest)
    with pytest.raises(ValueError):
        write(tmp_path, manifest, tmp_path / "export.json")
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    path.write_bytes(b"original")
    with pytest.raises(ValueError):
        write(tmp_path, manifest, path)
    assert path.read_bytes() == b"original"
    link = tmp_path.parent / (tmp_path.name + "-link.json")
    link.symlink_to(path)
    with pytest.raises(ValueError):
        write(tmp_path, manifest, link)
    assert path.read_bytes() == b"original"


@pytest.mark.parametrize("limits", [{"max_rows": 1}, {"max_bytes": 12}, {"max_rows": True}])
def test_ssv_ac14_budgets_leave_no_output(tmp_path, manifest, load, limits):
    load()
    publish(tmp_path, manifest)
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    with pytest.raises(ValueError):
        write(tmp_path, manifest, path, **limits)
    assert not path.exists()


def test_ssv_ac14_cli_portable_export_and_consumer_fixture(tmp_path):
    config = config_file(tmp_path)
    assert (
        invoke(
            config,
            "run",
            "--input",
            str(FIXTURES / "invented-valid.json"),
            "--receipt",
            str(FIXTURES / "invented-valid.receipt.json"),
        ).returncode
        == 0
    )
    assert (
        invoke(config, "publish", "--trade-date", "2026-11-09", "--scope", "nyse_trf").returncode
        == 0
    )
    path = tmp_path / "research.json"
    result = invoke(
        config,
        "export-research",
        "--destination",
        str(path),
        "--output-use",
        "invented_fixture",
        "--scope",
        "nyse_trf",
    )
    assert result.returncode == 0, result.stderr
    assert consume(path)["rows"][0]["symbol_raw"] == "000SYN.A"
    fixture = consume(FIXTURES / "invented-research-v1.json")
    assert fixture["rows"][0]["short_volume"] == "40.1250"
    with pytest.raises(ValueError):
        consume(path, expected_scopes=["nasdaq_chicago_trf"])
    with pytest.raises((ValueError, PermissionError)):
        consume(path, output_use="personal_noncommercial_internal_research")


@pytest.mark.parametrize(
    "damage", ["version", "units", "scope", "quantity", "lineage", "unknown", "rights"]
)
def test_ssv_ac14_consumer_rejects_invalid_contract(tmp_path, manifest, load, damage):
    load()
    publish(tmp_path, manifest)
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    write(tmp_path, manifest, path)
    value = deepcopy(json.loads(path.read_bytes()))
    if damage == "version":
        value["contract"] += "-unknown"
    elif damage == "units":
        value["coverage"]["quantity_unit"] = "USD"
    elif damage == "scope":
        value["request"]["facility_ids"] = ["nasdaq_chicago_trf"]
    elif damage == "quantity":
        value["rows"][0]["short_volume"] = 40.0
    elif damage == "lineage":
        value["versions"] = []
    elif damage == "rights":
        value["rights"]["redistribution"] = "allowed"
    else:
        value["credentials"] = "not-for-export"
    with pytest.raises((ValueError, PermissionError)):
        export.validate(value, expected_scopes=["nyse_trf"], output_use="invented_fixture")


@pytest.mark.parametrize(
    "damage",
    [
        "aggregate",
        "ratio",
        "component_date",
        "null_quantity",
        "bad_sequence",
        "malformed_partition",
        "malformed_version",
    ],
)
def test_ssv_ac14_consumer_checks_arithmetic_and_structural_lineage(
    tmp_path, manifest, load, damage
):
    load()
    publish(tmp_path, manifest)
    path = tmp_path.parent / (tmp_path.name + "-export.json")
    write(tmp_path, manifest, path)
    value = json.loads(path.read_bytes())
    if damage == "aggregate":
        value["rows"][0]["short_volume"] = "90"
    elif damage == "ratio":
        value["rows"][0]["short_share"] = "0.9"
    elif damage == "component_date":
        value["versions"][0]["trade_date"] = "2026-11-08"
    elif damage == "null_quantity":
        value["versions"][0]["short_volume"] = None
    elif damage == "bad_sequence":
        value["versions"][0]["capture_sequence"] = True
    elif damage == "malformed_partition":
        value["coverage"]["current_partitions"] = [None]
    else:
        value["versions"] = [None]
    with pytest.raises(ValueError):
        export.validate(value, expected_scopes=["nyse_trf"], output_use="invented_fixture")


def test_ssv_ac01_ac14_provider_export_requires_its_own_grant_before_read(
    tmp_path, manifest, monkeypatch
):
    grant = dict(
        purpose="personal_noncommercial_internal_research",
        route="finra_query_api",
        authorization="accepted_api_terms",
        permission_reference="invented-terms-test",
        credential_type="public",
        operations=["transform"],
    )
    monkeypatch.setattr(export.publication, "read", lambda *a, **k: pytest.fail("source read"))
    with pytest.raises(PermissionError):
        write(
            tmp_path / "absent",
            manifest,
            tmp_path / "out.json",
            approved_use=grant,
            operation="transform",
            output_use="personal_noncommercial_internal_research",
        )
    assert list(tmp_path.iterdir()) == []


def test_ssv_ac14_atomic_publish_failure_leaves_no_partial_artifact(
    tmp_path, manifest, load, monkeypatch
):
    load()
    publish(tmp_path, manifest)
    path = tmp_path.parent / (tmp_path.name + "-export.json")

    def fail(*args):
        raise OSError("invented interruption")

    monkeypatch.setattr(export.os, "link", fail)
    with pytest.raises(OSError):
        write(tmp_path, manifest, path)
    assert not path.exists() and not list(path.parent.glob(".ssv-export-*"))
