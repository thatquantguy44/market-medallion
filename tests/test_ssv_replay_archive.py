"""SSV-AC11: replay copies original Bronze evidence without allocating captures."""

import pytest
from test_ssv_bronze import fixture_grant
from test_ssv_parser import manifest as manifest
from test_ssv_pipeline import ingest

from market_data_pipeline.pipelines.finra_short_sale_volume.io import replay_archive

__all__ = ["manifest"]
OPTIONS = dict(approved_use=fixture_grant(), operation="fixture")


def selected(root, manifest, **limits):
    values = dict(
        max_captures=1000, max_bytes=20 * 1024**2, max_rows=5000, max_total_bytes=128 * 1024**2
    )
    values.update(limits)
    return replay_archive.inventory(root, manifest, **OPTIONS, **values)


def test_ssv_ac11_archive_copy_retains_exact_receipts_sequences_and_outcomes(tmp_path, manifest):
    source, target = tmp_path / "source", tmp_path / "target"
    result = ingest(source, manifest)
    source_inventory = selected(source, manifest)
    assert source_inventory["captures"][0]["receipt"]["capture_id"] == result["capture_id"]
    target.mkdir()
    replay_archive.copy(
        source, target, manifest, source_inventory, **OPTIONS, max_bytes=20 * 1024**2
    )
    before = {
        str(p.relative_to(source / "bronze")): p.read_bytes()
        for p in (source / "bronze").rglob("*")
        if p.is_file()
    }
    after = {
        str(p.relative_to(target / "bronze")): p.read_bytes()
        for p in (target / "bronze").rglob("*")
        if p.is_file()
    }
    assert before == after
    assert selected(target, manifest) == source_inventory


@pytest.mark.parametrize("damage", ["hash", "profile", "symlink"])
def test_ssv_ac11_archive_metadata_damage_refuses(tmp_path, manifest, damage):
    import json

    source = tmp_path / "source"
    ingest(source, manifest)
    path = next((source / "bronze/outcomes").glob("*/*.json"))
    value = json.loads(path.read_bytes())
    if damage == "symlink":
        path.unlink()
        path.symlink_to(source / "warehouse.json")
    else:
        value["outcome_sha256" if damage == "hash" else "profile"] = "invalid"
        from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import encoded

        path.write_bytes(encoded(value))
    with pytest.raises(ValueError):
        selected(source, manifest)


def test_ssv_ac11_archive_clock_regression_refuses_original_sequence_order(tmp_path, manifest):
    from datetime import timedelta
    from uuid import uuid4

    from test_ssv_bronze import NOW
    from test_ssv_parser import FIXTURES, receipt

    from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze

    source = tmp_path / "source"
    ingest(source, manifest)
    payload = (FIXTURES / "invented-valid.json").read_bytes()
    saved = bronze.archive_input(
        source,
        payload,
        manifest,
        receipt(payload),
        **OPTIONS,
        run_id=str(uuid4()),
        archived_at=NOW - timedelta(seconds=1),
    )
    bronze.parse_capture(source, saved["capture_id"], manifest, **OPTIONS)
    with pytest.raises(ValueError, match="regressed"):
        selected(source, manifest)
