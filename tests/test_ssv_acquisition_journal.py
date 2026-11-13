"""SSV-AC12: bound acquisition run/event evidence cannot become a false success."""

import json

import pytest
from test_ssv_bronze import NOW, fixture_grant
from test_ssv_parser import manifest as manifest

from market_data_pipeline.pipelines.finra_short_sale_volume.acquisition_config import (
    AcquisitionConfig,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.acquisition_journal import open_run

__all__ = ["manifest"]


def test_ssv_ac12_journal_records_immutable_events_and_explicit_result(tmp_path, manifest):
    config = AcquisitionConfig(tmp_path, manifest, "fixture", fixture_grant())
    with open_run(config, {"partitions": []}, clock=lambda: NOW) as run:
        run.event(dict(phase="partition", error_code="transport_failed", capture_id=None))
        run.finish(dict(status="INCOMPLETE", partitions=[]))
        run_id = run.run_id
    root = tmp_path / "acquisition/runs" / run_id
    assert json.loads((root / "result.json").read_bytes())["status"] == "INCOMPLETE"
    assert json.loads(next((root / "events").iterdir()).read_bytes())["capture_id"] is None
    assert "secret" not in (root / "request.json").read_text()


@pytest.mark.parametrize("damage", ["binding", "missing", "symlink"])
def test_ssv_ac12_journal_bound_damage_refuses_recreation(tmp_path, manifest, damage):
    config = AcquisitionConfig(tmp_path, manifest, "fixture", fixture_grant())
    with open_run(config, {"partitions": []}, clock=lambda: NOW):
        pass
    if damage == "binding":
        (tmp_path / "acquisition.json").write_text("{}")
    elif damage == "missing":
        (tmp_path / "acquisition/identity.json").unlink()
    else:
        path = tmp_path / "acquisition/runs"
        path.rename(tmp_path / "saved-runs")
        path.symlink_to(tmp_path / "saved-runs", target_is_directory=True)
    with pytest.raises(ValueError):
        with open_run(config, {"partitions": []}, clock=lambda: NOW):
            pass
