"""SSV-AC12: separate strict acquisition YAML keeps credential values out of config."""

import pytest
import yaml
from test_ssv_bronze import provider_grant
from test_ssv_cli import REPO

from market_data_pipeline.pipelines.finra_short_sale_volume.acquisition_config import load_config


def config_file(tmp_path, **changes):
    value = dict(
        schema_id="finra-ssv-acquisition-config/1",
        root="store",
        manifest=str(REPO / "manifests/finra-short-sale-volume.yml"),
        operation="acquire",
        approved_use=provider_grant(),
        live=False,
        network={},
        limits={},
        credentials=dict(client_id_env="FINRA_API_KEY", client_secret_env="FINRA_PASSWORD_SECRET"),
    )
    value.update(changes)
    path = tmp_path / "acquisition.yml"
    path.write_text(yaml.safe_dump(value))
    return path


def test_ssv_ac12_config_is_pure_and_does_not_read_credentials_or_start_network(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FINRA_PASSWORD_SECRET", "invented-not-loaded")
    config = load_config(config_file(tmp_path))
    assert config.root == tmp_path / "store" and config.live is False
    assert config.network.timeout_seconds == 30 and config.limits.page_rows == 5000
    assert "invented-not-loaded" not in repr(config)
    assert not config.root.exists()


@pytest.mark.parametrize(
    "changes",
    [
        dict(live=1),
        dict(secret="invented-secret"),
        dict(
            credentials=dict(
                client_id_env="secret-value", client_secret_env="FINRA_PASSWORD_SECRET"
            )
        ),
        dict(network=dict(proxy_url="http://secret")),
        dict(limits=dict(page_rows=5001)),
        dict(approved_use=None),
        dict(operation="local_import"),
    ],
)
def test_ssv_ac12_config_rejects_unadmitted_or_unknown_settings(tmp_path, changes):
    with pytest.raises((ValueError, PermissionError)):
        load_config(config_file(tmp_path, **changes))
    assert not (tmp_path / "store").exists()
