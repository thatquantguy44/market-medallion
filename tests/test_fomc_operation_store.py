"""FOMC-AC06: owned audit migration and crash-safe receipt recovery, no network."""

from datetime import UTC, datetime

import pytest
from test_fomc_postgres import store as store

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import PostgresBronzeStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.dependencies import (
    PostgresDependencyStore,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore

__all__ = ["store"]
pytestmark = pytest.mark.postgres


@pytest.fixture
def audit(store):
    bronze = PostgresBronzeStore(store)
    bronze.migrate()
    PostgresDependencyStore(bronze).migrate()
    text = PostgresTextStore(bronze)
    text.migrate()
    PostgresChunkStore(text).migrate()
    audit = OperationStore(store)
    audit.migrate()
    audit.migrate()
    return audit


def test_audit_recovery_is_exact_and_does_not_invent_a_result(audit, tmp_path):
    from psycopg.errors import CheckViolation

    identity = audit.begin({"started_at": datetime.now(UTC).isoformat()}, root=tmp_path)
    audit.store.connection.execute(
        audit.store._query(
            "ALTER TABLE {schema}.operation_records ADD CONSTRAINT fail_test CHECK(false)"
        )
    )
    with pytest.raises(CheckViolation):
        audit.append(identity, {"event": "captured"}, root=tmp_path)
    assert audit.read_run(identity, root=tmp_path)["records"] == []
    audit.store.connection.execute(
        audit.store._query("ALTER TABLE {schema}.operation_records DROP CONSTRAINT fail_test")
    )
    recovered = audit.recover(identity, root=tmp_path)
    assert recovered["records"][0]["payload"] == {"event": "captured"}
    assert recovered["result"] is None
    audit.append(identity, {"status": "partial"}, root=tmp_path, kind="result")
    with pytest.raises(ValueError, match="final result"):
        audit.append(identity, {}, root=tmp_path)
    assert audit.latest(root=tmp_path)["result"] == {"status": "partial"}


def test_audit_migration_rejects_unowned_or_partial_tables(audit):
    store = audit.store
    store.connection.execute(
        store._query("UPDATE {schema}.operation_state SET contract_id=%s"), ("foreign",)
    )
    with pytest.raises(ValueError, match="identity"):
        audit.migrate()
    store.connection.execute(
        store._query("UPDATE {schema}.operation_state SET contract_id=%s"),
        ("fomc-operations-postgres/1",),
    )
    store.connection.execute(store._query("DROP TABLE {schema}.operation_records"))
    with pytest.raises(ValueError, match="incomplete"):
        audit.migrate()
