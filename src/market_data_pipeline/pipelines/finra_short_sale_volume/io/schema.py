"""SSV-AC06 SQLite contract: facts and full partition membership commit together."""

CONTRACT = "finra-ssv-sqlite/1"
SCHEMA = """
PRAGMA user_version=1;
CREATE TABLE identity (body TEXT NOT NULL);
CREATE TABLE attempts (
 attempt_id TEXT PRIMARY KEY, capture_id TEXT NOT NULL, run_id TEXT NOT NULL,
 trade_date TEXT NOT NULL, facility_id TEXT NOT NULL, archived_at TEXT NOT NULL,
 capture_sequence INTEGER NOT NULL, started_at TEXT NOT NULL, completed_at TEXT,
 status TEXT NOT NULL, error_code TEXT, profile TEXT NOT NULL
);
CREATE TABLE captures (
 capture_id TEXT PRIMARY KEY, capture_sequence INTEGER NOT NULL UNIQUE,
 archived_at TEXT NOT NULL, trade_date TEXT NOT NULL, facility_id TEXT NOT NULL,
 status TEXT NOT NULL, body TEXT NOT NULL
);
CREATE INDEX capture_partition ON captures(trade_date,facility_id,archived_at,capture_sequence);
CREATE TABLE versions (
 version_id TEXT PRIMARY KEY, trade_date TEXT NOT NULL, facility_id TEXT NOT NULL,
 symbol_raw TEXT NOT NULL, first_seen_at TEXT NOT NULL, capture_sequence INTEGER NOT NULL,
 capture_id TEXT NOT NULL REFERENCES captures(capture_id) DEFERRABLE INITIALLY DEFERRED,
 removed INTEGER NOT NULL CHECK(removed IN (0,1)), body TEXT NOT NULL
);
CREATE INDEX version_key ON versions
 (trade_date,facility_id,symbol_raw,first_seen_at,capture_sequence);
CREATE TABLE capture_rows (
 capture_id TEXT NOT NULL REFERENCES captures(capture_id) DEFERRABLE INITIALLY DEFERRED,
 symbol_raw TEXT NOT NULL, version_id TEXT NOT NULL REFERENCES versions(version_id),
 source_row INTEGER NOT NULL, PRIMARY KEY(capture_id,symbol_raw)
);
CREATE TABLE partitions (
 trade_date TEXT NOT NULL, facility_id TEXT NOT NULL,
 capture_id TEXT NOT NULL REFERENCES captures(capture_id),
 PRIMARY KEY(trade_date,facility_id)
);
CREATE VIEW gold AS
 SELECT v.version_id,v.trade_date,v.facility_id,v.symbol_raw,v.body,m.source_row,p.capture_id
 FROM partitions p JOIN capture_rows m ON p.capture_id=m.capture_id
 JOIN versions v ON v.version_id=m.version_id WHERE v.removed=0;
"""
