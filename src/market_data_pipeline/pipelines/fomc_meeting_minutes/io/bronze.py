"""FOMC immutable local capture and explicit, independently versioned PostgreSQL archive."""

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from market_data_pipeline.pipelines.fomc_meeting_minutes.io.postgres import CONTRACT
from market_data_pipeline.pipelines.fomc_meeting_minutes.manifest import manifest_from_dict

MIGRATION = "fomc-bronze-postgres/1"
LINK_MIGRATION = "fomc-bronze-lineage-postgres/1"
DISCOVERY_MIGRATION = "fomc-bronze-discovery-postgres/1"
_HEADERS = {"content-type", "content-length", "last-modified", "date", "etag"}


def _identity(value):
    try:
        result = str(uuid.UUID(value))
        if result != value:
            raise ValueError
        return result
    except (TypeError, ValueError, AttributeError):
        raise ValueError("FOMC capture ID must be canonical UUID text") from None


def _write(path, payload):
    # Publish an fsynced complete file exclusively; never expose a partial target.
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import _sync_directory

    temporary = path.parent / (".write-" + uuid.uuid4().hex)
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _json(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def capture(
    root,
    payload,
    *,
    manifest,
    store,
    source_url,
    kind,
    captured_at,
    status,
    headers,
    max_bytes,
    body_complete=True,
    transport_error=False,
    attempt_id=None,
    startup_error=False,
):
    """Write complete received bytes locally first, then append them to PostgreSQL.

    Even rejected received bodies are recorded in full. If database persistence
    fails, the local receipt remains without a commit marker and can be retried.
    This API takes already-received bytes; it does not itself fetch or truncate.
    """
    manifest_from_dict(manifest.to_dict())
    canonical = manifest.capture_url(source_url, kind)
    if (
        not isinstance(payload, bytes)
        or not isinstance(captured_at, datetime)
        or captured_at.utcoffset() is None
        or type(startup_error) is not bool
        or (startup_error and not transport_error)
        or type(transport_error) is not bool
        or (transport_error and (status is not None or payload or body_complete))
        or (not transport_error and (type(status) is not int or not 100 <= status <= 599))
        or type(max_bytes) is not int
        or max_bytes <= 0
        or not isinstance(headers, dict)
        or type(body_complete) is not bool
    ):
        raise ValueError("invalid FOMC capture fields")
    safe_headers = {}
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() in _HEADERS:
            if not isinstance(value, str) or len(value) > 1024 or any(ord(c) < 32 for c in value):
                raise ValueError("invalid FOMC response header")
            safe_headers[key.lower()] = value
    reasons = []
    if startup_error:
        reasons.append("startup_probe_error")
    if transport_error:
        reasons.append("transport_error")
    elif status != 200:
        reasons.append("http_status")
    if len(payload) > max_bytes:
        reasons.append("size_limit")
    if not payload:
        reasons.append("empty_body")
    if not body_complete:
        reasons.append("incomplete_body")
    if "content-length" in safe_headers:
        try:
            if int(safe_headers["content-length"]) != len(payload):
                reasons.append("content_length_mismatch")
        except ValueError:
            reasons.append("invalid_content_length")
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.recovery import (
        begin_attempt,
        read_attempt,
    )

    identity = attempt_id or begin_attempt(
        root,
        manifest=manifest,
        source_url=canonical,
        kind=kind,
        max_bytes=max_bytes,
        started_at=datetime.now(UTC),
        phase="local_import",
    )
    intent = read_attempt(root, identity)
    expected = {
        "source_url": canonical,
        "kind": kind,
        "max_bytes": max_bytes,
        "manifest_sha256": hashlib.sha256(_json(manifest.to_dict())).hexdigest(),
    }
    if any(intent[k] != value for k, value in expected.items()):
        raise ValueError("FOMC capture conflicts with its acquisition intent")
    receipt = {
        "schema_id": "fomc-bronze-capture/1",
        "capture_id": identity,
        "attempt_phase": intent["phase"],
        "source_url": canonical,
        "kind": kind,
        "captured_at": captured_at.astimezone(UTC).isoformat(),
        "http_status": status,
        "headers": safe_headers,
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "accepted": not reasons,
        "reasons": reasons,
        "max_bytes": max_bytes,
        "body_complete": body_complete,
        "manifest_sha256": hashlib.sha256(_json(manifest.to_dict())).hexdigest(),
    }
    directory = Path(root) / "captures" / identity
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import (
        _sync_directory,
        make_directory,
    )

    make_directory(directory.parent)
    directory.mkdir(mode=0o700, exist_ok=False)
    _sync_directory(directory.parent)
    _write(directory / "receipt.json", _json(receipt))
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import store_payload

    os.link(store_payload(root, payload), directory / "payload.bin")
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.objects import _sync_directory

    _sync_directory(directory)
    commit_capture(root, identity, store)
    return receipt


def read_capture(root, capture_id):
    directory = Path(root) / "captures" / _identity(capture_id)
    try:
        receipt = json.loads((directory / "receipt.json").read_bytes())
        payload = (directory / "payload.bin").read_bytes()
        if (
            not isinstance(receipt, dict)
            or receipt.get("schema_id") != "fomc-bronze-capture/1"
            or receipt.get("capture_id") != capture_id
            or receipt.get("size_bytes") != len(payload)
            or receipt.get("sha256") != hashlib.sha256(payload).hexdigest()
        ):
            raise ValueError
        marker = directory / "committed.json"
        if marker.exists():
            expected = {
                "capture_id": capture_id,
                "receipt_sha256": hashlib.sha256(_json(receipt)).hexdigest(),
            }
            if json.loads(marker.read_bytes()) != expected:
                raise ValueError
    except (OSError, ValueError, UnicodeError):
        raise ValueError("FOMC capture receipt or payload hash mismatch") from None
    return receipt, payload


def commit_capture(root, capture_id, store):
    receipt, payload = read_capture(root, capture_id)
    store.save_capture(receipt, payload)
    marker = Path(root) / "captures" / capture_id / "committed.json"
    value = _json(
        {"capture_id": capture_id, "receipt_sha256": hashlib.sha256(_json(receipt)).hexdigest()}
    )
    try:
        _write(marker, value)
    except FileExistsError:
        if marker.read_bytes() != value:
            raise ValueError("FOMC capture commit marker mismatch") from None
    from market_data_pipeline.pipelines.fomc_meeting_minutes.io.recovery import finish_attempt

    finish_attempt(
        root,
        capture_id,
        {"result": "captured", "receipt_sha256": hashlib.sha256(_json(receipt)).hexdigest()},
    )


class PostgresBronzeStore:
    """Explicit component migration; does not alter the inventory catalog marker or pointer."""

    def __init__(self, inventory_store):
        self.store = inventory_store

    def migrate(self):
        store, conn = self.store, self.store.connection
        with conn.transaction():
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("market-medallion:" + store.config.schema,),
            )
            marker = conn.execute(
                store._query("SELECT contract_id FROM {schema}.catalog_state")
            ).fetchall()
            if marker != [(CONTRACT,)]:
                raise ValueError("FOMC inventory ownership marker mismatch")
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname=%s", (store.config.schema,)
                )
            }
            base = {"bronze_schema_migrations", "bronze_captures"}
            links = {"bronze_inventory_links", "bronze_document_links"}
            existing = tables & base
            if existing and existing != base:
                raise ValueError("incomplete or unowned FOMC Bronze schema")
            if not existing:
                if tables & (links | {"bronze_inventory_discovery"}):
                    raise ValueError("unowned FOMC Bronze lineage/discovery tables")
                conn.execute(
                    store._query(
                        "CREATE TABLE {schema}.bronze_schema_migrations "
                        "(migration_id text PRIMARY KEY, "
                        "applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
                    )
                )
                conn.execute(
                    store._query(
                        "CREATE TABLE {schema}.bronze_captures (capture_id uuid PRIMARY KEY, "
                        "source_url text NOT NULL, captured_at timestamptz NOT NULL, "
                        "sha256 text NOT NULL, receipt jsonb NOT NULL, payload bytea NOT NULL)"
                    )
                )
                conn.execute(
                    store._query(
                        "INSERT INTO {schema}.bronze_schema_migrations (migration_id) VALUES (%s)"
                    ),
                    (MIGRATION,),
                )
                migrations = {MIGRATION}
            else:
                migrations = {
                    row[0]
                    for row in conn.execute(
                        store._query("SELECT migration_id FROM {schema}.bronze_schema_migrations")
                    )
                }
                if migrations not in (
                    {MIGRATION},
                    {MIGRATION, LINK_MIGRATION},
                    {MIGRATION, LINK_MIGRATION, DISCOVERY_MIGRATION},
                ):
                    raise ValueError("FOMC Bronze migration identity mismatch")
            if LINK_MIGRATION in migrations:
                if tables & links != links:
                    raise ValueError("incomplete FOMC Bronze lineage migration")
                self._migrate_discovery(tables, migrations)
                return
            if tables & links:
                raise ValueError("unowned FOMC Bronze lineage tables")
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_inventory_links ("
                    "capture_id uuid NOT NULL REFERENCES {schema}.bronze_captures(capture_id), "
                    "run_id uuid NOT NULL REFERENCES {schema}.inventory_runs(run_id), "
                    "source_page_url text NOT NULL, PRIMARY KEY(capture_id,run_id), "
                    "FOREIGN KEY(run_id,source_page_url) "
                    "REFERENCES {schema}.inventory_pages(run_id,source_page_url))"
                )
            )
            conn.execute(
                store._query(
                    "CREATE TABLE {schema}.bronze_document_links ("
                    "capture_id uuid NOT NULL REFERENCES {schema}.bronze_captures(capture_id), "
                    "run_id uuid NOT NULL, document_id text NOT NULL, "
                    "PRIMARY KEY(capture_id,run_id,document_id), "
                    "FOREIGN KEY(run_id,document_id) "
                    "REFERENCES {schema}.inventory_documents(run_id,document_id))"
                )
            )
            conn.execute(
                store._query(
                    "INSERT INTO {schema}.bronze_schema_migrations (migration_id) VALUES (%s)"
                ),
                (LINK_MIGRATION,),
            )

            self._migrate_discovery(tables, migrations)

    def _migrate_discovery(self, tables, migrations):
        table = "bronze_inventory_discovery"
        if DISCOVERY_MIGRATION in migrations:
            if table not in tables:
                raise ValueError("incomplete FOMC Bronze discovery migration")
            return
        if table in tables:
            raise ValueError("unowned FOMC Bronze discovery table")
        self.store.connection.execute(
            self.store._query(
                "CREATE TABLE {schema}.bronze_inventory_discovery ("
                "run_id uuid PRIMARY KEY REFERENCES {schema}.inventory_runs(run_id), "
                "evidence jsonb NOT NULL, evidence_sha256 text NOT NULL)"
            )
        )
        self.store.connection.execute(
            self.store._query(
                "INSERT INTO {schema}.bronze_schema_migrations (migration_id) VALUES (%s)"
            ),
            (DISCOVERY_MIGRATION,),
        )

    def read_inventory_discovery(self, run_id):
        """Read hash-checked selected-page evidence, never claim complete documents."""
        row = self.store.connection.execute(
            self.store._query(
                "SELECT evidence,evidence_sha256 FROM {schema}.bronze_inventory_discovery "
                "WHERE run_id=%s"
            ),
            (_identity(run_id),),
        ).fetchone()
        if row is None:
            return None
        if hashlib.sha256(_json(row[0])).hexdigest() != row[1]:
            raise ValueError("FOMC discovery evidence hash mismatch")
        return row[0]

    def save_capture(self, receipt, payload):
        store, conn = self.store, self.store.connection
        identity = _identity(receipt["capture_id"])
        if hashlib.sha256(payload).hexdigest() != receipt.get("sha256") or len(
            payload
        ) != receipt.get("size_bytes"):
            raise ValueError("FOMC capture hash mismatch")
        from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
            PostgresObjectStore,
        )

        objects = PostgresObjectStore(self)
        with conn.transaction():
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("market-medallion:" + store.config.schema,),
            )
            active = objects.active()
            if active:
                objects.save_object(receipt["sha256"], payload)
            conn.execute(
                store._query(
                    "INSERT INTO {schema}.bronze_captures "
                    "(capture_id,source_url,captured_at,sha256,receipt,payload) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(capture_id) DO NOTHING"
                ),
                (
                    identity,
                    receipt["source_url"],
                    receipt["captured_at"],
                    receipt["sha256"],
                    store._Jsonb(receipt),
                    None if active else payload,
                ),
            )
            if active:
                objects.associate(identity, receipt["sha256"])
            row = self._capture_row(identity, active)
            if (
                row is None
                or row[0] != receipt["source_url"]
                or row[2] != receipt["sha256"]
                or row[3] != receipt
                or bytes(row[4]) != payload
                or row[1] != datetime.fromisoformat(receipt["captured_at"])
            ):
                raise ValueError("FOMC capture ID already has different evidence")

    def _capture_row(self, capture_id, objects_active):
        query = (
            (
                "SELECT c.source_url,c.captured_at,c.sha256,c.receipt,o.payload "
                "FROM {schema}.bronze_captures c JOIN {schema}.bronze_capture_objects l "
                "ON l.capture_id=c.capture_id JOIN {schema}.bronze_objects o ON o.sha256=l.sha256 "
                "WHERE c.capture_id=%s"
            )
            if objects_active
            else (
                "SELECT source_url,captured_at,sha256,receipt,payload "
                "FROM {schema}.bronze_captures WHERE capture_id=%s"
            )
        )
        return self.store.connection.execute(self.store._query(query), (capture_id,)).fetchone()

    def read_capture(self, capture_id):
        from market_data_pipeline.pipelines.fomc_meeting_minutes.io.object_postgres import (
            PostgresObjectStore,
        )

        with self.store.connection.transaction():
            self.store.connection.execute(
                "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s,0))",
                ("market-medallion:" + self.store.config.schema,),
            )
            row = self._capture_row(_identity(capture_id), PostgresObjectStore(self).active())
        if row is None:
            raise ValueError("FOMC capture not found")
        receipt, payload = row[3], bytes(row[4])
        if (
            row[0] != receipt.get("source_url")
            or row[2] != receipt.get("sha256")
            or row[1] != datetime.fromisoformat(receipt["captured_at"])
            or receipt.get("size_bytes") != len(payload)
            or receipt.get("sha256") != hashlib.sha256(payload).hexdigest()
        ):
            raise ValueError("FOMC database capture hash mismatch")
        return receipt, payload

    def link_inventory(self, capture_id, run_id):
        """Associate a successful inventory capture with an existing matching page claim."""
        capture_id, run_id = _identity(capture_id), _identity(run_id)
        receipt, _ = self.read_capture(capture_id)
        if receipt.get("kind") != "inventory" or not receipt.get("accepted"):
            raise ValueError("FOMC inventory link requires an accepted inventory capture")
        store = self.store
        with store.connection.transaction():
            row = store.connection.execute(
                store._query(
                    "SELECT 1 FROM {schema}.inventory_pages WHERE run_id=%s AND source_page_url=%s"
                ),
                (run_id, receipt["source_url"]),
            ).fetchone()
            if not row:
                raise ValueError("FOMC inventory capture does not match the inventory page")
            store.connection.execute(
                store._query(
                    "INSERT INTO {schema}.bronze_inventory_links "
                    "(capture_id,run_id,source_page_url) "
                    "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING"
                ),
                (capture_id, run_id, receipt["source_url"]),
            )

    def link_document(self, capture_id, run_id, document_id):
        """Associate an accepted capture with the exact URL claim in one inventory run."""
        capture_id, run_id = _identity(capture_id), _identity(run_id)
        receipt, _ = self.read_capture(capture_id)
        if receipt.get("kind") != "document" or not receipt.get("accepted"):
            raise ValueError("FOMC document link requires an accepted document capture")
        store = self.store
        with store.connection.transaction():
            row = store.connection.execute(
                store._query(
                    "SELECT 1 FROM {schema}.inventory_documents "
                    "WHERE run_id=%s AND document_id=%s AND source_url=%s"
                ),
                (run_id, document_id, receipt["source_url"]),
            ).fetchone()
            if not row:
                raise ValueError("FOMC capture does not match the inventory document URL")
            store.connection.execute(
                store._query(
                    "INSERT INTO {schema}.bronze_document_links (capture_id,run_id,document_id) "
                    "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING"
                ),
                (capture_id, run_id, document_id),
            )

    def document_meetings(self, capture_id):
        """Return every retained meeting association, including bundled-source claims."""
        self.read_capture(capture_id)
        rows = self.store.connection.execute(
            self.store._query(
                "SELECT DISTINCT l.run_id,l.document_id,e.meeting_id,e.source_page_url "
                "FROM {schema}.bronze_document_links l JOIN {schema}.inventory_entries e "
                "ON e.run_id=l.run_id AND e.document_id=l.document_id "
                "WHERE l.capture_id=%s "
                "ORDER BY l.run_id,l.document_id,e.meeting_id,e.source_page_url"
            ),
            (_identity(capture_id),),
        ).fetchall()
        return [
            dict(
                zip(
                    ("run_id", "document_id", "meeting_id", "source_page_url"),
                    (str(row[0]), *row[1:]),
                    strict=True,
                )
            )
            for row in rows
        ]

    def publish_inventory_capture(self, capture_id, manifest, *, root):
        """Publish one page only when it does not drop existing source-page coverage."""
        return self.publish_inventory_captures([capture_id], manifest, root=root)

    def publish_inventory_captures(
        self, capture_ids, manifest, *, root, historical_years=None, start_year=None
    ):
        """Atomically publish an issue-free page union and link every archived page.

        Passing historical_years=(start,end) requires both source inventories and
        exactly their discovered page set for that bounded historical selection.
        Without it this remains a metadata union with no discovery coverage claim.
        Neither mode certifies complete source documents or extracted text.
        """
        from dataclasses import asdict

        from market_data_pipeline.pipelines.fomc_meeting_minutes.config import validate_start_year
        from market_data_pipeline.pipelines.fomc_meeting_minutes.data.discovery import (
            CALENDAR_URL,
            INDEX_URL,
            discover_inventory_pages,
        )
        from market_data_pipeline.pipelines.fomc_meeting_minutes.data.inventory import (
            inventory_from_dict,
        )
        from market_data_pipeline.pipelines.fomc_meeting_minutes.data.native import (
            parse_native_page,
        )

        manifest_from_dict(manifest.to_dict())
        if historical_years is not None and (
            not isinstance(historical_years, tuple)
            or len(historical_years) != 2
            or any(type(year) is not int for year in historical_years)
        ):
            raise ValueError("FOMC discovery requires an explicit historical year pair")
        if start_year is not None:
            start_year = validate_start_year(start_year)
            if historical_years is None or historical_years[0] < start_year:
                raise ValueError("FOMC publication is before the configured operating scope")
        if (
            not isinstance(capture_ids, (list, tuple))
            or not 1 <= len(capture_ids) <= 128
            or len(set(capture_ids)) != len(capture_ids)
        ):
            raise ValueError("FOMC publication requires 1–128 distinct captures")
        digest = hashlib.sha256(_json(manifest.to_dict())).hexdigest()
        pages, captures = [], {}
        urls = set()
        for capture_id in capture_ids:
            receipt, payload = self.read_capture(capture_id)
            marker = Path(root) / "captures" / _identity(capture_id) / "committed.json"
            if not marker.is_file() or read_capture(root, capture_id) != (receipt, payload):
                raise ValueError("FOMC publication requires matching committed filesystem evidence")
            if (
                receipt.get("kind") != "inventory"
                or not receipt.get("accepted")
                or receipt.get("manifest_sha256") != digest
            ):
                raise ValueError("FOMC publication requires an accepted matching inventory capture")
            if receipt["source_url"] in urls:
                raise ValueError("FOMC publication requires one capture per source page")
            urls.add(receipt["source_url"])
            captures[receipt["source_url"]] = (capture_id, receipt, payload)
        evidence = None
        if INDEX_URL in urls and historical_years is None:
            raise ValueError("FOMC year-index publication requires explicit historical years")
        if historical_years is not None:
            if not {INDEX_URL, CALENDAR_URL}.issubset(urls):
                raise ValueError("FOMC discovery requires both source inventories")
            discovery = discover_inventory_pages(
                captures[INDEX_URL][2],
                captures[CALENDAR_URL][2],
                manifest,
                start_year=historical_years[0],
                end_year=historical_years[1],
            )
            if discovery.issues:
                raise ValueError("FOMC inventory has unresolved discovery issues")
            if urls != set(discovery.expected_pages):
                raise ValueError("FOMC inventory does not match discovered expected pages")
            evidence = {
                "schema_id": "fomc-inventory-publication-discovery/1",
                "coverage_kind": "selected_inventory_pages",
                "native_parser_profile_id": "fomc-native-inventory/2",
                "manifest_sha256": digest,
                "discovery": discovery.to_dict(),
                "capture_ids": {url: captures[url][0] for url in sorted(urls)},
            }
            pages.append({"source_page_url": INDEX_URL, "meetings": []})
        for url, (_, receipt, payload) in sorted(captures.items()):
            if url == INDEX_URL:
                continue
            parsed = parse_native_page(payload, url, manifest)
            if parsed.issues or not parsed.inventory.meetings:
                raise ValueError("FOMC inventory has unresolved discovery issues or no meetings")
            if start_year is not None and any(
                m.committee_body != "fomc" or any(day.year < start_year for day in m.meeting_dates)
                for m in parsed.inventory.meetings
            ):
                raise ValueError("FOMC inventory contains meetings outside the operating scope")
            if evidence is not None:
                evidence["operating_start_year"] = start_year
                evidence.setdefault("native_page_evidence", {})[url] = {
                    "exclusions": [asdict(e) for e in parsed.exclusions],
                    "cross_references": [asdict(c) for c in parsed.cross_references],
                }
            meetings = {m.meeting_id: m for m in parsed.inventory.meetings}
            docs = {d.document_id: d for d in parsed.inventory.documents}
            rows = []
            for entry in parsed.inventory.entries:
                meeting = meetings[entry.meeting_id]
                documents = []
                if entry.document_id:
                    document = docs[entry.document_id]
                    documents.append(
                        {
                            "document_kind": document.document_kind,
                            "document_label_raw": entry.document_label_raw,
                            "href": entry.href_raw,
                            "representation": document.representation,
                            "provider_release_date": (
                                entry.provider_release_date.isoformat()
                                if entry.provider_release_date
                                else None
                            ),
                        }
                    )
                rows.append(
                    {
                        "committee_body": meeting.committee_body,
                        "event_kind": meeting.event_kind,
                        "meeting_dates": [day.isoformat() for day in meeting.meeting_dates],
                        "meeting_label_raw": entry.meeting_label_raw,
                        "session_label_raw": meeting.session_label_raw,
                        "minutes_status": entry.minutes_status,
                        "documents": documents,
                    }
                )
            pages.append({"source_page_url": receipt["source_url"], "meetings": rows})
        union = inventory_from_dict({"schema_id": manifest.schema_id, "pages": pages}, manifest)
        with self.store.connection.transaction():
            self.store.connection.execute(
                self.store._query(
                    "SELECT contract_id FROM {schema}.catalog_state WHERE singleton=true FOR UPDATE"
                )
            )
            current = self.store.read_inventory()
            if current and not set(current["source_pages"]).issubset(urls):
                raise ValueError("FOMC publication would drop existing source pages")
            result = self.store.save_inventory(union, manifest, input_kind="normalized_inventory")
            for capture_id in capture_ids:
                self.link_inventory(capture_id, result["run_id"])
            if evidence is not None:
                self.store.connection.execute(
                    self.store._query(
                        "INSERT INTO {schema}.bronze_inventory_discovery "
                        "(run_id,evidence,evidence_sha256) VALUES (%s,%s,%s)"
                    ),
                    (
                        result["run_id"],
                        self.store._Jsonb(evidence),
                        hashlib.sha256(_json(evidence)).hexdigest(),
                    ),
                )
        return result
