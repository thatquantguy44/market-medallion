"""FOMC-AC06: source-owned bounded acquisition with immutable rejected attempts."""

import time
from collections import Counter
from datetime import UTC, datetime

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.assets import asset_framing_valid
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.dependencies import (
    discover_html_dependencies,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.discovery import (
    CALENDAR_URL,
    INDEX_URL,
    discover_inventory_pages,
    parse_historical_index,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.native import parse_native_page
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.operations import (
    retry_delay,
    select_documents,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import extract_text
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.acquisition import (
    AcquisitionLimits,
    AcquisitionRun,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.bronze import (
    PostgresBronzeStore,
    read_capture,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.catalog import PostgresTextCatalog
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunk_catalog import (
    PostgresChunkCatalog,
)
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.chunks import PostgresChunkStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.operation_store import OperationStore
from market_data_pipeline.pipelines.fomc_meeting_minutes.io.text import PostgresTextStore
from market_data_pipeline.shared.files import writer_lock


class RunAbort(ValueError):
    pass


class Batch:
    def __init__(
        self,
        cfg,
        settings,
        store,
        manifest,
        years,
        mode,
        run_id,
        *,
        now=None,
        monotonic=None,
        sleep=None,
    ):
        self.cfg, self.settings, self.store, self.manifest = cfg, settings, store, manifest
        self.years, self.mode, self.run_id = years, mode, run_id
        self.now = now or (lambda: datetime.now(UTC))
        self.clock = monotonic or time.monotonic
        self.sleep = sleep or time.sleep
        self.started = self.clock()
        self.last_request = None
        self.audit = OperationStore(store)
        self.bronze = PostgresBronzeStore(store)
        self.text = PostgresTextStore(self.bronze)
        self.chunks = PostgresChunkStore(self.text)
        self.text_catalog = PostgresTextCatalog(self.text)
        self.chunk_catalog = PostgresChunkCatalog(self.chunks)
        self.unknown_capture_bytes = False
        self.startup_attempt_id = None
        self.received = 0
        self.run = None
        self.cache = {}
        self.history = []
        self.native = {}
        self.documents = {}
        self.assets = {}
        self.dependency_sets = {}
        self.outcomes = Counter()
        self.selected_documents = 0
        self.source_outcomes = {}
        self.excluded = []

    def event(self, **value):
        self.audit.append(self.run_id, value, root=self.cfg.root)

    def check(self):
        if self.clock() - self.started >= self.settings.max_elapsed_seconds:
            raise RunAbort("elapsed_budget")

    def load_cache(self):
        rows = self.store.connection.execute(
            self.store._query(
                "SELECT capture_id FROM {schema}.bronze_captures "
                "WHERE receipt->>'accepted'='true' ORDER BY captured_at,capture_id LIMIT %s"
            ),
            (self.settings.max_capture_history + 1,),
        ).fetchall()
        if len(rows) > self.settings.max_capture_history:
            raise RunAbort("capture_history_limit")
        for (identity,) in rows:
            receipt, payload = self.text.dependencies._verified(
                str(identity), self.manifest, self.cfg.root
            )
            self.cache[receipt["source_url"]] = (receipt, payload)
            self.history.append(receipt)

    def _valid(self, receipt, payload, kind):
        if receipt["kind"] != kind:
            return False
        if kind == "asset":
            return asset_framing_valid(payload, receipt["source_url"])
        if kind == "inventory":
            return True
        result = extract_text(payload, receipt["source_url"], self.manifest)
        self.native[receipt["capture_id"]] = result
        return result["status"] != "failed"

    def _wait(self, seconds):
        self.check()
        if seconds >= self.settings.max_elapsed_seconds - (self.clock() - self.started):
            raise RunAbort("elapsed_budget")
        if seconds > 0:
            self.sleep(seconds)
        self.check()

    def start(self):
        self.check()
        limits = AcquisitionLimits(
            max_requests=self.settings.max_requests - 2,
            inventory_max_bytes=self.settings.inventory_max_bytes,
            document_max_bytes=max(self.settings.document_max_bytes, self.settings.asset_max_bytes),
        )

        def startup(identity):
            self.startup_attempt_id = identity
            self.event(event="startup_intent", attempt_id=identity)

        self.run = AcquisitionRun.start(
            self.cfg.root,
            manifest=self.manifest,
            store=self.bronze,
            source_url=INDEX_URL,
            settings=self.cfg.network,
            limits=limits,
            start_year=self.cfg.start_year,
            on_attempt=startup,
        )
        self.last_request = self.clock()
        self.event(
            event="route_selected",
            attempt_id=self.run.startup_attempt_id,
            route=self.run.client.route,
            fallback_used=self.run.client.fallback_used,
            reserved_probe_requests=2,
        )

    def fetch(self, url, kind, *, reuse=False):
        self.check()
        if reuse and url in self.cache:
            receipt, payload = self.cache[url]
            if self._valid(receipt, payload, kind):
                self.event(
                    event="reused", capture_id=receipt["capture_id"], source_url=url, kind=kind
                )
                return receipt, payload, True
        limit = getattr(self.settings, kind + "_max_bytes")
        for attempt in range(1, self.settings.max_attempts + 1):
            self.check()
            if self.run.requests >= self.run.limits.max_requests:
                raise RunAbort("request_budget")
            remaining = self.settings.max_total_bytes - self.received
            if remaining <= 1:
                raise RunAbort("byte_budget")
            if self.last_request is not None:
                self._wait(
                    max(
                        0,
                        self.settings.request_interval_seconds - (self.clock() - self.last_request),
                    )
                )
            self.last_request = self.clock()
            try:
                receipt = self.run.acquire(
                    url, kind, max_bytes=min(limit, remaining - 1), now=self.now
                )
            except Exception:
                identity = self.run.last_attempt_id
                try:
                    receipt, payload = read_capture(self.cfg.root, identity)
                    self.received += receipt["size_bytes"]
                    self.event(
                        event="capture_uncommitted",
                        capture_id=identity,
                        source_url=url,
                        bytes=receipt["size_bytes"],
                    )
                except Exception:
                    self.unknown_capture_bytes = True
                raise RunAbort("capture_write_failed") from None
            payload = self.bronze.read_capture(receipt["capture_id"])[1]
            self.received += receipt["size_bytes"]
            self.history.append(receipt)
            self.event(
                event="capture",
                capture_id=receipt["capture_id"],
                source_url=url,
                kind=kind,
                accepted=receipt["accepted"],
                http_status=receipt["http_status"],
                bytes=receipt["size_bytes"],
                attempt=attempt,
            )
            self.check()
            if receipt["accepted"]:
                self.cache[url] = (receipt, payload)
                return receipt, payload, False
            if self.received >= self.settings.max_total_bytes:
                raise RunAbort("byte_budget")
            delay = retry_delay(receipt, attempt, self.now(), self.settings)
            if delay is None:
                raise RunAbort("source_rejected")
            self.event(event="retry", source_url=url, delay_seconds=delay)
            self._wait(delay)
        raise RunAbort("source_rejected")

    def inventories(self):
        index = self.fetch(INDEX_URL, "inventory")[0]
        calendar = self.fetch(CALENDAR_URL, "inventory")[0]
        ib = self.cache[INDEX_URL][1]
        cb = self.cache[CALENDAR_URL][1]
        parsed_index = parse_historical_index(
            ib, INDEX_URL, self.manifest, coverage_start_year=self.cfg.start_year
        )
        parsed_calendar = parse_native_page(cb, CALENDAR_URL, self.manifest)
        if parsed_index.issues or parsed_calendar.issues:
            raise RunAbort("inventory_issues")
        labels = [p.year for p in parsed_index.year_pages] + list(parsed_calendar.inventory_years)
        if not labels:
            raise RunAbort("inventory_issues")
        self.coverage_years = (self.cfg.start_year, max(labels))
        discovery = discover_inventory_pages(
            ib,
            cb,
            self.manifest,
            start_year=self.coverage_years[0],
            end_year=self.coverage_years[1],
        )
        if discovery.issues:
            raise RunAbort("inventory_issues")
        if len(discovery.expected_pages) > self.settings.max_inventory_pages:
            raise RunAbort("inventory_page_limit")
        selected_urls = {p.source_page_url for p in parsed_index.year_pages if p.year in self.years}
        captures = {INDEX_URL: index["capture_id"], CALENDAR_URL: calendar["capture_id"]}
        for url in discovery.expected_pages:
            if url in captures:
                continue
            receipt, _, _ = self.fetch(url, "inventory", reuse=url not in selected_urls)
            captures[url] = receipt["capture_id"]
        self.inventory_ids = list(captures.values())
        self.inventory, _ = self.bronze.prepare_inventory_captures(
            self.inventory_ids,
            self.manifest,
            root=self.cfg.root,
            historical_years=self.coverage_years,
            start_year=self.cfg.start_year,
        )
        selected = select_documents(self.inventory, self.years, self.settings)
        self.selected_documents = len(selected)
        meetings = {
            m.meeting_id
            for m in self.inventory.meetings
            if any(d.year in self.years for d in m.meeting_dates)
        }
        self.source_outcomes = dict(
            Counter(
                e.minutes_status
                for e in self.inventory.entries
                if e.meeting_id in meetings and not e.document_id
            )
        )
        return selected

    def visit(self, url, kind, stack=()):
        self.check()
        if url in stack:
            raise RunAbort("cyclic_dependency")
        target = self.assets if kind == "asset" else self.documents
        if url in target:
            return target[url]
        maximum = self.settings.max_assets if kind == "asset" else self.settings.max_documents
        if len(target) >= maximum:
            raise RunAbort("asset_limit" if kind == "asset" else "document_limit")
        receipt, payload, reused = self.fetch(url, kind, reuse=self.mode == "backfill")
        if not self._valid(receipt, payload, kind):
            raise RunAbort("invalid_document" if kind == "document" else "invalid_asset")
        target[url] = receipt["capture_id"]
        if kind == "document":
            self.outcomes["reused" if reused else "acquired"] += 1
            if not url.lower().endswith(".pdf"):
                dependencies = discover_html_dependencies(payload, url, self.manifest)
                if dependencies.issues:
                    raise RunAbort("dependency_issues")
                children = [
                    self.visit(d.source_url, d.kind, (*stack, url))
                    for d in dependencies.dependencies
                ]
                self.dependency_sets[receipt["capture_id"]] = children
        return receipt["capture_id"]

    def publish(self, before, finalize):
        self.check()
        prior = (
            self.text_catalog.read(self.manifest, root=self.cfg.root) if before["text"] else None
        )
        direct = {d.source_url: d.document_id for d in self.inventory.documents}
        candidates = set(self.documents.values())
        if prior:
            candidates |= {b["capture_id"] for b in prior["bindings"]}
        selected_urls = set(self.documents)
        candidates |= {
            r["capture_id"]
            for r in self.history
            if r["accepted"] and r["kind"] == "document" and r["source_url"] in selected_urls
        }
        with writer_lock(self.cfg.root), self.store.connection.transaction():
            for table in ["catalog_state", "text_state", "chunk_state"]:
                self.store.connection.execute(
                    self.store._query(
                        "SELECT singleton FROM {schema}." + table + " WHERE singleton FOR UPDATE"
                    )
                )
            if self.audit.pointers() != before:
                raise RunAbort("catalog_changed")
            native = self.bronze.publish_inventory_captures(
                self.inventory_ids,
                self.manifest,
                root=self.cfg.root,
                historical_years=self.coverage_years,
                start_year=self.cfg.start_year,
            )
            for identity in sorted(candidates):
                receipt, _ = self.bronze.read_capture(identity)
                if receipt["source_url"] in direct:
                    self.bronze.link_document(
                        identity, native["run_id"], direct[receipt["source_url"]]
                    )
            for parent, children in self.dependency_sets.items():
                self.text.dependencies.commit(parent, children, self.manifest, root=self.cfg.root)
            bindings = []
            for identity in sorted(candidates):
                receipt, payload = self.text.dependencies._verified(
                    identity, self.manifest, self.cfg.root
                )
                associations = self.text.dependencies.meeting_associations(identity)
                if not any(a["run_id"] == native["run_id"] for a in associations):
                    self.excluded.append(
                        {"capture_id": identity, "outcome": "no_current_inventory_reference"}
                    )
                    continue
                result = self.native.get(identity) or extract_text(
                    payload, receipt["source_url"], self.manifest
                )
                if result["status"] == "failed":
                    if identity in self.documents.values():
                        raise RunAbort("invalid_document")
                    self.excluded.append(
                        {"capture_id": identity, "outcome": "invalid_prior_capture"}
                    )
                    continue
                try:
                    self.text.verify_dependencies(
                        receipt, payload, self.manifest, root=self.cfg.root
                    )
                except ValueError:
                    if identity in self.documents.values():
                        raise RunAbort("unverified_dependencies") from None
                    self.excluded.append(
                        {"capture_id": identity, "outcome": "unverified_prior_dependencies"}
                    )
                    continue
                self.text.save_result(identity, result, self.manifest, root=self.cfg.root)
                bindings.append({"capture_id": identity, "extraction_id": result["extraction_id"]})
            self.check()
            if not bindings:
                raise RunAbort("no_document_inputs")
            published = self.text_catalog.publish(
                bindings,
                self.manifest,
                root=self.cfg.root,
                inventory_run_id=native["run_id"],
                start_year=self.cfg.start_year,
                _locked=True,
            )
            self.chunk_catalog.publish(
                self.manifest,
                root=self.cfg.root,
                text_catalog_id=published["catalog_id"],
                _locked=True,
            )
            self.check()
            after = self.audit.pointers()
            finalize(after)
            return after
