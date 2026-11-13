"""SSV-AC12: finite admitted synchronous acquisition with source-owned retry evidence."""

import hashlib
import json
import os
import re
import time
from datetime import UTC, datetime
from urllib.request import Request
from zoneinfo import ZoneInfo

from market_data_pipeline.pipelines.finra_short_sale_volume import pipeline
from market_data_pipeline.pipelines.finra_short_sale_volume.acquisition_config import (
    AcquisitionConfig,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.data.acquisition import plan
from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import _unique_object
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import (
    encoded,
    reject_credentials,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze, warehouse
from market_data_pipeline.pipelines.finra_short_sale_volume.io.acquisition_http import (
    DATA_URL,
    METADATA_URL,
    TOKEN_URL,
    Budget,
    Credentials,
    Session,
    Stop,
    retry_delay,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io.acquisition_journal import open_run
from market_data_pipeline.pipelines.finra_short_sale_volume.manifest import validate_metadata
from market_data_pipeline.shared.http import prepare_http

NETWORK_STARTUP_URL = TOKEN_URL
_TRANSIENT = {429, 500, 502, 503, 504}


def _preflight_root(config):
    root = config.root
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise ValueError("FINRA acquisition root is unsafe")
    bound = (root / "warehouse").exists() or (root / "warehouse.json").exists()
    if not bound:
        if (root / "bronze").exists() or (root / "bronze").is_symlink():
            raise ValueError("resolve original FINRA archived input before acquisition")
        if (
            root.exists()
            and next(root.iterdir(), None) is not None
            and not (root / "acquisition.json").is_file()
        ):
            raise ValueError("FINRA acquisition requires an empty or source-owned root")
        return
    operation = "fixture" if config.operation == "fixture" else "transform"
    with warehouse.connect(
        root, config.manifest, approved_use=config.approved_use, operation=operation
    ) as db:
        if db.execute(
            "SELECT 1 FROM attempts WHERE status='STARTED' OR capture_id NOT IN "
            "(SELECT capture_id FROM captures) LIMIT 1"
        ).fetchone():
            raise ValueError("resume unresolved FINRA source captures before acquisition")
        rows = db.execute("SELECT capture_id FROM captures LIMIT 100001").fetchall()
        if len(rows) > 100000:
            raise ValueError("FINRA capture inventory budget exceeded")
        selected = set()
        for index, path in enumerate((root / "bronze/captures").iterdir()):
            if index >= 100000 or path.is_symlink() or not path.is_dir():
                raise ValueError("FINRA capture inventory is unsafe or exceeds budget")
            selected.add(bronze._identity(path.name))
        if selected != {r[0] for r in rows}:
            raise ValueError("resolve original FINRA archived input before acquisition")


class _Runner:
    def __init__(self, config, session, run, clock, sleep):
        self.config, self.session, self.run = config, session, run
        self.clock, self.sleep = clock, sleep
        self.token = None
        self.expires = 0
        self.metadata_sha256 = None

    def capture(self, reply, partition):
        if reply.status is None:
            return dict(status="TRANSPORT_FAILED", capture_id=None, error_code=reply.error_code)
        try:
            reject_credentials(reply.body)
        except ValueError:
            return dict(
                status="UNSAFE_PAYLOAD", capture_id=None, error_code="credential_body_refused"
            )
        query = self.query(partition)
        receipt = dict(
            url=DATA_URL,
            method="POST",
            request_data_version=self.config.manifest.data_version,
            request_payload=query,
            http_status=reply.status,
            headers=reply.headers,
            bytes=len(reply.body),
            sha256=hashlib.sha256(reply.body).hexdigest(),
            body_complete=reply.complete,
        )
        retrieved_at = self.clock() if reply.complete else None
        saved = bronze.archive_input(
            self.config.root,
            reply.body,
            self.config.manifest,
            receipt,
            approved_use=self.config.approved_use,
            operation=self.config.operation,
            run_id=self.run.run_id,
            archived_at=self.clock(),
            retrieved_at=retrieved_at,
            max_bytes=self.config.limits.max_response_bytes,
            max_rows=self.config.limits.page_rows,
        )
        operation = "fixture" if self.config.operation == "fixture" else "transform"
        try:
            result = pipeline.ingest_capture(
                self.config.root,
                saved["capture_id"],
                self.config.manifest,
                approved_use=self.config.approved_use,
                operation=operation,
                max_bytes=self.config.limits.max_response_bytes,
                max_rows=self.config.limits.page_rows,
                clock=self.clock,
            )
        except (ValueError, RuntimeError, OSError):
            return dict(
                status="EXECUTION_FAILED",
                capture_id=saved["capture_id"],
                error_code="ingestion_failed",
            )
        return dict(
            status=result["status"],
            capture_id=saved["capture_id"],
            error_code=reply.error_code or result["error_code"],
            accepted_rows=result["accepted_rows"],
            new_versions=result["new_versions"],
            receipt_sha256=saved["receipt_sha256"],
        )

    def query(self, partition):
        return dict(
            limit=self.config.limits.page_rows,
            offset=0,
            fields=list(self.config.manifest.to_dict()["fields"]),
            compareFilters=[
                dict(
                    compareType="EQUAL",
                    fieldName="tradeReportDate",
                    fieldValue=partition["trade_date"],
                ),
                dict(
                    compareType="EQUAL", fieldName="marketCode", fieldValue=partition["market_code"]
                ),
            ],
        )

    def request(self, request, phase, partition=None):
        limit = (
            self.config.limits.max_response_bytes
            if phase == "partition"
            else min(65536, self.config.limits.max_response_bytes)
        )
        for attempt in range(1, self.config.limits.max_attempts + 1):
            reply = self.session.exchange(request, limit=limit)
            details = self.capture(reply, partition) if partition is not None else {}
            if partition is not None:
                partition.update(details)
            event = dict(
                phase=phase,
                attempt=attempt,
                http_status=reply.status,
                body_bytes=len(reply.body),
                body_complete=reply.complete,
                error_code=reply.error_code
                or ("http_error" if reply.status not in (200, 204) else None),
                **{k: v for k, v in details.items() if k != "error_code"},
            )
            if partition is not None:
                event.update(
                    trade_date=partition["trade_date"], facility_id=partition["facility_id"]
                )
                event["error_code"] = details.get("error_code") or event["error_code"]
            self.run.event(event)
            if reply.error_code == "duration_budget":
                raise Stop("duration_budget")
            if details.get("status") == "EXECUTION_FAILED":
                raise Stop("ingestion_failed")
            transient = reply.status in _TRANSIENT or reply.error_code in {
                "transport_failed",
                "read_failed",
                "truncated_body",
            }
            if not transient or attempt == self.config.limits.max_attempts:
                return reply
            self.session.budget.wait(
                retry_delay(reply.retry_after, attempt, self.clock()), self.sleep
            )
        raise Stop("retry_budget")

    def authenticate(self, credentials):
        request = Request(
            TOKEN_URL,
            data=b"",
            headers={
                "Authorization": credentials.basic(),
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            },
            method="POST",
        )
        reply = self.request(request, "auth")
        try:
            if reply.status != 200 or not reply.complete:
                raise ValueError
            value = json.loads(reply.body, object_pairs_hook=_unique_object)
            token = value["access_token"]
            expires = value["expires_in"]
            if (
                isinstance(expires, str)
                and expires.isascii()
                and expires.isdigit()
                and len(expires) <= 6
            ):
                expires = int(expires)
            if (
                value.get("token_type", "").lower() != "bearer"
                or not isinstance(token, str)
                or not 1 <= len(token) <= 60000
                or not token.isascii()
                or any(ord(c) < 33 or ord(c) > 126 for c in token)
                or type(expires) is not int
                or not 31 <= expires <= 86400
            ):
                raise ValueError
            self.token = token
            self.expires = self.session.budget.monotonic() + expires - 30
        except (ValueError, KeyError, TypeError, AttributeError, UnicodeError):
            raise Stop("authentication_failed") from None

    def metadata(self):
        reply = self.request(
            Request(METADATA_URL, headers=self.headers(), method="GET"), "metadata"
        )
        try:
            if reply.status != 200 or not reply.complete:
                raise ValueError
            reject_credentials(reply.body)
            validate_metadata(json.loads(reply.body, object_pairs_hook=_unique_object))
            self.run.metadata(reply.body)
            self.metadata_sha256 = hashlib.sha256(reply.body).hexdigest()
        except (ValueError, KeyError, TypeError, UnicodeError):
            raise Stop("metadata_rejected") from None

    def headers(self):
        return {
            "Authorization": "Bearer " + self.token,
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Data-Version": str(self.config.manifest.data_version),
        }


def make_plan(config, *, start, end, scopes, overlap_days=2, clock=lambda: datetime.now(UTC)):
    """Validate and preview acquisition without reading environment or creating state."""
    if not isinstance(config, AcquisitionConfig):
        raise ValueError("FINRA acquisition requires source-owned configuration")
    config.validate()
    return plan(
        config.manifest,
        start,
        end,
        scopes,
        as_of=clock().astimezone(ZoneInfo("America/New_York")).date(),
        overlap_days=overlap_days,
        limits=config.limits,
    )


def acquire(
    config,
    *,
    start,
    end,
    scopes,
    overlap_days=2,
    live=False,
    transport=None,
    environ=None,
    clock=lambda: datetime.now(UTC),
    monotonic=time.monotonic,
    sleep=time.sleep,
):
    if not isinstance(config, AcquisitionConfig):
        raise ValueError("FINRA acquisition requires source-owned configuration")
    config.validate()
    if live is not True or not config.live:
        raise PermissionError("FINRA acquisition requires configuration and command live opt-ins")
    if config.operation == "fixture" and transport is None:
        raise PermissionError("FINRA fixture acquisition requires an injected transport")
    request_plan = plan(
        config.manifest,
        start,
        end,
        scopes,
        as_of=clock().astimezone(ZoneInfo("America/New_York")).date(),
        overlap_days=overlap_days,
        limits=config.limits,
    )
    environment = os.environ if environ is None else environ
    credentials = Credentials(
        environment.get(config.client_id_env), environment.get(config.client_secret_env)
    )
    _preflight_root(config)
    budget = Budget(config.limits, monotonic=monotonic)
    partitions = [
        dict(p, status="NOT_ATTEMPTED", capture_id=None) for p in request_plan["partitions"]
    ]
    route = "not_selected"
    fallback = False
    error_code = None
    runner = None
    with open_run(config, request_plan, clock=clock) as run:
        try:
            _preflight_root(config)
            budget.probes(config.network.probe_timeout_seconds)
            opener = transport
            if opener is None:
                try:
                    opener = prepare_http(
                        Request(
                            NETWORK_STARTUP_URL,
                            headers={"Authorization": credentials.basic()},
                            method="HEAD",
                        ),
                        config.network,
                        allow_redirects=False,
                    )
                except ValueError as error:
                    rejected = re.fullmatch(
                        r"network startup rejected: HTTP ([0-9]{3})", str(error)
                    )
                    code = "startup_probe_rejected" if rejected else "network_startup_failed"
                    run.event(
                        dict(
                            phase="probe",
                            error_code=code,
                            http_status=int(rejected[1]) if rejected else None,
                        )
                    )
                    raise Stop(code) from None
                route, fallback = opener.route, opener.fallback_used
            else:
                route = "injected"
            runner = _Runner(
                config,
                Session(opener, budget, timeout=config.network.timeout_seconds),
                run,
                clock,
                sleep,
            )
            runner.authenticate(credentials)
            runner.metadata()
            for partition in partitions:
                if monotonic() >= runner.expires:
                    runner.authenticate(credentials)
                reply = runner.request(
                    Request(
                        DATA_URL,
                        data=encoded(runner.query(partition)),
                        headers={**runner.headers(), "Content-Type": "application/json"},
                        method="POST",
                    ),
                    "partition",
                    partition,
                )
                if reply.status in (401, 403):
                    raise Stop("access_denied")
            budget.time()
        except Stop as error:
            error_code = error.code
            run.event(dict(phase="run", error_code=error_code))
        except (ValueError, RuntimeError, OSError, RecursionError):
            error_code = "execution_failed"
            run.event(dict(phase="run", error_code=error_code))
        complete = sum(p["status"] == "APPLIED" for p in partitions)
        result = dict(
            schema_id="finra-ssv-acquisition-result/1",
            status="COMPLETE"
            if error_code is None and complete == len(partitions)
            else "INCOMPLETE",
            error_code=error_code,
            complete_partitions=complete,
            planned_partitions=len(partitions),
            partitions=partitions,
            plan=request_plan,
            http_attempts=max(0, budget.requests - 2),
            probe_request_allowance=2,
            request_budget_used=budget.requests,
            response_bytes=budget.bytes,
            retry_wait_seconds=budget.waited,
            network_route=route,
            network_fallback_used=fallback,
            metadata_sha256=runner.metadata_sha256 if runner else None,
        )
        return run.finish(result)
