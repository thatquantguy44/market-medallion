"""SSV-AC12 bounded run-scoped transport; credentials/replies never expose bodies in repr."""

import base64
import math
import ssl
from dataclasses import dataclass, field
from datetime import UTC
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead
from urllib.error import HTTPError, URLError

TOKEN_URL = (
    "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials"
)
METADATA_URL = "https://api.finra.org/metadata/group/otcMarket/name/regShoDaily"
DATA_URL = "https://api.finra.org/data/group/otcMarket/name/regShoDaily"


class Stop(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__("FINRA acquisition stopped: " + code)


@dataclass(frozen=True)
class Credentials:
    client_id: str = field(repr=False)
    client_secret: str = field(repr=False)

    def __post_init__(self):
        if (
            any(
                not isinstance(v, str)
                or not 1 <= len(v) <= 4096
                or not v.isascii()
                or any(ord(c) < 33 or ord(c) > 126 for c in v)
                for v in (self.client_id, self.client_secret)
            )
            or ":" in self.client_id
        ):
            raise ValueError("FINRA credential variables are missing or invalid")

    def basic(self):
        return (
            "Basic "
            + base64.b64encode((self.client_id + ":" + self.client_secret).encode()).decode()
        )


class Budget:
    def __init__(self, limits, *, monotonic):
        self.limits = limits
        self.monotonic = monotonic
        self.started = monotonic()
        self.requests = self.bytes = 0
        self.waited = 0.0

    def time(self, needed=0):
        elapsed = self.monotonic() - self.started
        if elapsed < 0 or elapsed + needed > self.limits.max_duration_seconds:
            raise Stop("duration_budget")

    def request(self, limit, timeout):
        self.time(timeout)
        if self.requests >= self.limits.max_requests:
            raise Stop("request_budget")
        # Reserve a bounded overflow sentinel before opening the response.
        if self.bytes + limit + 1 > self.limits.max_total_bytes:
            raise Stop("byte_budget")
        self.requests += 1

    def probes(self, timeout):
        self.time(2 * timeout)
        if self.requests + 2 > self.limits.max_requests:
            raise Stop("request_budget")
        self.requests += 2

    def wait(self, delay, sleep):
        if type(delay) not in (int, float) or not math.isfinite(delay) or delay < 0:
            raise Stop("invalid_retry_after")
        self.time(delay)
        if self.waited + delay > self.limits.max_wait_seconds:
            raise Stop("wait_budget")
        self.waited += delay
        sleep(delay)
        self.time()


@dataclass(frozen=True)
class Reply:
    status: int | None
    body: bytes = field(repr=False)
    headers: dict = field(repr=False)
    complete: bool = False
    error_code: str | None = None
    retry_after: str | None = field(default=None, repr=False)


class Session:
    def __init__(self, opener, budget, *, timeout):
        self.opener = opener
        self.budget = budget
        self.timeout = timeout

    def exchange(self, request, *, limit):
        allowed = {TOKEN_URL: "POST", METADATA_URL: "GET", DATA_URL: "POST"}
        if request.full_url not in allowed or request.get_method() != allowed[request.full_url]:
            raise ValueError("FINRA transport requires fixed reviewed endpoints and methods")
        if type(limit) is not int or not 1 <= limit <= 3000000:
            raise ValueError("FINRA transport requires a bounded response limit")
        self.budget.request(limit, self.timeout)
        try:
            response = self.opener(request, timeout=self.timeout)
        except HTTPError as error:
            response = error
        except (URLError, OSError) as error:
            tls = isinstance(getattr(error, "reason", error), ssl.SSLError)
            return Reply(None, b"", {}, error_code="tls_failed" if tls else "transport_failed")
        try:
            status = getattr(response, "status", None) or getattr(response, "code", None)
            headers = {}
            problem = None
            for index, (key, value) in enumerate(response.headers.items()):
                if (
                    index >= 100
                    or not isinstance(key, str)
                    or not isinstance(value, str)
                    or len(key) > 256
                    or len(value) > 8192
                ):
                    problem = "invalid_headers"
                    break
                name = key.lower()
                if name in headers:
                    problem = "duplicate_headers"
                headers[name] = value
            if type(status) is not int or not 100 <= status <= 599:
                status = None
                problem = "invalid_status"
            try:
                payload = response.read(limit + 1)
            except IncompleteRead as error:
                payload = error.partial
                problem = "truncated_body"
            except (OSError, URLError):
                payload = b""
                problem = "read_failed"
            if not isinstance(payload, bytes) or len(payload) > limit + 1:
                raise Stop("invalid_transport_body")
            self.budget.bytes += len(payload)
            if len(payload) > limit:
                problem = "oversized_body"
                payload = payload[:limit]
            length = headers.get("content-length")
            if length is not None and (
                not length.isascii()
                or not length.isdigit()
                or len(length) > 12
                or int(length) != len(payload)
            ):
                problem = problem or "truncated_body"
            return Reply(
                status, payload, headers, problem is None, problem, headers.get("retry-after")
            )
        finally:
            response.close()


def retry_delay(value, attempt, now):
    if value is None:
        return 2 ** (attempt - 1)
    if not isinstance(value, str) or len(value) > 256:
        raise Stop("invalid_retry_after")
    if value.isascii() and value.isdigit() and len(value) <= 10:
        return int(value)
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            raise ValueError
        return max(0, (parsed.astimezone(UTC) - now.astimezone(UTC)).total_seconds())
    except (ValueError, TypeError, OverflowError):
        raise Stop("invalid_retry_after") from None
