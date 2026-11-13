"""SSV-AC12: bounded transport, incomplete evidence and source-owned retry delays."""

from datetime import timedelta
from http.client import IncompleteRead
from io import BytesIO
from urllib.error import URLError
from urllib.request import Request

import pytest
from test_ssv_bronze import NOW

from market_data_pipeline.pipelines.finra_short_sale_volume.data.acquisition import Limits
from market_data_pipeline.pipelines.finra_short_sale_volume.io.acquisition_http import (
    Budget,
    Credentials,
    Session,
    Stop,
    retry_delay,
)

URL = "https://api.finra.org/data/group/otcMarket/name/regShoDaily"


class Response(BytesIO):
    def __init__(self, body=b"[]", status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
        }


def session(reply, **limits):
    budget = Budget(Limits(**limits), monotonic=lambda: 0)

    def open_request(request, **kwargs):
        if isinstance(reply, Exception):
            raise reply
        return reply

    return Session(open_request, budget, timeout=1), budget


def test_ssv_ac12_transport_counts_bytes_and_disallows_unregistered_urls():
    client, budget = session(Response(b"[]"))
    reply = client.exchange(Request(URL, method="POST"), limit=100)
    assert reply.complete and reply.body == b"[]" and budget.requests == 1 and budget.bytes == 2
    with pytest.raises(ValueError):
        client.exchange(Request("https://unreviewed.invalid", method="POST"), limit=100)


@pytest.mark.parametrize("kind", ["oversize", "length", "read"])
def test_ssv_ac12_transport_retains_bounded_incomplete_prefix(kind):
    response = Response(b"123456")
    if kind == "length":
        response.headers["Content-Length"] = "99"
    if kind == "read":

        def fail(*args):
            raise IncompleteRead(b"123", 6)

        response.read = fail
    client, budget = session(response)
    reply = client.exchange(Request(URL, method="POST"), limit=4 if kind == "oversize" else 100)
    assert not reply.complete and reply.error_code
    assert reply.body == (
        b"1234" if kind == "oversize" else b"123456" if kind == "length" else b"123"
    )
    assert budget.bytes == len(reply.body) + (1 if kind == "oversize" else 0)
    assert response.closed


def test_ssv_ac12_timeout_has_no_fabricated_response_or_private_diagnostic():
    client, budget = session(URLError("invented-private-secret"))
    reply = client.exchange(Request(URL, method="POST"), limit=100)
    assert reply.status is None and reply.body == b"" and not reply.complete
    assert "secret" not in repr(reply) and budget.requests == 1


@pytest.mark.parametrize(
    "changes", [dict(max_requests=1), dict(max_total_bytes=10), dict(max_duration_seconds=1)]
)
def test_ssv_ac12_budget_stops_before_an_excess_request(changes):
    limits = Limits(**changes)
    budget = Budget(limits, monotonic=lambda: 0)
    if "max_requests" in changes:
        budget.requests = 1
    client = Session(
        lambda *a, **k: pytest.fail("budget exceeded before network"), budget, timeout=2
    )
    with pytest.raises(Stop):
        client.exchange(Request(URL, method="POST"), limit=100)


def test_ssv_ac12_retry_after_seconds_and_date_are_respected_without_early_retry():
    assert retry_delay("7", 1, NOW) == 7
    future = (NOW + timedelta(seconds=8)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert retry_delay(future, 1, NOW) == 8
    budget = Budget(Limits(max_wait_seconds=5), monotonic=lambda: 0)
    with pytest.raises(Stop):
        budget.wait(7, lambda delay: pytest.fail("must not retry early"))


def test_ssv_ac12_credentials_and_auth_response_never_appear_in_repr():
    credentials = Credentials("invented-id", "invented-private-secret")
    assert "secret" not in repr(credentials) and "invented-id" not in repr(credentials)
    client, _ = session(Response(b'{"access_token":"invented-private-secret"}'))
    reply = client.exchange(Request(URL, method="POST"), limit=100)
    assert "secret" not in repr(reply)
