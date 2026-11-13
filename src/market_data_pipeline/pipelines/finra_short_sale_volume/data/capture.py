"""SSV-AC05: pure public-receipt minimization and credential exclusion."""

import hashlib
import json
import re
from collections.abc import Mapping

from market_data_pipeline.pipelines.finra_short_sale_volume.data.api import _request

PARSER = "finra-ssv-api-page/1"
HEADERS = frozenset(
    {
        "content-type",
        "content-encoding",
        "content-length",
        "data-version",
        "record-total",
        "record-offset",
        "record-limit",
        "record-max-limit",
        "total-records-on-page",
    }
)
_MEDIA = re.compile(
    r'(?:application/json|text/html|text/plain|application/octet-stream)(?:\s*;\s*charset=(?:utf-8|"utf-8"|iso-8859-1|"iso-8859-1"))?',
    re.I,
)
_KEYS = re.compile(rb'("(?:[^"\\]|\\.)*")\s*:')
_AUTH = re.compile(rb"\b(?:Basic|Bearer)[ \t]+[A-Za-z0-9_./+=-]{8,}", re.I)
_CREDENTIALS = frozenset(
    {
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "token",
        "tokentype",
        "clientsecret",
        "apiclientsecret",
        "finraapiclientsecret",
        "clientid",
        "apiclientid",
        "apikey",
        "apisecret",
        "password",
        "passwordsecret",
        "authorization",
        "proxyauthorization",
        "cookie",
        "setcookie",
        "secret",
        "finraapikey",
        "finrapasswordsecret",
    }
)


def encoded(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def settings(max_bytes, max_rows):
    if any(type(v) is not int or v <= 0 for v in (max_bytes, max_rows)):
        raise ValueError("FINRA capture budgets require positive integers")
    return {"algorithm": PARSER, "max_bytes": max_bytes, "max_rows": max_rows}


def reject_credentials(payload):
    """Screen recognizable credentials, including escaped JSON key names.

    This lexical security check performs no dataset parsing/normalization. It is
    not a general secret detector; callers must supply only market-data responses.
    """
    if not isinstance(payload, bytes):
        raise ValueError("FINRA capture requires bytes")
    denied = bool(_AUTH.search(payload))
    for match in _KEYS.finditer(payload):
        if match.end(1) - match.start(1) > 1024:
            continue
        try:
            key = json.loads(match[1].decode("utf-8"))
        except (ValueError, UnicodeError):
            continue
        if re.sub("[^a-z0-9]", "", key.casefold()) in _CREDENTIALS:
            denied = True
            break
    if denied:
        raise ValueError("credential-bearing material cannot enter FINRA Bronze")


def _header_value(name, value):
    if not isinstance(value, str) or not value or not value.isprintable():
        return False
    if name == "content-type":
        return _MEDIA.fullmatch(value) is not None
    if name == "content-encoding":
        return value.lower() in {"identity", "gzip", "deflate", "br"}
    if name == "data-version":
        return re.fullmatch(r" *[0-9]{1,4096} *", value) is not None
    return re.fullmatch("[0-9]{1,4096}", value) is not None


def public_receipt(receipt, payload, manifest, *, max_rows):
    """Retain public scope only; invalid response claims stay explicit rejections."""
    try:
        _request(receipt, manifest, max_rows)
        query = json.loads(encoded(receipt["request_payload"]))
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ValueError("FINRA archive requires the admitted API request scope") from None
    issues = []
    headers = {}
    seen = set()
    raw = receipt.get("headers")
    if not isinstance(raw, Mapping):
        issues.append("invalid_response_headers")
    else:
        for key, value in raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                issues.append("invalid_response_headers")
                continue
            name = key.lower()
            if name in seen:
                issues.append("duplicate_response_headers")
            seen.add(name)
            if name not in HEADERS:
                continue
            if not _header_value(name, value):
                issues.append("invalid_response_headers")
            elif name not in headers:
                headers[name] = value
    byte_match = type(receipt.get("bytes")) is int and receipt["bytes"] == len(payload)
    sha = hashlib.sha256(payload).hexdigest()
    hash_match = receipt.get("sha256") == sha
    if not byte_match or not hash_match:
        issues.append("payload_receipt_mismatch")
    status = receipt.get("http_status")
    if type(status) is not int or not 100 <= status <= 599:
        status = None
        issues.append("invalid_http_status")
    if "body_complete" in receipt and receipt["body_complete"] is not True:
        issues.append("incomplete_body")
    return {
        "url": receipt["url"],
        "method": receipt["method"],
        "request_data_version": receipt["request_data_version"],
        "request_payload": query,
        "http_status": status,
        "headers": headers,
        "bytes": len(payload) if byte_match else None,
        "sha256": sha if hash_match else None,
    }, sorted(set(issues))
