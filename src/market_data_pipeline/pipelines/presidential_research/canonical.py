"""Canonical JSON for the PRES JavaScript boundary; distinct from donor hashes."""

import hashlib
import json
import math
import re

_LONE_SURROGATE = re.compile(r"[\ud800-\udfff]")


def _string(value: str) -> str:
    encoded = json.dumps(value, ensure_ascii=False)
    return _LONE_SURROGATE.sub(lambda match: f"\\u{ord(match.group()):04x}", encoded)


def _number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("Non-finite JSON number.")
    if value == 0:
        return "0"
    sign = "-" if value < 0 else ""
    mantissa, _, exponent = repr(abs(value)).partition("e")
    whole, _, fraction = mantissa.partition(".")
    raw = whole + fraction
    significant = raw.lstrip("0")
    n = len(whole) + int(exponent or 0) - (len(raw) - len(significant))
    digits = significant.rstrip("0")
    k = len(digits)
    if k <= n <= 21:
        return sign + digits + "0" * (n - k)
    if 0 < n <= 21:
        return sign + digits[:n] + "." + digits[n:]
    if -6 < n <= 0:
        return sign + "0." + "0" * -n + digits
    e = n - 1
    head = digits[0] + ("." + digits[1:] if k > 1 else "")
    return f"{sign}{head}e{'+' if e >= 0 else '-'}{abs(e)}"


def canonical_json(value: object) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        if abs(value) > 9_007_199_254_740_991:
            raise ValueError("Unsafe integer for JavaScript JSON.")
        return str(value)
    if isinstance(value, float):
        return _number(value)
    if isinstance(value, str):
        return _string(value)
    if isinstance(value, list):
        return "[" + ",".join(canonical_json(item) for item in value) + "]"
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings.")
        keys = sorted(value, key=lambda key: key.encode("utf-16-be", errors="surrogatepass"))
        return "{" + ",".join(f"{_string(key)}:{canonical_json(value[key])}" for key in keys) + "}"
    raise ValueError("Unsupported JSON value.")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
