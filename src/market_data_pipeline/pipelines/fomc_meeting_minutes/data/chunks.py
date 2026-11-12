"""FOMC-AC07: pure deterministic unit/paragraph chunks and exact Unicode coverage."""

import hashlib
import platform
import re
import unicodedata

from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import digest
from market_data_pipeline.pipelines.fomc_meeting_minutes.data.text import validate as validate_text

ALGORITHM = "fomc-unit-paragraph-chunks/1"
TOKENIZER = "unicode-word-punctuation-pieces/1"


def profile(
    *,
    max_tokens=650,
    overlap_tokens=75,
    min_tokens=None,
    max_chunks=10000,
    max_codepoints=20_000_000,
):
    if min_tokens is None and type(max_tokens) is int:
        min_tokens = min(500, max_tokens)
    if (
        any(
            type(v) is not int
            for v in [max_tokens, overlap_tokens, min_tokens, max_chunks, max_codepoints]
        )
        or not 0 <= overlap_tokens < min_tokens <= max_tokens <= 10000
        or not 1 <= max_chunks <= 100000
        or not 1 <= max_codepoints <= 100_000_000
    ):
        raise ValueError("FOMC chunk profile requires bounded sizes and progressing overlap")
    return {
        "algorithm": ALGORITHM,
        "tokenizer": TOKENIZER,
        "python_version": platform.python_version(),
        "unicode_version": unicodedata.unidata_version,
        "max_word_codepoints": 32,
        "max_unit_tokens": 500000,
        "max_tokens": max_tokens,
        "min_tokens": min_tokens,
        "overlap_tokens": overlap_tokens,
        "max_chunks": max_chunks,
        "max_codepoints": max_codepoints,
        "eligibility": "native_issue_free_unit/1",
    }


def _tokens(text):
    # Unicode matching/spans: https://docs.python.org/3.12/library/re.html#re.finditer
    tokens = []
    for match in re.finditer(r"\w+|[^\w\s]", text):
        for start in range(match.start(), match.end(), 32):
            if len(tokens) >= 500000:
                raise ValueError("FOMC chunk unit token limit exceeded")
            tokens.append((start, min(start + 32, match.end())))
    return tokens


def _windows(text, settings):
    tokens = _tokens(text)
    if not tokens:
        if text:
            yield 0, len(text), 0, 0
        return
    start = 0
    while start < len(tokens):
        stop = min(start + settings["max_tokens"], len(tokens))
        if stop < len(tokens):
            candidates = []
            for i in range(start + settings["min_tokens"], stop + 1):
                gap = text[tokens[i - 1][1] : tokens[i][0]]
                if "\n" in gap:
                    candidates.append((2 if "\n\n" in gap else 1, i))
            if candidates:
                stop = max(candidates)[1]
        left = 0 if start == 0 else tokens[start][0]
        right = len(text) if stop == len(tokens) else tokens[stop][0]
        yield left, right, start, stop
        if stop == len(tokens):
            break
        start = stop - settings["overlap_tokens"]


def _reason_map(extraction):
    attributed = {c for u in extraction["units"] for c in u["issues"]}
    unknown = set(extraction["issues"]) - attributed
    result = {}
    for unit in extraction["units"]:
        reasons = set(unit["issues"]) | unknown
        if unit["assets"]:
            reasons.add("image_content")
        if not unit["text"].strip():
            reasons.add("missing_native_text")
        result[unit["ordinal"]] = sorted(reasons)
    return result


def _covered(rows):
    end = count = 0
    for row in sorted(rows, key=lambda r: r["start"]):
        count += max(0, row["end"] - max(end, row["start"]))
        end = max(end, row["end"])
    return count


def _coverage(extraction, chunks):
    reasons = _reason_map(extraction)
    grouped = {}
    for row in chunks:
        grouped.setdefault(row["unit_ordinal"], []).append(row)
    result = []
    for unit in extraction["units"]:
        rows = grouped.get(unit["ordinal"], [])
        result.append(
            {
                "unit_ordinal": unit["ordinal"],
                "page": unit["page"],
                "heading": unit["heading"],
                "start": unit["start"],
                "end": unit["end"],
                "characters": len(unit["text"]),
                "issues": reasons[unit["ordinal"]],
                "covered_characters": _covered(rows),
                "eligible_characters": _covered([r for r in rows if r["eligible"]]),
                "review_characters": _covered([r for r in rows if not r["eligible"]]),
            }
        )
    return result


def _chunk_id(version, extraction, settings, unit, start, end):
    return digest([version, extraction["extraction_id"], digest(settings), unit, start, end])


def chunk_text(extraction, *, document_version_id, **options):
    validate_text(extraction)
    if extraction["status"] == "failed":
        raise ValueError("FOMC failed extraction cannot produce successful chunks")
    if not isinstance(document_version_id, str) or not re.fullmatch(
        "[0-9a-f]{64}", document_version_id
    ):
        raise ValueError("FOMC chunks require an observed document version ID")
    settings = profile(**options)
    if len(extraction["full_text"]) > settings["max_codepoints"]:
        raise ValueError("FOMC chunk character limit exceeded")
    rows = []
    reason_map = _reason_map(extraction)
    for unit in extraction["units"]:
        reasons = reason_map[unit["ordinal"]]
        for left, right, token_start, token_end in _windows(unit["text"], settings):
            if len(rows) >= settings["max_chunks"]:
                raise ValueError("FOMC chunk count limit exceeded")
            start, end = unit["start"] + left, unit["start"] + right
            text = extraction["full_text"][start:end]
            rows.append(
                {
                    "chunk_id": _chunk_id(
                        document_version_id, extraction, settings, unit["ordinal"], start, end
                    ),
                    "ordinal": len(rows),
                    "document_version_id": document_version_id,
                    "extraction_id": extraction["extraction_id"],
                    "source_url": extraction["source_url"],
                    "unit_ordinal": unit["ordinal"],
                    "page": unit["page"],
                    "heading": unit["heading"],
                    "start": start,
                    "end": end,
                    "text": text,
                    "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                    "token_start": token_start,
                    "token_end": token_end,
                    "token_count": token_end - token_start,
                    "eligible": not reasons,
                    "review_reasons": reasons,
                }
            )
    separators = [
        [a["end"], b["start"]]
        for a, b in zip(extraction["units"], extraction["units"][1:], strict=False)
    ]
    result = {
        "schema_id": "fomc-chunks/1",
        "extraction_id": extraction["extraction_id"],
        "document_version_id": document_version_id,
        "source_url": extraction["source_url"],
        "source_sha256": extraction["source_sha256"],
        "extraction_text_sha256": extraction["text_sha256"],
        "extraction_status": extraction["status"],
        "document_issues": extraction["issues"],
        "profile": settings,
        "profile_sha256": digest(settings),
        "chunks": rows,
        "coverage": _coverage(extraction, rows),
        "separator_spans": separators,
        "separator_characters": sum(b - a for a, b in separators),
    }
    result["set_id"] = digest(result)
    return validate(result, extraction)


def validate(value, extraction):
    """Integrity and coverage verification uses stored spans, without re-chunking."""
    validate_text(extraction)
    try:
        data = dict(value)
        identity = data.pop("set_id")
        settings = value["profile"]
        rows = value["chunks"]
        units = extraction["units"]
        checks = [
            identity == digest(data),
            value["schema_id"] == "fomc-chunks/1",
            value["profile_sha256"] == digest(settings),
            settings["algorithm"] == ALGORITHM,
            settings["tokenizer"] == TOKENIZER,
            value["extraction_id"] == extraction["extraction_id"],
            value["source_url"] == extraction["source_url"],
            value["source_sha256"] == extraction["source_sha256"],
            value["extraction_text_sha256"] == extraction["text_sha256"],
            value["extraction_status"] == extraction["status"] != "failed",
            value["document_issues"] == extraction["issues"],
            len(rows) <= settings["max_chunks"],
            len(extraction["full_text"]) <= settings["max_codepoints"],
        ]
        reason_map = _reason_map(extraction)
        options = {
            k: settings[k]
            for k in ("max_tokens", "min_tokens", "overlap_tokens", "max_chunks", "max_codepoints")
        }
        expected = profile(**options)
        expected.update({k: settings[k] for k in ("python_version", "unicode_version")})
        checks.extend(
            [
                settings == expected,
                bool(re.fullmatch(r"[0-9]+(?:\.[0-9]+){1,2}", settings["python_version"])),
                bool(re.fullmatch(r"[0-9]+(?:\.[0-9]+){1,2}", settings["unicode_version"])),
            ]
        )
        previous = {}
        for i, row in enumerate(rows):
            unit = units[row["unit_ordinal"]]
            reasons = reason_map[unit["ordinal"]]
            checks.extend(
                [
                    row["ordinal"] == i,
                    row["extraction_id"] == extraction["extraction_id"],
                    row["document_version_id"] == value["document_version_id"],
                    row["source_url"] == extraction["source_url"],
                    row["page"] == unit["page"],
                    row["heading"] == unit["heading"],
                    unit["start"] <= row["start"] < row["end"] <= unit["end"],
                    row["text"] == extraction["full_text"][row["start"] : row["end"]],
                    row["text_sha256"] == hashlib.sha256(row["text"].encode()).hexdigest(),
                    row["chunk_id"]
                    == _chunk_id(
                        value["document_version_id"],
                        extraction,
                        settings,
                        unit["ordinal"],
                        row["start"],
                        row["end"],
                    ),
                    row["eligible"] == (not reasons),
                    row["review_reasons"] == reasons,
                    0
                    <= row["token_count"]
                    == row["token_end"] - row["token_start"]
                    <= settings["max_tokens"],
                ]
            )
            old = previous.get(unit["ordinal"])
            checks.append(
                row["token_start"] == 0
                if old is None
                else row["token_start"] == old["token_end"] - settings["overlap_tokens"]
                and row["start"] > old["start"]
                and row["end"] > old["end"]
            )
            previous[unit["ordinal"]] = row
        coverage = _coverage(extraction, rows)
        checks.extend(
            [
                value["coverage"] == coverage,
                all(u["covered_characters"] == u["characters"] for u in coverage),
                len({r["chunk_id"] for r in rows}) == len(rows),
            ]
        )
        separators = [[a["end"], b["start"]] for a, b in zip(units, units[1:], strict=False)]
        checks.extend(
            [
                value["separator_spans"] == separators,
                value["separator_characters"] == sum(b - a for a, b in separators),
            ]
        )
        if not all(checks):
            raise ValueError
    except (KeyError, TypeError, ValueError, IndexError):
        raise ValueError("FOMC chunk integrity or coverage mismatch") from None
    return value
