"""FOMC-AC01 pinned normalized inventory profile and reviewed URL boundary.

Official source/type/access references: specs/spec017/README.md.
This contract does not implement HTML discovery or authorize network operations.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import yaml

from market_data_pipeline.shared.yaml import UniqueLoader

DOCUMENT_KINDS = (
    "minutes",
    "historical_minutes",
    "minutes_of_actions",
    "record_of_policy_actions",
    "memorandum_of_discussion",
)
COMMITTEE_BODIES = ("fomc", "executive_committee")
_PROFILE = {
    "source": "federal_reserve_board",
    "dataset": "fomc-meeting-minutes",
    "schema_id": "fomc-inventory/1",
    "official_host": "www.federalreserve.gov",
    "first_meeting_year": 1936,
}


@dataclass(frozen=True)
class Manifest:
    schema_id: str
    official_host: str
    first_meeting_year: int
    document_kinds: tuple[str, ...]
    committee_bodies: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            **_PROFILE,
            "schema_id": self.schema_id,
            "official_host": self.official_host,
            "first_meeting_year": self.first_meeting_year,
            "document_kinds": list(self.document_kinds),
            "committee_bodies": list(self.committee_bodies),
        }

    def _resolve_url(self, value, base_url):
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or any(c.isspace() or not c.isprintable() for c in value)
            or "\\" in value
            or "%" in value
        ):
            raise ValueError("invalid FOMC URL characters or length")
        try:
            resolved = urlsplit(urljoin(base_url, value) if base_url else value)
        except ValueError:
            raise ValueError("invalid FOMC URL") from None
        if resolved.scheme != "https" or resolved.netloc != self.official_host or resolved.query:
            raise ValueError(
                "FOMC URLs require the reviewed HTTPS host without credentials or query"
            )
        if any(part in (".", "..") for part in resolved.path.split("/")):
            raise ValueError("ambiguous FOMC document path")
        return resolved

    def capture_url(self, value, kind):
        if kind not in ("inventory", "document", "asset"):
            raise ValueError("unknown FOMC source kind")
        url = (
            self.canonical_asset_url(value)
            if kind == "asset"
            else self.canonical_url(value, source_page=kind == "inventory")
        )
        if url != value or (
            kind == "document" and not url.lower().endswith((".pdf", ".htm", ".html"))
        ):
            raise ValueError("FOMC capture requires a canonical reviewed URL")
        return url

    def canonical_asset_url(self, value, *, base_url=None):
        """Reviewed PNG/GIF/JPEG assets; paths come from archived article references."""
        resolved = self._resolve_url(value, base_url)
        if resolved.fragment or not (
            re.fullmatch(r"/sep[0-9]{8}figure[0-9]+[a-z]?\.png", resolved.path)
            or re.fullmatch(r"/monetarypolicy/files/[^/]+\.png", resolved.path)
            or re.fullmatch(r"/monetarypolicy/files/[A-Za-z0-9_.-]+\.(?:gif|jpg)", resolved.path)
            or re.fullmatch(
                r"/monetarypolicy/gifjpg/[A-Za-z0-9_.-]+\.(?:png|gif|jpg)", resolved.path
            )
        ):
            raise ValueError("unreviewed FOMC content-asset path or format")
        return urlunsplit(("https", self.official_host, resolved.path, "", ""))

    def canonical_url(
        self, value: object, *, base_url: str | None = None, source_page: bool = False
    ) -> str:
        """Resolve reviewed HTTPS references; callers retain raw href as evidence."""
        # Archived 2003 year pages use HTTP legacy references. Retain the raw href,
        # but resolve only that reviewed folder to HTTPS; acquisition stays HTTPS-only.
        if (
            not source_page
            and base_url
            and isinstance(value, str)
            and value.startswith("http://www.federalreserve.gov/fomc/minutes/")
        ):
            value = "https://" + value[len("http://") :]
        resolved = self._resolve_url(value, base_url)
        if source_page:
            path = resolved.path
            match = re.fullmatch(r"/monetarypolicy/fomchistorical([0-9]{4})\.htm", path)
            allowed = path in (
                "/monetarypolicy/fomccalendars.htm",
                "/monetarypolicy/fomc_historical_year.htm",
            )
            if resolved.fragment or not (
                allowed or (match and int(match[1]) >= self.first_meeting_year)
            ):
                raise ValueError("unreviewed FOMC inventory page")
        elif not resolved.path.startswith(("/monetarypolicy/", "/fomc/minutes/", "/fomc/MINUTES/")):
            raise ValueError("unreviewed FOMC document path")
        return urlunsplit(("https", self.official_host, resolved.path, "", ""))


def manifest_from_dict(value: object) -> Manifest:
    """Accept only this reviewed profile; this is not an acquisition/use grant."""
    expected = {
        **_PROFILE,
        "document_kinds": list(DOCUMENT_KINDS),
        "committee_bodies": list(COMMITTEE_BODIES),
    }
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ValueError("FOMC manifest requires exactly the reviewed fields")
    if any(type(value[k]) is not type(v) or value[k] != v for k, v in expected.items()):
        raise ValueError("FOMC manifest differs from the reviewed normalized inventory profile")
    return Manifest(
        value["schema_id"],
        value["official_host"],
        value["first_meeting_year"],
        DOCUMENT_KINDS,
        COMMITTEE_BODIES,
    )


def load_manifest(path: Path) -> Manifest:
    try:
        return manifest_from_dict(yaml.load(path.read_text(), Loader=UniqueLoader))
    except (yaml.YAMLError, UnicodeError):
        raise ValueError("invalid FOMC manifest YAML") from None
