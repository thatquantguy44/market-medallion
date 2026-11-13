from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


class ConfigError(ValueError):
    """Raised when a source catalog cannot describe a reproducible run."""


MANUAL_CONTROL_EVIDENCE_ROLE = "manual_reviewed_political_control_evidence"


@dataclass(frozen=True)
class EvidenceCapture:
    """A content-pinned copy of one component in a manual evidence bundle."""

    locator: str
    captured_at: str
    sha256: str


@dataclass(frozen=True)
class SourceSpec:
    id: str
    title: str
    url: str
    parser: str
    authority: str
    role: str
    enabled: bool
    return_basis: str | None = None
    unit: str | None = None
    locators: tuple[str, ...] = ()
    captured_locator: str | None = None
    captured_at: str | None = None
    captured_sha256: str | None = None
    captures: tuple[EvidenceCapture, ...] = ()


@dataclass(frozen=True)
class PipelineConfig:
    path: Path
    project_id: str
    currency: str
    annual_start_year: int
    annual_end_year: int
    sources: tuple[SourceSpec, ...]

    def source(self, source_id: str) -> SourceSpec:
        for source in self.sources:
            if source.id == source_id:
                return source
        raise ConfigError(f"Unknown source ID: {source_id}")

    @property
    def manual_control_source_ids(self) -> set[str]:
        """IDs that are eligible to support a reviewed political-control row."""
        return {source.id for source in self.sources if source.role == MANUAL_CONTROL_EVIDENCE_ROLE}


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty string")
    return value


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


def _optional_text_list(value: object, name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ConfigError(f"{name} must be a list of non-empty strings")
    return tuple(_required_text(item, f"{name}[{index}]") for index, item in enumerate(value))


def _captures(value: object, name: str) -> tuple[EvidenceCapture, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ConfigError(f"{name} must be an array of tables")
    captures: list[EvidenceCapture] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ConfigError(f"{name}[{index}] must be a table")
        captures.append(
            EvidenceCapture(
                locator=_required_text(item.get("locator"), f"{name}[{index}].locator"),
                captured_at=_required_text(item.get("captured_at"), f"{name}[{index}].captured_at"),
                sha256=_required_text(item.get("sha256"), f"{name}[{index}].sha256"),
            )
        )
    return tuple(captures)


def load_config(path: str | Path) -> PipelineConfig:
    config_path = Path(path)
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)
    pipeline = data.get("pipeline")
    if not isinstance(pipeline, dict) or pipeline.get("schema_version") != 1:
        raise ConfigError("pipeline.schema_version must be 1")
    start = pipeline.get("annual_start_year")
    end = pipeline.get("annual_end_year")
    if not isinstance(start, int) or not isinstance(end, int) or start > end:
        raise ConfigError("annual_start_year and annual_end_year must be ordered integers")
    raw_sources = data.get("source")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ConfigError("source catalog must contain at least one [[source]] block")
    sources: list[SourceSpec] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_sources):
        if not isinstance(raw, dict):
            raise ConfigError(f"source[{index}] must be a table")
        source_id = _required_text(raw.get("id"), f"source[{index}].id")
        if source_id in seen:
            raise ConfigError(f"duplicate source ID: {source_id}")
        seen.add(source_id)
        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ConfigError(f"source[{index}].enabled must be true or false")
        captures = _captures(raw.get("capture"), f"source[{index}].capture")
        source = SourceSpec(
            id=source_id,
            title=_required_text(raw.get("title"), f"source[{index}].title"),
            url=_required_text(raw.get("url"), f"source[{index}].url"),
            parser=_required_text(raw.get("parser"), f"source[{index}].parser"),
            authority=_required_text(raw.get("authority"), f"source[{index}].authority"),
            role=_required_text(raw.get("role"), f"source[{index}].role"),
            enabled=enabled,
            return_basis=raw.get("return_basis")
            if isinstance(raw.get("return_basis"), str)
            else None,
            unit=raw.get("unit") if isinstance(raw.get("unit"), str) else None,
            locators=_optional_text_list(raw.get("locators"), f"source[{index}].locators"),
            captured_locator=_optional_text(
                raw.get("captured_locator"), f"source[{index}].captured_locator"
            ),
            captured_at=_optional_text(raw.get("captured_at"), f"source[{index}].captured_at"),
            captured_sha256=_optional_text(
                raw.get("captured_sha256"), f"source[{index}].captured_sha256"
            ),
            captures=captures,
        )
        if source.role == MANUAL_CONTROL_EVIDENCE_ROLE:
            pinned_locators = {capture.locator for capture in source.captures}
            unpinned = sorted(set(source.locators) - pinned_locators)
            if unpinned:
                raise ConfigError(
                    f"manual control-evidence source {source.id} has locator(s) "
                    "without a content-pinned capture: " + ", ".join(unpinned)
                )
        sources.append(source)
    return PipelineConfig(
        path=config_path.resolve(),
        project_id=_required_text(pipeline.get("project_id"), "pipeline.project_id"),
        currency=_required_text(pipeline.get("currency"), "pipeline.currency"),
        annual_start_year=start,
        annual_end_year=end,
        sources=tuple(sources),
    )
