from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileRecord:
    path: Path
    size: int
    mtime: float
    ctime: float
    birthtime: float | None
    ext: str
    meaningful_xattr_count: int
    asset_key: str | None
    asset_component: bool


@dataclass(frozen=True)
class DuplicateRow:
    digest: str
    size: int
    path: Path


@dataclass(frozen=True)
class PlanRow:
    digest: str
    size: int
    keep_path: Path
    move_path: Path
    reason: str
    keep_score: float
    move_score: float


@dataclass(frozen=True)
class ManualReviewRow:
    digest: str
    size: int
    path: Path
    reason: str


@dataclass(frozen=True)
class PlanPolicy:
    protect_asset_components: bool = True
    prefer_earliest_origin_time: bool = True
    prefer_richer_metadata: bool = True
    prefer_roots: tuple[str, ...] = ()



@dataclass(frozen=True)
class TreePlanRow:
    unit_id: str
    source_path: Path
    destination_path: Path
    mode: str
    bucket: str
    asset_key: str | None
    is_asset_component: bool
