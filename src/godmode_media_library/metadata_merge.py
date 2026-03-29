"""Metadata merge plan creation and ExifTool-based execution.

Given a duplicate group, creates a plan to copy missing metadata tags
from donor files into the survivor, then executes it via ExifTool.
Follows plan-then-apply safety pattern.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .metadata_richness import MetadataDiff, merge_candidates
from .utils import sha256_file, write_tsv

logger = logging.getLogger(__name__)

# Tags that should never be copied between files
_UNCOPYABLE_TAGS = {
    "SourceFile",
    "FileName",
    "Directory",
    "FileSize",
    "FileModifyDate",
    "FileAccessDate",
    "FileInodeChangeDate",
    "FilePermissions",
    "FileType",
    "FileTypeExtension",
    "MIMEType",
    "ExifToolVersion",
    "ThumbnailImage",
    "PreviewImage",
    "JpgFromRaw",
}

# Tags where values should be merged (union) rather than overwritten
_LIST_TAGS = {
    "Keywords",
    "Subject",
    "HierarchicalSubject",
    "CatalogSets",
    "TagsList",
    "LastKeywordXMP",
    "LastKeywordIPTC",
}

# Tags that are camera-specific binary data — only copy if survivor has none
_MAKERNOTE_PREFIXES = ("MakerNotes:", "Canon:", "Nikon:", "Sony:", "Fujifilm:", "Olympus:", "Panasonic:", "Pentax:")


@dataclass
class MergeAction:
    """A single metadata merge action."""

    tag: str
    value: Any
    source_path: str
    action_type: str  # "copy", "merge_list", "skip_conflict", "skip_makernotes", "skip_uncopyable"


@dataclass
class MergePlan:
    """Complete merge plan for a survivor file."""

    survivor_path: str
    survivor_hash: str
    actions: list[MergeAction] = field(default_factory=list)
    conflicts: list[MergeAction] = field(default_factory=list)
    skipped: list[MergeAction] = field(default_factory=list)


@dataclass
class MergeResult:
    """Result of executing a merge plan."""

    applied: int = 0
    skipped: int = 0
    conflicts: int = 0
    backup_path: str | None = None
    error: str | None = None


def _is_makernote_tag(tag: str) -> bool:
    """Check if tag belongs to a MakerNotes group."""
    return any(tag.startswith(prefix) for prefix in _MAKERNOTE_PREFIXES)


def _is_uncopyable(tag: str) -> bool:
    """Check if tag should never be copied."""
    suffix = tag.split(":")[-1] if ":" in tag else tag
    return suffix in _UNCOPYABLE_TAGS


def _is_list_tag(tag: str) -> bool:
    """Check if tag values should be merged as lists."""
    suffix = tag.split(":")[-1] if ":" in tag else tag
    return suffix in _LIST_TAGS


def _survivor_has_makernotes(survivor_meta: dict[str, Any]) -> bool:
    """Check if survivor already has MakerNotes data."""
    return any(_is_makernote_tag(k) for k in survivor_meta)


def create_merge_plan(
    survivor_path: str,
    survivor_meta: dict[str, Any],
    diff: MetadataDiff,
) -> MergePlan:
    """Create a metadata merge plan for a survivor file.

    Analyzes partial tags (present in donors but not survivor) and
    conflicting tags, producing a list of actions.

    Args:
        survivor_path: Path to the file that will survive deduplication.
        survivor_meta: Full metadata dict of the survivor.
        diff: Metadata diff across the duplicate group.
    """
    try:
        survivor_hash = sha256_file(Path(survivor_path))
    except OSError:
        survivor_hash = ""

    plan = MergePlan(survivor_path=survivor_path, survivor_hash=survivor_hash)
    has_makernotes = _survivor_has_makernotes(survivor_meta)

    # 1. Process partial tags (merge candidates — present in donors, missing from survivor)
    candidates = merge_candidates(diff, survivor_path)
    for tag, (source_path, value) in candidates.items():
        if _is_uncopyable(tag):
            plan.skipped.append(
                MergeAction(
                    tag=tag,
                    value=value,
                    source_path=source_path,
                    action_type="skip_uncopyable",
                )
            )
            continue

        if _is_makernote_tag(tag):
            if has_makernotes:
                plan.skipped.append(
                    MergeAction(
                        tag=tag,
                        value=value,
                        source_path=source_path,
                        action_type="skip_makernotes",
                    )
                )
            else:
                plan.actions.append(
                    MergeAction(
                        tag=tag,
                        value=value,
                        source_path=source_path,
                        action_type="copy",
                    )
                )
            continue

        if _is_list_tag(tag):
            plan.actions.append(
                MergeAction(
                    tag=tag,
                    value=value,
                    source_path=source_path,
                    action_type="merge_list",
                )
            )
        else:
            plan.actions.append(
                MergeAction(
                    tag=tag,
                    value=value,
                    source_path=source_path,
                    action_type="copy",
                )
            )

    # 2. Process list-type conflicts — try to merge lists
    for tag, path_values in diff.conflicts.items():
        if _is_uncopyable(tag) or _is_makernote_tag(tag):
            continue

        if _is_list_tag(tag):
            # Merge all unique values from all copies
            all_vals: set[str] = set()
            source = ""
            for p, v in path_values.items():
                if isinstance(v, list):
                    all_vals.update(str(x) for x in v)
                else:
                    all_vals.add(str(v))
                if p != survivor_path:
                    source = p
            survivor_val = path_values.get(survivor_path)
            if isinstance(survivor_val, list):
                survivor_set = {str(x) for x in survivor_val}
            elif survivor_val is not None:
                survivor_set = {str(survivor_val)}
            else:
                survivor_set = set()
            new_vals = all_vals - survivor_set
            if new_vals:
                plan.actions.append(
                    MergeAction(
                        tag=tag,
                        value=sorted(new_vals),
                        source_path=source,
                        action_type="merge_list",
                    )
                )
        else:
            # Non-list conflict — survivor value preserved, log conflict
            survivor_val = path_values.get(survivor_path)
            for p, v in path_values.items():
                if p != survivor_path:
                    logger.info(
                        "Merge conflict for tag '%s' on %s: keeping survivor value %r "
                        "(discarded %r from %s)",
                        tag,
                        survivor_path,
                        survivor_val,
                        v,
                        p,
                    )
                    plan.conflicts.append(
                        MergeAction(
                            tag=tag,
                            value=v,
                            source_path=p,
                            action_type="skip_conflict",
                        )
                    )

    return plan


def write_merge_plan_tsv(path: Path, plan: MergePlan) -> None:
    """Write merge plan to TSV for human review."""
    header = ["survivor_path", "survivor_hash", "tag", "value", "source_path", "action_type"]
    rows = []
    for action in plan.actions:
        val_str = json.dumps(action.value) if isinstance(action.value, list) else str(action.value)
        rows.append((plan.survivor_path, plan.survivor_hash, action.tag, val_str, action.source_path, action.action_type))
    for action in plan.conflicts:
        val_str = json.dumps(action.value) if isinstance(action.value, list) else str(action.value)
        rows.append((plan.survivor_path, plan.survivor_hash, action.tag, val_str, action.source_path, action.action_type))
    for action in plan.skipped:
        val_str = json.dumps(action.value) if isinstance(action.value, list) else str(action.value)
        rows.append((plan.survivor_path, plan.survivor_hash, action.tag, val_str, action.source_path, action.action_type))
    write_tsv(path, header, rows)


def execute_merge(
    plan: MergePlan,
    *,
    bin_path: str = "exiftool",
    dry_run: bool = False,
) -> MergeResult:
    """Execute a metadata merge plan using ExifTool.

    ExifTool creates a backup file (_original suffix) automatically.

    Args:
        plan: The merge plan to execute.
        bin_path: ExifTool binary path.
        dry_run: If True, don't write anything.
    """
    result = MergeResult(conflicts=len(plan.conflicts))
    copyable = [a for a in plan.actions if a.action_type in ("copy", "merge_list")]

    if not copyable:
        logger.info("No metadata to merge for %s", plan.survivor_path)
        result.skipped = len(plan.skipped)
        return result

    # Verify survivor hash hasn't changed
    if plan.survivor_hash:
        try:
            current_hash = sha256_file(Path(plan.survivor_path))
        except OSError:
            result.error = "Cannot read survivor file"
            return result
        if current_hash != plan.survivor_hash:
            result.error = f"Survivor hash changed: expected {plan.survivor_hash[:16]}..., got {current_hash[:16]}..."
            return result

    if dry_run:
        result.applied = len(copyable)
        result.skipped = len(plan.skipped)
        logger.info("[DRY RUN] Would merge %d tags into %s", len(copyable), plan.survivor_path)
        return result

    binary = shutil.which(bin_path) if "/" not in bin_path else bin_path
    if not binary:
        result.error = "ExifTool not available"
        return result

    # Build ExifTool write command
    cmd = [binary, "-q", "-q", "-api", "LargeFileSupport=1"]

    for action in copyable:
        # Keep group prefix (e.g. EXIF:, XMP:) so that tags like
        # EXIF:DateTimeOriginal and XMP:DateTimeOriginal are written
        # to their correct groups instead of colliding as bare names.
        tag_name = action.tag

        if action.action_type == "merge_list":
            # Use += to append list values
            if isinstance(action.value, list):
                for val in action.value:
                    cmd.append(f"-{tag_name}+={val}")
            else:
                cmd.append(f"-{tag_name}+={action.value}")
        else:
            # Direct copy — write each list item separately to preserve
            # multi-value tag structure (e.g. IPTC:Keywords)
            if isinstance(action.value, list):
                for val in action.value:
                    cmd.append(f"-{tag_name}={val}")
            elif isinstance(action.value, float):
                cmd.append(f"-{tag_name}={action.value}")
            else:
                cmd.append(f"-{tag_name}={action.value}")

    # Protect path from being treated as ExifTool flag if it starts with '-'
    survivor = plan.survivor_path
    cmd.append(f"./{survivor}" if survivor.startswith("-") else survivor)

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)  # noqa: S603
    except subprocess.TimeoutExpired:
        result.error = "ExifTool write timeout"
        return result
    except FileNotFoundError:
        result.error = f"ExifTool not found: {binary}"
        return result

    if proc.returncode not in (0, 1):
        result.error = f"ExifTool write failed: {proc.stderr[:200]}"
        return result

    result.applied = len(copyable)
    result.skipped = len(plan.skipped)

    # Check for backup file
    backup = Path(plan.survivor_path + "_original")
    if backup.exists():
        result.backup_path = str(backup)

    logger.info("Merged %d tags into %s (backup: %s)", result.applied, plan.survivor_path, result.backup_path)
    return result
