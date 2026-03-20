"""Batch file renaming with pattern-based templates."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class RenameAction:
    original: Path
    new_name: str
    new_path: Path


@dataclass
class RenameResult:
    renamed: int = 0
    skipped: int = 0
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def plan_renames(
    paths: list[Path],
    pattern: str,
    *,
    start_number: int = 1,
) -> list[RenameAction]:
    """Plan batch renames using a pattern template.

    Pattern placeholders:
    - {n} or {n:03d} — sequential number (with optional formatting)
    - {ext} — original extension (without dot)
    - {name} — original filename without extension
    - {date} — date from filename if detectable (YYYYMMDD)
    - {parent} — parent directory name

    Args:
        paths: Files to rename.
        pattern: Rename pattern template.
        start_number: Starting number for {n} placeholder.
    """
    actions = []
    for i, path in enumerate(sorted(paths)):
        ext = path.suffix.lstrip(".")
        name = path.stem
        parent = path.parent.name

        # Try to extract date from filename
        date_match = re.search(r"(\d{4})[_-]?(\d{2})[_-]?(\d{2})", name)
        date = f"{date_match.group(1)}{date_match.group(2)}{date_match.group(3)}" if date_match else ""

        new_name = pattern.format(
            n=start_number + i,
            ext=ext,
            name=name,
            date=date,
            parent=parent,
        )

        # Ensure extension is preserved if not in pattern
        if ext and not new_name.endswith(f".{ext}"):
            new_name = f"{new_name}.{ext}"

        new_path = path.parent / new_name
        if new_path != path:
            actions.append(RenameAction(original=path, new_name=new_name, new_path=new_path))

    return actions


def apply_renames(
    actions: list[RenameAction],
    *,
    dry_run: bool = False,
) -> RenameResult:
    """Apply planned renames.

    Args:
        actions: List of rename actions from plan_renames().
        dry_run: If True, don't actually rename files.
    """
    result = RenameResult()

    # Check for conflicts first
    targets = {}
    for action in actions:
        target_str = str(action.new_path)
        if target_str in targets:
            result.errors.append(f"Conflict: {action.original} and {targets[target_str]} both map to {action.new_path}")
            result.skipped += 1
            continue
        targets[target_str] = action.original

        if action.new_path.exists() and action.new_path != action.original:
            result.errors.append(f"Target exists: {action.new_path}")
            result.skipped += 1
            continue

        if not action.original.exists():
            result.errors.append(f"Source missing: {action.original}")
            result.skipped += 1
            continue

        if not dry_run:
            try:
                action.original.rename(action.new_path)
            except OSError as e:
                result.errors.append(f"Rename failed: {action.original} -> {action.new_path}: {e}")
                result.skipped += 1
                continue

        result.renamed += 1

    return result
