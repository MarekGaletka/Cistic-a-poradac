from __future__ import annotations

import csv
import datetime as dt
import hashlib
import logging
import os
from collections.abc import Iterable, Iterator
from pathlib import Path


def ensure_path(value: str | Path) -> Path:
    """Ensure value is a Path object. Use at API boundaries.

    Converts string paths to Path objects; passes through existing Path instances
    unchanged. Useful for normalising inputs at function boundaries so internal
    code can always rely on Path objects.
    """
    if isinstance(value, Path):
        return value
    return Path(value)


NOISE_XATTR_NAMES = {
    "com.apple.quarantine",
    "com.apple.provenance",
    "com.apple.lastuseddate#PS",
}


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def iter_files(roots: Iterable[Path]) -> Iterator[Path]:
    _logger = logging.getLogger(__name__)
    visited_real_dirs: set[str] = set()
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        real_root = os.path.realpath(root)
        if real_root in visited_real_dirs:
            _logger.warning("Symlink loop detected: root %s -> %s (already visited), skipping", root, real_root)
            continue
        visited_real_dirs.add(real_root)
        for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
            # Check real path of current directory to detect symlink loops
            real_dir = os.path.realpath(dirpath)
            if real_dir in visited_real_dirs and dirpath != str(root):
                _logger.warning("Symlink loop detected: %s -> %s (already visited), skipping", dirpath, real_dir)
                dirnames.clear()  # prevent further descent
                continue
            visited_real_dirs.add(real_dir)
            for fname in filenames:
                fpath = Path(dirpath) / fname
                if fpath.is_symlink():
                    continue
                yield fpath


def safe_stat_birthtime(path: Path) -> float | None:
    try:
        st = path.stat()
    except OSError:
        return None
    birth = getattr(st, "st_birthtime", None)
    if birth is not None:
        return float(birth)
    # Windows: st_ctime usually maps to creation time.
    if os.name == "nt":
        return float(st.st_ctime)
    return None


def meaningful_xattr_count(path: Path) -> int:
    listxattr = getattr(os, "listxattr", None)
    if listxattr is None:
        return 0
    try:
        names = listxattr(path)
    except OSError:
        return 0
    count = 0
    for name in names:
        if name not in NOISE_XATTR_NAMES:
            count += 1
    return count


def write_tsv(path: Path, header: list[str], rows: Iterable[Iterable[object]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(header)
        for row in rows:
            writer.writerow(list(row))


def read_tsv_dict(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return [dict(row) for row in reader]


def path_startswith(path: Path, prefixes: tuple[str, ...]) -> int | None:
    resolved = path.resolve()
    for idx, prefix in enumerate(prefixes):
        try:
            if resolved.is_relative_to(Path(prefix).resolve()):
                return idx
        except (TypeError, ValueError):
            continue
    return None
