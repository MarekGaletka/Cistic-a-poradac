from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path

from .asset_sets import build_asset_membership, summarize_asset_sets
from .models import DuplicateRow, FileRecord
from .utils import (
    ensure_dir,
    iter_files,
    meaningful_xattr_count,
    read_tsv_dict,
    safe_stat_birthtime,
    sha256_file,
    utc_stamp,
    write_tsv,
)

DEFAULT_DEDUP_EXTS = {
    "jpg",
    "jpeg",
    "png",
    "heic",
    "gif",
    "mov",
    "mp4",
    "m4v",
    "avi",
    "mkv",
    "pdf",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "pages",
    "dng",
    "cr2",
    "cr3",
    "nef",
    "arw",
    "raw",
    "aae",
    "xmp",
}


def collect_file_records(
    roots: Iterable[Path],
) -> list[FileRecord]:
    all_paths = list(iter_files(roots))
    path_to_key, path_is_component, _ = build_asset_membership(all_paths)

    records: list[FileRecord] = []
    for path in all_paths:
        try:
            st = path.stat()
        except OSError:
            continue

        ext = path.suffix.lower().lstrip(".")
        birthtime = safe_stat_birthtime(path)
        mxc = meaningful_xattr_count(path)

        records.append(
            FileRecord(
                path=path,
                size=int(st.st_size),
                mtime=float(st.st_mtime),
                ctime=float(st.st_ctime),
                birthtime=birthtime,
                ext=ext,
                meaningful_xattr_count=mxc,
                asset_key=path_to_key.get(path),
                asset_component=path_is_component.get(path, False),
            )
        )
    return records


def exact_duplicates(
    records: list[FileRecord],
    min_size_bytes: int,
    dedup_exts: set[str],
) -> list[DuplicateRow]:
    by_size: dict[int, list[FileRecord]] = defaultdict(list)
    for rec in records:
        if rec.size < min_size_bytes:
            continue
        if rec.ext not in dedup_exts:
            continue
        by_size[rec.size].append(rec)

    duplicates: list[DuplicateRow] = []
    for size, same_size in by_size.items():
        if len(same_size) < 2:
            continue

        by_hash: dict[str, list[FileRecord]] = defaultdict(list)
        for rec in same_size:
            try:
                digest = sha256_file(rec.path)
            except OSError:
                continue
            by_hash[digest].append(rec)

        for digest, files in by_hash.items():
            if len(files) < 2:
                continue
            for rec in sorted(files, key=lambda x: str(x.path)):
                duplicates.append(DuplicateRow(digest=digest, size=size, path=rec.path))

    duplicates.sort(key=lambda r: (r.digest, str(r.path)))
    return duplicates


def duplicate_group_summary(rows: list[DuplicateRow]) -> list[tuple[int, int, int, str]]:
    grouped: dict[str, list[DuplicateRow]] = defaultdict(list)
    for row in rows:
        grouped[row.digest].append(row)

    summary: list[tuple[int, int, int, str]] = []
    for digest, items in grouped.items():
        count = len(items)
        size = items[0].size
        reclaimable = (count - 1) * size
        summary.append((count, size, reclaimable, digest))

    summary.sort(key=lambda x: x[2], reverse=True)
    return summary


def write_audit_run(
    roots: list[Path],
    out_dir: Path,
    min_size_bytes: int = 500 * 1024,
    large_file_threshold_bytes: int = 500 * 1024 * 1024,
    dedup_exts: set[str] | None = None,
    run_name: str | None = None,
) -> Path:
    dedup_exts = dedup_exts or DEFAULT_DEDUP_EXTS
    run_folder = run_name or f"audit_{utc_stamp()}"
    run_dir = out_dir / run_folder
    ensure_dir(run_dir)

    records = collect_file_records(roots)
    duplicates = exact_duplicates(records, min_size_bytes=min_size_bytes, dedup_exts=dedup_exts)
    groups = duplicate_group_summary(duplicates)

    ext_counts = Counter(rec.ext if rec.ext else "(noext)" for rec in records)
    top_large = [
        (rec.size, str(rec.path))
        for rec in sorted(records, key=lambda x: x.size, reverse=True)
        if rec.size >= large_file_threshold_bytes
    ]

    path_to_key, path_is_component, key_to_exts = build_asset_membership([r.path for r in records])
    asset_summary = summarize_asset_sets(key_to_exts)

    write_tsv(
        run_dir / "file_inventory.tsv",
        [
            "path",
            "size",
            "mtime",
            "ctime",
            "birthtime",
            "ext",
            "meaningful_xattr_count",
            "asset_key",
            "asset_component",
        ],
        (
            (
                str(rec.path),
                rec.size,
                f"{rec.mtime:.6f}",
                f"{rec.ctime:.6f}",
                "" if rec.birthtime is None else f"{rec.birthtime:.6f}",
                rec.ext,
                rec.meaningful_xattr_count,
                rec.asset_key or "",
                int(rec.asset_component),
            )
            for rec in sorted(records, key=lambda x: str(x.path))
        ),
    )

    write_tsv(
        run_dir / "extension_counts.tsv",
        ["count", "ext"],
        ((count, ext) for ext, count in ext_counts.most_common()),
    )

    write_tsv(
        run_dir / "files_over_threshold.tsv",
        ["size", "path"],
        top_large,
    )

    write_tsv(
        run_dir / "exact_duplicates.tsv",
        ["hash", "size", "path"],
        ((row.digest, row.size, str(row.path)) for row in duplicates),
    )

    write_tsv(
        run_dir / "duplicate_groups_summary.tsv",
        ["count", "size", "reclaimable", "hash"],
        groups,
    )

    write_tsv(
        run_dir / "asset_sets.tsv",
        ["asset_key", "extensions", "member_count"],
        (
            (
                key,
                ",".join(sorted(exts)),
                len(exts),
            )
            for key, exts in sorted(key_to_exts.items())
        ),
    )

    summary = {
        "roots": [str(p) for p in roots],
        "run_dir": str(run_dir),
        "total_files": len(records),
        "duplicates_rows": len(duplicates),
        "duplicate_groups": len(groups),
        "reclaimable_bytes_exact_duplicates": sum(g[2] for g in groups),
        "asset_summary": asset_summary,
        "min_size_bytes": min_size_bytes,
        "dedup_extensions": sorted(dedup_exts),
    }

    with (run_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=True, indent=2)

    return run_dir


def load_inventory(inventory_path: Path) -> dict[Path, FileRecord]:
    records: dict[Path, FileRecord] = {}
    for row in read_tsv_dict(inventory_path):
        path = Path(row["path"])
        birth = row.get("birthtime", "")
        records[path] = FileRecord(
            path=path,
            size=int(row["size"]),
            mtime=float(row["mtime"]),
            ctime=float(row["ctime"]),
            birthtime=float(birth) if birth else None,
            ext=row["ext"],
            meaningful_xattr_count=int(row.get("meaningful_xattr_count", "0")),
            asset_key=row.get("asset_key", "") or None,
            asset_component=bool(int(row.get("asset_component", "0"))),
        )
    return records


def load_exact_duplicates(path: Path) -> list[DuplicateRow]:
    rows: list[DuplicateRow] = []
    for row in read_tsv_dict(path):
        rows.append(DuplicateRow(digest=row["hash"], size=int(row["size"]), path=Path(row["path"])))
    return rows
