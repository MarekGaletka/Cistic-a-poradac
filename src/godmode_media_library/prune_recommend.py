from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .audit import DEFAULT_DEDUP_EXTS, collect_file_records, exact_duplicates
from .models import PlanPolicy
from .planning import create_plan
from .utils import ensure_dir, write_tsv


@dataclass(frozen=True)
class PruneRecommendResult:
    run_dir: Path
    recommendations_tsv: Path
    recommended_paths_txt: Path
    summary_json: Path
    total_recommendations: int
    quarantine_candidates: int
    manual_review: int
    estimated_reclaim_bytes: int


def _is_noise_file(path: Path) -> tuple[bool, str]:
    name = path.name.lower()
    if name in {".ds_store", "thumbs.db", "desktop.ini"}:
        return True, "system_noise_file"
    if name.startswith("._"):
        return True, "appledouble_sidecar_noise"
    return False, ""


def recommend_prune(
    *,
    roots: list[Path],
    run_dir: Path,
    policy: PlanPolicy,
    min_size_bytes: int = 500 * 1024,
    include_system_noise: bool = True,
) -> PruneRecommendResult:
    ensure_dir(run_dir)

    records = collect_file_records(roots)
    inventory = {r.path: r for r in records}
    inode_by_path: dict[Path, tuple[int, int]] = {}
    for rec in records:
        try:
            st = rec.path.stat()
        except OSError:
            continue
        inode_by_path[rec.path] = (int(st.st_dev), int(st.st_ino))

    duplicates = exact_duplicates(records, min_size_bytes=min_size_bytes, dedup_exts=DEFAULT_DEDUP_EXTS)
    plan_rows, manual_rows = create_plan(duplicates, inventory, policy)

    recommendations: list[tuple[object, ...]] = []
    selected_paths: list[str] = []
    rec_id = 1
    reclaimable = 0
    quarantine_candidates = 0
    manual_review = 0

    for row in plan_rows:
        move_inode = inode_by_path.get(row.move_path)
        keep_inode = inode_by_path.get(row.keep_path)
        same_inode = move_inode is not None and keep_inode is not None and move_inode == keep_inode

        if same_inode:
            action = "manual_review"
            reason = "hardlink_alias_not_real_duplicate"
            confidence = "high"
            is_manual = 1
            est_reclaim = 0
            note = "Paths share inode, deleting one path alone does not reclaim bytes."
            manual_review += 1
        else:
            action = "quarantine_candidate"
            reason = "exact_duplicate_sha256"
            confidence = "high"
            is_manual = 0
            est_reclaim = int(row.size)
            note = "Exact content duplicate with selected primary copy."
            reclaimable += est_reclaim
            selected_paths.append(str(row.move_path))
            quarantine_candidates += 1

        recommendations.append(
            (
                f"R{rec_id:06d}",
                str(row.move_path),
                action,
                reason,
                confidence,
                is_manual,
                str(row.keep_path),
                row.size,
                est_reclaim,
                note,
            )
        )
        rec_id += 1

    for row in manual_rows:
        recommendations.append(
            (
                f"R{rec_id:06d}",
                str(row.path),
                "manual_review",
                row.reason,
                "medium",
                1,
                "",
                row.size,
                0,
                "Protected/ambiguous duplicate candidate, review required.",
            )
        )
        rec_id += 1
        manual_review += 1

    if include_system_noise:
        seen = {r[1] for r in recommendations}
        for rec in records:
            if str(rec.path) in seen:
                continue
            ok, reason = _is_noise_file(rec.path)
            if not ok:
                continue
            recommendations.append(
                (
                    f"R{rec_id:06d}",
                    str(rec.path),
                    "quarantine_candidate",
                    reason,
                    "medium",
                    0,
                    "",
                    rec.size,
                    rec.size,
                    "System metadata/noise file candidate.",
                )
            )
            rec_id += 1
            reclaimable += rec.size
            quarantine_candidates += 1
            selected_paths.append(str(rec.path))

    recommendations.sort(key=lambda x: (x[2] != "quarantine_candidate", -int(x[8]), x[1]))

    recommendations_tsv = run_dir / "prune_recommendations.tsv"
    write_tsv(
        recommendations_tsv,
        [
            "recommendation_id",
            "path",
            "action",
            "reason",
            "confidence",
            "requires_manual_review",
            "keep_path",
            "size",
            "estimated_reclaim_bytes",
            "note",
        ],
        recommendations,
    )

    selected_unique = sorted(set(selected_paths))
    recommended_paths_txt = run_dir / "recommended_paths.txt"
    recommended_paths_txt.write_text("\n".join(selected_unique) + ("\n" if selected_unique else ""), encoding="utf-8")

    summary_json = run_dir / "prune_summary.json"
    summary = {
        "roots": [str(r) for r in roots],
        "records_scanned": len(records),
        "total_recommendations": len(recommendations),
        "quarantine_candidates": quarantine_candidates,
        "manual_review": manual_review,
        "estimated_reclaim_bytes": reclaimable,
        "recommendations_tsv": str(recommendations_tsv),
        "recommended_paths_txt": str(recommended_paths_txt),
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")

    return PruneRecommendResult(
        run_dir=run_dir,
        recommendations_tsv=recommendations_tsv,
        recommended_paths_txt=recommended_paths_txt,
        summary_json=summary_json,
        total_recommendations=len(recommendations),
        quarantine_candidates=quarantine_candidates,
        manual_review=manual_review,
        estimated_reclaim_bytes=reclaimable,
    )
