from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .asset_sets import build_asset_membership
from .audit import collect_file_records
from .labels import load_labels_table, merge_label_updates, write_labels_table
from .utils import ensure_dir, write_tsv

PEOPLE_EXTS = {
    "jpg",
    "jpeg",
    "png",
    "bmp",
    "tif",
    "tiff",
    "webp",
    "heic",
    "heif",
}


@dataclass(frozen=True)
class FaceRecord:
    path: Path
    face_index: int
    cluster_id: int
    person_label: str


@dataclass(frozen=True)
class AutoPeopleResult:
    labels_out: Path
    report_path: Path
    faces_path: Path
    clusters_path: Path
    missing_path: Path
    scanned_files: int
    candidate_files: int
    processed_images: int
    faces_detected: int
    clusters: int
    touched_labels: int
    changed_labels: int
    unresolved_candidates: int
    model_engine: str


def _load_face_libs() -> tuple[object, object, object, object]:
    try:
        try:
            import pillow_heif  # type: ignore

            pillow_heif.register_heif_opener()
        except Exception:
            # HEIF plugin is optional. If unavailable, HEIC files may fail to decode.
            pass

        import face_recognition  # type: ignore
        import numpy as np  # type: ignore
        from PIL import Image  # type: ignore
        from sklearn.cluster import DBSCAN  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency path
        raise RuntimeError(
            "Auto-people requires optional dependencies. Install with: "
            "pip install face-recognition scikit-learn pillow numpy"
        ) from exc
    return face_recognition, np, Image, DBSCAN


def _resize_if_needed(image_arr: object, max_dimension: int, np_mod: object, pil_image_cls: object) -> tuple[object, float]:
    shape = getattr(image_arr, "shape", None)
    if not shape or len(shape) < 2:
        return image_arr, 1.0

    h = int(shape[0])
    w = int(shape[1])
    if max(h, w) <= max_dimension:
        return image_arr, 1.0

    scale = max_dimension / float(max(h, w))
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    image = pil_image_cls.fromarray(image_arr)
    image = image.resize((new_w, new_h))
    return np_mod.array(image), scale


def auto_people_labels(
    *,
    roots: list[Path],
    labels_in: Path | None,
    labels_out: Path,
    report_dir: Path,
    model: str = "hog",
    max_dimension: int = 1600,
    eps: float = 0.5,
    min_samples: int = 2,
    person_prefix: str = "Person",
    overwrite_people: bool = False,
) -> AutoPeopleResult:
    records = collect_file_records(roots)
    candidate_paths = sorted({rec.path.resolve() for rec in records if rec.ext.lower() in PEOPLE_EXTS})

    processed_images = 0
    encodings: list[object] = []
    origin: list[tuple[Path, int]] = []
    unresolved: list[tuple[Path, str]] = []

    face_recognition = None
    np = None
    pil_image_cls = None
    dbscan_cls = None

    if candidate_paths:
        face_recognition, np, pil_image_cls, dbscan_cls = _load_face_libs()

        for path in candidate_paths:
            try:
                image = face_recognition.load_image_file(str(path))
                image, _scale = _resize_if_needed(image, max_dimension=max_dimension, np_mod=np, pil_image_cls=pil_image_cls)
                locations = face_recognition.face_locations(image, model=model)
                vectors = face_recognition.face_encodings(image, known_face_locations=locations)
                processed_images += 1
            except Exception as exc:
                unresolved.append((path, f"load_or_detect_error:{type(exc).__name__}"))
                continue

            if not vectors:
                unresolved.append((path, "no_face_detected"))
                continue

            for idx, vec in enumerate(vectors):
                encodings.append(vec)
                origin.append((path, idx))

    if encodings and np is not None and dbscan_cls is not None:
        data = np.vstack(encodings)
        cluster_model = dbscan_cls(eps=eps, min_samples=min_samples, metric="euclidean")
        cluster_labels = cluster_model.fit_predict(data)
    elif np is not None:
        cluster_labels = np.array([], dtype=int)
    else:
        cluster_labels = []

    unique_clusters = sorted({int(v) for v in cluster_labels if int(v) >= 0})
    person_name_by_cluster: dict[int, str] = {}
    for i, cluster_id in enumerate(unique_clusters, start=1):
        person_name_by_cluster[cluster_id] = f"{person_prefix}_{i:03d}"

    face_rows: list[FaceRecord] = []
    labels_by_path: dict[Path, set[str]] = defaultdict(set)
    cluster_members: dict[str, list[Path]] = defaultdict(list)

    for (path, face_idx), cluster_id_raw in zip(origin, cluster_labels, strict=False):
        cluster_id = int(cluster_id_raw)
        person_label = person_name_by_cluster.get(cluster_id, "")
        if person_label:
            labels_by_path[path].add(person_label)
            cluster_members[person_label].append(path)

        face_rows.append(
            FaceRecord(
                path=path,
                face_index=face_idx,
                cluster_id=cluster_id,
                person_label=person_label,
            )
        )

    path_to_key, _, key_to_exts = build_asset_membership(candidate_paths)
    key_to_paths: dict[str, list[Path]] = {k: [] for k in key_to_exts}
    for p, key in path_to_key.items():
        key_to_paths.setdefault(key, []).append(p)

    updates: dict[Path, dict[str, str]] = {}
    for path, names in labels_by_path.items():
        if not names:
            continue
        joined = ";".join(sorted(names))
        key = path_to_key.get(path)
        if key:
            for member in key_to_paths.get(key, [path]):
                updates[member.resolve()] = {"people": joined}
        else:
            updates[path.resolve()] = {"people": joined}

    header, table = load_labels_table(labels_in)
    touched, changed = merge_label_updates(
        table,
        updates,
        overwrite_people=overwrite_people,
        overwrite_place=False,
    )
    write_labels_table(labels_out, header, table)

    ensure_dir(report_dir)
    faces_path = report_dir / "auto_people_faces.tsv"
    write_tsv(
        faces_path,
        ["path", "face_index", "cluster_id", "person_label"],
        (
            (str(row.path), row.face_index, row.cluster_id, row.person_label)
            for row in sorted(face_rows, key=lambda r: (str(r.path), r.face_index))
        ),
    )

    clusters_path = report_dir / "auto_people_clusters.tsv"
    write_tsv(
        clusters_path,
        ["person_label", "member_count", "sample_paths"],
        (
            (
                name,
                len(paths),
                ";".join(str(p) for p in sorted(set(paths), key=str)[:5]),
            )
            for name, paths in sorted(cluster_members.items(), key=lambda x: x[0])
        ),
    )

    missing_path = report_dir / "auto_people_missing.tsv"
    write_tsv(
        missing_path,
        ["path", "reason"],
        ((str(path), reason) for path, reason in sorted(unresolved, key=lambda x: str(x[0]))),
    )

    model_engine = f"face_recognition:{model}" if candidate_paths else "not_run:no_candidate_images"

    report = {
        "roots": [str(r) for r in roots],
        "labels_in": str(labels_in) if labels_in else "",
        "labels_out": str(labels_out),
        "scanned_files": len(records),
        "candidate_files": len(candidate_paths),
        "processed_images": processed_images,
        "faces_detected": len(face_rows),
        "cluster_count": len(unique_clusters),
        "touched_labels": touched,
        "changed_labels": changed,
        "unresolved_candidates": len(unresolved),
        "model_engine": model_engine,
        "faces_path": str(faces_path),
        "clusters_path": str(clusters_path),
        "missing_path": str(missing_path),
    }
    report_path = report_dir / "auto_people_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")

    return AutoPeopleResult(
        labels_out=labels_out,
        report_path=report_path,
        faces_path=faces_path,
        clusters_path=clusters_path,
        missing_path=missing_path,
        scanned_files=len(records),
        candidate_files=len(candidate_paths),
        processed_images=processed_images,
        faces_detected=len(face_rows),
        clusters=len(unique_clusters),
        touched_labels=touched,
        changed_labels=changed,
        unresolved_candidates=len(unresolved),
        model_engine=model_engine,
    )
