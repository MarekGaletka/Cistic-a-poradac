"""Face detection module that persists results to the catalog database.

Wraps face_recognition library to detect faces, compute encodings,
cluster them with DBSCAN, and store everything persistently.
Supports incremental matching against known persons.
"""

from __future__ import annotations

import logging
import struct
from collections.abc import Callable
from dataclasses import dataclass

from .catalog import Catalog

logger = logging.getLogger(__name__)

IMAGE_EXTS = {"jpg", "jpeg", "png", "bmp", "tif", "tiff", "webp", "heic", "heif"}


@dataclass
class FaceScanResult:
    files_processed: int = 0
    faces_detected: int = 0
    faces_matched: int = 0
    errors: int = 0


def _load_libs():
    """Import optional face_recognition + numpy."""
    try:
        try:
            import pillow_heif  # type: ignore

            pillow_heif.register_heif_opener()
        except ImportError:
            logger.debug("pillow_heif not available, HEIF support disabled")
        import face_recognition  # type: ignore
        import numpy as np  # type: ignore
        from PIL import Image  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Face detection requires: pip install godmode-media-library[people]") from exc
    return face_recognition, np, Image


def _resize_if_needed(image_arr, max_dimension: int, np_mod, pil_image_cls):
    shape = getattr(image_arr, "shape", None)
    if not shape or len(shape) < 2:
        return image_arr, 1.0
    h, w = int(shape[0]), int(shape[1])
    if max(h, w) <= max_dimension:
        return image_arr, 1.0
    scale = max_dimension / float(max(h, w))
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))
    image = pil_image_cls.fromarray(image_arr)
    image = image.resize((new_w, new_h))
    return np_mod.array(image), scale


def detect_faces_in_file(
    catalog: Catalog,
    file_id: int,
    file_path: str,
    *,
    model: str = "hog",
    max_dimension: int = 1600,
    encrypt_fn: Callable | None = None,
) -> int:
    """Detect faces in a single image file and persist to catalog.

    Returns the number of faces detected.
    """
    face_recognition, np, Image = _load_libs()

    try:
        image = face_recognition.load_image_file(file_path)
    except Exception as exc:
        logger.warning("Cannot load image %s: %s", file_path, exc)
        return 0

    image, scale = _resize_if_needed(image, max_dimension, np, Image)

    # Use upsample=2 for better detection on high-res / downscaled images
    locations = face_recognition.face_locations(
        image,
        number_of_times_to_upsample=2,
        model=model,
    )
    if not locations:
        return 0

    encodings = face_recognition.face_encodings(image, known_face_locations=locations)

    for idx, (loc, enc) in enumerate(zip(locations, encodings, strict=False)):
        top, right, bottom, left = loc
        # Scale bbox back to original image coordinates
        if scale != 1.0:
            inv = 1.0 / scale
            top = int(round(top * inv))
            right = int(round(right * inv))
            bottom = int(round(bottom * inv))
            left = int(round(left * inv))

        raw_bytes = enc.tobytes() if hasattr(enc, 'tobytes') else bytes(struct.pack("<128d", *enc))
        encoding_blob = encrypt_fn(enc) if encrypt_fn else raw_bytes
        catalog.insert_face(
            file_id=file_id,
            face_index=idx,
            bbox=(top, right, bottom, left),
            encoding_blob=encoding_blob,
        )

    return len(locations)


def scan_new_faces(
    catalog: Catalog,
    *,
    model: str = "hog",
    max_dimension: int = 1600,
    encrypt_fn: Callable | None = None,
    progress_fn: Callable[[int, int], None] | None = None,
) -> FaceScanResult:
    """Detect faces in all image files that don't have faces yet.

    Args:
        catalog: Open catalog instance.
        model: 'hog' (CPU) or 'cnn' (GPU).
        max_dimension: Max image dimension for detection.
        encrypt_fn: Function to encrypt encoding bytes.
        progress_fn: Callback(processed, total) for progress updates.
    """
    pending = catalog.files_without_faces(IMAGE_EXTS)
    total = len(pending)
    result = FaceScanResult()

    if total == 0:
        return result

    # Verify face_recognition is available before processing
    _load_libs()

    for i, (file_id, path) in enumerate(pending):
        try:
            n = detect_faces_in_file(
                catalog,
                file_id,
                path,
                model=model,
                max_dimension=max_dimension,
                encrypt_fn=encrypt_fn,
            )
            result.files_processed += 1
            result.faces_detected += n
        except Exception as exc:
            logger.warning("Face detection error for %s: %s", path, exc)
            result.errors += 1

        if progress_fn and (i % 5 == 0 or i == total - 1):
            progress_fn(i + 1, total)

    catalog.commit()
    return result


def cluster_faces(
    catalog: Catalog,
    *,
    eps: float = 0.5,
    min_samples: int = 2,
    decrypt_fn: Callable | None = None,
    person_prefix: str = "Person",
    max_clusters: int = 1000,
) -> dict[int, list[int]]:
    """Cluster all face encodings with DBSCAN and create/update persons.

    Preserves existing named-person assignments: if any face in a cluster
    already belongs to a user-named person (not auto-generated Person_NNN),
    the entire cluster is assigned to that person.  Only truly new clusters
    get fresh Person_NNN names.

    Returns mapping of cluster_id -> list of face_ids.
    """
    import numpy as np
    from sklearn.cluster import DBSCAN  # type: ignore

    all_enc = catalog.get_all_encodings()
    if not all_enc:
        return {}

    face_ids = []
    vectors = []
    for face_id, blob in all_enc:
        try:
            vec = decrypt_fn(blob) if decrypt_fn else list(struct.unpack("<128d", blob))
            face_ids.append(face_id)
            vectors.append(vec)
        except Exception as exc:
            logger.warning("Cannot decrypt encoding for face %d: %s", face_id, exc)

    if not vectors:
        return {}

    data = np.vstack(vectors)
    labels = DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean").fit_predict(data)

    clusters: dict[int, list[int]] = {}
    cluster_limit_hit = False
    for face_id, cluster_id in zip(face_ids, labels, strict=False):
        cluster_id = int(cluster_id)
        catalog.set_face_cluster(face_id, cluster_id)
        if cluster_id >= 0:
            is_new_cluster = cluster_id not in clusters
            if is_new_cluster and len(clusters) >= max_clusters:
                logger.warning(
                    "Cluster limit reached (%d). Stopping clustering early; "
                    "remaining faces will be unassigned.",
                    max_clusters,
                )
                cluster_limit_hit = True
                break
            clusters.setdefault(cluster_id, []).append(face_id)

    if cluster_limit_hit:
        logger.warning(
            "Returning %d clusters (limit: %d). Some faces were not clustered.",
            len(clusters),
            max_clusters,
        )

    # ── Build lookup: face_id -> existing person_id (batch query) ──
    face_person_map: dict[int, int | None] = {fid: None for fid in face_ids}
    chunk_size = 500
    for i in range(0, len(face_ids), chunk_size):
        chunk = face_ids[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = catalog.conn.execute(
            f"SELECT id, person_id FROM faces WHERE id IN ({placeholders})", chunk  # noqa: S608
        ).fetchall()
        for row in rows:
            face_person_map[row[0]] = row[1]

    # ── Build set of "user-named" person IDs (not auto-generated Person_NNN) ──
    import re
    auto_name_re = re.compile(rf"^{re.escape(person_prefix)}_\d{{3,}}$")
    all_persons = {p["id"]: p for p in catalog.get_all_persons()}
    named_person_ids = {
        pid for pid, p in all_persons.items()
        if p["name"] and not auto_name_re.match(p["name"])
    }

    # ── Assign clusters, preserving named persons ──
    auto_counter = 0
    used_names = {p["name"] for p in all_persons.values()}

    for _cluster_id, fids in sorted(clusters.items()):
        # Check if any face in this cluster already belongs to a named person
        existing_named_pid = None
        for fid in fids:
            pid = face_person_map.get(fid)
            if pid and pid in named_person_ids:
                existing_named_pid = pid
                break

        if existing_named_pid is not None:
            # Cluster matches a known named person — assign all faces to them
            person_id = existing_named_pid
        else:
            # New cluster — generate next available Person_NNN name
            auto_counter += 1
            name = f"{person_prefix}_{auto_counter:03d}"
            while name in used_names:
                auto_counter += 1
                name = f"{person_prefix}_{auto_counter:03d}"
            used_names.add(name)
            person_id = catalog.upsert_person(name, sample_face_id=fids[0])

        for fid in fids:
            catalog.assign_face_to_person(fid, person_id)

    # ── Clean up orphaned auto-named persons with 0 faces ──
    catalog._refresh_person_counts()
    for pid, p in all_persons.items():
        if auto_name_re.match(p["name"]):
            cnt = catalog.conn.execute(
                "SELECT face_count FROM persons WHERE id = ?", (pid,)
            ).fetchone()
            if cnt and cnt[0] == 0:
                catalog.conn.execute("DELETE FROM persons WHERE id = ?", (pid,))

    catalog.commit()
    return clusters


def match_face_to_known(
    catalog: Catalog,
    encoding,
    *,
    decrypt_fn: Callable | None = None,
    threshold: float = 0.6,
) -> int | None:
    """Try to match a single face encoding against known persons.

    Returns person_id if a match is found within threshold, else None.
    """
    import numpy as np

    persons = catalog.get_all_persons()
    if not persons:
        return None

    # Collect sample face IDs and batch-load their encodings
    sample_ids = [p.get("sample_face_id") for p in persons if p.get("sample_face_id") is not None]
    if not sample_ids:
        return None

    # Batch query all sample face encodings in one go
    encodings_map: dict[int, bytes] = {}
    chunk_size = 500
    for i in range(0, len(sample_ids), chunk_size):
        chunk = sample_ids[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = catalog.conn.execute(
            f"SELECT id, encoding FROM faces WHERE id IN ({placeholders}) AND encoding IS NOT NULL",  # noqa: S608
            chunk,
        ).fetchall()
        for row in rows:
            encodings_map[row[0]] = row[1]

    best_person_id = None
    best_distance = float("inf")

    enc_vec = np.array(encoding)

    for person in persons:
        sample_face_id = person.get("sample_face_id")
        if sample_face_id is None or sample_face_id not in encodings_map:
            continue

        blob = encodings_map[sample_face_id]
        try:
            ref_vec = np.array(decrypt_fn(blob)) if decrypt_fn else np.array(struct.unpack("<128d", blob))
        except (struct.error, ValueError, TypeError) as exc:
            logger.debug("Failed to decode face encoding for person: %s", exc)
            continue

        distance = float(np.linalg.norm(enc_vec - ref_vec))
        if distance < best_distance:
            best_distance = distance
            best_person_id = person["id"]

    if best_distance <= threshold and best_person_id is not None:
        return best_person_id
    return None


def crop_face_thumbnail(
    file_path: str,
    bbox: dict,
    size: int = 150,
    padding: float = 0.3,
) -> bytes | None:
    """Crop a face from an image and return JPEG bytes.

    Args:
        file_path: Path to the source image.
        bbox: Dict with top, right, bottom, left keys.
        size: Output thumbnail size (square).
        padding: Extra padding around face as fraction of face size.
    """
    try:
        from PIL import Image
    except ImportError:
        return None

    try:
        try:
            import pillow_heif  # type: ignore

            pillow_heif.register_heif_opener()
        except ImportError:
            pass

        with Image.open(file_path) as img:
            img_w, img_h = img.size

            top = bbox.get("top", 0)
            right = bbox.get("right", 0)
            bottom = bbox.get("bottom", 0)
            left = bbox.get("left", 0)

            face_h = bottom - top
            face_w = right - left
            pad_h = int(face_h * padding)
            pad_w = int(face_w * padding)

            crop_top = max(0, top - pad_h)
            crop_left = max(0, left - pad_w)
            crop_bottom = min(img_h, bottom + pad_h)
            crop_right = min(img_w, right + pad_w)

            face_img = img.crop((crop_left, crop_top, crop_right, crop_bottom))
            face_img = face_img.resize((size, size), Image.LANCZOS)

            import io

            buf = io.BytesIO()
            face_img.convert("RGB").save(buf, format="JPEG", quality=85)
            return buf.getvalue()
    except (OSError, ValueError, KeyError) as exc:
        logger.warning("Cannot crop face from %s: %s", file_path, exc)
        return None
