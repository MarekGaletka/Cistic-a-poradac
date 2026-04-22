from __future__ import annotations

import io
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..shared import (
    _create_task,
    _finish_task,
    _open_catalog,
    _return_catalog,
    _thumb_cache_get,
    _thumb_cache_put,
    _update_progress,
    logger,
)

if TYPE_CHECKING:
    from ...catalog import Catalog

router = APIRouter()


# ── Pydantic request models ──────────────────────────────────────────


class PersonRenameRequest(BaseModel):
    name: str


class PersonMergeRequest(BaseModel):
    merge_ids: list[int]


class FaceAssignRequest(BaseModel):
    person_id: int | None = None


class FaceDetectRequest(BaseModel):
    model: str = "hog"
    max_dimension: int = 1600


class FaceClusterRequest(BaseModel):
    eps: float = 0.5
    min_samples: int = 2


# ── Helpers ───────────────────────────────────────────────────────────


def _sync_person_labels(cat: Catalog, person_id: int) -> None:
    """Sync labels table when person name changes — update people column for all files with this person's faces."""
    faces = cat.get_faces_for_person(person_id, limit=100000)
    file_ids = {f["file_id"] for f in faces}
    for fid in file_ids:
        all_faces = cat.get_faces_for_file(fid)
        names = sorted({f["person_name"] for f in all_faces if f.get("person_name")})
        people_str = ";".join(names)
        cat.upsert_label(fid, people=people_str)


# ── Person endpoints ─────────────────────────────────────────────────


@router.get("/persons")
def list_persons(request: Request):
    """List all persons with face counts."""
    cat = _open_catalog(request)
    try:
        persons = cat.get_all_persons()
        return {"persons": persons, "total": len(persons)}
    finally:
        _return_catalog(cat)


@router.get("/persons/{person_id}")
def get_person(request: Request, person_id: int):
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        return person
    finally:
        _return_catalog(cat)


@router.put("/persons/{person_id}/name")
def rename_person(request: Request, person_id: int, body: PersonRenameRequest):
    """Rename a person."""
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        cat.update_person_name(person_id, body.name)
        # Sync labels table for files containing this person
        _sync_person_labels(cat, person_id)
        cat.commit()
        return {"status": "ok", "person_id": person_id, "name": body.name}
    finally:
        _return_catalog(cat)


@router.post("/persons/{person_id}/merge")
def merge_persons(request: Request, person_id: int, body: PersonMergeRequest):
    """Merge other persons into this one."""
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        reassigned = cat.merge_persons(person_id, body.merge_ids)
        _sync_person_labels(cat, person_id)
        cat.commit()
        return {"status": "ok", "reassigned_faces": reassigned}
    finally:
        _return_catalog(cat)


@router.delete("/persons/{person_id}")
def delete_person(request: Request, person_id: int):
    cat = _open_catalog(request)
    try:
        person = cat.get_person(person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        cat.delete_person(person_id)
        cat.commit()
        return {"status": "ok"}
    finally:
        _return_catalog(cat)


@router.get("/persons/{person_id}/faces")
def get_person_faces(
    request: Request,
    person_id: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    cat = _open_catalog(request)
    try:
        faces = cat.get_faces_for_person(person_id, limit=limit, offset=offset)
        return {"faces": faces, "count": len(faces), "person_id": person_id}
    finally:
        _return_catalog(cat)


# ── Face endpoints ────────────────────────────────────────────────────


@router.get("/faces")
def list_faces(
    request: Request,
    person_id: int | None = Query(None),
    unidentified: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List faces, optionally filtered by person or unidentified status."""
    cat = _open_catalog(request)
    try:
        if unidentified:
            faces = cat.get_unidentified_faces(limit=limit, offset=offset)
        elif person_id is not None:
            faces = cat.get_faces_for_person(person_id, limit=limit, offset=offset)
        else:
            # All faces
            cur = cat.conn.execute(
                """SELECT f.id, f.file_id, f.face_index, f.person_id,
                          f.bbox_top, f.bbox_right, f.bbox_bottom, f.bbox_left,
                          f.confidence, f.cluster_id, fi.path, p.name as person_name
                   FROM faces f
                   JOIN files fi ON f.file_id = fi.id
                   LEFT JOIN persons p ON f.person_id = p.id
                   ORDER BY f.person_id NULLS LAST, f.id LIMIT ? OFFSET ?""",
                (limit, offset),
            )
            faces = [
                {
                    "id": r[0],
                    "file_id": r[1],
                    "face_index": r[2],
                    "person_id": r[3],
                    "bbox": {"top": r[4], "right": r[5], "bottom": r[6], "left": r[7]},
                    "confidence": r[8],
                    "cluster_id": r[9],
                    "path": r[10],
                    "person_name": r[11] or "",
                }
                for r in cur.fetchall()
            ]
        return {"faces": faces, "count": len(faces)}
    finally:
        _return_catalog(cat)


@router.get("/faces/stats")
def face_stats(request: Request):
    cat = _open_catalog(request)
    try:
        return cat.face_stats()
    finally:
        _return_catalog(cat)


@router.get("/faces/{face_id}/thumbnail")
def get_face_thumbnail(request: Request, face_id: int, size: int = Query(150, ge=32, le=512)):
    """Return a cropped face thumbnail as JPEG."""
    from godmode_media_library.face_detect import crop_face_thumbnail

    cat = _open_catalog(request)
    try:
        face = cat.get_face_by_id(face_id)
        if face is None:
            raise HTTPException(404, "Face not found")

        # Check thumbnail cache
        cache_key = f"face_{face_id}_{size}"
        cached = _thumb_cache_get(cache_key, size)
        if cached:
            return StreamingResponse(io.BytesIO(cached), media_type="image/jpeg")

        data = crop_face_thumbnail(face["path"], face["bbox"], size=size)
        if data is None:
            raise HTTPException(404, "Cannot generate face thumbnail")

        _thumb_cache_put(cache_key, size, data)
        return StreamingResponse(io.BytesIO(data), media_type="image/jpeg")
    finally:
        _return_catalog(cat)


@router.put("/faces/{face_id}/person")
def assign_face_to_person(request: Request, face_id: int, body: FaceAssignRequest):
    """Assign a face to a person, or unassign (person_id=null)."""
    cat = _open_catalog(request)
    try:
        face = cat.get_face_by_id(face_id)
        if face is None:
            raise HTTPException(404, "Face not found")
        if body.person_id is None:
            # Unassign — remove face from its current person
            old_person_id = face.get("person_id")
            cat.conn.execute("UPDATE faces SET person_id = NULL WHERE id = ?", (face_id,))
            if old_person_id:
                cat._refresh_person_counts([old_person_id])
            cat.commit()
            return {"status": "ok", "face_id": face_id, "person_id": None}
        person = cat.get_person(body.person_id)
        if person is None:
            raise HTTPException(404, "Person not found")
        cat.assign_face_to_person(face_id, body.person_id)
        _sync_person_labels(cat, body.person_id)
        cat.commit()
        return {"status": "ok", "face_id": face_id, "person_id": body.person_id}
    finally:
        _return_catalog(cat)


@router.get("/faces/by-file")
def get_file_faces(request: Request, path: str = Query(...)):
    """Get all detected faces in a specific file."""
    cat = _open_catalog(request)
    try:
        file_row = cat.get_file_by_path(path)
        if file_row is None:
            raise HTTPException(404, "File not found")
        faces = cat.get_faces_for_file(file_row.id)
        return {"faces": faces, "file_path": path}
    finally:
        _return_catalog(cat)


@router.post("/faces/detect")
def trigger_face_detection(request: Request, background: BackgroundTasks, body: FaceDetectRequest):
    """Start background face detection for all unscanned images."""
    task = _create_task("face_detect")

    def _run():
        try:
            from godmode_media_library.face_crypto import get_encrypt_fn
            from godmode_media_library.face_detect import scan_new_faces

            cat = _open_catalog(request)
            encrypt_fn = get_encrypt_fn(enabled=True)

            def on_progress(done, total):
                _update_progress(task.id, {"done": done, "total": total})

            try:
                result = scan_new_faces(
                    cat,
                    model=body.model,
                    max_dimension=body.max_dimension,
                    encrypt_fn=encrypt_fn,
                    progress_fn=on_progress,
                )
                _finish_task(task.id, result={
                    "files_processed": result.files_processed,
                    "faces_detected": result.faces_detected,
                    "errors": result.errors,
                })
            finally:
                _return_catalog(cat)
        except Exception as exc:
            _finish_task(task.id, error=str(exc))
            logger.exception("Face detection task failed")

    background.add_task(_run)
    return {"task_id": task.id, "status": "started"}


@router.post("/faces/cluster")
def trigger_face_clustering(request: Request, background: BackgroundTasks, body: FaceClusterRequest):
    """Start background face clustering."""
    task = _create_task("face_cluster")

    def _run():
        try:
            from godmode_media_library.face_crypto import get_decrypt_fn
            from godmode_media_library.face_detect import cluster_faces

            cat = _open_catalog(request)
            decrypt_fn = get_decrypt_fn(enabled=True)
            try:
                clusters = cluster_faces(
                    cat,
                    eps=body.eps,
                    min_samples=body.min_samples,
                    decrypt_fn=decrypt_fn,
                )
                _finish_task(task.id, result={
                    "clusters": len(clusters),
                    "total_faces_clustered": sum(len(v) for v in clusters.values()),
                })
            finally:
                _return_catalog(cat)
        except Exception as exc:
            _finish_task(task.id, error=str(exc))
            logger.exception("Face clustering task failed")

    background.add_task(_run)
    return {"task_id": task.id, "status": "started"}


@router.post("/faces/privacy/consent")
def record_privacy_consent(request: Request):
    """Record that the user has consented to face encoding storage."""
    cat = _open_catalog(request)
    try:
        cat.set_privacy_flag("consent_given", datetime.now(timezone.utc).isoformat())
        return {"status": "ok", "consent_given": True}
    finally:
        _return_catalog(cat)


@router.get("/faces/privacy")
def get_privacy_status(request: Request):
    """Check if privacy consent was given and encryption status."""
    cat = _open_catalog(request)
    try:
        consent = cat.get_privacy_flag("consent_given")
        return {
            "consent_given": consent is not None,
            "consent_timestamp": consent,
            "encryption_enabled": True,
        }
    finally:
        _return_catalog(cat)


@router.delete("/faces/privacy/encodings")
def wipe_face_encodings(request: Request):
    """Delete all stored face encodings for privacy. Face records remain but without biometric data."""
    cat = _open_catalog(request)
    try:
        count = cat.wipe_face_encodings()
        return {"status": "ok", "encodings_wiped": count}
    finally:
        _return_catalog(cat)


@router.post("/persons/create")
def create_person(request: Request, body: PersonRenameRequest):
    """Create a new named person."""
    cat = _open_catalog(request)
    try:
        person_id = cat.upsert_person(body.name)
        cat.commit()
        return {"status": "ok", "person_id": person_id, "name": body.name}
    finally:
        _return_catalog(cat)


@router.post("/persons/cleanup")
def cleanup_auto_persons(request: Request):
    """Delete all auto-generated Person_NNN persons and unassign their faces.

    This is useful before re-clustering to clear duplicates. User-named
    persons (any name that doesn't match Person_NNN) are preserved.
    """
    cat = _open_catalog(request)
    try:
        auto_re = re.compile(r"^Person_\d{3,}$")
        all_persons = cat.get_all_persons()
        deleted = 0
        faces_freed = 0
        for p in all_persons:
            if auto_re.match(p["name"]):
                cnt = cat.conn.execute(
                    "SELECT COUNT(*) FROM faces WHERE person_id = ?", (p["id"],)
                ).fetchone()[0]
                cat.conn.execute(
                    "UPDATE faces SET person_id = NULL WHERE person_id = ?", (p["id"],)
                )
                cat.conn.execute("DELETE FROM persons WHERE id = ?", (p["id"],))
                deleted += 1
                faces_freed += cnt
        cat.conn.commit()
        return {"status": "ok", "persons_deleted": deleted, "faces_freed": faces_freed}
    finally:
        _return_catalog(cat)
