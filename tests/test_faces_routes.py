"""Tests for faces route endpoints (web/routes/faces.py).

Targets coverage improvement from ~67% to 85%+.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

from godmode_media_library.catalog import Catalog

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_db(tmp_path):
    db_path = tmp_path / "faces_test.db"
    cat = Catalog(db_path)
    cat.open()
    cat.close()
    return db_path


@pytest.fixture
def catalog_db_with_faces(tmp_path):
    """Catalog with files, persons, and faces for testing."""
    db_path = tmp_path / "faces_test.db"
    cat = Catalog(db_path)
    cat.open()
    conn = cat._conn

    # Insert files
    now_iso = "2024-01-15T12:00:00+00:00"
    conn.execute(
        "INSERT INTO files (id, path, size, ext, mtime, ctime, first_seen, last_scanned) "
        "VALUES (1, '/photos/face1.jpg', 1024, '.jpg', 1704067200.0, 1704067200.0, ?, ?)",
        (now_iso, now_iso),
    )
    conn.execute(
        "INSERT INTO files (id, path, size, ext, mtime, ctime, first_seen, last_scanned) "
        "VALUES (2, '/photos/face2.jpg', 2048, '.jpg', 1704067200.0, 1704067200.0, ?, ?)",
        (now_iso, now_iso),
    )

    # Insert persons
    now_ts = "2024-01-15T12:00:00+00:00"
    conn.execute("INSERT INTO persons (id, name, face_count, created_at, updated_at) VALUES (1, 'Alice', 2, ?, ?)", (now_ts, now_ts))
    conn.execute("INSERT INTO persons (id, name, face_count, created_at, updated_at) VALUES (2, 'Bob', 1, ?, ?)", (now_ts, now_ts))
    conn.execute("INSERT INTO persons (id, name, face_count, created_at, updated_at) VALUES (3, 'Person_001', 0, ?, ?)", (now_ts, now_ts))

    # Insert faces
    conn.execute(
        "INSERT INTO faces (id, file_id, face_index, person_id, bbox_top, bbox_right, bbox_bottom, bbox_left, confidence, created_at) "
        "VALUES (1, 1, 0, 1, 10, 100, 110, 10, 0.95, ?)",
        (now_ts,),
    )
    conn.execute(
        "INSERT INTO faces (id, file_id, face_index, person_id, bbox_top, bbox_right, bbox_bottom, bbox_left, confidence, created_at) "
        "VALUES (2, 1, 1, 2, 20, 200, 220, 20, 0.88, ?)",
        (now_ts,),
    )
    conn.execute(
        "INSERT INTO faces (id, file_id, face_index, person_id, bbox_top, bbox_right, bbox_bottom, bbox_left, confidence, created_at) "
        "VALUES (3, 2, 0, 1, 30, 150, 180, 30, 0.92, ?)",
        (now_ts,),
    )
    # Unidentified face
    conn.execute(
        "INSERT INTO faces (id, file_id, face_index, person_id, bbox_top, bbox_right, bbox_bottom, bbox_left, confidence, created_at) "
        "VALUES (4, 2, 1, NULL, 40, 200, 240, 40, 0.75, ?)",
        (now_ts,),
    )

    conn.commit()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_db):
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db)
    return TestClient(app)


@pytest.fixture
def client_faces(catalog_db_with_faces):
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_db_with_faces)
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /api/persons
# ---------------------------------------------------------------------------


class TestListPersons:
    def test_list_persons_empty(self, client):
        resp = client.get("/api/persons")
        assert resp.status_code == 200
        data = resp.json()
        assert data["persons"] == []
        assert data["total"] == 0

    def test_list_persons_with_data(self, client_faces):
        resp = client_faces.get("/api/persons")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        names = {p["name"] for p in data["persons"]}
        assert "Alice" in names
        assert "Bob" in names


# ---------------------------------------------------------------------------
# GET /api/persons/{person_id}
# ---------------------------------------------------------------------------


class TestGetPerson:
    def test_get_person_exists(self, client_faces):
        resp = client_faces.get("/api/persons/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Alice"

    def test_get_person_not_found(self, client_faces):
        resp = client_faces.get("/api/persons/999")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PUT /api/persons/{person_id}/name
# ---------------------------------------------------------------------------


class TestRenamePerson:
    def test_rename_person(self, client_faces):
        resp = client_faces.put("/api/persons/1/name", json={"name": "Alice Smith"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["name"] == "Alice Smith"

    def test_rename_person_not_found(self, client_faces):
        resp = client_faces.put("/api/persons/999/name", json={"name": "Nobody"})
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/persons/{person_id}/merge
# ---------------------------------------------------------------------------


class TestMergePersons:
    def test_merge_persons(self, client_faces):
        resp = client_faces.post("/api/persons/1/merge", json={"merge_ids": [2]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "reassigned_faces" in data

    def test_merge_persons_target_not_found(self, client_faces):
        resp = client_faces.post("/api/persons/999/merge", json={"merge_ids": [1]})
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /api/persons/{person_id}
# ---------------------------------------------------------------------------


class TestDeletePerson:
    def test_delete_person(self, client_faces):
        resp = client_faces.delete("/api/persons/2")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_delete_person_not_found(self, client_faces):
        resp = client_faces.delete("/api/persons/999")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/persons/{person_id}/faces
# ---------------------------------------------------------------------------


class TestGetPersonFaces:
    def test_get_person_faces(self, client_faces):
        resp = client_faces.get("/api/persons/1/faces")
        assert resp.status_code == 200
        data = resp.json()
        assert data["person_id"] == 1
        assert data["count"] >= 1

    def test_get_person_faces_with_pagination(self, client_faces):
        resp = client_faces.get("/api/persons/1/faces?limit=1&offset=0")
        assert resp.status_code == 200
        assert resp.json()["count"] <= 1


# ---------------------------------------------------------------------------
# GET /api/faces
# ---------------------------------------------------------------------------


class TestListFaces:
    def test_list_all_faces(self, client_faces):
        resp = client_faces.get("/api/faces")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 4  # 4 faces total

    def test_list_faces_by_person(self, client_faces):
        resp = client_faces.get("/api/faces?person_id=1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1

    def test_list_unidentified_faces(self, client_faces):
        resp = client_faces.get("/api/faces?unidentified=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1

    def test_list_faces_empty_catalog(self, client):
        resp = client.get("/api/faces")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_list_faces_pagination(self, client_faces):
        resp = client_faces.get("/api/faces?limit=2&offset=0")
        assert resp.status_code == 200
        assert resp.json()["count"] <= 2


# ---------------------------------------------------------------------------
# GET /api/faces/stats
# ---------------------------------------------------------------------------


class TestFaceStats:
    def test_face_stats_empty(self, client):
        resp = client.get("/api/faces/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_face_stats_with_data(self, client_faces):
        resp = client_faces.get("/api/faces/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# GET /api/faces/{face_id}/thumbnail
# ---------------------------------------------------------------------------


class TestFaceThumbnail:
    def test_face_thumbnail_not_found(self, client_faces):
        resp = client_faces.get("/api/faces/999/thumbnail")
        assert resp.status_code == 404

    def test_face_thumbnail_crop_fails(self, client_faces):
        with (
            patch(
                "godmode_media_library.face_detect.crop_face_thumbnail",
                return_value=None,
            ),
            patch(
                "godmode_media_library.web.routes.faces._thumb_cache_get",
                return_value=None,
            ),
        ):
            resp = client_faces.get("/api/faces/1/thumbnail")
        assert resp.status_code == 404

    def test_face_thumbnail_success(self, client_faces):
        fake_jpeg = b"\xff\xd8\xff\xe0" + b"\x00" * 100
        with (
            patch(
                "godmode_media_library.face_detect.crop_face_thumbnail",
                return_value=fake_jpeg,
            ),
            patch(
                "godmode_media_library.web.routes.faces._thumb_cache_get",
                return_value=None,
            ),
        ):
            resp = client_faces.get("/api/faces/1/thumbnail?size=100")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "image/jpeg"

    def test_face_thumbnail_cache_hit(self, client_faces):
        fake_jpeg = b"\xff\xd8\xff\xe0" + b"\x00" * 50
        with patch(
            "godmode_media_library.web.routes.faces._thumb_cache_get",
            return_value=fake_jpeg,
        ):
            resp = client_faces.get("/api/faces/1/thumbnail")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# PUT /api/faces/{face_id}/person — assign / unassign
# ---------------------------------------------------------------------------


class TestAssignFace:
    def test_assign_face_to_person(self, client_faces):
        resp = client_faces.put("/api/faces/4/person", json={"person_id": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["person_id"] == 1

    def test_unassign_face(self, client_faces):
        resp = client_faces.put("/api/faces/1/person", json={"person_id": None})
        assert resp.status_code == 200
        data = resp.json()
        assert data["person_id"] is None

    def test_assign_face_not_found(self, client_faces):
        resp = client_faces.put("/api/faces/999/person", json={"person_id": 1})
        assert resp.status_code == 404

    def test_assign_face_to_nonexistent_person(self, client_faces):
        resp = client_faces.put("/api/faces/4/person", json={"person_id": 999})
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/faces/by-file
# ---------------------------------------------------------------------------


class TestGetFileFaces:
    def test_get_file_faces(self, client_faces):
        resp = client_faces.get("/api/faces/by-file?path=/photos/face1.jpg")
        assert resp.status_code == 200
        data = resp.json()
        assert data["file_path"] == "/photos/face1.jpg"
        assert len(data["faces"]) >= 1

    def test_get_file_faces_not_found(self, client_faces):
        resp = client_faces.get("/api/faces/by-file?path=/nonexistent.jpg")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/faces/detect — background task
# ---------------------------------------------------------------------------


class TestFaceDetect:
    def test_detect_starts_task(self, client_faces):
        resp = client_faces.post("/api/faces/detect", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_detect_custom_params(self, client_faces):
        resp = client_faces.post("/api/faces/detect", json={"model": "cnn", "max_dimension": 800})
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ---------------------------------------------------------------------------
# POST /api/faces/cluster — background task
# ---------------------------------------------------------------------------


class TestFaceCluster:
    def test_cluster_starts_task(self, client_faces):
        resp = client_faces.post("/api/faces/cluster", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert "task_id" in data
        assert data["status"] == "started"

    def test_cluster_custom_params(self, client_faces):
        resp = client_faces.post("/api/faces/cluster", json={"eps": 0.4, "min_samples": 3})
        assert resp.status_code == 200
        assert "task_id" in resp.json()


# ---------------------------------------------------------------------------
# POST /api/faces/privacy/consent
# ---------------------------------------------------------------------------


class TestPrivacyConsent:
    def test_record_consent(self, client):
        resp = client.post("/api/faces/privacy/consent")
        assert resp.status_code == 200
        data = resp.json()
        assert data["consent_given"] is True

    def test_get_privacy_status_no_consent(self, client):
        resp = client.get("/api/faces/privacy")
        assert resp.status_code == 200
        data = resp.json()
        assert data["consent_given"] is False

    def test_get_privacy_status_after_consent(self, client):
        client.post("/api/faces/privacy/consent")
        resp = client.get("/api/faces/privacy")
        assert resp.status_code == 200
        data = resp.json()
        assert data["consent_given"] is True
        assert data["consent_timestamp"] is not None


# ---------------------------------------------------------------------------
# DELETE /api/faces/privacy/encodings
# ---------------------------------------------------------------------------


class TestWipeEncodings:
    def test_wipe_encodings(self, client):
        resp = client.delete("/api/faces/privacy/encodings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "encodings_wiped" in data


# ---------------------------------------------------------------------------
# POST /api/persons/create
# ---------------------------------------------------------------------------


class TestCreatePerson:
    def test_create_person(self, client):
        resp = client.post("/api/persons/create", json={"name": "Charlie"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["name"] == "Charlie"
        assert "person_id" in data

    def test_create_person_duplicate_name(self, client):
        """upsert_person should return existing person for same name."""
        resp1 = client.post("/api/persons/create", json={"name": "Diana"})
        resp2 = client.post("/api/persons/create", json={"name": "Diana"})
        assert resp1.status_code == 200
        assert resp2.status_code == 200
        assert resp1.json()["person_id"] == resp2.json()["person_id"]


# ---------------------------------------------------------------------------
# POST /api/persons/cleanup
# ---------------------------------------------------------------------------


class TestCleanupAutoPersons:
    def test_cleanup_no_auto_persons(self, client):
        resp = client.post("/api/persons/cleanup")
        assert resp.status_code == 200
        data = resp.json()
        assert data["persons_deleted"] == 0
        assert data["faces_freed"] == 0

    def test_cleanup_removes_auto_persons(self, client_faces):
        """Person_001 should be cleaned up, Alice and Bob preserved."""
        resp = client_faces.post("/api/persons/cleanup")
        assert resp.status_code == 200
        data = resp.json()
        assert data["persons_deleted"] == 1  # Person_001

        # Verify Alice and Bob still exist
        resp2 = client_faces.get("/api/persons")
        names = {p["name"] for p in resp2.json()["persons"]}
        assert "Alice" in names
        assert "Bob" in names
        assert "Person_001" not in names
