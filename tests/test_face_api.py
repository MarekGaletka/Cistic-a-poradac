"""Tests for face/person REST API endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from godmode_media_library.catalog import Catalog, CatalogFileRow
from godmode_media_library.web.app import create_app


@pytest.fixture
def catalog_path(tmp_path):
    db = tmp_path / "test.db"
    cat = Catalog(db)
    cat.open()
    # Insert a sample file
    photo_path = str(tmp_path / "photo.jpg")
    row = CatalogFileRow(
        id=None,
        path=photo_path,
        size=5000,
        mtime=1.0,
        ctime=1.0,
        birthtime=1.0,
        ext="jpg",
        sha256="abc",
        inode=None,
        device=None,
        nlink=1,
        asset_key=None,
        asset_component=False,
        xattr_count=0,
        first_seen="2024-01-01",
        last_scanned="2024-01-01",
    )
    file_id = cat.upsert_file(row)
    # Insert faces and a person
    pid = cat.upsert_person("Alice")
    f1 = cat.insert_face(file_id=file_id, face_index=0, bbox=(10, 90, 90, 10))
    cat.assign_face_to_person(f1, pid)
    cat.insert_face(file_id=file_id, face_index=1, bbox=(20, 80, 80, 20))
    cat.commit()
    cat.close()
    return db


@pytest.fixture
def client(catalog_path):
    app = create_app(catalog_path=catalog_path)
    return TestClient(app)


def test_list_persons(client):
    resp = client.get("/api/persons")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 1
    assert any(p["name"] == "Alice" for p in data["persons"])


def test_get_person(client):
    # First get list to find Alice's id
    persons = client.get("/api/persons").json()["persons"]
    alice = next(p for p in persons if p["name"] == "Alice")
    resp = client.get(f"/api/persons/{alice['id']}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Alice"


def test_get_person_not_found(client):
    resp = client.get("/api/persons/9999")
    assert resp.status_code == 404


def test_rename_person(client):
    persons = client.get("/api/persons").json()["persons"]
    alice = next(p for p in persons if p["name"] == "Alice")
    resp = client.put(f"/api/persons/{alice['id']}/name", json={"name": "Alice Nováková"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Alice Nováková"


def test_create_person(client):
    resp = client.post("/api/persons/create", json={"name": "Bob"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Bob"
    assert "person_id" in resp.json()


def test_delete_person(client):
    # Create then delete
    resp = client.post("/api/persons/create", json={"name": "ToDelete"})
    pid = resp.json()["person_id"]
    resp = client.delete(f"/api/persons/{pid}")
    assert resp.status_code == 200
    # Verify gone
    resp = client.get(f"/api/persons/{pid}")
    assert resp.status_code == 404


def test_list_faces(client):
    resp = client.get("/api/faces")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 2


def test_list_unidentified_faces(client):
    resp = client.get("/api/faces?unidentified=true")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 1  # face_index=1 is unidentified


def test_face_stats(client):
    resp = client.get("/api/faces/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_faces" in data
    assert "total_persons" in data
    assert data["total_faces"] >= 2


def test_get_file_faces(client, catalog_path):
    # Get the photo path from catalog
    cat = Catalog(catalog_path)
    cat.open()
    files = cat.query_files(limit=1)
    cat.close()
    path = files[0].path
    resp = client.get(f"/api/faces/by-file?path={path}")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["faces"]) >= 2


def test_assign_face_to_person(client):
    # Create a new person
    resp = client.post("/api/persons/create", json={"name": "Charlie"})
    charlie_id = resp.json()["person_id"]

    # Get unidentified faces
    faces = client.get("/api/faces?unidentified=true").json()["faces"]
    if faces:
        face_id = faces[0]["id"]
        resp = client.put(f"/api/faces/{face_id}/person", json={"person_id": charlie_id})
        assert resp.status_code == 200


def test_merge_persons(client):
    # Create two persons and merge
    r1 = client.post("/api/persons/create", json={"name": "MergeA"})
    r2 = client.post("/api/persons/create", json={"name": "MergeB"})
    pid1 = r1.json()["person_id"]
    pid2 = r2.json()["person_id"]

    resp = client.post(f"/api/persons/{pid1}/merge", json={"merge_ids": [pid2]})
    assert resp.status_code == 200


def test_privacy_consent(client):
    # Check initial state
    resp = client.get("/api/faces/privacy")
    assert resp.status_code == 200
    assert resp.json()["consent_given"] is False

    # Give consent
    resp = client.post("/api/faces/privacy/consent")
    assert resp.status_code == 200

    # Verify
    resp = client.get("/api/faces/privacy")
    assert resp.json()["consent_given"] is True


def test_wipe_encodings(client):
    resp = client.delete("/api/faces/privacy/encodings")
    assert resp.status_code == 200
    assert "encodings_wiped" in resp.json()


def test_person_faces(client):
    persons = client.get("/api/persons").json()["persons"]
    if persons:
        pid = persons[0]["id"]
        resp = client.get(f"/api/persons/{pid}/faces")
        assert resp.status_code == 200
        assert "faces" in resp.json()
