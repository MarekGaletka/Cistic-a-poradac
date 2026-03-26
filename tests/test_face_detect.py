"""Tests for face detection, catalog face/person operations, and face_crypto."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.catalog import Catalog


@pytest.fixture
def catalog(tmp_path):
    db = tmp_path / "test.db"
    cat = Catalog(db)
    cat.open()
    yield cat
    cat.close()


@pytest.fixture
def sample_file(catalog):
    """Insert a sample file and return its id."""
    from godmode_media_library.catalog import CatalogFileRow

    row = CatalogFileRow(
        id=None,
        path="/tmp/test_photo.jpg",
        size=1000,
        mtime=1.0,
        ctime=1.0,
        birthtime=1.0,
        ext="jpg",
        sha256="abc123",
        inode=None,
        device=None,
        nlink=1,
        asset_key=None,
        asset_component=False,
        xattr_count=0,
        first_seen="2024-01-01",
        last_scanned="2024-01-01",
    )
    return catalog.upsert_file(row)


# ── Schema v6 tables exist ──


def test_faces_table_exists(catalog):
    cur = catalog.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='faces'")
    assert cur.fetchone() is not None


def test_persons_table_exists(catalog):
    cur = catalog.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='persons'")
    assert cur.fetchone() is not None


def test_face_privacy_table_exists(catalog):
    cur = catalog.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='face_privacy'")
    assert cur.fetchone() is not None


# ── Face CRUD ──


def test_insert_and_get_face(catalog, sample_file):
    face_id = catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(10, 100, 110, 10),
        encoding_blob=b"test_encoding",
        cluster_id=0,
        confidence=0.95,
    )
    assert face_id > 0

    faces = catalog.get_faces_for_file(sample_file)
    assert len(faces) == 1
    assert faces[0]["id"] == face_id
    assert faces[0]["bbox"]["top"] == 10
    assert faces[0]["bbox"]["right"] == 100
    assert faces[0]["confidence"] == 0.95


def test_get_face_by_id(catalog, sample_file):
    face_id = catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(10, 100, 110, 10),
    )
    face = catalog.get_face_by_id(face_id)
    assert face is not None
    assert face["path"] == "/tmp/test_photo.jpg"


def test_get_face_by_id_not_found(catalog):
    assert catalog.get_face_by_id(9999) is None


def test_files_without_faces(catalog, sample_file):
    pending = catalog.files_without_faces()
    assert len(pending) == 1
    assert pending[0] == (sample_file, "/tmp/test_photo.jpg")

    # After inserting a face, file should no longer be pending
    catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0))
    pending2 = catalog.files_without_faces()
    assert len(pending2) == 0


def test_get_all_encodings(catalog, sample_file):
    catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(0, 0, 0, 0),
        encoding_blob=b"enc1",
    )
    catalog.insert_face(
        file_id=sample_file,
        face_index=1,
        bbox=(0, 0, 0, 0),
        encoding_blob=None,
    )
    encs = catalog.get_all_encodings()
    assert len(encs) == 1
    assert encs[0][1] == b"enc1"


def test_unidentified_faces(catalog, sample_file):
    catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0))
    unid = catalog.get_unidentified_faces()
    assert len(unid) == 1
    # Unidentified faces have no person_id in the returned dict (they have cluster_id instead)


# ── Person CRUD ──


def test_create_and_get_person(catalog):
    pid = catalog.upsert_person("Jan Novák")
    assert pid > 0

    person = catalog.get_person(pid)
    assert person["name"] == "Jan Novák"
    assert person["face_count"] == 0


def test_get_all_persons(catalog):
    catalog.upsert_person("Alice")
    catalog.upsert_person("Bob")
    persons = catalog.get_all_persons()
    assert len(persons) == 2


def test_update_person_name(catalog):
    pid = catalog.upsert_person("Unknown")
    catalog.update_person_name(pid, "Jan Novák")
    catalog.commit()

    person = catalog.get_person(pid)
    assert person["name"] == "Jan Novák"


def test_assign_face_to_person(catalog, sample_file):
    pid = catalog.upsert_person("Alice")
    fid = catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0))
    catalog.assign_face_to_person(fid, pid)
    catalog.commit()

    person = catalog.get_person(pid)
    assert person["face_count"] == 1

    faces = catalog.get_faces_for_person(pid)
    assert len(faces) == 1


def test_merge_persons(catalog, sample_file):
    p1 = catalog.upsert_person("Alice")
    p2 = catalog.upsert_person("Alice duplicate")

    f1 = catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0))
    f2 = catalog.insert_face(file_id=sample_file, face_index=1, bbox=(0, 0, 0, 0))
    catalog.assign_face_to_person(f1, p1)
    catalog.assign_face_to_person(f2, p2)

    reassigned = catalog.merge_persons(p1, [p2])
    assert reassigned == 1

    # p2 should be deleted
    assert catalog.get_person(p2) is None
    # p1 should have 2 faces
    assert catalog.get_person(p1)["face_count"] == 2


def test_delete_person(catalog, sample_file):
    pid = catalog.upsert_person("Alice")
    fid = catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0))
    catalog.assign_face_to_person(fid, pid)
    catalog.delete_person(pid)
    catalog.commit()

    assert catalog.get_person(pid) is None
    # Face should still exist but unidentified
    face = catalog.get_face_by_id(fid)
    assert face is not None
    assert face["person_id"] is None


# ── Privacy ──


def test_privacy_flags(catalog):
    assert catalog.get_privacy_flag("consent_given") is None
    catalog.set_privacy_flag("consent_given", "2024-01-01T00:00:00Z")
    assert catalog.get_privacy_flag("consent_given") == "2024-01-01T00:00:00Z"


def test_wipe_encodings(catalog, sample_file):
    catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(0, 0, 0, 0),
        encoding_blob=b"secret_biometric_data",
    )
    wiped = catalog.wipe_face_encodings()
    assert wiped == 1
    encs = catalog.get_all_encodings()
    assert len(encs) == 0


# ── Face stats ──


def test_face_stats(catalog, sample_file):
    pid = catalog.upsert_person("Alice")
    f1 = catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0))
    catalog.insert_face(file_id=sample_file, face_index=1, bbox=(0, 0, 0, 0))
    catalog.assign_face_to_person(f1, pid)

    stats = catalog.face_stats()
    assert stats["total_faces"] == 2
    assert stats["total_persons"] == 1
    assert stats["identified_faces"] == 1
    assert stats["unidentified_faces"] == 1


def test_stats_includes_face_data(catalog):
    s = catalog.stats()
    assert "total_faces" in s
    assert "total_persons" in s


# ── face_crypto ──


def test_encrypt_decrypt_noop():
    from godmode_media_library.face_crypto import decrypt_encoding_noop, encrypt_encoding_noop

    floats = [float(i) for i in range(128)]
    blob = encrypt_encoding_noop(floats)
    assert len(blob) == 128 * 8  # 128 doubles
    result = decrypt_encoding_noop(blob)
    assert len(result) == 128
    for a, b in zip(floats, result, strict=True):
        assert abs(a - b) < 1e-10


def test_encrypt_decrypt_with_cryptography(tmp_path):
    """Test actual Fernet encryption if cryptography is installed."""
    try:
        from cryptography.fernet import Fernet  # noqa: F401
    except ImportError:
        pytest.skip("cryptography not installed")

    from godmode_media_library.face_crypto import (
        decrypt_encoding,
        encrypt_encoding,
    )

    # Use a temporary key path
    with patch("godmode_media_library.face_crypto._KEY_PATH", tmp_path / "test_face.key"):
        floats = [float(i) * 0.1 for i in range(128)]
        blob = encrypt_encoding(floats)
        # Encrypted blob should be larger than raw (Fernet adds overhead)
        assert len(blob) > 128 * 8
        result = decrypt_encoding(blob)
        assert len(result) == 128
        for a, b in zip(floats, result, strict=True):
            assert abs(a - b) < 1e-10


def test_get_encrypt_fn():
    from godmode_media_library.face_crypto import (
        encrypt_encoding,
        encrypt_encoding_noop,
        get_encrypt_fn,
    )

    assert get_encrypt_fn(enabled=False) is encrypt_encoding_noop
    assert get_encrypt_fn(enabled=True) is encrypt_encoding


# ── face_detect module ──


def test_crop_face_thumbnail_missing_file():
    from godmode_media_library.face_detect import crop_face_thumbnail

    result = crop_face_thumbnail(
        "/nonexistent/file.jpg",
        {"top": 0, "right": 100, "bottom": 100, "left": 0},
    )
    assert result is None


def test_scan_new_faces_empty_catalog(catalog):
    from godmode_media_library.face_detect import scan_new_faces

    # No image files in catalog
    result = scan_new_faces(catalog)
    assert result.files_processed == 0
    assert result.faces_detected == 0


def test_face_detect_integration(catalog, sample_file):
    """Test detect_faces_in_file with mocked face_recognition."""
    import numpy as np

    mock_fr = MagicMock()
    mock_fr.load_image_file.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_fr.face_locations.return_value = [(10, 90, 90, 10)]
    mock_fr.face_encodings.return_value = [np.random.rand(128)]

    with patch("godmode_media_library.face_detect._load_libs", return_value=(mock_fr, np, MagicMock())):
        from godmode_media_library.face_detect import detect_faces_in_file

        count = detect_faces_in_file(catalog, sample_file, "/tmp/test_photo.jpg")
        assert count == 1

    faces = catalog.get_faces_for_file(sample_file)
    assert len(faces) == 1
    assert faces[0]["bbox"]["top"] == 10


# ── Schema migration ──


def test_schema_v5_to_v6_migration(tmp_path):
    """Test that opening an old v5 database migrates to v6."""
    db = tmp_path / "old.db"
    # Create v5 database manually
    import sqlite3

    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO meta VALUES ('schema_version', '5')")
    conn.execute(
        "CREATE TABLE files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE, "
        "size INTEGER, mtime REAL, ctime REAL, birthtime REAL, ext TEXT DEFAULT '', "
        "sha256 TEXT, inode INTEGER, device INTEGER, nlink INTEGER DEFAULT 1, "
        "asset_key TEXT, asset_component INTEGER DEFAULT 0, xattr_count INTEGER DEFAULT 0, "
        "first_seen TEXT, last_scanned TEXT, duration_seconds REAL, width INTEGER, "
        "height INTEGER, video_codec TEXT, audio_codec TEXT, bitrate INTEGER, phash TEXT, "
        "date_original TEXT, camera_make TEXT, camera_model TEXT, gps_latitude REAL, "
        "gps_longitude REAL, metadata_richness REAL)"
    )
    conn.execute("CREATE TABLE labels (file_id INTEGER PRIMARY KEY, people TEXT DEFAULT '', place TEXT DEFAULT '', updated_at TEXT)")
    conn.execute("CREATE TABLE file_notes (file_id INTEGER PRIMARY KEY, note TEXT, updated_at TEXT)")
    conn.execute("CREATE TABLE file_ratings (file_id INTEGER PRIMARY KEY, rating INTEGER CHECK(rating >= 1 AND rating <= 5))")
    conn.execute("CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, color TEXT DEFAULT '#58a6ff')")
    conn.execute("CREATE TABLE file_tags (file_id INTEGER, tag_id INTEGER, PRIMARY KEY (file_id, tag_id))")
    conn.execute(
        "CREATE TABLE scans (id INTEGER PRIMARY KEY, root TEXT, started_at TEXT, "
        "finished_at TEXT, files_scanned INTEGER DEFAULT 0, files_new INTEGER DEFAULT 0, "
        "files_changed INTEGER DEFAULT 0, files_removed INTEGER DEFAULT 0)"
    )
    conn.execute("CREATE TABLE duplicates (group_id TEXT, file_id INTEGER, is_primary INTEGER DEFAULT 0)")
    conn.execute("CREATE TABLE file_metadata (file_id INTEGER PRIMARY KEY, raw_json TEXT, extracted_at TEXT)")
    conn.commit()
    conn.close()

    # Open with new Catalog — should migrate
    cat = Catalog(db)
    cat.open()

    # Verify v6+ tables exist
    tables = {r[0] for r in cat.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "persons" in tables
    assert "faces" in tables
    assert "face_privacy" in tables
    assert "shares" in tables  # v7

    # Verify version updated to latest
    ver = cat.conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
    assert ver == "10"

    cat.close()
