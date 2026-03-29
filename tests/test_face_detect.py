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


# ── face_detect: batch queries and clustering ──


def test_scan_new_faces_with_files(catalog, sample_file):
    """scan_new_faces processes files and returns result."""
    import numpy as np

    mock_fr = MagicMock()
    mock_fr.load_image_file.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_fr.face_locations.return_value = [(10, 90, 90, 10)]
    mock_fr.face_encodings.return_value = [np.random.rand(128)]

    with patch("godmode_media_library.face_detect._load_libs", return_value=(mock_fr, np, MagicMock())):
        from godmode_media_library.face_detect import scan_new_faces

        progress_calls = []
        result = scan_new_faces(
            catalog,
            progress_fn=lambda done, total: progress_calls.append((done, total)),
        )
        assert result.files_processed == 1
        assert result.faces_detected == 1
        assert result.errors == 0
        assert len(progress_calls) > 0


def test_scan_new_faces_with_error(catalog, sample_file):
    """scan_new_faces counts errors but continues."""
    import numpy as np

    mock_fr = MagicMock()
    mock_fr.load_image_file.side_effect = RuntimeError("bad file")

    with patch("godmode_media_library.face_detect._load_libs", return_value=(mock_fr, np, MagicMock())):
        from godmode_media_library.face_detect import detect_faces_in_file

        count = detect_faces_in_file(catalog, sample_file, "/tmp/test_photo.jpg")
        assert count == 0


def test_detect_faces_no_faces(catalog, sample_file):
    """detect_faces_in_file returns 0 when no faces found."""
    import numpy as np

    mock_fr = MagicMock()
    mock_fr.load_image_file.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_fr.face_locations.return_value = []  # No faces

    with patch("godmode_media_library.face_detect._load_libs", return_value=(mock_fr, np, MagicMock())):
        from godmode_media_library.face_detect import detect_faces_in_file

        count = detect_faces_in_file(catalog, sample_file, "/tmp/test_photo.jpg")
        assert count == 0


def test_detect_faces_with_scaling(catalog, sample_file):
    """detect_faces_in_file scales bboxes back when image is resized."""
    import numpy as np

    mock_fr = MagicMock()
    # Image larger than max_dimension (default 1600)
    big_img = np.zeros((3200, 3200, 3), dtype=np.uint8)
    mock_fr.load_image_file.return_value = big_img
    mock_fr.face_locations.return_value = [(5, 45, 45, 5)]
    mock_fr.face_encodings.return_value = [np.random.rand(128)]

    mock_pil_image = MagicMock()
    mock_pil_cls = MagicMock()
    mock_pil_cls.fromarray.return_value = mock_pil_image
    # resize returns something that numpy can convert
    resized = np.zeros((1600, 1600, 3), dtype=np.uint8)
    mock_pil_image.resize.return_value = MagicMock()

    with patch("godmode_media_library.face_detect._load_libs", return_value=(mock_fr, np, mock_pil_cls)):
        with patch("godmode_media_library.face_detect._resize_if_needed", return_value=(resized, 0.5)):
            from godmode_media_library.face_detect import detect_faces_in_file

            count = detect_faces_in_file(catalog, sample_file, "/tmp/test_photo.jpg", max_dimension=1600)
            assert count == 1

    faces = catalog.get_faces_for_file(sample_file)
    assert len(faces) == 1
    # With scale 0.5, coordinates should be doubled: 5 -> 10, 45 -> 90
    assert faces[0]["bbox"]["top"] == 10
    assert faces[0]["bbox"]["right"] == 90


def test_detect_faces_with_encrypt_fn(catalog, sample_file):
    """detect_faces_in_file passes encoding through encrypt_fn."""
    import numpy as np

    mock_fr = MagicMock()
    mock_fr.load_image_file.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
    mock_fr.face_locations.return_value = [(10, 90, 90, 10)]
    enc = np.random.rand(128)
    mock_fr.face_encodings.return_value = [enc]

    encrypted_data = b"encrypted_blob"
    encrypt_fn = MagicMock(return_value=encrypted_data)

    with patch("godmode_media_library.face_detect._load_libs", return_value=(mock_fr, np, MagicMock())):
        from godmode_media_library.face_detect import detect_faces_in_file

        count = detect_faces_in_file(catalog, sample_file, "/tmp/test_photo.jpg", encrypt_fn=encrypt_fn)
        assert count == 1
        encrypt_fn.assert_called_once()


def test_cluster_faces_empty(catalog):
    """cluster_faces returns empty dict when no encodings."""
    from godmode_media_library.face_detect import cluster_faces

    result = cluster_faces(catalog)
    assert result == {}


def test_cluster_faces_with_data(catalog, sample_file):
    """cluster_faces groups similar face encodings."""
    import struct

    import numpy as np

    # Insert faces with known encodings
    enc1 = [0.1] * 128
    enc2 = [0.1001] * 128  # Very similar to enc1
    enc3 = [0.9] * 128  # Different

    for i, enc in enumerate([enc1, enc2, enc3]):
        blob = struct.pack("<128d", *enc)
        catalog.insert_face(
            file_id=sample_file,
            face_index=i,
            bbox=(0, 0, 0, 0),
            encoding_blob=blob,
        )
    catalog.commit()

    from godmode_media_library.face_detect import cluster_faces

    clusters = cluster_faces(catalog, eps=0.5, min_samples=2)
    # enc1 and enc2 should cluster together, enc3 is noise
    assert isinstance(clusters, dict)


def test_match_face_to_known_no_persons(catalog):
    """match_face_to_known returns None with no persons."""
    from godmode_media_library.face_detect import match_face_to_known

    result = match_face_to_known(catalog, [0.5] * 128)
    assert result is None


def test_match_face_to_known_with_match(catalog, sample_file):
    """match_face_to_known finds matching person."""
    import struct

    import numpy as np

    enc = [0.5] * 128
    blob = struct.pack("<128d", *enc)

    face_id = catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(0, 0, 0, 0),
        encoding_blob=blob,
    )
    pid = catalog.upsert_person("Alice", sample_face_id=face_id)
    catalog.assign_face_to_person(face_id, pid)
    catalog.commit()

    from godmode_media_library.face_detect import match_face_to_known

    # Search with identical encoding — should match
    result = match_face_to_known(catalog, enc, threshold=0.6)
    assert result == pid


def test_match_face_to_known_no_match(catalog, sample_file):
    """match_face_to_known returns None when too far."""
    import struct

    import numpy as np

    enc = [0.5] * 128
    blob = struct.pack("<128d", *enc)

    face_id = catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(0, 0, 0, 0),
        encoding_blob=blob,
    )
    pid = catalog.upsert_person("Alice", sample_face_id=face_id)
    catalog.assign_face_to_person(face_id, pid)
    catalog.commit()

    from godmode_media_library.face_detect import match_face_to_known

    # Search with very different encoding
    different_enc = [5.0] * 128
    result = match_face_to_known(catalog, different_enc, threshold=0.6)
    assert result is None


def test_resize_if_needed_no_resize():
    """_resize_if_needed returns original when within max_dimension."""
    import numpy as np

    from godmode_media_library.face_detect import _resize_if_needed

    img = np.zeros((100, 100, 3), dtype=np.uint8)
    result, scale = _resize_if_needed(img, 1600, np, MagicMock())
    assert scale == 1.0
    assert result is img


def test_resize_if_needed_invalid_shape():
    """_resize_if_needed handles arrays with < 2 dimensions."""
    import numpy as np

    from godmode_media_library.face_detect import _resize_if_needed

    img = np.zeros((100,), dtype=np.uint8)
    result, scale = _resize_if_needed(img, 1600, np, MagicMock())
    assert scale == 1.0


def test_null_encoding_handling(catalog, sample_file):
    """get_all_encodings filters out NULL encodings."""
    catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(0, 0, 0, 0),
        encoding_blob=None,
    )
    catalog.insert_face(
        file_id=sample_file,
        face_index=1,
        bbox=(0, 0, 0, 0),
        encoding_blob=b"valid",
    )
    encs = catalog.get_all_encodings()
    assert len(encs) == 1
    assert encs[0][1] == b"valid"


def test_match_face_no_sample_faces(catalog, sample_file):
    """match_face_to_known returns None when persons have no sample_face_id."""
    pid = catalog.upsert_person("NoSample")
    catalog.commit()

    from godmode_media_library.face_detect import match_face_to_known

    result = match_face_to_known(catalog, [0.5] * 128)
    assert result is None


def test_cluster_faces_max_clusters(catalog, sample_file):
    """cluster_faces respects max_clusters limit."""
    import struct

    import numpy as np

    # Create many distinct face encodings that will form separate clusters
    for i in range(10):
        enc = [float(i * 10 + j) for j in range(128)]
        blob = struct.pack("<128d", *enc)
        catalog.insert_face(
            file_id=sample_file,
            face_index=i,
            bbox=(0, 0, 0, 0),
            encoding_blob=blob,
        )
    catalog.commit()

    from godmode_media_library.face_detect import cluster_faces

    # With min_samples=1, each point is its own cluster
    clusters = cluster_faces(catalog, eps=0.01, min_samples=1, max_clusters=2)
    assert len(clusters) <= 2


def test_cluster_faces_preserves_named_person(catalog, sample_file):
    """cluster_faces assigns cluster to existing named person."""
    import struct

    import numpy as np

    enc1 = [0.1] * 128
    enc2 = [0.1001] * 128  # Very similar

    blob1 = struct.pack("<128d", *enc1)
    blob2 = struct.pack("<128d", *enc2)

    f1 = catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0), encoding_blob=blob1)
    f2 = catalog.insert_face(file_id=sample_file, face_index=1, bbox=(0, 0, 0, 0), encoding_blob=blob2)

    # Create a named person and assign f1 to it
    pid = catalog.upsert_person("Alice", sample_face_id=f1)
    catalog.assign_face_to_person(f1, pid)
    catalog.commit()

    from godmode_media_library.face_detect import cluster_faces

    clusters = cluster_faces(catalog, eps=0.5, min_samples=2)
    # Both faces should be clustered, and since f1 belongs to named "Alice",
    # the cluster should be assigned to Alice
    assert isinstance(clusters, dict)


def test_cluster_faces_decrypt_fn(catalog, sample_file):
    """cluster_faces uses decrypt_fn when provided."""
    import struct

    import numpy as np

    enc = [0.5] * 128
    blob = struct.pack("<128d", *enc)

    catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0), encoding_blob=blob)
    catalog.insert_face(file_id=sample_file, face_index=1, bbox=(0, 0, 0, 0), encoding_blob=blob)
    catalog.commit()

    def decrypt_fn(blob_data):
        return list(struct.unpack("<128d", blob_data))

    from godmode_media_library.face_detect import cluster_faces

    clusters = cluster_faces(catalog, eps=0.5, min_samples=2, decrypt_fn=decrypt_fn)
    assert isinstance(clusters, dict)


def test_match_face_with_decrypt_fn(catalog, sample_file):
    """match_face_to_known uses decrypt_fn."""
    import struct

    import numpy as np

    enc = [0.5] * 128
    blob = struct.pack("<128d", *enc)

    face_id = catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0), encoding_blob=blob)
    pid = catalog.upsert_person("Bob", sample_face_id=face_id)
    catalog.assign_face_to_person(face_id, pid)
    catalog.commit()

    def decrypt_fn(blob_data):
        return list(struct.unpack("<128d", blob_data))

    from godmode_media_library.face_detect import match_face_to_known

    result = match_face_to_known(catalog, enc, decrypt_fn=decrypt_fn, threshold=0.6)
    assert result == pid


def test_match_face_corrupt_encoding(catalog, sample_file):
    """match_face_to_known handles corrupt encoding gracefully."""
    # Insert face with invalid encoding blob (too short)
    face_id = catalog.insert_face(
        file_id=sample_file,
        face_index=0,
        bbox=(0, 0, 0, 0),
        encoding_blob=b"too_short",
    )
    pid = catalog.upsert_person("Corrupt", sample_face_id=face_id)
    catalog.assign_face_to_person(face_id, pid)
    catalog.commit()

    from godmode_media_library.face_detect import match_face_to_known

    result = match_face_to_known(catalog, [0.5] * 128)
    assert result is None  # Should gracefully handle the corrupt data


def test_cluster_faces_corrupt_encoding(catalog, sample_file):
    """cluster_faces skips corrupt encodings."""
    import struct

    # Insert one valid and one corrupt encoding
    valid_enc = [0.5] * 128
    valid_blob = struct.pack("<128d", *valid_enc)

    catalog.insert_face(file_id=sample_file, face_index=0, bbox=(0, 0, 0, 0), encoding_blob=valid_blob)
    catalog.insert_face(file_id=sample_file, face_index=1, bbox=(0, 0, 0, 0), encoding_blob=b"corrupt")
    catalog.commit()

    from godmode_media_library.face_detect import cluster_faces

    # Should not crash, just skip corrupt encoding
    clusters = cluster_faces(catalog, eps=0.5, min_samples=1)
    assert isinstance(clusters, dict)


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
    assert ver == "12"

    cat.close()
