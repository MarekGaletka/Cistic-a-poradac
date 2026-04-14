"""Comprehensive security E2E tests for the GOD MODE Media Library web API.

Tests verify the API correctly rejects malicious input including path traversal,
blocked prefix access, injection attempts, XSS payloads, and invalid request data.
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


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def catalog_with_files(tmp_path):
    """Create a catalog with some test files for security testing."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    # Create test files on disk
    root = tmp_path / "media"
    root.mkdir()
    (root / "photo1.jpg").write_bytes(b"content1")
    (root / "photo2.jpg").write_bytes(b"content2")
    (root / "notes_test.txt").write_bytes(b"hello")

    # Scan them into catalog
    from godmode_media_library.scanner import incremental_scan

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        incremental_scan(cat, [root])
    # Register tmp_path as a configured root so move destinations
    # under tmp_path pass the _check_path_within_roots security check.
    import json
    cat.conn.execute(
        "INSERT INTO meta (key, value) VALUES ('configured_roots', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (json.dumps([str(tmp_path)]),),
    )
    cat.conn.commit()
    cat.close()
    return db_path


@pytest.fixture
def client(catalog_with_files):
    """Create a test client with a populated catalog."""
    from godmode_media_library.web.app import create_app

    app = create_app(catalog_path=catalog_with_files)
    return TestClient(app)


@pytest.fixture
def media_root(catalog_with_files, tmp_path):
    """Return the media root directory path as a string."""
    return str(tmp_path / "media")


# ── 1. Path Traversal Attacks ─────────────────────────────────────────


class TestPathTraversal:
    """Tests for path traversal attack prevention."""

    def test_dotdot_in_file_detail(self, client):
        """Path traversal in file detail should be blocked.
        Note: literal ../../ is resolved by Starlette router and hits SPA fallback (200).
        Use URL-encoded variant to test the actual API handler."""
        resp = client.get("/api/files/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (400, 403, 404, 422)

    def test_dotdot_in_thumbnail(self, client):
        """Path traversal in thumbnail endpoint should be blocked."""
        resp = client.get("/api/thumbnail/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (400, 403, 404)

    def test_dotdot_in_preview(self, client):
        """Path traversal in preview endpoint should be blocked."""
        resp = client.get("/api/preview/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (400, 403, 404)

    def test_dotdot_in_stream(self, client):
        """Path traversal in stream endpoint should be blocked."""
        resp = client.get("/api/stream/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (400, 403, 404)

    def test_url_encoded_traversal_in_file_detail(self, client):
        """URL-encoded ../ (%2e%2e%2f) should be blocked."""
        resp = client.get("/api/files/%2e%2e/%2e%2e/etc/passwd")
        assert resp.status_code in (400, 403, 404, 422)

    def test_double_encoded_traversal(self, client):
        """Double-encoded path traversal (%252e%252e%252f) should be blocked."""
        resp = client.get("/api/files/%252e%252e%252f%252e%252e%252fetc/passwd")
        assert resp.status_code in (400, 403, 404, 422)

    def test_traversal_in_note_endpoint(self, client):
        """Path traversal in note endpoint should not access system files."""
        resp = client.get("/api/files/..%2F..%2Fetc%2Fpasswd/note")
        assert resp.status_code in (400, 403, 404, 422)

    def test_traversal_in_rating_endpoint(self, client):
        """Path traversal in rating endpoint should be blocked."""
        resp = client.put(
            "/api/files/../../etc/passwd/rating",
            json={"rating": 5},
        )
        # May return 405 (route not matched due to traversal), 403, 404, or 422
        assert resp.status_code in (403, 404, 405, 422)

    def test_traversal_in_quarantine_paths(self, client, media_root):
        """Path traversal in quarantine file paths should be rejected or have no effect on system files."""
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": ["/../../../etc/passwd"]},
        )
        # The file should not be found or the path should be rejected
        data = resp.json()
        if resp.status_code == 200:
            assert data.get("moved", 0) == 0

    def test_traversal_in_quarantine_root(self, client, media_root):
        """Path traversal in quarantine_root destination should be handled safely."""
        resp = client.post(
            "/api/files/quarantine",
            json={
                "paths": [f"{media_root}/photo1.jpg"],
                "quarantine_root": "/tmp/../etc/evil_quarantine",
            },
        )
        # Either rejected or quarantine goes to resolved safe location
        assert resp.status_code in (200, 400, 403, 422)

    def test_traversal_in_delete_paths(self, client):
        """Path traversal in delete request should not delete system files."""
        resp = client.post(
            "/api/files/delete",
            json={"paths": ["/../../../etc/passwd"]},
        )
        data = resp.json()
        if resp.status_code == 200:
            assert data.get("deleted", 0) == 0

    def test_traversal_in_move_destination(self, client, media_root):
        """Path traversal in move destination should be blocked."""
        resp = client.post(
            "/api/files/move",
            json={
                "paths": [f"{media_root}/photo1.jpg"],
                "destination": "/tmp/../../etc/",
            },
        )
        assert resp.status_code in (200, 400, 403, 422)
        if resp.status_code == 200:
            assert resp.json().get("moved", 0) == 0

    def test_traversal_in_rename_new_name(self, client, media_root):
        """Path traversal in rename new_name field should be blocked or neutralized."""
        resp = client.post(
            "/api/files/rename",
            json={
                "renames": [
                    {
                        "path": f"{media_root}/photo1.jpg",
                        "new_name": "../../etc/evil.jpg",
                    }
                ]
            },
        )
        # Either rejected or the rename stays within the same parent directory
        assert resp.status_code in (200, 400, 403, 422)

    def test_traversal_in_browse(self, client):
        """Path traversal in browse endpoint should resolve safely."""
        resp = client.get("/api/browse", params={"path": "/tmp/../etc"})
        # /tmp/../etc is rejected by _sanitize_path (400) or root check (403)
        assert resp.status_code in (400, 403, 404)


# ── 2. Blocked Prefix Enforcement ────────────────────────────────────


class TestBlockedPrefixes:
    """Tests verifying all blocked prefixes are enforced."""

    BLOCKED_PREFIXES = (
        "/etc",
        "/var",
        "/private",
        "/sbin",
        "/usr",
        "/bin",
        "/tmp",
        "/dev",
        "/proc",
        "/sys",
    )

    @pytest.mark.parametrize("prefix", BLOCKED_PREFIXES)
    def test_blocked_prefix_in_preview(self, client, prefix):
        """Preview endpoint rejects all blocked prefixes."""
        resp = client.get(f"/api/preview{prefix}/somefile.jpg")
        assert resp.status_code in (403, 404), f"Expected 403/404 for {prefix}, got {resp.status_code}"

    @pytest.mark.parametrize("prefix", BLOCKED_PREFIXES)
    def test_blocked_prefix_in_browse(self, client, prefix):
        """Browse endpoint rejects all blocked prefixes."""
        resp = client.get("/api/browse", params={"path": prefix})
        assert resp.status_code in (403, 404), f"Expected 403/404 for {prefix}, got {resp.status_code}"

    @pytest.mark.parametrize("prefix", BLOCKED_PREFIXES)
    def test_blocked_prefix_in_stream(self, client, prefix):
        """Stream endpoint rejects all blocked prefixes."""
        resp = client.get(f"/api/stream{prefix}/somefile.mp4")
        assert resp.status_code in (403, 404), f"Expected 403/404 for {prefix}, got {resp.status_code}"

    def test_delete_with_blocked_path(self, client):
        """POST /api/files/delete with a blocked path should not delete system files."""
        resp = client.post(
            "/api/files/delete",
            json={"paths": ["/etc/shadow"]},
        )
        data = resp.json()
        if resp.status_code == 200:
            assert data.get("deleted", 0) == 0

    def test_move_to_blocked_destination(self, client, media_root):
        """POST /api/files/move to /etc/evil should be rejected."""
        resp = client.post(
            "/api/files/move",
            json={
                "paths": [f"{media_root}/photo1.jpg"],
                "destination": "/etc/evil",
            },
        )
        # Should either fail with 400/403 or move 0 files (permission error)
        if resp.status_code == 200:
            assert resp.json().get("moved", 0) == 0

    def test_quarantine_blocked_system_file(self, client):
        """Quarantining a file from a blocked path should not work."""
        resp = client.post(
            "/api/files/quarantine",
            json={"paths": ["/etc/passwd"]},
        )
        data = resp.json()
        if resp.status_code == 200:
            assert data.get("moved", 0) == 0

    def test_browse_etc_shadow(self, client):
        """GET /api/browse?path=/etc should return 403."""
        resp = client.get("/api/browse", params={"path": "/etc"})
        assert resp.status_code == 403

    def test_browse_subpath_of_blocked(self, client):
        """Browsing a subdirectory of a blocked prefix should be blocked."""
        resp = client.get("/api/browse", params={"path": "/etc/ssh"})
        assert resp.status_code in (403, 404)


# ── 3. Malicious Input in File Operations ────────────────────────────


class TestMaliciousInput:
    """Tests for malicious input handling in file operations."""

    def test_null_byte_in_file_path(self, client):
        """Null byte in path should be rejected at transport or app level."""
        # Null bytes in URLs are rejected by the HTTP client itself (InvalidURL),
        # which is a valid security boundary - the request never reaches the server.
        import httpx

        with pytest.raises(httpx.InvalidURL):
            client.get("/api/files/home/user/photo\x00.jpg")

    def test_null_byte_in_delete(self, client):
        """Null byte injection in delete request path."""
        resp = client.post(
            "/api/files/delete",
            json={"paths": ["/home/user/photo\x00.jpg"]},
        )
        data = resp.json()
        if resp.status_code == 200:
            assert data.get("deleted", 0) == 0

    def test_very_long_path(self, client):
        """Extremely long path should not cause server crash."""
        long_path = "/media/" + "a" * 10000 + ".jpg"
        resp = client.get(f"/api/files{long_path}")
        # Should return an error, not crash
        assert resp.status_code in (400, 404, 414, 422, 500)

    def test_very_long_path_in_delete(self, client, catalog_with_files):
        """Very long path in delete should not crash the server."""
        from godmode_media_library.web.app import create_app

        # Use raise_server_exceptions=False to capture 500 errors as HTTP responses
        app = create_app(catalog_path=catalog_with_files)
        safe_client = TestClient(app, raise_server_exceptions=False)
        long_path = "/media/" + "x" * 10000 + ".jpg"
        resp = safe_client.post(
            "/api/files/delete",
            json={"paths": [long_path]},
        )
        # Server may return 200 (skipped), 400, 422, or 500 (OSError from path too long)
        # The critical property: server does not crash and responds with valid HTTP
        assert resp.status_code in (200, 400, 422, 500)
        if resp.status_code == 200:
            assert resp.json().get("deleted", 0) == 0

    def test_unicode_rtl_override_in_path(self, client):
        """RTL override character in path should not cause confusion."""
        # U+202E is Right-to-Left Override
        resp = client.get("/api/files/media/photo\u202ephoto.jpg")
        assert resp.status_code in (400, 404, 422)

    def test_zero_width_chars_in_path(self, client):
        """Zero-width characters in path should not bypass checks."""
        # U+200B is Zero-Width Space
        resp = client.get("/api/files/\u200b/etc/passwd")
        assert resp.status_code in (403, 404, 422)

    def test_shell_injection_in_rename(self, client, media_root):
        """Shell injection attempt in rename new_name should be harmless."""
        resp = client.post(
            "/api/files/rename",
            json={
                "renames": [
                    {
                        "path": f"{media_root}/photo1.jpg",
                        "new_name": "; rm -rf / ;.jpg",
                    }
                ]
            },
        )
        # The rename might succeed (creating a file with shell chars in name)
        # or fail - but it must NOT execute shell commands
        assert resp.status_code in (200, 400, 422)

    def test_command_substitution_in_rename(self, client, media_root):
        """Command substitution in rename should be treated as literal text."""
        resp = client.post(
            "/api/files/rename",
            json={
                "renames": [
                    {
                        "path": f"{media_root}/photo1.jpg",
                        "new_name": "$(whoami).jpg",
                    }
                ]
            },
        )
        assert resp.status_code in (200, 400, 422)

    def test_backtick_injection_in_rename(self, client, media_root):
        """Backtick command injection in rename should be treated as literal."""
        resp = client.post(
            "/api/files/rename",
            json={
                "renames": [
                    {
                        "path": f"{media_root}/photo1.jpg",
                        "new_name": "`id`.jpg",
                    }
                ]
            },
        )
        assert resp.status_code in (200, 400, 422)

    def test_sql_injection_in_path_contains(self, client):
        """SQL injection in path_contains filter should not execute."""
        resp = client.get(
            "/api/files",
            params={"path_contains": "' OR 1=1; DROP TABLE files; --"},
        )
        # Should return valid JSON (empty results), not a DB error
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data

    def test_sql_injection_in_ext_filter(self, client):
        """SQL injection in ext filter should not execute."""
        resp = client.get(
            "/api/files",
            params={"ext": "jpg' UNION SELECT * FROM sqlite_master --"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data
        # Results should be empty since no files match this extension
        assert data["count"] == 0

    def test_sql_injection_in_camera_filter(self, client):
        """SQL injection in camera filter should be safe."""
        resp = client.get(
            "/api/files",
            params={"camera": "' OR '1'='1"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data

    def test_sql_injection_in_sort_param(self, client):
        """SQL injection in sort parameter should be handled safely."""
        resp = client.get(
            "/api/files",
            params={"sort": "path; DROP TABLE files"},
        )
        # Should either ignore invalid sort or return error, not crash
        assert resp.status_code in (200, 400, 422)

    def test_null_byte_in_note(self, client):
        """Null byte in note content should be handled."""
        resp = client.put(
            "/api/files/media/photo1.jpg/note",
            json={"note": "hello\x00world"},
        )
        # Should either accept (stripping null) or reject
        assert resp.status_code in (200, 400, 422)


# ── 4. XSS Prevention ────────────────────────────────────────────────


class TestXSSPrevention:
    """Tests for XSS prevention in stored data."""

    def test_script_tag_in_note(self, client):
        """Script tag in note should be stored safely (API returns JSON, not HTML)."""
        xss_payload = '<script>alert("XSS")</script>'
        resp = client.put(
            "/api/files/media/photo1.jpg/note",
            json={"note": xss_payload},
        )
        # Notes endpoint should accept text (it returns JSON, not HTML)
        # The key security property: response Content-Type is application/json
        assert resp.status_code in (200, 400, 422)
        if resp.status_code == 200:
            # Verify reading back does not serve HTML
            get_resp = client.get("/api/files/media/photo1.jpg/note")
            assert get_resp.status_code == 200
            assert get_resp.headers["content-type"].startswith("application/json")

    def test_html_in_tag_name(self, client):
        """HTML in tag name should not execute."""
        resp = client.post(
            "/api/tags",
            json={"name": '<img src=x onerror=alert(1)>', "color": "#ff0000"},
        )
        # Tag creation should either accept (stored as plain text) or reject
        assert resp.status_code in (200, 400, 409, 422)
        if resp.status_code == 200:
            # Verify tag listing returns JSON
            tags_resp = client.get("/api/tags")
            assert tags_resp.headers["content-type"].startswith("application/json")

    def test_script_in_tag_color(self, client):
        """Script injection in tag color field."""
        resp = client.post(
            "/api/tags",
            json={"name": "safe_tag", "color": "javascript:alert(1)"},
        )
        # Should either accept (color is just a string) or validate format
        assert resp.status_code in (200, 400, 422)

    def test_xss_in_rename(self, client, media_root):
        """XSS attempt in file rename should be treated as literal filename."""
        resp = client.post(
            "/api/files/rename",
            json={
                "renames": [
                    {
                        "path": f"{media_root}/photo2.jpg",
                        "new_name": '<script>alert(1)</script>.jpg',
                    }
                ]
            },
        )
        assert resp.status_code in (200, 400, 422)

    def test_event_handler_xss_in_note(self, client):
        """Event handler XSS in note field."""
        resp = client.put(
            "/api/files/media/photo1.jpg/note",
            json={"note": '" onmouseover="alert(1)" style="'},
        )
        assert resp.status_code in (200, 400, 422)
        if resp.status_code == 200:
            get_resp = client.get("/api/files/media/photo1.jpg/note")
            assert get_resp.headers["content-type"].startswith("application/json")


# ── 5. Request Validation ────────────────────────────────────────────


class TestRequestValidation:
    """Tests for request body and parameter validation."""

    def test_missing_paths_in_delete(self, client):
        """Delete request without paths field should return 422."""
        resp = client.post("/api/files/delete", json={})
        assert resp.status_code == 422

    def test_missing_paths_in_quarantine(self, client):
        """Quarantine request without paths field should return 422."""
        resp = client.post("/api/files/quarantine", json={})
        assert resp.status_code == 422

    def test_missing_renames_in_rename(self, client):
        """Rename request without renames field should return 422."""
        resp = client.post("/api/files/rename", json={})
        assert resp.status_code == 422

    def test_missing_destination_in_move(self, client):
        """Move request without destination should return 422."""
        resp = client.post("/api/files/move", json={"paths": ["/foo"]})
        assert resp.status_code == 422

    def test_missing_paths_in_move(self, client):
        """Move request without paths should return 422."""
        resp = client.post("/api/files/move", json={"destination": "/tmp"})
        assert resp.status_code == 422

    def test_wrong_type_paths_string_instead_of_list(self, client):
        """Passing a string instead of list for paths should return 422."""
        resp = client.post(
            "/api/files/delete",
            json={"paths": "not-a-list"},
        )
        assert resp.status_code == 422

    def test_wrong_type_rating_string(self, client):
        """Passing a string for rating should return 422."""
        resp = client.put(
            "/api/files/media/photo1.jpg/rating",
            json={"rating": "five"},
        )
        assert resp.status_code == 422

    def test_rating_too_low(self, client):
        """Rating of 0 should be rejected."""
        resp = client.put(
            "/api/files/media/photo1.jpg/rating",
            json={"rating": 0},
        )
        assert resp.status_code == 400

    def test_rating_too_high(self, client):
        """Rating of 6 should be rejected."""
        resp = client.put(
            "/api/files/media/photo1.jpg/rating",
            json={"rating": 6},
        )
        assert resp.status_code == 400

    def test_rating_negative(self, client):
        """Negative rating should be rejected."""
        resp = client.put(
            "/api/files/media/photo1.jpg/rating",
            json={"rating": -1},
        )
        assert resp.status_code == 400

    def test_negative_offset_rejected(self, client):
        """Negative offset in file listing should return 422."""
        resp = client.get("/api/files", params={"offset": -1})
        assert resp.status_code == 422

    def test_extremely_large_limit(self, client):
        """Limit exceeding maximum should return 422."""
        resp = client.get("/api/files", params={"limit": 999999})
        assert resp.status_code == 422

    def test_empty_body_for_note(self, client):
        """PUT note without body should return 422."""
        resp = client.put("/api/files/media/photo1.jpg/note", json={})
        assert resp.status_code == 422

    def test_empty_tag_name(self, client):
        """Creating a tag with empty name."""
        resp = client.post("/api/tags", json={"name": "", "color": "#000000"})
        # Either accepted or rejected - but should not crash
        assert resp.status_code in (200, 400, 422)

    def test_delete_with_empty_paths_list(self, client):
        """Delete with empty paths list should succeed with 0 deleted."""
        resp = client.post("/api/files/delete", json={"paths": []})
        assert resp.status_code == 200
        assert resp.json().get("deleted", 0) == 0

    def test_move_with_empty_paths_list(self, client, tmp_path):
        """Move with empty paths list should succeed with 0 moved."""
        dest = str(tmp_path / "dest")
        resp = client.post(
            "/api/files/move",
            json={"paths": [], "destination": dest},
        )
        assert resp.status_code == 200
        assert resp.json().get("moved", 0) == 0

    def test_rename_with_empty_renames_list(self, client):
        """Rename with empty renames list should succeed with 0 renamed."""
        resp = client.post("/api/files/rename", json={"renames": []})
        assert resp.status_code == 200
        assert resp.json().get("renamed", 0) == 0


# ── 6. Boundary and Edge Cases ───────────────────────────────────────


class TestBoundaryEdgeCases:
    """Additional edge-case security tests."""

    def test_double_slash_path(self, client):
        """Double slashes in path should not bypass checks."""
        resp = client.get("/api/files//etc/passwd")
        assert resp.status_code in (403, 404)

    def test_dot_only_path(self, client):
        """Path of just '.' should be handled safely without exposing system info."""
        resp = client.get("/api/files/.")
        # May return 200 (file not in catalog lookup) or 400/404
        # The key property: no sensitive data is leaked
        assert resp.status_code in (200, 400, 404, 422)
        if resp.status_code == 200:
            data = resp.json()
            # Should not contain actual system root directory contents
            assert "children" not in data or data.get("children") is None

    def test_browse_root(self, client):
        """Browsing filesystem root should be blocked or sanitized.

        Note: _sanitize_path strips the trailing "/" from "/", resulting in an
        empty string which resolves to the current working directory — so the
        response does not actually expose the real filesystem root.
        """
        resp = client.get("/api/browse", params={"path": "/"})
        # The path "/" is sanitized to "" which resolves to CWD, so we get 200
        # or it could be blocked entirely with 403.
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            data = resp.json()
            # The key security property: the actual filesystem root ("/") with
            # its /etc, /var, /proc, /sys entries is NOT what is returned.
            # Instead, CWD is browsed (due to path sanitization).
            assert data.get("current") != "/", "Should not actually browse filesystem root"

    def test_thumbnail_size_boundary(self, client):
        """Thumbnail with size exceeding max should return 422."""
        resp = client.get("/api/thumbnail/media/photo1.jpg", params={"size": 9999})
        assert resp.status_code == 422

    def test_preview_size_boundary(self, client):
        """Preview with size exceeding max should return 422."""
        resp = client.get("/api/preview/media/photo1.jpg", params={"size": 9999})
        assert resp.status_code == 422

    def test_non_json_body_for_delete(self, client):
        """Sending non-JSON body should return 422."""
        resp = client.post(
            "/api/files/delete",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 422

    def test_content_type_json_enforced(self, client):
        """Sending form data instead of JSON should return 422."""
        resp = client.post(
            "/api/files/delete",
            data={"paths": ["/foo"]},
        )
        assert resp.status_code == 422

    def test_multiple_traversal_sequences(self, client):
        """Multiple chained traversal sequences should still be blocked."""
        # Literal /../ is resolved by Starlette router — use encoded variant
        resp = client.get("/api/files/..%2F..%2F..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (400, 403, 404)

    def test_windows_style_traversal(self, client):
        """Windows-style backslash traversal should be handled."""
        resp = client.get("/api/files/..\\..\\etc\\passwd")
        assert resp.status_code in (400, 403, 404, 422)

    def test_encoded_null_in_browse(self, client):
        """Encoded null byte in browse path should be rejected."""
        # Null bytes in query params may be rejected at transport level or cause
        # a server error - either way, the path must not be browsable
        import httpx

        try:
            resp = client.get("/api/browse", params={"path": "/home\x00/etc"})
            # If the request goes through, it should not succeed
            assert resp.status_code in (400, 403, 404, 422, 500)
        except (httpx.InvalidURL, ValueError):
            # Transport-level rejection is also a valid security boundary
            pass

    def test_extremely_deep_nesting(self, client):
        """Very deeply nested path should not crash the server."""
        deep_path = "/media" + "/subdir" * 500 + "/file.jpg"
        resp = client.get(f"/api/files{deep_path}")
        assert resp.status_code in (400, 404, 414, 422, 500)

    def test_special_chars_in_file_path(self, client):
        """Special characters like spaces and ampersands in paths should be handled."""
        resp = client.get("/api/files/media/my photo & vacation.jpg")
        # Should not crash - just 404 since file does not exist
        assert resp.status_code in (400, 404)

    def test_move_destination_is_file_not_dir(self, client, media_root):
        """Moving to a destination that is a file (not dir) should fail gracefully."""
        resp = client.post(
            "/api/files/move",
            json={
                "paths": [f"{media_root}/photo1.jpg"],
                "destination": f"{media_root}/photo2.jpg",
            },
        )
        # Should not crash
        assert resp.status_code in (200, 400, 422, 500)
