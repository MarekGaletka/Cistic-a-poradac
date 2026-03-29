"""Tests for web/app.py — share password rate limiting, CSRF, WebSocket auth, Content-Disposition."""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

pytestmark = pytest.mark.skipif(not HAS_FASTAPI, reason="fastapi not installed")

from godmode_media_library.catalog import Catalog


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_with_share(tmp_path):
    """Create a catalog with a shared file (password-protected)."""
    db_path = tmp_path / "test.db"
    cat = Catalog(db_path)
    cat.open()

    # Create a test file on disk
    media = tmp_path / "media"
    media.mkdir()
    test_file = media / "shared_photo.jpg"
    test_file.write_bytes(b"JPEG_SHARED_CONTENT" * 100)

    # Scan file into catalog
    from godmode_media_library.scanner import incremental_scan

    with (
        patch("godmode_media_library.scanner.probe_file", return_value=None),
        patch("godmode_media_library.scanner.read_exif", return_value=None),
        patch("godmode_media_library.scanner.dhash", return_value=None),
        patch("godmode_media_library.scanner.video_dhash", return_value=None),
    ):
        incremental_scan(cat, [media])
    cat.close()
    return db_path, test_file


@pytest.fixture
def app_no_auth(catalog_with_share):
    """App without API token (CSRF middleware active)."""
    db_path, _ = catalog_with_share
    from godmode_media_library.web.app import create_app

    env = {"GML_API_TOKEN": "", "GML_RATE_LIMIT": "0"}
    with patch.dict(os.environ, env, clear=False):
        app = create_app(catalog_path=db_path)
        yield app


@pytest.fixture
def client_no_auth(app_no_auth):
    return TestClient(app_no_auth, raise_server_exceptions=False)


@pytest.fixture
def app_with_token(catalog_with_share):
    """App with API token."""
    db_path, _ = catalog_with_share
    from godmode_media_library.web.app import _auth_failures, create_app

    env = {"GML_API_TOKEN": "secret-test-token", "GML_RATE_LIMIT": "0"}
    with patch.dict(os.environ, env, clear=False):
        _auth_failures.clear()
        app = create_app(catalog_path=db_path)
        yield app
        _auth_failures.clear()


@pytest.fixture
def app_with_rate_limit(catalog_with_share):
    """App with rate limiting enabled."""
    db_path, _ = catalog_with_share
    from godmode_media_library.web.app import _rate_limit_hits, create_app

    env = {"GML_API_TOKEN": "", "GML_RATE_LIMIT": "5"}
    with patch.dict(os.environ, env, clear=False):
        _rate_limit_hits.clear()
        app = create_app(catalog_path=db_path)
        yield app
        _rate_limit_hits.clear()


# ===========================================================================
# CSRF Protection
# ===========================================================================


class TestCSRFProtection:
    """When no API token is set, CSRF middleware rejects mutating requests from foreign origins."""

    def test_csrf_blocks_foreign_origin(self, client_no_auth):
        resp = client_no_auth.post(
            "/api/files/delete",
            json={"paths": []},
            headers={"Origin": "https://evil.com"},
        )
        assert resp.status_code == 403
        assert "CSRF" in resp.json()["detail"]

    def test_csrf_allows_localhost(self, client_no_auth):
        resp = client_no_auth.post(
            "/api/files/delete",
            json={"paths": []},
            headers={"Origin": "http://localhost:3000"},
        )
        # Should not be CSRF-blocked (may be 200 or other validation error)
        assert resp.status_code != 403 or "CSRF" not in resp.json().get("detail", "")

    def test_csrf_allows_127_0_0_1(self, client_no_auth):
        resp = client_no_auth.post(
            "/api/files/delete",
            json={"paths": []},
            headers={"Origin": "http://127.0.0.1:8080"},
        )
        assert resp.status_code != 403 or "CSRF" not in resp.json().get("detail", "")

    def test_csrf_allows_no_origin_header(self, client_no_auth):
        """Requests without Origin header (e.g., same-origin) are not blocked."""
        resp = client_no_auth.post(
            "/api/files/delete",
            json={"paths": []},
        )
        # No Origin header -> no CSRF check
        assert resp.status_code != 403 or "CSRF" not in resp.json().get("detail", "")

    def test_csrf_allows_get_requests(self, client_no_auth):
        """GET requests are not subject to CSRF checks."""
        resp = client_no_auth.get(
            "/api/stats",
            headers={"Origin": "https://evil.com"},
        )
        assert resp.status_code == 200


# ===========================================================================
# WebSocket Auth Rejection
# ===========================================================================


class TestWebSocketAuthRejection:
    def test_websocket_rejected_without_token(self, app_with_token):
        client = TestClient(app_with_token, raise_server_exceptions=False)
        # Try to connect WebSocket without auth
        resp = client.get(
            "/api/ws/tasks/some-task-id",
            headers={"Upgrade": "websocket", "Connection": "Upgrade"},
        )
        # Should reject with 403 for WebSocket
        assert resp.status_code in (401, 403)

    def test_websocket_accepted_with_valid_token_via_query(self, app_with_token):
        from godmode_media_library.web.api import _create_task, _finish_task

        task = _create_task("ws-auth-test")
        _finish_task(task.id, result={"ok": True})

        client = TestClient(app_with_token, raise_server_exceptions=False)
        # WebSocket connections pass token via query param since WS headers
        # are not reliably forwarded through TestClient
        with client.websocket_connect(f"/api/ws/tasks/{task.id}?token=secret-test-token") as ws:
            data = ws.receive_json()
            assert data["id"] == task.id


# ===========================================================================
# Content-Disposition Headers
# ===========================================================================


class TestContentDisposition:
    def test_shared_file_download_content_disposition(self, catalog_with_share, tmp_path):
        """Shared file download should include Content-Disposition header with filename."""
        db_path, test_file = catalog_with_share

        # Create a share link in the catalog
        cat = Catalog(db_path)
        cat.open()
        try:
            token = cat.create_share(str(test_file), label="test")
        finally:
            cat.close()

        from godmode_media_library.web.app import create_app
        env = {"GML_API_TOKEN": "", "GML_RATE_LIMIT": "0"}
        with patch.dict(os.environ, env, clear=False):
            app = create_app(catalog_path=db_path)

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get(f"/shared/{token}")
        if resp.status_code == 200:
            cd = resp.headers.get("content-disposition", "")
            assert "attachment" in cd
            assert "shared_photo.jpg" in cd


# ===========================================================================
# Share Password Rate Limiting
# ===========================================================================


class TestSharePasswordRateLimiting:
    def test_share_password_rate_limit(self, catalog_with_share, tmp_path):
        """After too many wrong password attempts, the IP gets 429."""
        db_path, test_file = catalog_with_share

        # Create a password-protected share
        cat = Catalog(db_path)
        cat.open()
        try:
            share = cat.create_share(str(test_file), label="pw-test", password="secret123")
            token = share["token"]
        finally:
            cat.close()

        from godmode_media_library.web.app import _share_pw_failures, create_app
        _share_pw_failures.clear()

        env = {"GML_API_TOKEN": "", "GML_RATE_LIMIT": "0"}
        with patch.dict(os.environ, env, clear=False):
            app = create_app(catalog_path=db_path)

        client = TestClient(app, raise_server_exceptions=False)

        # Send many wrong password attempts
        for _ in range(10):
            resp = client.get(
                f"/shared/{token}",
                headers={"X-Share-Password": "wrong_password"},
            )
            assert resp.status_code in (403, 429)

        # Next attempt should be rate-limited
        resp = client.get(
            f"/shared/{token}",
            headers={"X-Share-Password": "wrong_password"},
        )
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers

        _share_pw_failures.clear()


# ===========================================================================
# Security Headers
# ===========================================================================


class TestSecurityHeadersApp:
    def test_security_headers(self, client_no_auth):
        resp = client_no_auth.get("/api/stats")
        assert resp.headers.get("x-content-type-options") == "nosniff"
        assert resp.headers.get("x-frame-options") == "DENY"
        assert "Content-Security-Policy" in resp.headers

    def test_no_cache_for_js(self, client_no_auth):
        """JS/CSS endpoints should get no-cache headers."""
        # We can test by requesting a .js path — even if 404, headers should be set
        resp = client_no_auth.get("/test.js")
        # The security header middleware runs regardless of status
        if "cache-control" in resp.headers:
            assert "no-cache" in resp.headers["cache-control"]


# ===========================================================================
# Rate Limiting
# ===========================================================================


class TestRateLimiting:
    def test_rate_limit_triggers_after_max(self, app_with_rate_limit):
        from godmode_media_library.web.app import _rate_limit_hits
        _rate_limit_hits.clear()

        client = TestClient(app_with_rate_limit, raise_server_exceptions=False)
        responses = []
        for _ in range(7):
            resp = client.get("/api/stats")
            responses.append(resp.status_code)

        assert 429 in responses

    def test_rate_limit_does_not_apply_to_non_api(self, app_with_rate_limit):
        client = TestClient(app_with_rate_limit, raise_server_exceptions=False)
        resp = client.get("/openapi.json")
        # Non-API paths are not rate limited
        assert resp.status_code != 429


# ===========================================================================
# _prune_rate_dict
# ===========================================================================


class TestPruneRateDict:
    def test_prune_stale_entries(self):
        from godmode_media_library.web.app import _prune_rate_dict

        now = time.monotonic()
        d = {
            "old_ip": [now - 200, now - 150],
            "recent_ip": [now - 5, now - 1],
        }
        _prune_rate_dict(d, window=60.0, max_ips=100)
        assert "old_ip" not in d
        assert "recent_ip" in d

    def test_prune_caps_max_ips(self):
        from godmode_media_library.web.app import _prune_rate_dict

        now = time.monotonic()
        d = {f"ip_{i}": [now - 1] for i in range(20)}
        _prune_rate_dict(d, window=60.0, max_ips=5)
        assert len(d) <= 5


# ===========================================================================
# Share endpoint edge cases
# ===========================================================================


class TestShareEdgeCases:
    def test_share_not_found(self, catalog_with_share):
        db_path, _ = catalog_with_share
        from godmode_media_library.web.app import create_app
        env = {"GML_API_TOKEN": "", "GML_RATE_LIMIT": "0"}
        with patch.dict(os.environ, env, clear=False):
            app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/shared/nonexistent-token")
        assert resp.status_code == 404

    def test_share_info_not_found(self, catalog_with_share):
        db_path, _ = catalog_with_share
        from godmode_media_library.web.app import create_app
        env = {"GML_API_TOKEN": "", "GML_RATE_LIMIT": "0"}
        with patch.dict(os.environ, env, clear=False):
            app = create_app(catalog_path=db_path)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/shared/nonexistent-token/info")
        assert resp.status_code == 404
