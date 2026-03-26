"""Tests for cloud REST API endpoints."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from godmode_media_library.cloud import RcloneRemote
from godmode_media_library.web.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(catalog_path=tmp_path / "test.db")
    return TestClient(app)


def test_cloud_status(client):
    with (
        patch("godmode_media_library.cloud.check_rclone", return_value=False),
        patch("godmode_media_library.cloud.detect_native_cloud_paths", return_value=[]),
    ):
        resp = client.get("/api/cloud/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "rclone_installed" in data
        assert "sources" in data
        assert "providers" in data


def test_cloud_remotes_no_rclone(client):
    with patch("godmode_media_library.cloud.check_rclone", return_value=False):
        resp = client.get("/api/cloud/remotes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["installed"] is False
        assert data["remotes"] == []


def test_cloud_remotes_with_rclone(client):
    mock_remotes = [RcloneRemote(name="mega", type="mega")]
    with (
        patch("godmode_media_library.cloud.check_rclone", return_value=True),
        patch("godmode_media_library.cloud.rclone_version", return_value="1.67.0"),
        patch("godmode_media_library.cloud.list_remotes", return_value=mock_remotes),
    ):
        resp = client.get("/api/cloud/remotes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["installed"] is True
        assert len(data["remotes"]) == 1
        assert data["remotes"][0]["name"] == "mega"
        assert data["remotes"][0]["label"] == "MEGA"


def test_cloud_native_paths(client):
    mock_paths = [{"name": "MEGA", "path": "/home/user/MEGA", "type": "native_sync", "icon": "📦"}]
    with patch("godmode_media_library.cloud.detect_native_cloud_paths", return_value=mock_paths):
        resp = client.get("/api/cloud/native")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["paths"][0]["name"] == "MEGA"


def test_cloud_providers(client):
    resp = client.get("/api/cloud/providers")
    assert resp.status_code == 200
    data = resp.json()
    assert "mega" in data["providers"]
    assert "pcloud" in data["providers"]
    assert "drive" in data["providers"]


def test_cloud_provider_guide(client):
    resp = client.get("/api/cloud/providers/mega")
    assert resp.status_code == 200
    data = resp.json()
    assert data["provider"] == "MEGA"
    assert len(data["steps"]) == 4


def test_cloud_provider_guide_not_found(client):
    resp = client.get("/api/cloud/providers/nonexistent")
    assert resp.status_code == 404
