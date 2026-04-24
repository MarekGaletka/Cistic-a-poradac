"""Tests for hash-based verification logic (Session 5, item 5.4).

Tests get_native_hash_type() backend map and rclone_verify_transfer() logic.
Uses monkeypatching to avoid real rclone calls.
"""

import subprocess

from godmode_media_library.cloud import (
    _BACKEND_HASH_MAP,
    get_native_hash_type,
    rclone_verify_transfer,
)


class TestBackendHashMap:
    def test_google_drive_uses_md5(self):
        assert _BACKEND_HASH_MAP["drive"] == "md5"

    def test_onedrive_uses_sha1(self):
        assert _BACKEND_HASH_MAP["onedrive"] == "sha1"

    def test_dropbox_uses_own_hash(self):
        assert _BACKEND_HASH_MAP["dropbox"] == "dropbox"

    def test_pcloud_uses_sha256(self):
        assert _BACKEND_HASH_MAP["pcloud"] == "sha256"

    def test_s3_uses_md5(self):
        assert _BACKEND_HASH_MAP["s3"] == "md5"

    def test_mega_not_in_map(self):
        assert "mega" not in _BACKEND_HASH_MAP


class TestGetNativeHashType:
    def test_returns_hash_for_known_remote(self, monkeypatch):
        """Simulate rclone listremotes --long returning a known backend."""

        def fake_run(*args, **kwargs):
            result = subprocess.CompletedProcess(args[0], 0)
            result.stdout = "mydrive:        drive\nmypcloud:       pcloud\n"
            result.stderr = ""
            return result

        monkeypatch.setattr("godmode_media_library.cloud.check_rclone", lambda: True)
        monkeypatch.setattr("subprocess.run", fake_run)

        assert get_native_hash_type("mydrive") == "md5"
        assert get_native_hash_type("mypcloud") == "sha256"

    def test_returns_none_for_unknown_backend(self, monkeypatch):
        def fake_run(*args, **kwargs):
            result = subprocess.CompletedProcess(args[0], 0)
            result.stdout = "myremote:       ftp\n"
            result.stderr = ""
            return result

        monkeypatch.setattr("godmode_media_library.cloud.check_rclone", lambda: True)
        monkeypatch.setattr("subprocess.run", fake_run)

        assert get_native_hash_type("myremote") is None

    def test_returns_none_when_rclone_missing(self, monkeypatch):
        monkeypatch.setattr("godmode_media_library.cloud.check_rclone", lambda: False)
        assert get_native_hash_type("any") is None

    def test_returns_none_on_timeout(self, monkeypatch):
        def fake_run(*args, **kwargs):
            raise subprocess.TimeoutExpired(args[0], 10)

        monkeypatch.setattr("godmode_media_library.cloud.check_rclone", lambda: True)
        monkeypatch.setattr("subprocess.run", fake_run)

        assert get_native_hash_type("remote") is None


class TestRcloneVerifyTransfer:
    def test_file_not_found(self, monkeypatch):
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_check_file",
            lambda remote, path, **kw: {"exists": False, "size": None},
        )
        result = rclone_verify_transfer("remote:", "path/file.jpg")
        assert result["verified"] is False
        assert "not found" in result["error"].lower()

    def test_size_mismatch(self, monkeypatch):
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_check_file",
            lambda remote, path, **kw: {"exists": True, "size": 500, "size_match": False},
        )
        result = rclone_verify_transfer("remote:", "path/file.jpg", expected_size=1000)
        assert result["verified"] is False
        assert result["size_ok"] is False
        assert "mismatch" in result["error"].lower()

    def test_size_match_no_hash(self, monkeypatch):
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_check_file",
            lambda remote, path, **kw: {"exists": True, "size": 1000, "size_match": True},
        )
        result = rclone_verify_transfer("remote:", "path/file.jpg", expected_size=1000)
        assert result["verified"] is True
        assert result["size_ok"] is True

    def test_hash_match(self, monkeypatch):
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_check_file",
            lambda remote, path, **kw: {"exists": True, "size": 1000, "size_match": True},
        )
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_hashsum",
            lambda remote, path, **kw: "abc123def456",
        )
        result = rclone_verify_transfer(
            "remote:",
            "path/file.jpg",
            expected_size=1000,
            expected_hash="ABC123DEF456",
            hash_type="md5",
        )
        assert result["verified"] is True
        assert result["hash_ok"] is True

    def test_hash_mismatch(self, monkeypatch):
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_check_file",
            lambda remote, path, **kw: {"exists": True, "size": 1000, "size_match": True},
        )
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_hashsum",
            lambda remote, path, **kw: "different_hash",
        )
        result = rclone_verify_transfer(
            "remote:",
            "path/file.jpg",
            expected_size=1000,
            expected_hash="expected_hash",
            hash_type="md5",
        )
        assert result["verified"] is False
        assert result["hash_ok"] is False
        assert "mismatch" in result["error"].lower()

    def test_hash_unavailable_falls_back(self, monkeypatch):
        """When hash can't be computed, verification still passes on size alone."""
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_check_file",
            lambda remote, path, **kw: {"exists": True, "size": 1000, "size_match": True},
        )
        monkeypatch.setattr(
            "godmode_media_library.cloud.rclone_hashsum",
            lambda remote, path, **kw: None,
        )
        result = rclone_verify_transfer(
            "remote:",
            "path/file.jpg",
            expected_size=1000,
            expected_hash="abc123",
            hash_type="md5",
        )
        assert result["verified"] is True
        assert result["hash_ok"] is None
