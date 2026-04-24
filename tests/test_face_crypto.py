"""Unit tests for face_crypto.py — encryption/decryption of face encodings."""

import json
import struct
from pathlib import Path
from unittest.mock import patch

import pytest

from godmode_media_library.face_crypto import (
    _BLOB_VERSION,
    _ENCODING_SIZE,
    _FLOAT_FMT,
    _HEADER_FMT,
    _HEADER_SIZE,
    _key_path,
    _keystore_path,
    _reset_cache,
    decrypt_encoding,
    decrypt_encoding_noop,
    delete_key,
    encrypt_encoding,
    encrypt_encoding_noop,
    get_decrypt_fn,
    get_encrypt_fn,
    get_key_info,
    rotate_key,
)

# ── Helper to set up a fresh keystore environment ────────────────────


def _patch_paths(tmp_path):
    """Return a context manager that patches both key and keystore paths."""
    key_file = tmp_path / "face.key"
    ks_file = tmp_path / "face_keystore.json"

    class _ctx:
        def __enter__(self):
            _reset_cache()
            self._p1 = patch("godmode_media_library.face_crypto._key_path", return_value=key_file)
            self._p2 = patch(
                "godmode_media_library.face_crypto._keystore_path",
                return_value=ks_file,
            )
            self._p1.__enter__()
            self._p2.__enter__()
            return self

        def __exit__(self, *args):
            self._p2.__exit__(*args)
            self._p1.__exit__(*args)
            _reset_cache()

    ctx = _ctx()
    ctx.key_file = key_file
    ctx.ks_file = ks_file
    return ctx


# ── Noop (unencrypted) roundtrip ─────────────────────────────────────


class TestNoopRoundtrip:
    def test_roundtrip_list(self):
        original = [float(i) for i in range(_ENCODING_SIZE)]
        blob = encrypt_encoding_noop(original)
        recovered = decrypt_encoding_noop(blob)
        assert recovered == original

    def test_roundtrip_preserves_floats(self):
        original = [0.1 * i for i in range(_ENCODING_SIZE)]
        blob = encrypt_encoding_noop(original)
        recovered = decrypt_encoding_noop(blob)
        for a, b in zip(original, recovered, strict=False):
            assert abs(a - b) < 1e-12

    def test_noop_blob_size(self):
        original = [0.0] * _ENCODING_SIZE
        blob = encrypt_encoding_noop(original)
        # 128 doubles * 8 bytes = 1024
        assert len(blob) == _ENCODING_SIZE * 8

    def test_noop_with_numpy_like_object(self):
        """Simulate a numpy array with .tolist() method."""

        class FakeArray:
            def __init__(self, data):
                self._data = data

            def tolist(self):
                return self._data

        original = [1.5] * _ENCODING_SIZE
        fake = FakeArray(original)
        blob = encrypt_encoding_noop(fake)
        recovered = decrypt_encoding_noop(blob)
        assert recovered == original


# ── Encrypted roundtrip (with real Fernet) ───────────────────────────


class TestEncryptedRoundtrip:
    def test_encrypt_decrypt_roundtrip(self, tmp_path):
        """Full encryption/decryption cycle with a temp keystore."""
        with _patch_paths(tmp_path):
            original = [float(i) * 0.01 for i in range(_ENCODING_SIZE)]
            blob = encrypt_encoding(original)
            recovered = decrypt_encoding(blob)

            for a, b in zip(original, recovered, strict=False):
                assert abs(a - b) < 1e-12

    def test_encrypted_blob_is_larger_than_raw(self, tmp_path):
        with _patch_paths(tmp_path):
            original = [0.0] * _ENCODING_SIZE
            encrypted_blob = encrypt_encoding(original)
            raw_blob = encrypt_encoding_noop(original)
            # Fernet adds overhead (IV + HMAC + padding) + 2-byte header
            assert len(encrypted_blob) > len(raw_blob)

    def test_wrong_key_rejects(self, tmp_path):
        """Decryption with a different key must fail."""
        from cryptography.fernet import Fernet

        key1 = Fernet.generate_key().decode("ascii")
        key2 = Fernet.generate_key().decode("ascii")

        with _patch_paths(tmp_path) as ctx:
            # Create keystore with key1
            ks = {
                "active_key_id": 0,
                "keys": {"0": {"key": key1, "created": "2026-01-01T00:00:00+00:00"}},
            }
            ctx.ks_file.write_text(json.dumps(ks))
            _reset_cache()

            original = [1.0] * _ENCODING_SIZE
            blob = encrypt_encoding(original)

            # Replace key in keystore with key2
            ks["keys"]["0"]["key"] = key2
            ctx.ks_file.write_text(json.dumps(ks))
            _reset_cache()

            with pytest.raises((Exception,)):  # noqa: B017  # InvalidToken or similar
                decrypt_encoding(blob)

    def test_key_auto_generated(self, tmp_path):
        """If no keystore exists, one is created automatically."""
        with _patch_paths(tmp_path) as ctx:
            assert not ctx.ks_file.exists()

            original = [2.0] * _ENCODING_SIZE
            blob = encrypt_encoding(original)
            assert ctx.ks_file.exists()
            recovered = decrypt_encoding(blob)
            assert recovered == original


# ── get_encrypt_fn / get_decrypt_fn ──────────────────────────────────


class TestFunctionSelectors:
    def test_enabled_returns_crypto_fn(self):
        assert get_encrypt_fn(enabled=True) is encrypt_encoding
        assert get_decrypt_fn(enabled=True) is decrypt_encoding

    def test_disabled_returns_noop_fn(self):
        assert get_encrypt_fn(enabled=False) is encrypt_encoding_noop
        assert get_decrypt_fn(enabled=False) is decrypt_encoding_noop


# ── delete_key ───────────────────────────────────────────────────────


class TestDeleteKey:
    def test_delete_existing_keystore(self, tmp_path):
        with _patch_paths(tmp_path) as ctx:
            ctx.ks_file.write_text('{"active_key_id": 0, "keys": {}}')
            assert delete_key() is True
            assert not ctx.ks_file.exists()

    def test_delete_legacy_key_file(self, tmp_path):
        with _patch_paths(tmp_path) as ctx:
            ctx.key_file.write_text("dummy-key")
            assert delete_key() is True
            assert not ctx.key_file.exists()

    def test_delete_nonexistent_key(self, tmp_path):
        with _patch_paths(tmp_path):
            assert delete_key() is False


# ── _key_path ────────────────────────────────────────────────────────


class TestKeyPath:
    def test_key_path_returns_path(self):
        result = _key_path()
        assert isinstance(result, Path)
        assert result.name == "face.key"

    def test_keystore_path_returns_path(self):
        result = _keystore_path()
        assert isinstance(result, Path)
        assert result.name == "face_keystore.json"


# ── Key rotation tests ───────────────────────────────────────────────


class TestKeyRotation:
    def test_key_rotation_roundtrip(self, tmp_path):
        """Encrypt with key 0, rotate, encrypt with key 1, both decrypt."""
        with _patch_paths(tmp_path):
            original_0 = [float(i) for i in range(_ENCODING_SIZE)]
            blob_0 = encrypt_encoding(original_0)

            new_id = rotate_key()
            assert new_id == 1

            original_1 = [float(i) * 2.0 for i in range(_ENCODING_SIZE)]
            blob_1 = encrypt_encoding(original_1)

            # Both blobs must decrypt correctly
            recovered_0 = decrypt_encoding(blob_0)
            recovered_1 = decrypt_encoding(blob_1)

            assert recovered_0 == original_0
            assert recovered_1 == original_1

    def test_legacy_blob_decryption(self, tmp_path):
        """A blob without version header still decrypts using key 0."""
        from cryptography.fernet import Fernet

        with _patch_paths(tmp_path) as ctx:
            # Create a keystore manually
            key_b64 = Fernet.generate_key().decode("ascii")
            ks = {
                "active_key_id": 0,
                "keys": {"0": {"key": key_b64, "created": "2026-01-01T00:00:00+00:00"}},
            }
            ctx.ks_file.write_text(json.dumps(ks))
            _reset_cache()

            # Create a legacy blob (no header, just raw Fernet output)
            f = Fernet(key_b64.encode("ascii"))
            original = [3.14] * _ENCODING_SIZE
            raw = struct.pack(_FLOAT_FMT, *original)
            legacy_blob = f.encrypt(raw)

            # Must decrypt without errors
            recovered = decrypt_encoding(legacy_blob)
            for a, b in zip(original, recovered, strict=False):
                assert abs(a - b) < 1e-12

    def test_versioned_blob_format(self, tmp_path):
        """Check the 2-byte header is present and correct."""
        with _patch_paths(tmp_path):
            original = [0.0] * _ENCODING_SIZE
            blob = encrypt_encoding(original)

            # First 2 bytes are the header
            version, key_id = struct.unpack(_HEADER_FMT, blob[:_HEADER_SIZE])
            assert version == _BLOB_VERSION
            assert key_id == 0  # first key

    def test_rotate_key_increments_id(self, tmp_path):
        """Each rotation increments the active key ID."""
        with _patch_paths(tmp_path):
            # Auto-creates key 0
            encrypt_encoding([0.0] * _ENCODING_SIZE)

            id1 = rotate_key()
            assert id1 == 1

            id2 = rotate_key()
            assert id2 == 2

            id3 = rotate_key()
            assert id3 == 3

    def test_get_key_info(self, tmp_path):
        """get_key_info returns metadata without secret material."""
        with _patch_paths(tmp_path):
            # Trigger keystore creation
            encrypt_encoding([0.0] * _ENCODING_SIZE)
            rotate_key()

            info = get_key_info()
            assert info["key_count"] == 2
            assert info["active_key_id"] == 1
            assert 0 in info["keys"]
            assert 1 in info["keys"]
            # No secret key material exposed
            for entry in info["keys"].values():
                assert "key" not in entry
                assert "created" in entry

    def test_migrate_from_old_keyfile(self, tmp_path):
        """Legacy face.key is migrated into the keystore on first load."""
        from cryptography.fernet import Fernet

        with _patch_paths(tmp_path) as ctx:
            # Simulate old-style face.key
            old_key = Fernet.generate_key()
            ctx.key_file.write_bytes(old_key)

            # Encrypt with the migrated key
            original = [42.0] * _ENCODING_SIZE
            blob = encrypt_encoding(original)

            # Keystore should exist now
            assert ctx.ks_file.exists()

            # The migrated key should be key 0
            ks = json.loads(ctx.ks_file.read_text())
            assert ks["active_key_id"] == 0
            assert ks["keys"]["0"]["key"] == old_key.decode("ascii")

            # Decryption works
            recovered = decrypt_encoding(blob)
            assert recovered == original

    def test_rotate_preserves_all_keys(self, tmp_path):
        """Rotating never removes old keys from the keystore."""
        with _patch_paths(tmp_path) as ctx:
            encrypt_encoding([0.0] * _ENCODING_SIZE)
            rotate_key()
            rotate_key()

            ks = json.loads(ctx.ks_file.read_text())
            assert "0" in ks["keys"]
            assert "1" in ks["keys"]
            assert "2" in ks["keys"]
            assert ks["active_key_id"] == 2

    def test_blob_after_rotation_uses_new_key(self, tmp_path):
        """After rotation, new blobs use the new active key ID in the header."""
        with _patch_paths(tmp_path):
            encrypt_encoding([0.0] * _ENCODING_SIZE)
            rotate_key()

            blob = encrypt_encoding([1.0] * _ENCODING_SIZE)
            version, key_id = struct.unpack(_HEADER_FMT, blob[:_HEADER_SIZE])
            assert version == _BLOB_VERSION
            assert key_id == 1


# ── _write_keystore without os.fchmod (Windows) ──────────────────


class TestWriteKeystoreNoFchmod:
    def test_write_keystore_without_fchmod(self, tmp_path, monkeypatch):
        """Keystore write succeeds even when os.fchmod is unavailable."""
        monkeypatch.delattr("os.fchmod", raising=False)

        with _patch_paths(tmp_path) as ctx:
            original = [0.0] * _ENCODING_SIZE
            blob = encrypt_encoding(original)
            assert ctx.ks_file.exists()
            recovered = decrypt_encoding(blob)
            assert recovered == original
