"""Unit tests for face_crypto.py — encryption/decryption of face encodings."""

import struct
from pathlib import Path
from unittest.mock import patch

import pytest

from godmode_media_library.face_crypto import (
    _ENCODING_SIZE,
    _FLOAT_FMT,
    _key_path,
    decrypt_encoding,
    decrypt_encoding_noop,
    delete_key,
    encrypt_encoding,
    encrypt_encoding_noop,
    get_decrypt_fn,
    get_encrypt_fn,
)


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
        for a, b in zip(original, recovered):
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
        """Full encryption/decryption cycle with a temp key."""
        from cryptography.fernet import Fernet

        key = Fernet.generate_key()
        key_file = tmp_path / "face.key"
        key_file.write_bytes(key)

        with patch("godmode_media_library.face_crypto._key_path", return_value=key_file):
            original = [float(i) * 0.01 for i in range(_ENCODING_SIZE)]
            blob = encrypt_encoding(original)
            recovered = decrypt_encoding(blob)

            for a, b in zip(original, recovered):
                assert abs(a - b) < 1e-12

    def test_encrypted_blob_is_larger_than_raw(self, tmp_path):
        from cryptography.fernet import Fernet

        key = Fernet.generate_key()
        key_file = tmp_path / "face.key"
        key_file.write_bytes(key)

        with patch("godmode_media_library.face_crypto._key_path", return_value=key_file):
            original = [0.0] * _ENCODING_SIZE
            encrypted_blob = encrypt_encoding(original)
            raw_blob = encrypt_encoding_noop(original)
            # Fernet adds overhead (IV + HMAC + padding)
            assert len(encrypted_blob) > len(raw_blob)

    def test_wrong_key_rejects(self, tmp_path):
        """Decryption with a different key must fail."""
        from cryptography.fernet import Fernet, InvalidToken

        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        key_file = tmp_path / "face.key"

        # Encrypt with key1
        key_file.write_bytes(key1)
        with patch("godmode_media_library.face_crypto._key_path", return_value=key_file):
            original = [1.0] * _ENCODING_SIZE
            blob = encrypt_encoding(original)

        # Try to decrypt with key2
        key_file.write_bytes(key2)
        with patch("godmode_media_library.face_crypto._key_path", return_value=key_file):
            with pytest.raises(Exception):  # InvalidToken or similar
                decrypt_encoding(blob)

    def test_key_auto_generated(self, tmp_path):
        """If no key exists, _ensure_key creates one."""
        key_file = tmp_path / "config" / "gml" / "face.key"
        assert not key_file.exists()

        with patch("godmode_media_library.face_crypto._key_path", return_value=key_file):
            original = [2.0] * _ENCODING_SIZE
            blob = encrypt_encoding(original)
            assert key_file.exists()
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
    def test_delete_existing_key(self, tmp_path):
        key_file = tmp_path / "face.key"
        key_file.write_text("dummy-key")
        with patch("godmode_media_library.face_crypto._key_path", return_value=key_file):
            assert delete_key() is True
            assert not key_file.exists()

    def test_delete_nonexistent_key(self, tmp_path):
        key_file = tmp_path / "nonexistent.key"
        with patch("godmode_media_library.face_crypto._key_path", return_value=key_file):
            assert delete_key() is False


# ── _key_path ────────────────────────────────────────────────────────


class TestKeyPath:
    def test_key_path_returns_path(self):
        result = _key_path()
        assert isinstance(result, Path)
        assert result.name == "face.key"
