"""Encryption for face encodings (biometric data at rest).

Uses Fernet symmetric encryption. The key is stored at ~/.config/gml/face.key
with restrictive permissions (0o600). If the database is copied without the
key file, face encodings are unreadable.
"""

from __future__ import annotations

import logging
import os
import struct
from pathlib import Path

logger = logging.getLogger(__name__)

_KEY_PATH = Path.home() / ".config" / "gml" / "face.key"

# 128 floats × 8 bytes each = 1024 bytes per encoding
_ENCODING_SIZE = 128
_FLOAT_FMT = f"<{_ENCODING_SIZE}d"


def _key_path() -> Path:
    return _KEY_PATH


def _ensure_key() -> bytes:
    """Load or generate the Fernet encryption key."""
    from cryptography.fernet import Fernet

    kp = _key_path()
    if kp.exists():
        return kp.read_bytes().strip()

    key = Fernet.generate_key()
    kp.parent.mkdir(parents=True, exist_ok=True)
    kp.write_bytes(key)
    os.chmod(str(kp), 0o600)
    logger.info("Generated new face encryption key at %s", kp)
    return key


def _get_fernet():
    from cryptography.fernet import Fernet

    return Fernet(_ensure_key())


def encrypt_encoding(encoding) -> bytes:
    """Encrypt a 128D face encoding (numpy array or list of floats) to bytes."""
    floats = encoding.tolist() if hasattr(encoding, "tolist") else list(encoding)
    raw = struct.pack(_FLOAT_FMT, *floats)
    return _get_fernet().encrypt(raw)


def decrypt_encoding(blob: bytes):
    """Decrypt an encrypted encoding blob back to a list of 128 floats."""
    raw = _get_fernet().decrypt(blob)
    return list(struct.unpack(_FLOAT_FMT, raw))


def encrypt_encoding_noop(encoding) -> bytes:
    """Store encoding as raw bytes without encryption (for when encryption is disabled)."""
    floats = encoding.tolist() if hasattr(encoding, "tolist") else list(encoding)
    return struct.pack(_FLOAT_FMT, *floats)


def decrypt_encoding_noop(blob: bytes):
    """Read raw encoding bytes (no encryption)."""
    return list(struct.unpack(_FLOAT_FMT, blob))


def get_encrypt_fn(enabled: bool = True):
    """Return the appropriate encrypt function based on config."""
    return encrypt_encoding if enabled else encrypt_encoding_noop


def get_decrypt_fn(enabled: bool = True):
    """Return the appropriate decrypt function based on config."""
    return decrypt_encoding if enabled else decrypt_encoding_noop


def delete_key() -> bool:
    """Delete the encryption key file. Returns True if deleted."""
    kp = _key_path()
    if kp.exists():
        kp.unlink()
        logger.info("Deleted face encryption key at %s", kp)
        return True
    return False
