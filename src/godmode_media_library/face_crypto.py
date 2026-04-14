"""Encryption for face encodings (biometric data at rest).

Uses Fernet symmetric encryption with key rotation support. Keys are stored
in a JSON keystore at ~/.config/gml/face_keystore.json. Each encrypted blob
carries a 2-byte version header (version byte + key_id byte) so that blobs
encrypted with older keys can always be decrypted.

Legacy blobs (created before key rotation support) have no header and are
decrypted using key 0 (the original key).

If the database is copied without the keystore, face encodings are unreadable.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import struct
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_KEY_PATH = Path.home() / ".config" / "gml" / "face.key"
_KEYSTORE_PATH = Path.home() / ".config" / "gml" / "face_keystore.json"

# 128 floats x 8 bytes each = 1024 bytes per encoding
_ENCODING_SIZE = 128
_FLOAT_FMT = f"<{_ENCODING_SIZE}d"

# Versioned blob header
_BLOB_VERSION = 0x01
_HEADER_FMT = "BB"  # version (1 byte) + key_id (1 byte)
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)


def _key_path() -> Path:
    return _KEY_PATH


def _keystore_path() -> Path:
    return _KEYSTORE_PATH


# ── Keystore management ──────────────────────────────────────────────


_keystore_cache: dict[str, Any] | None = None


def _load_keystore() -> dict[str, Any]:
    """Load or create the keystore, migrating from legacy face.key if needed."""
    global _keystore_cache
    if _keystore_cache is not None:
        return _keystore_cache

    ks_path = _keystore_path()
    if ks_path.exists():
        _keystore_cache = json.loads(ks_path.read_text())
        return _keystore_cache

    # Check for legacy single-key file and migrate
    legacy_path = _key_path()
    if legacy_path.exists():
        key_b64 = legacy_path.read_bytes().strip().decode("ascii")
        now = datetime.now(timezone.utc).isoformat()
        keystore = {
            "active_key_id": 0,
            "keys": {
                "0": {"key": key_b64, "created": now},
            },
        }
        _write_keystore(keystore)
        _keystore_cache = keystore
        logger.info("Migrated legacy face.key to keystore at %s", ks_path)
        return _keystore_cache

    # No keystore or legacy key -- generate fresh key 0
    from cryptography.fernet import Fernet

    key_b64 = Fernet.generate_key().decode("ascii")
    now = datetime.now(timezone.utc).isoformat()
    keystore = {
        "active_key_id": 0,
        "keys": {
            "0": {"key": key_b64, "created": now},
        },
    }
    _write_keystore(keystore)
    _keystore_cache = keystore
    logger.info("Generated new face keystore at %s", ks_path)
    return _keystore_cache


def _write_keystore(keystore: dict[str, Any]) -> None:
    """Atomically write keystore to disk with restrictive permissions."""
    ks_path = _keystore_path()
    ks_path.parent.mkdir(parents=True, exist_ok=True)

    data = json.dumps(keystore, indent=2).encode("utf-8")

    # Write to temp file then rename for atomicity
    fd, tmp_name = tempfile.mkstemp(
        dir=str(ks_path.parent), prefix=".face_keystore_", suffix=".tmp"
    )
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, data)
        os.close(fd)
        fd = -1  # mark as closed
        os.rename(tmp_name, str(ks_path))
    except BaseException:
        if fd >= 0:
            os.close(fd)
        # Clean up temp file on failure
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise


def _reset_cache() -> None:
    """Reset internal caches (for testing)."""
    global _keystore_cache, _fernet_cache
    _keystore_cache = None
    _fernet_cache.clear()


# ── Fernet instances keyed by key_id ─────────────────────────────────

_fernet_cache: dict[int, Any] = {}


def _get_fernet(key_id: int | None = None):
    """Get a Fernet instance for the given key_id (or active key if None)."""
    from cryptography.fernet import Fernet

    ks = _load_keystore()
    if key_id is None:
        key_id = ks["active_key_id"]

    if key_id not in _fernet_cache:
        key_entry = ks["keys"].get(str(key_id))
        if key_entry is None:
            raise KeyError(f"Key ID {key_id} not found in keystore")
        _fernet_cache[key_id] = Fernet(key_entry["key"].encode("ascii"))

    return _fernet_cache[key_id]


# ── Legacy compatibility ─────────────────────────────────────────────

# Keep the old _ensure_key for any code that might call it directly,
# but route through the keystore.
_fernet_instance = None


def _ensure_key() -> bytes:
    """Load or generate the Fernet encryption key (legacy compat)."""
    ks = _load_keystore()
    active_id = ks["active_key_id"]
    return ks["keys"][str(active_id)]["key"].encode("ascii")


# ── Encrypt / Decrypt ────────────────────────────────────────────────


def encrypt_encoding(encoding) -> bytes:
    """Encrypt a 128D face encoding (numpy array or list of floats) to bytes.

    Produces a versioned blob: [version(1B), key_id(1B), fernet_payload...].
    """
    ks = _load_keystore()
    active_id = ks["active_key_id"]
    f = _get_fernet(active_id)

    floats = encoding.tolist() if hasattr(encoding, "tolist") else list(encoding)
    raw = struct.pack(_FLOAT_FMT, *floats)
    encrypted = f.encrypt(raw)

    header = struct.pack(_HEADER_FMT, _BLOB_VERSION, active_id)
    return header + encrypted


def decrypt_encoding(blob: bytes):
    """Decrypt an encrypted encoding blob back to a list of 128 floats.

    Supports both versioned blobs (with 2-byte header) and legacy blobs
    (no header, decrypted with key 0).
    """
    if len(blob) >= _HEADER_SIZE:
        version, key_id = struct.unpack(_HEADER_FMT, blob[:_HEADER_SIZE])
        if version == _BLOB_VERSION:
            f = _get_fernet(key_id)
            raw = f.decrypt(blob[_HEADER_SIZE:])
            return list(struct.unpack(_FLOAT_FMT, raw))

    # Legacy blob (no header) -- decrypt with key 0
    f = _get_fernet(0)
    raw = f.decrypt(blob)
    return list(struct.unpack(_FLOAT_FMT, raw))


# ── Key rotation ─────────────────────────────────────────────────────


def rotate_key() -> int:
    """Generate a new Fernet key and set it as the active key.

    Returns the new key ID. Old keys are never deleted so that existing
    blobs can still be decrypted.
    """
    from cryptography.fernet import Fernet

    ks = _load_keystore()

    # Find next key ID
    existing_ids = [int(k) for k in ks["keys"]]
    new_id = max(existing_ids) + 1

    key_b64 = Fernet.generate_key().decode("ascii")
    now = datetime.now(timezone.utc).isoformat()

    ks["keys"][str(new_id)] = {"key": key_b64, "created": now}
    ks["active_key_id"] = new_id

    _write_keystore(ks)

    # Invalidate caches so new key gets picked up
    global _keystore_cache
    _keystore_cache = ks  # keep it fresh
    # Clear fernet cache to ensure new key is loaded on next use
    _fernet_cache.pop(new_id, None)

    logger.info("Rotated face encryption key: new active key ID %d", new_id)
    return new_id


def get_key_info() -> dict[str, Any]:
    """Return information about the keystore.

    Returns a dict with:
      - key_count: total number of keys
      - active_key_id: the current active key ID
      - keys: dict of key_id -> {"created": iso_timestamp} (no secret material)
    """
    ks = _load_keystore()
    keys_info = {}
    for kid, entry in ks["keys"].items():
        keys_info[int(kid)] = {"created": entry.get("created", "unknown")}

    return {
        "key_count": len(ks["keys"]),
        "active_key_id": ks["active_key_id"],
        "keys": keys_info,
    }


# ── Noop (unencrypted) functions ─────────────────────────────────────


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
    """Delete the keystore file (and legacy key file if present). Returns True if deleted."""
    _reset_cache()
    deleted = False

    ks_path = _keystore_path()
    if ks_path.exists():
        ks_path.unlink()
        deleted = True
        logger.info("Deleted face keystore at %s", ks_path)

    legacy_path = _key_path()
    if legacy_path.exists():
        legacy_path.unlink()
        deleted = True
        logger.info("Deleted legacy face key at %s", legacy_path)

    return deleted
