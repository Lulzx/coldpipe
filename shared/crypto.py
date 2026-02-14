"""Fernet-based credential encryption for mailbox passwords."""

from __future__ import annotations

import logging

from config.settings import DATA_DIR

log = logging.getLogger(__name__)

_KEYFILE = DATA_DIR / ".keyfile"


def _get_or_create_key() -> bytes | None:
    """Read or create the Fernet encryption key."""
    try:
        if _KEYFILE.exists():
            return _KEYFILE.read_bytes().strip()
        from cryptography.fernet import Fernet

        key = Fernet.generate_key()
        _KEYFILE.parent.mkdir(parents=True, exist_ok=True)
        _KEYFILE.write_bytes(key)
        _KEYFILE.chmod(0o600)
        return key
    except Exception:
        log.warning("crypto_key_unavailable", exc_info=True)
        return None


def encrypt(plaintext: str) -> str:
    """Encrypt a string with Fernet. Returns plaintext if key unavailable."""
    if not plaintext:
        return plaintext
    key = _get_or_create_key()
    if key is None:
        return plaintext
    from cryptography.fernet import Fernet

    f = Fernet(key)
    return f.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    """Decrypt a Fernet-encrypted string. Returns as-is if not encrypted or key unavailable."""
    if not ciphertext:
        return ciphertext
    key = _get_or_create_key()
    if key is None:
        return ciphertext
    try:
        from cryptography.fernet import Fernet

        f = Fernet(key)
        return f.decrypt(ciphertext.encode()).decode()
    except Exception:
        # Not encrypted or wrong key — return as-is
        return ciphertext
