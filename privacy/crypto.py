"""Envelope encryption for per-subject crypto shredding.

Every data subject gets their own data key (DEK). Personal fields are encrypted with it, and the DEK
itself is stored wrapped by a key-encryption key (KEK) that never leaves the key management system.
Erasing a subject means destroying their DEK: the ciphertext survives in Kafka segments, backups and
partner extracts, and stops meaning anything everywhere at once.

Two encryption modes, because the choice has consequences downstream:

* randomised (the default) uses a fresh nonce per value, so the same email encrypts differently
  each time. Nothing downstream can join or group on it, which is usually what you want.
* deterministic derives the nonce from the plaintext with HMAC, so equal values produce equal
  ciphertext and an equality join still works. It leaks equality: anyone reading the column can see
  which rows share a value, and for a low-cardinality field that is close to revealing it. Use it
  only where a downstream system genuinely has to match on the field.

The KEK here comes from an environment variable so the demo runs offline. In a real deployment it is
a Cloud KMS key and `wrap`/`unwrap` become KMS Encrypt/Decrypt calls, which is why they are split
out: the DEK is the only thing this code ever holds in the clear.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_BYTES = 12
KEY_BYTES = 32
KEK_ENV = "ERASURE_KEK"


class ShreddedKeyError(LookupError):
    """Raised when a subject's key is gone, which is the expected state after erasure."""


def new_data_key() -> bytes:
    return AESGCM.generate_key(bit_length=KEY_BYTES * 8)


def kek_from_env() -> bytes:
    """The key-encryption key, or a fixed development one.

    A missing KEK is only tolerable locally. In any deployed environment this is a KMS key and a
    missing value should stop the process rather than silently fall back.
    """
    configured = os.environ.get(KEK_ENV)
    if configured:
        return base64.b64decode(configured)
    return hashlib.sha256(b"local-development-kek-not-for-real-data").digest()


def wrap(data_key: bytes, kek: bytes | None = None) -> str:
    """Encrypt a DEK for storage. In production this is a KMS Encrypt call."""
    return encrypt_bytes(data_key, kek or kek_from_env())


def unwrap(wrapped: str, kek: bytes | None = None) -> bytes:
    return decrypt_bytes(wrapped, kek or kek_from_env())


def encrypt_bytes(plaintext: bytes, key: bytes, nonce: bytes | None = None) -> str:
    nonce = nonce or os.urandom(NONCE_BYTES)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)
    return base64.b64encode(nonce + ciphertext).decode("ascii")


def decrypt_bytes(token: str, key: bytes) -> bytes:
    raw = base64.b64decode(token)
    return AESGCM(key).decrypt(raw[:NONCE_BYTES], raw[NONCE_BYTES:], None)


def encrypt_field(value: str | None, data_key: bytes, deterministic: bool = False) -> str | None:
    """Encrypt one personal field. None stays None, so a missing value is still missing."""
    if value is None:
        return None
    nonce = _deterministic_nonce(value, data_key) if deterministic else None
    return encrypt_bytes(value.encode("utf-8"), data_key, nonce)


def decrypt_field(token: str | None, data_key: bytes) -> str | None:
    if token is None:
        return None
    return decrypt_bytes(token, data_key).decode("utf-8")


def _deterministic_nonce(value: str, data_key: bytes) -> bytes:
    """Nonce derived from the value under the subject's own key.

    Keyed rather than a plain hash, so the same value under two different subjects still encrypts
    differently. Equality only leaks within one subject's own rows.
    """
    return hmac.new(data_key, value.encode("utf-8"), hashlib.sha256).digest()[:NONCE_BYTES]
