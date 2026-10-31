"""Envelope encryption for per-tenant secrets.

AES-256-GCM throughout. A per-workspace key-encryption key (KEK) is derived from the
regional master key with HKDF; it wraps a random 32-byte data key (AAD = key_id), and the
data key seals the payload (AAD = caller context). Errors never carry plaintext or keys.
"""

import base64
import binascii
import os
import struct
from dataclasses import dataclass
from typing import Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from pydantic import SecretStr, field_validator

from sijil_core.settings import CoreSettings

_MAGIC = b"SJE1"
_NONCE_LEN = 12
_KEY_LEN = 32


class DecryptionError(Exception):
    """Authentication or format failure. Deliberately carries no detail."""


class KeySettings(CoreSettings):
    master_key: SecretStr

    @field_validator("master_key")
    @classmethod
    def _check_master_key(cls, v: SecretStr) -> SecretStr:
        try:
            raw = base64.b64decode(v.get_secret_value(), validate=True)
        except (binascii.Error, ValueError):
            raise ValueError("master_key must be base64") from None
        if len(raw) != _KEY_LEN:
            raise ValueError("master_key must decode to exactly 32 bytes")
        return v

    def master_key_bytes(self) -> bytes:
        return base64.b64decode(self.master_key.get_secret_value())


@dataclass(frozen=True)
class DataKey:
    plaintext: bytes
    wrapped: bytes


class KeyService(Protocol):
    def generate_data_key(self, key_id: str) -> DataKey: ...

    def unwrap(self, key_id: str, wrapped: bytes) -> bytes: ...


class LocalKeyService(KeyService):
    def __init__(self, master_key: bytes) -> None:
        if len(master_key) != _KEY_LEN:
            raise ValueError("master key must be 32 bytes")
        self._master = master_key

    def derive(self, info: bytes) -> bytes:
        return HKDF(algorithm=hashes.SHA256(), length=_KEY_LEN, salt=None, info=info).derive(
            self._master
        )

    def _kek(self, key_id: str) -> bytes:
        return self.derive(b"sijil:kek:" + key_id.encode())

    def generate_data_key(self, key_id: str) -> DataKey:
        plaintext = os.urandom(_KEY_LEN)
        nonce = os.urandom(_NONCE_LEN)
        sealed = AESGCM(self._kek(key_id)).encrypt(nonce, plaintext, key_id.encode())
        return DataKey(plaintext=plaintext, wrapped=nonce + sealed)

    def unwrap(self, key_id: str, wrapped: bytes) -> bytes:
        if len(wrapped) <= _NONCE_LEN:
            raise DecryptionError
        try:
            return AESGCM(self._kek(key_id)).decrypt(
                wrapped[:_NONCE_LEN], wrapped[_NONCE_LEN:], key_id.encode()
            )
        except InvalidTag:
            raise DecryptionError from None


@dataclass(frozen=True)
class Envelope:
    key_id: str
    wrapped_key: bytes
    nonce: bytes
    ciphertext: bytes

    def to_bytes(self) -> bytes:
        kid = self.key_id.encode()
        return (
            _MAGIC
            + struct.pack(">H", len(kid))
            + kid
            + struct.pack(">H", len(self.wrapped_key))
            + self.wrapped_key
            + self.nonce
            + self.ciphertext
        )

    @classmethod
    def from_bytes(cls, b: bytes) -> "Envelope":
        try:
            if b[:4] != _MAGIC:
                raise DecryptionError
            pos = 4
            (klen,) = struct.unpack_from(">H", b, pos)
            pos += 2
            kid = b[pos : pos + klen]
            pos += klen
            (wlen,) = struct.unpack_from(">H", b, pos)
            pos += 2
            wrapped = b[pos : pos + wlen]
            pos += wlen
            nonce = b[pos : pos + _NONCE_LEN]
            ciphertext = b[pos + _NONCE_LEN :]
            if len(kid) != klen or len(wrapped) != wlen or len(nonce) != _NONCE_LEN:
                raise DecryptionError
            return cls(kid.decode(), wrapped, nonce, ciphertext)
        except (struct.error, UnicodeDecodeError):
            raise DecryptionError from None


def seal(plaintext: bytes, ks: KeyService, key_id: str, context: str) -> Envelope:
    dk = ks.generate_data_key(key_id)
    nonce = os.urandom(_NONCE_LEN)
    ct = AESGCM(dk.plaintext).encrypt(nonce, plaintext, context.encode())
    return Envelope(key_id=key_id, wrapped_key=dk.wrapped, nonce=nonce, ciphertext=ct)


def open_envelope(env: Envelope, ks: KeyService, context: str) -> bytes:
    data_key = ks.unwrap(env.key_id, env.wrapped_key)
    try:
        return AESGCM(data_key).decrypt(env.nonce, env.ciphertext, context.encode())
    except (InvalidTag, ValueError):
        raise DecryptionError from None
