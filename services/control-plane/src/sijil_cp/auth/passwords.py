"""Password hashing: argon2id with argon2-cffi defaults."""

import secrets

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

MIN_LENGTH = 12
MAX_LENGTH = 128

_hasher = PasswordHasher()
# Verified against when the account doesn't exist, so an unknown email costs one argon2 verify,
# the same as a wrong password (no enumeration by timing).
_DUMMY_HASH = _hasher.hash(secrets.token_urlsafe(32))


def acceptable(pw: str) -> bool:
    return MIN_LENGTH <= len(pw) <= MAX_LENGTH


def hash_password(pw: str) -> str:
    return _hasher.hash(pw)


def verify_password(hash: str | None, pw: str) -> bool:
    if hash is None:
        try:
            _hasher.verify(_DUMMY_HASH, pw)
        except VerificationError:
            pass
        return False
    try:
        return _hasher.verify(hash, pw)
    except (VerificationError, InvalidHashError):
        return False
