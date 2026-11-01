"""TOTP (RFC 6238) and recovery codes.

30 s steps, 6 digits, one step of drift either way. A step is accepted at most once per user:
callers persist the returned step and pass it back as `last_step`, and every step at or before
it is refused, so a code can't be replayed inside its window.
"""

import hashlib
import hmac
import secrets
from datetime import datetime

import pyotp

from sijil_core.crypto import LocalKeyService

ISSUER = "Sijil"
INTERVAL_S = 30
DIGITS = 6
VALID_WINDOW = 1
RECOVERY_CODE_COUNT = 10
RECOVERY_CODE_LEN = 10
RECOVERY_INFO = b"sijil:recovery-codes"
_BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"


def new_secret() -> str:
    return pyotp.random_base32()


def provisioning_uri(secret: str, email: str) -> str:
    return pyotp.TOTP(secret, digits=DIGITS, interval=INTERVAL_S).provisioning_uri(
        name=email, issuer_name=ISSUER
    )


def current_step(now: datetime) -> int:
    return int(now.timestamp()) // INTERVAL_S


def verify(secret: str, code: str, last_step: int | None, now: datetime) -> int | None:
    """The accepted time-step, or None. Steps at or before `last_step` are never accepted."""
    if len(code) != DIGITS or not code.isascii() or not code.isdigit():
        return None
    otp = pyotp.TOTP(secret, digits=DIGITS, interval=INTERVAL_S)
    now_step = current_step(now)
    accepted = None
    # Compare every candidate, so timing doesn't reveal which step matched.
    for step in range(now_step - VALID_WINDOW, now_step + VALID_WINDOW + 1):
        match = hmac.compare_digest(otp.generate_otp(step), code)
        if match and accepted is None and (last_step is None or step > last_step):
            accepted = step
    return accepted


def new_recovery_codes() -> list[str]:
    return [
        "".join(secrets.choice(_BASE32) for _ in range(RECOVERY_CODE_LEN))
        for _ in range(RECOVERY_CODE_COUNT)
    ]


def normalise_recovery_code(code: str) -> str:
    """Forgive case, spaces and dashes; the alphabet itself is fixed."""
    return "".join(ch for ch in code.upper() if ch not in " -\t")


def recovery_hmac(keys: LocalKeyService, code: str) -> bytes:
    key = keys.derive(RECOVERY_INFO)
    return hmac.new(key, normalise_recovery_code(code).encode(), hashlib.sha256).digest()
