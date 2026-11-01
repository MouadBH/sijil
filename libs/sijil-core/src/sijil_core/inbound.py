"""Inbound email addresses: `{slug}.{token}@{domain}`.

The token is the secret part; the slug is only a human-readable prefix. Routing uses the token.
"""

import re
import secrets

_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567"  # lower-case base32
TOKEN_CHARS = 10
_LOCAL = re.compile(rf"[a-z0-9-]+\.([a-z2-7]{{{TOKEN_CHARS}}})")


def new_token() -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(TOKEN_CHARS))


def address(slug: str, token: str, domain: str) -> str:
    return f"{slug}.{token}@{domain}"


def parse(addr: str, domain: str) -> str | None:
    """The token of an address in `domain` (case-insensitive), or None for anything else."""
    local, sep, host = addr.lower().partition("@")
    if not sep or host != domain.lower():
        return None
    match = _LOCAL.fullmatch(local)
    return match.group(1) if match else None
