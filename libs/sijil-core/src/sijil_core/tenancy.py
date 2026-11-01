"""Naming shared by the control plane (which records it) and the worker (which builds it).

Every runtime resource of a tenant (containers, network, volumes, databases, roles) is named
from `tenant_ref`, a short opaque id derived from the workspace id. The name-derived slug is for
display and URLs only: it can be long, and it would reveal the organisation's name to anyone
who can list containers or `pg_database`.
"""

import base64
import hashlib
from uuid import UUID

REF_CHARS = 16  # base32 characters, 80 bits


def tenant_ref(workspace_id: UUID) -> str:
    """`t` + 16 lower-case base32 characters, a pure function of the workspace id."""
    digest = hashlib.sha256(b"sijil-tenant:" + workspace_id.bytes).digest()
    return "t" + base64.b32encode(digest).decode().lower()[:REF_CHARS]


def tenant_db_names(ref: str) -> tuple[str, str]:
    """(Paperless database, index database) for a tenant ref; each is also its role name."""
    return f"paperless_{ref}", f"index_{ref}"
