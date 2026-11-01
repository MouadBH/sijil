"""Naming shared by the control plane (which records it) and the worker (which builds it)."""


def tenant_db_names(slug: str) -> tuple[str, str]:
    """(Paperless database, index database) for a workspace slug; each is also its role name."""
    names = slug.replace("-", "_")
    return f"paperless_{names}", f"index_{names}"
