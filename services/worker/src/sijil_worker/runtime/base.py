"""What provisioning needs from a tenant runtime. Every method is idempotent."""

from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID

# The Paperless superuser inside every tenant (matches the compose sample tenants).
ADMIN_USERNAME = "sijil-admin"

TenantStatus = Literal["absent", "starting", "running", "stopped"]


@dataclass(frozen=True)
class TenantSpec:
    workspace_id: UUID
    slug: str
    db_name: str
    index_db_name: str
    db_password: str
    index_db_password: str
    secret_key: str
    admin_password: str

    def __repr__(self) -> str:
        return f"TenantSpec(workspace_id={self.workspace_id}, slug={self.slug!r})"


@dataclass(frozen=True)
class TenantHandle:
    url: str


class TenantRuntime(Protocol):
    async def ensure_databases(self, spec: TenantSpec) -> None: ...

    async def ensure_running(self, spec: TenantSpec) -> TenantHandle: ...

    async def status(self, slug: str) -> TenantStatus: ...

    async def destroy(self, slug: str) -> None: ...
