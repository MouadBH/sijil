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
    # Opaque short id (`sijil_core.tenancy.tenant_ref`) naming every runtime resource.
    ref: str
    db_name: str
    index_db_name: str
    db_password: str
    index_db_password: str
    secret_key: str
    admin_password: str

    def __repr__(self) -> str:
        return f"TenantSpec(workspace_id={self.workspace_id}, ref={self.ref!r})"


@dataclass(frozen=True)
class TenantHandle:
    url: str


class TenantRuntime(Protocol):
    async def ensure_databases(self, spec: TenantSpec) -> None: ...

    async def ensure_index_schema(self, spec: TenantSpec) -> None: ...

    async def ensure_running(self, spec: TenantSpec) -> TenantHandle: ...

    async def status(self, ref: str) -> TenantStatus: ...

    async def stop(self, ref: str) -> None:
        """Stops the tenant's containers and keeps everything else (volumes, network,
        databases) for `start`: a hibernated workspace (ADM-5)."""
        ...

    async def start(self, ref: str) -> None:
        """Starts a stopped tenant's containers again. LookupError when it has none."""
        ...

    async def destroy(self, ref: str) -> None: ...
