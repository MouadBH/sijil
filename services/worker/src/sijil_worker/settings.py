from ipaddress import IPv4Network
from typing import Annotated, Any

from pydantic import RedisDsn, field_validator
from pydantic_settings import NoDecode

from sijil_core.crypto import KeySettings


class ExtractionSettings(KeySettings):
    """What extraction and routing need, and nothing for provisioning: the extraction worker
    boots with this alone (least privilege: no provisioner role, no Docker settings)."""

    redis_url: RedisDsn
    # Control-plane database, as the app role (DML only), same as the control plane.
    database_url: str
    # Extraction (M3): the in-region Ollama only. Its host must be on `ai_allowed_hosts`
    # (comma-separated in SIJIL_AI_ALLOWED_HOSTS), or the client refuses to start.
    extraction_model: str = "qwen3:4b"
    ollama_url: str = "http://ollama:11434"
    ai_allowed_hosts: Annotated[frozenset[str], NoDecode] = frozenset({"ollama"})

    @field_validator("ai_allowed_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, v: Any) -> Any:
        if isinstance(v, str):
            return frozenset(h.strip().lower() for h in v.split(",") if h.strip())
        return v


class WorkerSettings(ExtractionSettings):
    """The main worker: everything, provisioning included (all required)."""

    # Role allowed to CREATE ROLE/DATABASE for tenants; used only by the tenant runtime.
    provisioner_database_url: str
    tenant_image: str
    compose_project: str
    tessdata_volume: str
    # Tenant networks are carved (as /28) out of this pool, away from Docker's default pools.
    tenant_subnet_pool: IPv4Network = IPv4Network("10.240.0.0/16")
