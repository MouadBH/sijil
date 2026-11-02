from ipaddress import IPv4Network

from pydantic import RedisDsn

from sijil_core.crypto import KeySettings


class WorkerSettings(KeySettings):
    redis_url: RedisDsn
    # Control-plane database, as the app role (DML only), same as the control plane.
    database_url: str
    # Role allowed to CREATE ROLE/DATABASE for tenants; used only by the tenant runtime.
    provisioner_database_url: str
    tenant_image: str
    compose_project: str
    tessdata_volume: str
    # Tenant networks are carved (as /28) out of this pool, away from Docker's default pools.
    tenant_subnet_pool: IPv4Network = IPv4Network("10.240.0.0/16")
