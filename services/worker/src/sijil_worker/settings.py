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
