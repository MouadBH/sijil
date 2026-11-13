from ipaddress import IPv4Network
from typing import Annotated, Any

from pydantic import RedisDsn, SecretStr, field_validator
from pydantic_settings import NoDecode

from sijil_core.crypto import KeySettings
from sijil_core.export_report import decode_key
from sijil_core.settings import CoreSettings
from sijil_core.storage import StorageSettings


class BrokerSettings(CoreSettings):
    """What every Celery worker needs: its region and the broker."""

    redis_url: RedisDsn


class SealSettings(StorageSettings, BrokerSettings):
    """The seal worker (`-Q seal`): the control-plane database as the app role, the broker, and
    the object store through the sealing key (SIJIL_OBJECTSTORE_ACCESS_KEY is that key in its
    container). No master key, no provisioner role, no tenant or AI settings: it never reads a
    credential or a document's content."""

    database_url: str


class RemovalSettings(StorageSettings, BrokerSettings):
    """The removal worker (`-Q removal`): the control-plane database as the app role, the
    broker, and the object store through the removal key (SIJIL_OBJECTSTORE_ACCESS_KEY is that
    key in its container: it deletes a version and reads its lock, nothing else). No master key,
    no provisioner role, no tenant or AI settings, no app or seal key."""

    database_url: str


class ExportSettings(KeySettings, StorageSettings, BrokerSettings):
    """The export worker (`-Q export`): the control-plane database as the app role, the broker,
    the master key (to open the workspaces' index databases, as the control plane does), the
    object store through the export key (read originals, own the exports bucket) and the
    region's Ed25519 signing seed (SIJIL_EXPORT_SIGNING_KEY, base64 of the raw 32 bytes), which
    no other service holds. No provisioner role, no tenant or AI settings."""

    database_url: str
    export_signing_key: SecretStr

    @field_validator("export_signing_key")
    @classmethod
    def _check_seed(cls, v: SecretStr) -> SecretStr:
        decode_key(v.get_secret_value())  # fail at boot, not at the first export
        return v

    def signing_seed(self) -> bytes:
        return decode_key(self.export_signing_key.get_secret_value())


class AiSettings(CoreSettings):
    """The in-region Ollama, for the workers that call it. Its host must be on
    `ai_allowed_hosts` (comma-separated in SIJIL_AI_ALLOWED_HOSTS), or the client refuses to
    start."""

    # Embeddings of the chunks (M5, sijil_worker.embed) and of Ask's questions: 1024 dimensions.
    embedding_model: str = "bge-m3"
    ollama_url: str = "http://ollama:11434"
    ai_allowed_hosts: Annotated[frozenset[str], NoDecode] = frozenset({"ollama"})
    # The model's threads (SIJIL_OLLAMA_NUM_THREAD): as many as Ollama's CPU quota, since it
    # otherwise starts one per host CPU and gets throttled. None: Ollama's own choice.
    ollama_num_thread: int | None = None

    @field_validator("ai_allowed_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, v: Any) -> Any:
        if isinstance(v, str):
            return frozenset(h.strip().lower() for h in v.split(",") if h.strip())
        return v


class ExtractionSettings(KeySettings, BrokerSettings, AiSettings):
    """What extraction and routing need, and nothing for provisioning: the extraction worker
    boots with this alone (least privilege: no provisioner role, no Docker settings)."""

    # Control-plane database, as the app role (DML only), same as the control plane.
    database_url: str
    # Extraction (M3), on the in-region Ollama.
    extraction_model: str = "qwen3:4b"


class AskSettings(KeySettings, BrokerSettings, AiSettings):
    """The Ask worker (`-Q ask`, sijil_worker.ask): the control-plane database as the app role,
    the broker, the master key (to open the workspaces' index databases) and the in-region
    Ollama. No object-store key of any kind, no provisioner role, no tenant settings: it reads
    and writes the index databases only."""

    database_url: str
    # The chat model that reads the question and writes the answer (SIJIL_ASK_MODEL).
    ask_model: str = "qwen3:4b"


class WorkerSettings(ExtractionSettings):
    """The main worker: everything, provisioning included (all required)."""

    # Role allowed to CREATE ROLE/DATABASE for tenants; used only by the tenant runtime.
    provisioner_database_url: str
    tenant_image: str
    compose_project: str
    tessdata_volume: str
    # Tenant networks are carved (as /28) out of this pool, away from Docker's default pools.
    tenant_subnet_pool: IPv4Network = IPv4Network("10.240.0.0/16")
