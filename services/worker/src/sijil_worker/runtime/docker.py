"""`TenantRuntime` on the Docker Engine API, for development and CI.

One tenant = one internal network `{project}_tenant_{slug}` holding that tenant's Redis and
Paperless-ngx (the official image, configured by environment only, no published port). The
compose services the tenant needs, or that need the tenant, are attached to that network; they
are the only other members, so a tenant never reaches another tenant or the internet.

Every resource carries the labels `sijil.tenant={slug}` (plus `sijil.workspace={id}` and
`sijil.role`); `destroy` and the smoke test find tenants by them. Labels never carry secrets.
The Docker SDK is synchronous, so each public method runs in a worker thread.
"""

import asyncio
import contextlib
import logging
import re
from typing import Any, Final

import docker
from docker.errors import APIError, ImageNotFound, NotFound
from docker.models.containers import Container
from docker.models.networks import Network

from sijil_core.tenancy import tenant_db_names
from sijil_worker.runtime.base import ADMIN_USERNAME, TenantHandle, TenantSpec, TenantStatus
from sijil_worker.runtime.postgres import drop_tenant_databases, ensure_tenant_databases
from sijil_worker.settings import WorkerSettings

log = logging.getLogger(__name__)

# Same values as infra/compose/paperless/tenant.env (a unit test keeps them equal).
TENANT_BASE_ENV: Final[dict[str, str]] = {
    "PAPERLESS_DBENGINE": "postgresql",
    "PAPERLESS_DBHOST": "postgres",
    "PAPERLESS_DBPORT": "5432",
    "PAPERLESS_OCR_LANGUAGE": "ara+fra+eng",
    "PAPERLESS_OCR_OUTPUT_TYPE": "pdfa",
    "PAPERLESS_TIKA_ENABLED": "1",
    "PAPERLESS_TIKA_ENDPOINT": "http://tika:9998",
    "PAPERLESS_TIKA_GOTENBERG_ENDPOINT": "http://gotenberg:3000",
    "PAPERLESS_WEBSERVER_WORKERS": "1",
    "PAPERLESS_TASK_WORKERS": "1",
    "PAPERLESS_THREADS_PER_WORKER": "1",
    "PAPERLESS_TIME_ZONE": "Africa/Casablanca",
    "USERMAP_UID": "1000",
    "USERMAP_GID": "1000",
}
REDIS_IMAGE: Final = "redis:7.4.12-alpine"
# Compose services joined to every tenant network: the control plane and worker reach the
# tenant; the tenant reaches the shared helpers. The alias keeps the compose service name
# resolvable from inside the tenant network (e.g. PAPERLESS_DBHOST=postgres).
ATTACHED_SERVICES: Final = ("control-plane", "worker", "postgres", "tika", "gotenberg", "ollama")
REQUIRED_SERVICES: Final = frozenset({"postgres", "tika", "gotenberg"})
TESSDATA_PATH: Final = "/usr/share/tesseract-ocr/5/tessdata"
_SECOND_NS: Final = 1_000_000_000
_HEALTHCHECK: Final[dict[str, Any]] = {
    "test": ["CMD", "curl", "-fs", "-o", "/dev/null", "http://localhost:8000/accounts/login/"],
    "interval": 15 * _SECOND_NS,
    "timeout": 5 * _SECOND_NS,
    "retries": 20,
    "start_period": 60 * _SECOND_NS,
}
_SLUG: Final = re.compile(r"^[a-z0-9][a-z0-9-]{0,47}$")
TENANT_LABEL: Final = "sijil.tenant"


def validate_slug(slug: str) -> str:
    if not _SLUG.fullmatch(slug):
        raise ValueError("invalid tenant slug")
    return slug


def _already(exc: APIError) -> bool:
    return exc.status_code == 409 or "already exists" in str(exc.explanation or "")


class DockerRuntime:
    def __init__(self, client: docker.DockerClient, settings: WorkerSettings) -> None:
        self._client = client
        self._settings = settings
        self._project = settings.compose_project

    # Names ---------------------------------------------------------------------------------

    def network_name(self, slug: str) -> str:
        return f"{self._project}_tenant_{validate_slug(slug)}"

    def paperless_name(self, slug: str) -> str:
        return f"{self._project}-paperless-{validate_slug(slug)}"

    def redis_name(self, slug: str) -> str:
        return f"{self._project}-paperless-redis-{validate_slug(slug)}"

    def volume_name(self, slug: str, kind: str) -> str:
        return f"{self._project}_{validate_slug(slug)}_{kind}"

    def url(self, slug: str) -> str:
        return f"http://{self.paperless_name(slug)}:8000"

    @staticmethod
    def _labels(spec: TenantSpec, role: str) -> dict[str, str]:
        return {
            TENANT_LABEL: spec.slug,
            "sijil.workspace": str(spec.workspace_id),
            "sijil.role": role,
        }

    # TenantRuntime -------------------------------------------------------------------------

    async def ensure_databases(self, spec: TenantSpec) -> None:
        await ensure_tenant_databases(self._settings.provisioner_database_url, spec)

    async def ensure_running(self, spec: TenantSpec) -> TenantHandle:
        return await asyncio.to_thread(self._ensure_running, spec)

    async def status(self, slug: str) -> TenantStatus:
        return await asyncio.to_thread(self._status, slug)

    async def destroy(self, slug: str) -> None:
        await asyncio.to_thread(self._destroy_docker, slug)
        db_name, index_db_name = tenant_db_names(slug)
        await drop_tenant_databases(self._settings.provisioner_database_url, db_name, index_db_name)

    # Implementation (sync, run in a thread) ------------------------------------------------

    def _ensure_running(self, spec: TenantSpec) -> TenantHandle:
        slug = validate_slug(spec.slug)
        try:
            self._client.volumes.get(self._settings.tessdata_volume)
        except NotFound:
            raise RuntimeError("tessdata volume missing (run the tessdata-init service)") from None
        network = self._ensure_network(spec)
        self._attach_shared(network)
        volumes = {}
        for kind, path in (
            ("data", "/usr/src/paperless/data"),
            ("media", "/usr/src/paperless/media"),
        ):
            name = self.volume_name(slug, kind)
            self._client.volumes.create(name=name, labels=self._labels(spec, kind))  # idempotent
            volumes[name] = {"bind": path, "mode": "rw"}
        volumes[self._settings.tessdata_volume] = {"bind": TESSDATA_PATH, "mode": "ro"}

        self._ensure_container(
            self.redis_name(slug),
            image=REDIS_IMAGE,
            network=network.name,
            labels=self._labels(spec, "redis"),
            restart_policy={"Name": "unless-stopped"},
        )
        url = self.url(slug)
        self._ensure_container(
            self.paperless_name(slug),
            image=self._settings.tenant_image,
            network=network.name,
            labels=self._labels(spec, "paperless"),
            restart_policy={"Name": "unless-stopped"},
            environment={
                **TENANT_BASE_ENV,
                "PAPERLESS_DBNAME": spec.db_name,
                "PAPERLESS_DBUSER": spec.db_name,
                "PAPERLESS_DBPASS": spec.db_password,
                "PAPERLESS_REDIS": f"redis://{self.redis_name(slug)}:6379",
                "PAPERLESS_SECRET_KEY": spec.secret_key,
                "PAPERLESS_ADMIN_USER": ADMIN_USERNAME,
                "PAPERLESS_ADMIN_PASSWORD": spec.admin_password,
                "PAPERLESS_URL": url,
            },
            volumes=volumes,
            healthcheck=_HEALTHCHECK,
        )
        return TenantHandle(url=url)

    def _ensure_network(self, spec: TenantSpec) -> Network:
        name = self.network_name(spec.slug)
        try:
            network = self._client.networks.get(name)
        except NotFound:
            try:
                network = self._client.networks.create(
                    name, driver="bridge", internal=True, labels=self._labels(spec, "network")
                )
            except APIError as exc:
                if not _already(exc):
                    raise
                network = self._client.networks.get(name)
        # Never reuse a network that could route out, whoever created it.
        if network.attrs.get("Internal") is not True:
            raise RuntimeError(f"network {name} exists but is not internal")
        return network

    def _attach_shared(self, network: Network) -> None:
        for service in ATTACHED_SERVICES:
            containers = self._client.containers.list(
                filters={
                    "label": [
                        f"com.docker.compose.project={self._project}",
                        f"com.docker.compose.service={service}",
                    ]
                }
            )
            if not containers:
                if service in REQUIRED_SERVICES:
                    raise RuntimeError(f"compose service {service} is not running")
                log.warning("compose service %s not running; not attached", service)
            for container in containers:
                if network.name in container.attrs["NetworkSettings"]["Networks"]:
                    continue
                try:
                    network.connect(container, aliases=[service])
                except APIError as exc:
                    if not _already(exc):
                        raise

    def _ensure_container(self, name: str, **create: Any) -> Container:
        try:
            container = self._client.containers.get(name)
        except NotFound:
            container = self._create(name, create)
        if container.status != "running":
            container.start()
        return container

    def _create(self, name: str, create: dict[str, Any]) -> Container:
        for attempt in range(2):
            try:
                return self._client.containers.create(name=name, detach=True, **create)
            except ImageNotFound:
                if attempt:
                    raise
                self._client.images.pull(create["image"])
            except APIError as exc:
                if exc.status_code != 409:
                    raise
                return self._client.containers.get(name)  # created concurrently
        raise AssertionError("unreachable")  # pragma: no cover

    def _status(self, slug: str) -> TenantStatus:
        try:
            container = self._client.containers.get(self.paperless_name(slug))
        except NotFound:
            return "absent"
        state = container.attrs["State"]
        if not state.get("Running"):
            return "stopped"
        health = (state.get("Health") or {}).get("Status")
        return "running" if health in (None, "healthy") else "starting"

    def _destroy_docker(self, slug: str) -> None:
        validate_slug(slug)
        flt: dict[str, str | list[str] | bool] = {"label": f"{TENANT_LABEL}={slug}"}
        for container in self._client.containers.list(all=True, filters=flt):
            with contextlib.suppress(NotFound):
                container.remove(force=True)
        networks = {n.id: n for n in self._client.networks.list(filters=flt)}
        with contextlib.suppress(NotFound):
            named = self._client.networks.get(self.network_name(slug))
            networks[named.id] = named
        for network in networks.values():
            with contextlib.suppress(NotFound):
                network.reload()
                for member in network.containers:
                    with contextlib.suppress(NotFound, APIError):
                        network.disconnect(member, force=True)
                network.remove()
        for volume in self._client.volumes.list(filters=flt):
            with contextlib.suppress(NotFound):
                volume.remove(force=True)
