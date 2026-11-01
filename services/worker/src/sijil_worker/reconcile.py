"""Re-attach compose services to tenant networks.

Recreating a compose service (e.g. `docker compose up` after a rebuild) drops its attachment to
every tenant network, cutting active tenants off. `reconcile_tenants` connects each service in
`ATTACHED_SERVICES` to every network labelled `sijil.tenant` it is missing from. It is idempotent
and never raises for one bad network or container: it logs and moves on. Existing networks are
only connected to, never changed.
"""

import logging

import docker
from docker.errors import APIError, NotFound

from sijil_worker.runtime.docker import ATTACHED_SERVICES, TENANT_LABEL
from sijil_worker.settings import WorkerSettings

log = logging.getLogger(__name__)


def reconcile_tenants(client: docker.DockerClient, settings: WorkerSettings) -> list[str]:
    """Returns the `"<service>@<network>"` pairs it connected."""
    fixed: list[str] = []
    try:
        networks = client.networks.list(filters={"label": TENANT_LABEL})
        services = {
            service: client.containers.list(
                filters={
                    "label": [
                        f"com.docker.compose.project={settings.compose_project}",
                        f"com.docker.compose.service={service}",
                    ]
                }
            )
            for service in ATTACHED_SERVICES
        }
    except Exception:
        log.exception("reconcile: could not list networks or containers")
        return fixed
    for network in networks:
        try:
            network.reload()
            members = {c.id for c in network.containers}
        except Exception:
            log.exception("reconcile: could not inspect network %s", network.name)
            continue
        for service, containers in services.items():
            for container in containers:
                if container.id in members:
                    continue
                try:
                    network.connect(container, aliases=[service])
                except NotFound:
                    continue
                except APIError as exc:
                    if exc.status_code == 409 or "already exists" in str(exc.explanation or ""):
                        continue
                    log.exception("reconcile: %s@%s failed", service, network.name)
                    continue
                except Exception:
                    log.exception("reconcile: %s@%s failed", service, network.name)
                    continue
                log.info("reconcile: connected %s to %s", service, network.name)
                fixed.append(f"{service}@{network.name}")
    return fixed
