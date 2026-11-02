"""reconcile_tenants only touches this compose project's tenant networks."""

from typing import Any

from sijil_worker.reconcile import reconcile_tenants
from sijil_worker.settings import WorkerSettings


class Net:
    def __init__(self, name: str, labels: dict[str, str]) -> None:
        self.name = name
        self.attrs = {"Labels": labels}
        self.containers: list[Any] = []
        self.connected: list[str] = []

    def reload(self) -> None: ...

    def connect(self, container: Any, aliases: list[str]) -> None:
        self.connected.append(aliases[0])


class Container:
    id = "c1"


class Client:
    def __init__(self, nets: list[Net]) -> None:
        self.networks = self
        self.containers = self
        self._nets = nets

    def list(self, filters: dict[str, Any] | None = None) -> list[Any]:
        if filters and filters.get("label") == "sijil.tenant":
            return self._nets
        return [Container()]


def _settings() -> WorkerSettings:
    return WorkerSettings(
        region="ma-cas",
        environment="test",
        redis_url="redis://127.0.0.1:1/0",
        master_key="a" * 43 + "=",
        database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        provisioner_database_url="postgresql+asyncpg://x:y@127.0.0.1:1/z",
        tenant_image="img",
        compose_project="sijil",
        tessdata_volume="sijil_tessdata",
    )


def test_only_this_projects_networks_are_reconciled() -> None:
    mine = Net("sijil_tenant_taaaa", {"sijil.tenant": "taaaa", "sijil.project": "sijil"})
    legacy = Net("sijil_tenant_tbbbb", {"sijil.tenant": "tbbbb"})  # created before the label
    other = Net("other_tenant_tcccc", {"sijil.tenant": "tcccc", "sijil.project": "other"})
    spoof = Net("sijil_tenant_tdddd", {"sijil.tenant": "tdddd", "sijil.project": "other"})
    fixed = reconcile_tenants(Client([mine, legacy, other, spoof]), _settings())  # type: ignore[arg-type]
    assert {f.split("@")[1] for f in fixed} == {"sijil_tenant_taaaa", "sijil_tenant_tbbbb"}
    assert other.connected == [] and spoof.connected == []
    assert mine.connected and legacy.connected
