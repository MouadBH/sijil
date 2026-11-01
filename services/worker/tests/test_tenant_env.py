from pathlib import Path

from sijil_worker.runtime.docker import TENANT_BASE_ENV

TENANT_ENV = Path(__file__).parents[3] / "infra" / "compose" / "paperless" / "tenant.env"


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def test_base_env_matches_compose_tenant_env() -> None:
    """Provisioned tenants and the compose sample tenants must be configured identically."""
    assert TENANT_BASE_ENV == _parse_env_file(TENANT_ENV)
