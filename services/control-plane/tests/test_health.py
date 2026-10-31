import httpx


async def test_health_reports_region(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["region"] == "ma-cas"
    assert body["version"] == "0.0.0"


async def test_ready_returns_503_when_db_down(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/health/ready")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unavailable"
    assert body["checks"]["database"] == "down"
    assert body["checks"]["redis"] == "down"


async def test_openapi_served_under_v1(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/openapi.json")
    assert response.status_code == 200
    assert response.json()["info"]["title"] == "Sijil control plane"
