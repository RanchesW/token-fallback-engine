"""e2e тесты против поднятого стека (docker compose up).

ходят в сервис по http (4 воркера uvicorn), в админку фейк провайдеров,
а журнал проверяют прямо в бд
"""
import asyncpg
import httpx
import pytest

from .helpers import DATABASE_URL, FAST, PROVIDER_URLS, SERVICE_URL, Providers, new_user_id, pay


@pytest.fixture(scope="session", autouse=True)
def _stack_is_up():
    try:
        httpx.get(f"{SERVICE_URL}/health", timeout=5).raise_for_status()
    except httpx.HTTPError as exc:
        pytest.exit(f"service is not reachable at {SERVICE_URL} ({exc}); run `docker compose up` first", 2)


@pytest.fixture
async def api():
    async with httpx.AsyncClient(base_url=SERVICE_URL, timeout=30) as client:
        yield client


@pytest.fixture
async def db():
    conn = await asyncpg.connect(DATABASE_URL)
    yield conn
    await conn.close()


@pytest.fixture
async def providers():
    async with httpx.AsyncClient(timeout=10) as client:
        p = Providers(client)
        await p.reset()
        for name in PROVIDER_URLS:
            await p.configure(name, **FAST)
        yield p
        await p.reset()


@pytest.fixture
async def provider_timeout(api) -> float:
    return (await api.get("/health")).json()["provider_timeout"]


@pytest.fixture
def funded_user(api):
    async def make(tokens: int = 100) -> int:
        user_id = new_user_id()
        response = await pay(api, user_id, tokens)
        assert response.status_code == 200, response.text
        return user_id
    return make
