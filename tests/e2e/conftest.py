"""e2e тесты против поднятого стека (docker compose up).

ходят в сервис по http (4 воркера uvicorn), в админку фейк провайдеров,
а журнал проверяют прямо в бд
"""
import asyncio
import json
import os
import random
import time
import uuid

import asyncpg
import httpx
import pytest

from app.signing import sign

SERVICE_URL = os.environ.get("SERVICE_URL", "http://localhost:8000")
PROVIDER_URLS = {
    "alpha": os.environ.get("ALPHA_URL", "http://localhost:9001"),
    "beta": os.environ.get("BETA_URL", "http://localhost:9002"),
}
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/app")
SECRET = os.environ.get("WEBHOOK_SECRET", "dev-secret")

# в тестах провайдеры отвечают быстро, если тест не скажет иначе
FAST = {"delay_min": 0.1, "delay_max": 0.3}


class Providers:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def configure(self, name: str, **behaviour) -> None:
        response = await self.client.put(f"{PROVIDER_URLS[name]}/_admin/config", json=behaviour)
        response.raise_for_status()

    async def reset(self) -> None:
        for url in PROVIDER_URLS.values():
            (await self.client.post(f"{url}/_admin/reset")).raise_for_status()


def new_user_id() -> int:
    # у каждого теста свой юзер, чтоб тесты не видели чужие деньги и задачи
    return random.randint(10**12, 10**15)


async def signed_post(client: httpx.AsyncClient, path: str, payload: dict, secret: str = SECRET) -> httpx.Response:
    body = json.dumps(payload).encode()
    headers = {"X-Signature": sign(body, secret), "Content-Type": "application/json"}
    return await client.post(path, content=body, headers=headers)


async def pay(api: httpx.AsyncClient, user_id: int, tokens: int, payment_id: str | None = None) -> httpx.Response:
    return await signed_post(api, "/webhooks/payments", {
        "payment_id": payment_id or f"pay-{uuid.uuid4().hex}",
        "user_id": user_id,
        "tokens": tokens,
        "status": "succeeded",
    })


async def generate(api: httpx.AsyncClient, user_id: int, model: str, refs: int = 0, **extra) -> httpx.Response:
    return await api.post("/generate", json={
        "user_id": user_id,
        "model_slug": model,
        "prompt": "a cat in a hat",
        "ref_images": [f"https://img.example/{i}.png" for i in range(refs)],
    }, **extra)


async def wait_task(api: httpx.AsyncClient, task_id: str, until, timeout: float = 30, poll: float = 0.2) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        task = (await api.get(f"/tasks/{task_id}")).json()
        if until(task):
            return task
        if time.monotonic() > deadline:
            raise AssertionError(f"task did not reach the expected state in {timeout}s: {task}")
        await asyncio.sleep(poll)


def finished(task: dict) -> bool:
    return task["status"] in ("completed", "failed")


async def ledger_kinds(db: asyncpg.Connection, user_id: int) -> list[str]:
    rows = await db.fetch("SELECT kind FROM ledger WHERE user_id = $1 ORDER BY id", user_id)
    return [row["kind"] for row in rows]


async def balance_checked(db: asyncpg.Connection, user_id: int) -> int:
    """баланс юзера, заодно проверяем что он равен сумме журнала"""
    balance, total = await db.fetchrow(
        "SELECT balance, (SELECT coalesce(sum(delta), 0) FROM ledger WHERE user_id = $1) FROM users WHERE id = $1",
        user_id,
    )
    assert balance == total
    return balance


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
