import asyncio
import logging
import uuid

import asyncpg
import httpx

from . import engine
from .config import Settings

log = logging.getLogger(__name__)


class Background:
    """фоновые задачи, которые при остановке всё равно дожидаемся (или отменяем)"""

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task] = set()

    def spawn(self, coro, name: str | None = None) -> None:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("background task %s failed", task.get_name(), exc_info=task.exception())

    async def close(self, timeout: float) -> None:
        if not self._tasks:
            return
        _, pending = await asyncio.wait(self._tasks, timeout=timeout)
        for task in pending:
            # прерванные отправки остаются в бд как submitting, сторож живого воркера
            # переотправит их когда истечёт lease
            task.cancel()


class Dispatcher:
    def __init__(self, pool: asyncpg.Pool, http: httpx.AsyncClient, settings: Settings) -> None:
        self.pool = pool
        self.http = http
        self.settings = settings

    async def dispatch(self, attempt_id: uuid.UUID | None) -> None:
        """отправить попытку провайдеру. если отправка сразу упала, задача идёт на следующую ногу
        и её тоже сразу шлём, так что тут можно пройти несколько ног подряд"""
        while attempt_id is not None:
            attempt_id = await self._dispatch_one(attempt_id)

    async def _dispatch_one(self, attempt_id: uuid.UUID) -> uuid.UUID | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT a.id, a.provider, a.status, t.prompt, t.ref_images, m.slug AS model
                  FROM attempts a
                  JOIN tasks t ON t.id = a.task_id
                  JOIN models m ON m.id = t.model_id
                 WHERE a.id = $1
                """,
                attempt_id,
            )
        if row is None or row["status"] != "submitting":
            return None

        # пока ждём провайдера, коннект к бд не держим
        external_id, error = await self._send(row)

        async with self.pool.acquire() as conn:
            if external_id is not None:
                await engine.mark_submitted(conn, self.settings, attempt_id, external_id)
                return None
            log.warning("submit of attempt %s to %s failed: %s", attempt_id, row["provider"], error)
            settled = await engine.settle_attempt(
                conn, self.settings, attempt_id, error_type="submit_failed", only_if="submitting"
            )
            return settled.next_attempt

    async def _send(self, row) -> tuple[str | None, str | None]:
        provider = row["provider"]
        base_url = self.settings.provider_urls.get(provider)
        if base_url is None:
            return None, f"no URL configured for provider {provider!r}"
        body = {
            "model": row["model"],
            "prompt": row["prompt"],
            "ref_images": row["ref_images"],
            "callback_url": f"{self.settings.public_base_url}/webhooks/providers/{provider}",
            "client_ref": str(row["id"]),
        }
        try:
            response = await self.http.post(
                f"{base_url}/tasks",
                json=body,
                # чтоб переотправка после падения не создала у провайдера вторую задачу
                headers={"Idempotency-Key": str(row["id"])},
                timeout=self.settings.submit_timeout,
            )
        except httpx.HTTPError as exc:
            return None, f"{type(exc).__name__}: {exc}"
        if response.status_code not in (200, 201, 202):
            return None, f"HTTP {response.status_code}"
        try:
            return str(response.json()["external_id"]), None
        except (ValueError, KeyError, TypeError):
            return None, "response without external_id"
