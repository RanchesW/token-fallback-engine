"""сторож крутится в каждом воркере. лидера не выбираем, работу делим через локи строк:
кандидатов берём с SKIP LOCKED и перепроверяем под локом задачи,
так что 4 сторожа никогда не обработают одну попытку дважды"""
import asyncio
import logging
import random

import asyncpg

from . import engine
from .config import Settings
from .dispatch import Background, Dispatcher

log = logging.getLogger(__name__)

BATCH = 50


class Watchdog:
    def __init__(self, pool: asyncpg.Pool, settings: Settings, dispatcher: Dispatcher, background: Background):
        self.pool = pool
        self.settings = settings
        self.dispatcher = dispatcher
        self.background = background

    async def run(self) -> None:
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("watchdog tick failed")
            # немного рандома, чтоб воркеры не долбили бд в одну и ту же секунду
            await asyncio.sleep(self.settings.watchdog_interval * random.uniform(0.7, 1.3))

    async def tick(self) -> None:
        await self.expire_silent_attempts()
        await self.resend_stuck_attempts()

    async def expire_silent_attempts(self) -> None:
        """провайдер принял задачу, но молчит дольше PROVIDER_TIMEOUT"""
        async with self.pool.acquire() as conn:
            # дедлайны сравниваем по часам бд, а не воркера
            rows = await conn.fetch(
                "SELECT id FROM attempts WHERE status = 'waiting' AND deadline_at < now() "
                "ORDER BY deadline_at LIMIT $1",
                BATCH,
            )
            for row in rows:
                settled = await engine.settle_attempt(
                    conn, self.settings, row["id"], error_type="timeout", only_if="expired", skip_locked=True
                )
                if settled.next_attempt:
                    self.background.spawn(self.dispatcher.dispatch(settled.next_attempt))

    async def resend_stuck_attempts(self) -> None:
        """попытки, у которых отправитель умер между созданием строки и ответом провайдера"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE attempts
                   SET dispatch_lease_until = now() + make_interval(secs => $1),
                       dispatch_count = dispatch_count + 1
                 WHERE id IN (
                        SELECT id FROM attempts
                         WHERE status = 'submitting' AND dispatch_lease_until < now()
                         ORDER BY dispatch_lease_until
                         LIMIT $2
                           FOR UPDATE SKIP LOCKED)
                RETURNING id, dispatch_count
                """,
                self.settings.dispatch_lease, BATCH,
            )
            for row in rows:
                if row["dispatch_count"] <= self.settings.max_dispatches:
                    log.warning("resending stuck attempt %s (try %s)", row["id"], row["dispatch_count"])
                    self.background.spawn(self.dispatcher.dispatch(row["id"]))
                    continue
                settled = await engine.settle_attempt(
                    conn, self.settings, row["id"], error_type="submit_failed", only_if="submitting"
                )
                if settled.next_attempt:
                    self.background.spawn(self.dispatcher.dispatch(settled.next_attempt))
