"""жизненный цикл задачи.

любой переход задачи идёт в одной транзакции под локом строки задачи (SELECT ... FOR UPDATE).
вебхуки, сторож и ошибки отправки по одной задаче выстраиваются в очередь, и каждый
перечитывает попытку уже под локом. дубль события видит что попытка уже закрыта и ничего
не делает, за счёт этого возврат и бывает ровно один
"""
import logging
import uuid
from dataclasses import dataclass

import asyncpg

from .config import Settings
from .routing import PROVIDER_ERRORS, GiveUp, Leg, after_failure, build_plan

log = logging.getLogger(__name__)

ACTIVE = ("submitting", "waiting")


class DomainError(Exception):
    status_code = 400
    code = "bad_request"

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NotFound(DomainError):
    status_code = 404
    code = "not_found"


class NoSuitableLeg(DomainError):
    status_code = 400
    code = "no_suitable_leg"


class InsufficientTokens(DomainError):
    status_code = 402
    code = "insufficient_tokens"


class TooManyActiveTasks(DomainError):
    status_code = 429
    code = "too_many_active_tasks"


class Conflict(DomainError):
    status_code = 409
    code = "conflict"


@dataclass(frozen=True)
class Created:
    task_id: uuid.UUID
    # какую попытку отправлять; None если по Idempotency-Key вернули уже существующую задачу
    attempt_id: uuid.UUID | None


@dataclass(frozen=True)
class Settled:
    outcome: str
    next_attempt: uuid.UUID | None = None


def normalize_error(error_type: str | None) -> str:
    # незнакомый тип ошибки всё равно косяк провайдера, задачу из-за него насовсем не валим
    return error_type if error_type in PROVIDER_ERRORS else "technical"


async def create_task(
    conn: asyncpg.Connection,
    settings: Settings,
    *,
    user_id: int,
    model_slug: str,
    prompt: str,
    ref_images: list[str],
    idempotency_key: str | None = None,
) -> Created:
    model = await conn.fetchrow("SELECT id, price FROM models WHERE slug = $1", model_slug)
    if model is None:
        raise NotFound(f"model {model_slug!r} not found")
    rows = await conn.fetch(
        "SELECT id, provider, priority, max_ref_images, skip_on_moderation FROM legs WHERE model_id = $1",
        model["id"],
    )
    plan = build_plan((Leg(*row) for row in rows), len(ref_images))
    if not plan:
        raise NoSuitableLeg(f"no leg of {model_slug!r} accepts {len(ref_images)} reference images")

    async with conn.transaction():
        # лочим юзера, чтоб параллельные /generate одного юзера шли по очереди
        # и не проскочили лимит активных задач и проверку баланса
        user = await conn.fetchrow("SELECT balance FROM users WHERE id = $1 FOR UPDATE", user_id)
        if user is None:
            raise NotFound(f"user {user_id} not found")

        if idempotency_key:
            existing = await conn.fetchval(
                "SELECT id FROM tasks WHERE user_id = $1 AND idempotency_key = $2", user_id, idempotency_key
            )
            if existing:
                return Created(existing, None)

        active = await conn.fetchval(
            "SELECT count(*) FROM tasks WHERE user_id = $1 AND status = 'running'", user_id
        )
        if active >= settings.max_active_tasks:
            raise TooManyActiveTasks(f"user already has {active} active tasks")
        if user["balance"] < model["price"]:
            raise InsufficientTokens(f"need {model['price']} tokens, have {user['balance']}")

        task_id = uuid.uuid4()
        await conn.execute(
            """
            INSERT INTO tasks (id, user_id, model_id, prompt, ref_images, price, plan, status, idempotency_key)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'running', $8)
            """,
            task_id, user_id, model["id"], prompt, ref_images, model["price"],
            [leg.to_json() for leg in plan], idempotency_key,
        )
        await conn.execute(
            "INSERT INTO ledger (user_id, delta, kind, op_key, task_id) VALUES ($1, $2, 'charge', $3, $4)",
            user_id, -model["price"], f"charge:{task_id}", task_id,
        )
        attempt_id = await _start_attempt(conn, settings, task_id, seq=1, leg_index=0, provider=plan[0].provider)
    return Created(task_id, attempt_id)


async def _start_attempt(
    conn: asyncpg.Connection, settings: Settings, task_id: uuid.UUID, *, seq: int, leg_index: int, provider: str
) -> uuid.UUID:
    # строка попытки работает как outbox: кто создал, тот и держит lease на отправку.
    # если процесс упал до отправки, сторож подберёт попытку когда lease истечёт
    attempt_id = uuid.uuid4()
    await conn.execute(
        """
        INSERT INTO attempts (id, task_id, seq, leg_index, provider, status, dispatch_lease_until, dispatch_count)
        VALUES ($1, $2, $3, $4, $5, 'submitting', now() + make_interval(secs => $6), 1)
        """,
        attempt_id, task_id, seq, leg_index, provider, settings.dispatch_lease,
    )
    return attempt_id


async def mark_submitted(
    conn: asyncpg.Connection, settings: Settings, attempt_id: uuid.UUID, external_id: str
) -> bool:
    """провайдер ответил 202, с этого момента тикает таймаут"""
    result = await conn.execute(
        """
        UPDATE attempts
           SET status = 'waiting',
               external_id = coalesce(external_id, $2),
               submitted_at = now(),
               deadline_at = now() + make_interval(secs => $3)
         WHERE id = $1 AND status = 'submitting'
        """,
        attempt_id, external_id, settings.provider_timeout,
    )
    # 0 строк: вебхук пришёл раньше нас или попытку уже закрыли
    return result.endswith(" 1")


async def find_attempt(
    conn: asyncpg.Connection, provider: str, external_id: str, client_ref: str | None
) -> uuid.UUID | None:
    attempt_id = await conn.fetchval(
        "SELECT id FROM attempts WHERE provider = $1 AND external_id = $2", provider, external_id
    )
    if attempt_id is not None or not client_ref:
        return attempt_id
    # вебхук может прийти раньше чем мы сохранили external_id (быстрый провайдер или упали
    # сразу после 202). client_ref это id нашей попытки, провайдер возвращает его в подписанном теле
    try:
        ref = uuid.UUID(client_ref)
    except ValueError:
        return None
    return await conn.fetchval("SELECT id FROM attempts WHERE id = $1 AND provider = $2", ref, provider)


async def settle_attempt(
    conn: asyncpg.Connection,
    settings: Settings,
    attempt_id: uuid.UUID,
    *,
    result_url: str | None = None,
    error_type: str | None = None,
    external_id: str | None = None,
    only_if: str | None = None,
    skip_locked: bool = False,
) -> Settled:
    """применить исход к попытке: completed с result_url или failed с error_type.

    only_if это условие вызывающего, перепроверяется под локом:
      "submitting" - для ошибок отправки, только если попытку ещё никто не закрыл
      "expired"    - для сторожа, только если всё ещё waiting и дедлайн прошёл
    """
    async with conn.transaction():
        task_id = await conn.fetchval("SELECT task_id FROM attempts WHERE id = $1", attempt_id)
        if task_id is None:
            return Settled("unknown_attempt")
        task = await conn.fetchrow(
            "SELECT id, user_id, price, plan, status FROM tasks WHERE id = $1 FOR UPDATE"
            + (" SKIP LOCKED" if skip_locked else ""),
            task_id,
        )
        if task is None:
            # задачу сейчас держит другой процесс, сторож глянет на следующем тике
            return Settled("busy")
        attempt = await conn.fetchrow(
            "SELECT *, coalesce(deadline_at < now(), false) AS expired FROM attempts WHERE id = $1", attempt_id
        )

        if only_if == "submitting" and attempt["status"] != "submitting":
            return Settled("noop")
        if only_if == "expired" and not (attempt["status"] == "waiting" and attempt["expired"]):
            return Settled("noop")

        if external_id and attempt["external_id"] is None:
            await conn.execute("UPDATE attempts SET external_id = $2 WHERE id = $1", attempt_id, external_id)

        if result_url is not None:
            settled = await _on_completed(conn, task, attempt, result_url)
        else:
            settled = await _on_failed(conn, settings, task, attempt, error_type or "technical")

    log.info("attempt %s (%s) of task %s: %s", attempt_id, attempt["provider"], task_id, settled.outcome)
    return settled


async def _on_completed(conn: asyncpg.Connection, task, attempt, result_url: str) -> Settled:
    if attempt["status"] == "completed":
        return Settled("duplicate")

    if attempt["status"] in ACTIVE:
        await conn.execute(
            "UPDATE attempts SET status = 'completed', result_url = $2, finished_at = now() WHERE id = $1",
            attempt["id"], result_url,
        )
        await _complete_task(conn, task["id"], result_url)
        return Settled("completed")

    # поздний результат: мы уже забили на эту попытку (таймаут или ошибка отправки)
    if task["status"] == "running":
        # задача ещё крутится на следующей ноге и возврата не было,
        # так что берём готовый результат и другую ногу больше не ждём
        await conn.execute(
            "UPDATE attempts SET status = 'superseded', finished_at = now() "
            "WHERE task_id = $1 AND status IN ('submitting', 'waiting')",
            task["id"],
        )
        await conn.execute(
            "UPDATE attempts SET status = 'completed', result_url = $2, late = true WHERE id = $1",
            attempt["id"], result_url,
        )
        await _complete_task(conn, task["id"], result_url)
        return Settled("late_accepted")

    # задача уже закончилась: возврат был или другая нога доделала. деньги уже ушли,
    # поэтому результат просто сохраняем в попытке для сверки, юзеру его не показываем
    await conn.execute(
        "UPDATE attempts SET result_url = $2, late = true WHERE id = $1", attempt["id"], result_url
    )
    return Settled("late_ignored")


async def _on_failed(conn: asyncpg.Connection, settings: Settings, task, attempt, error_type: str) -> Settled:
    if attempt["status"] not in ACTIVE:
        return Settled("duplicate")

    await conn.execute(
        "UPDATE attempts SET status = 'failed', error_type = $2, finished_at = now() WHERE id = $1",
        attempt["id"], error_type,
    )
    plan = [Leg.from_json(leg) for leg in task["plan"]]
    moderation_seen = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM attempts WHERE task_id = $1 AND id <> $2 AND error_type = 'moderation')",
        task["id"], attempt["id"],
    )
    decision = after_failure(plan, attempt["leg_index"], error_type, moderation_seen)

    if isinstance(decision, GiveUp):
        await _fail_task(conn, task, decision.reason)
        return Settled("failed")

    next_id = await _start_attempt(
        conn, settings, task["id"],
        seq=attempt["seq"] + 1, leg_index=decision.index, provider=plan[decision.index].provider,
    )
    await conn.execute("UPDATE tasks SET updated_at = now() WHERE id = $1", task["id"])
    return Settled("retrying", next_id)


async def _complete_task(conn: asyncpg.Connection, task_id: uuid.UUID, result_url: str) -> None:
    await conn.execute(
        "UPDATE tasks SET status = 'completed', result_url = $2, finished_at = now(), updated_at = now() "
        "WHERE id = $1",
        task_id, result_url,
    )


async def _fail_task(conn: asyncpg.Connection, task, reason: str) -> None:
    await conn.execute(
        "UPDATE tasks SET status = 'failed', failure_reason = $2, finished_at = now(), updated_at = now() "
        "WHERE id = $1",
        task["id"], reason,
    )
    # op_key уникальный: если логика выше вдруг пропустит второй возврат,
    # инсерт упадёт и вся транзакция откатится, а не заплатим дважды
    await conn.execute(
        "INSERT INTO ledger (user_id, delta, kind, op_key, task_id) VALUES ($1, $2, 'refund', $3, $4)",
        task["user_id"], task["price"], f"refund:{task['id']}", task["id"],
    )
