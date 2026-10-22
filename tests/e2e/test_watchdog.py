import asyncio
import json
import uuid

from .conftest import balance_checked, finished, generate, ledger_kinds, wait_task


async def test_late_completed_after_watchdog(api, db, providers, funded_user, provider_timeout):
    """правило 6. оба провайдера принимают задачу, а completed шлют только через PROVIDER_TIMEOUT + 5с.

    A. video-long с 2 референсами, нога одна (beta). сторож сдаётся, задача failed,
       токены вернулись. поздний completed не должен отдать результат.
    B. image-fast: сторож сдаётся на alpha и идёт на beta. поздний completed от alpha
       приходит пока beta ещё работает: задача его берёт, попытка beta superseded,
       возврата нет.
    гоняем оба сразу, чтоб ждать PROVIDER_TIMEOUT один раз
    """
    for name in ("alpha", "beta"):
        await providers.configure(name, late_rate=1, late_delay=provider_timeout + 5)
    user_id = await funded_user(100)
    task_a = (await generate(api, user_id, "video-long", refs=2)).json()["task_id"]
    task_b = (await generate(api, user_id, "image-fast")).json()["task_id"]
    patience = provider_timeout + 20

    async def scenario_a():
        task = await wait_task(api, task_a, finished, timeout=patience)
        assert task["status"] == "failed"
        assert task["refunded"] is True
        assert [(a["provider"], a["error_type"]) for a in task["attempts"]] == [("beta", "timeout")]

        task = await wait_task(api, task_a, lambda t: t["attempts"][0]["late_completed"], timeout=20)
        assert task["status"] == "failed"
        assert task["result_url"] is None

    async def scenario_b():
        task = await wait_task(api, task_b, lambda t: len(t["attempts"]) == 2, timeout=patience)
        assert task["attempts"][0]["error_type"] == "timeout"

        task = await wait_task(api, task_b, finished, timeout=20)
        assert task["status"] == "completed"
        assert "alpha" in task["result_url"]
        assert task["refunded"] is False
        first, second = task["attempts"]
        assert (first["provider"], first["status"], first["late_completed"]) == ("alpha", "completed", True)
        assert (second["provider"], second["status"]) == ("beta", "superseded")

    await asyncio.gather(scenario_a(), scenario_b())

    # по одному списанию на задачу и один возврат только у A: 100 - 25 + 25 - 2
    assert sorted(await ledger_kinds(db, user_id)) == ["charge", "charge", "payment", "refund"]
    assert await balance_checked(db, user_id) == 98


async def test_resends_attempt_left_by_crashed_worker(api, db, providers, funded_user):
    """воркер закоммитил задачу и первую попытку и умер до запроса к провайдеру.
    воспроизводим это прямо в бд, сторож должен сам отправить"""
    user_id = await funded_user(10)
    model = await db.fetchrow("SELECT id, price FROM models WHERE slug = 'image-fast'")
    legs = await db.fetch(
        "SELECT id AS leg_id, provider, priority, max_ref_images, skip_on_moderation "
        "FROM legs WHERE model_id = $1 ORDER BY priority",
        model["id"],
    )
    task_id, attempt_id = uuid.uuid4(), uuid.uuid4()
    async with db.transaction():
        await db.execute(
            "INSERT INTO tasks (id, user_id, model_id, prompt, ref_images, price, plan, status) "
            "VALUES ($1, $2, $3, 'crash test', '[]', $4, $5::jsonb, 'running')",
            task_id, user_id, model["id"], model["price"], json.dumps([dict(leg) for leg in legs]),
        )
        await db.execute(
            "INSERT INTO ledger (user_id, delta, kind, op_key, task_id) VALUES ($1, $2, 'charge', $3, $4)",
            user_id, -model["price"], f"charge:{task_id}", task_id,
        )
        await db.execute(
            "INSERT INTO attempts (id, task_id, seq, leg_index, provider, status, dispatch_lease_until, dispatch_count) "
            "VALUES ($1, $2, 1, 0, 'alpha', 'submitting', now() - interval '1 second', 1)",
            attempt_id, task_id,
        )

    task = await wait_task(api, str(task_id), finished, timeout=20)

    assert task["status"] == "completed"
    assert [(a["provider"], a["status"]) for a in task["attempts"]] == [("alpha", "completed")]
    assert await balance_checked(db, user_id) == 8
