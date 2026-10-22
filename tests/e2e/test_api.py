import asyncio
import uuid

from .helpers import balance_checked, generate, ledger_kinds, new_user_id, signed_post


async def test_no_suitable_leg_is_400_and_charges_nothing(api, db, funded_user):
    user_id = await funded_user(100)

    response = await generate(api, user_id, "image-fast", refs=11)  # максимум 10 на beta

    assert response.status_code == 400
    assert await ledger_kinds(db, user_id) == ["payment"]
    assert await balance_checked(db, user_id) == 100


async def test_ref_count_picks_suitable_legs(api, providers, funded_user):
    # 5 референсов: alpha берёт до 4, значит начинаем с beta
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast", refs=5)).json()["task_id"]

    task = (await api.get(f"/tasks/{task_id}")).json()

    assert task["attempts"][0]["provider"] == "beta"


async def test_not_enough_tokens_is_402(api, db, funded_user):
    user_id = await funded_user(1)

    response = await generate(api, user_id, "image-fast")

    assert response.status_code == 402
    assert await balance_checked(db, user_id) == 1


async def test_unknown_user_and_model_are_404(api, funded_user):
    assert (await generate(api, new_user_id(), "image-fast")).status_code == 404
    assert (await generate(api, await funded_user(10), "no-such-model")).status_code == 404


async def test_fourth_active_task_is_429_even_when_sent_concurrently(api, db, providers, funded_user):
    await providers.configure("alpha", drop_rate=1)  # задачи так и висят активными
    user_id = await funded_user(100)

    responses = await asyncio.gather(*(generate(api, user_id, "image-fast") for _ in range(6)))

    codes = sorted(r.status_code for r in responses)
    assert codes == [202, 202, 202, 429, 429, 429]
    assert await ledger_kinds(db, user_id) == ["payment", "charge", "charge", "charge"]
    assert await balance_checked(db, user_id) == 94


async def test_idempotency_key_returns_same_task(api, db, providers, funded_user):
    user_id = await funded_user(10)
    headers = {"Idempotency-Key": uuid.uuid4().hex}

    first = await generate(api, user_id, "image-fast", headers=headers)
    second = await generate(api, user_id, "image-fast", headers=headers)

    assert (first.status_code, second.status_code) == (202, 200)
    assert first.json()["task_id"] == second.json()["task_id"]
    assert await ledger_kinds(db, user_id) == ["payment", "charge"]


async def test_provider_webhook_checks_signature_and_task(api):
    event = {"external_id": "nope", "status": "failed", "error_type": "technical"}
    assert (await signed_post(api, "/webhooks/providers/alpha", event, secret="wrong")).status_code == 401
    assert (await signed_post(api, "/webhooks/providers/alpha", event)).status_code == 404
    assert (await signed_post(api, "/webhooks/providers/gamma", event)).status_code == 404


async def test_balance_endpoint_shows_recent_ledger(api, funded_user):
    user_id = await funded_user(10)

    body = (await api.get(f"/users/{user_id}/balance")).json()

    assert body["balance"] == 10
    assert [(row["kind"], row["delta"], row["balance_after"]) for row in body["ledger"]] == [("payment", 10, 10)]
