import asyncio
import uuid

from .helpers import balance_checked, ledger_kinds, new_user_id, signed_post


async def test_20_concurrent_identical_payment_webhooks_credit_once(api, db):
    user_id = new_user_id()
    event = {"payment_id": f"pay-{uuid.uuid4().hex}", "user_id": user_id, "tokens": 50, "status": "succeeded"}

    responses = await asyncio.gather(*(signed_post(api, "/webhooks/payments", event) for _ in range(20)))

    assert [r.status_code for r in responses] == [200] * 20
    results = [r.json()["result"] for r in responses]
    assert results.count("credited") == 1
    assert results.count("duplicate") == 19
    assert await ledger_kinds(db, user_id) == ["payment"]
    assert await balance_checked(db, user_id) == 50
    assert (await api.get(f"/users/{user_id}/balance")).json()["balance"] == 50


async def test_pending_then_succeeded_credits_once(api, db):
    user_id = new_user_id()
    event = {"payment_id": f"pay-{uuid.uuid4().hex}", "user_id": user_id, "tokens": 7}

    assert (await signed_post(api, "/webhooks/payments", {**event, "status": "pending"})).json() == {"result": "recorded"}
    assert await balance_checked(db, user_id) == 0
    assert (await signed_post(api, "/webhooks/payments", {**event, "status": "succeeded"})).json() == {"result": "credited"}
    assert (await signed_post(api, "/webhooks/payments", {**event, "status": "succeeded"})).json() == {"result": "duplicate"}
    assert await balance_checked(db, user_id) == 7


async def test_same_payment_with_other_amount_is_rejected(api, db):
    user_id = new_user_id()
    event = {"payment_id": f"pay-{uuid.uuid4().hex}", "user_id": user_id, "tokens": 5, "status": "succeeded"}
    assert (await signed_post(api, "/webhooks/payments", event)).status_code == 200

    response = await signed_post(api, "/webhooks/payments", {**event, "tokens": 500})

    assert response.status_code == 409
    assert await balance_checked(db, user_id) == 5


async def test_bad_signature_is_rejected(api, db):
    user_id = new_user_id()
    event = {"payment_id": f"pay-{uuid.uuid4().hex}", "user_id": user_id, "tokens": 5, "status": "succeeded"}

    response = await signed_post(api, "/webhooks/payments", event, secret="wrong")

    assert response.status_code == 401
    assert await db.fetchval("SELECT count(*) FROM users WHERE id = $1", user_id) == 0
