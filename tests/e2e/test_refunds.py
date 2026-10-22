import asyncio

from .helpers import balance_checked, generate, ledger_kinds, signed_post, wait_task


def on_second_leg(task: dict) -> bool:
    return len(task["attempts"]) == 2 and task["attempts"][1]["status"] == "waiting"


async def test_10_concurrent_failures_on_last_leg_refund_once(api, db, providers, funded_user):
    await providers.configure("alpha", fail_rate=1, fail_type="technical")
    await providers.configure("beta", drop_rate=1)  # задачу принимает и молчит
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast")).json()["task_id"]
    task = await wait_task(api, task_id, on_second_leg)
    last = task["attempts"][1]
    assert last["provider"] == "beta"

    event = {"external_id": last["external_id"], "status": "failed", "error_type": "technical"}
    responses = await asyncio.gather(
        *(signed_post(api, "/webhooks/providers/beta", event) for _ in range(10))
    )

    assert [r.status_code for r in responses] == [200] * 10
    results = [r.json()["result"] for r in responses]
    assert results.count("failed") == 1
    assert results.count("duplicate") == 9

    task = (await api.get(f"/tasks/{task_id}")).json()
    assert task["status"] == "failed"
    assert task["failure_reason"] == "legs_exhausted"
    assert task["refunded"] is True
    assert await ledger_kinds(db, user_id) == ["payment", "charge", "refund"]
    assert await balance_checked(db, user_id) == 10


async def test_payload_error_fails_task_without_trying_next_leg(api, db, providers, funded_user):
    await providers.configure("alpha", fail_rate=1, fail_type="payload")
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast")).json()["task_id"]

    task = await wait_task(api, task_id, lambda t: t["status"] == "failed")

    assert task["failure_reason"] == "payload_rejected"
    assert [(a["provider"], a["error_type"]) for a in task["attempts"]] == [("alpha", "payload")]
    assert await ledger_kinds(db, user_id) == ["payment", "charge", "refund"]
    assert await balance_checked(db, user_id) == 10


async def test_moderation_skips_legs_marked_skip_on_moderation(api, db, providers, funded_user):
    # video-long: сначала beta, потом alpha со skip_on_moderation
    await providers.configure("beta", fail_rate=1, fail_type="moderation")
    user_id = await funded_user(30)
    task_id = (await generate(api, user_id, "video-long")).json()["task_id"]

    task = await wait_task(api, task_id, lambda t: t["status"] == "failed")

    assert [(a["provider"], a["error_type"]) for a in task["attempts"]] == [("beta", "moderation")]
    assert task["refunded"] is True
    assert await balance_checked(db, user_id) == 30
