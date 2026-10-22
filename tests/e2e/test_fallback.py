from .helpers import balance_checked, finished, generate, ledger_kinds, wait_task


async def test_first_leg_fails_task_completes_on_second(api, db, providers, funded_user):
    await providers.configure("alpha", fail_rate=1, fail_type="technical")
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast")).json()["task_id"]

    task = await wait_task(api, task_id, finished)

    assert task["status"] == "completed"
    assert "beta" in task["result_url"]
    history = [(a["provider"], a["status"], a["error_type"]) for a in task["attempts"]]
    assert history == [("alpha", "failed", "technical"), ("beta", "completed", None)]
    assert task["current_leg"]["provider"] == "beta"
    assert task["refunded"] is False
    assert await ledger_kinds(db, user_id) == ["payment", "charge"]
    assert await balance_checked(db, user_id) == 8


async def test_technical_failure_on_video_goes_to_flagged_leg(api, providers, funded_user):
    # technical на beta: alpha можно (skip_on_moderation важен только после модерации)
    await providers.configure("beta", fail_rate=1, fail_type="technical")
    user_id = await funded_user(30)
    task_id = (await generate(api, user_id, "video-long")).json()["task_id"]

    task = await wait_task(api, task_id, finished)

    assert task["status"] == "completed"
    assert [a["provider"] for a in task["attempts"]] == ["beta", "alpha"]


async def test_sync_503_moves_to_next_leg(api, providers, funded_user):
    await providers.configure("alpha", sync_error_rate=1, sync_error="503")
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast")).json()["task_id"]

    task = await wait_task(api, task_id, finished)

    assert task["status"] == "completed"
    history = [(a["provider"], a["error_type"]) for a in task["attempts"]]
    assert history == [("alpha", "submit_failed"), ("beta", None)]


async def test_sync_timeout_moves_to_next_leg(api, providers, funded_user):
    # alpha висит на POST /tasks дольше SUBMIT_TIMEOUT и вообще не отвечает
    await providers.configure("alpha", sync_error_rate=1, sync_error="timeout", sync_hang_seconds=30, drop_rate=1)
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast")).json()["task_id"]

    task = await wait_task(api, task_id, finished, timeout=30)

    assert task["status"] == "completed"
    assert [(a["provider"], a["error_type"]) for a in task["attempts"]] == [("alpha", "submit_failed"), ("beta", None)]


async def test_all_legs_fail_refunds(api, db, providers, funded_user):
    await providers.configure("alpha", fail_rate=1)
    await providers.configure("beta", sync_error_rate=1)
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast")).json()["task_id"]

    task = await wait_task(api, task_id, finished)

    assert task["status"] == "failed"
    assert [(a["provider"], a["error_type"]) for a in task["attempts"]] == [
        ("alpha", "technical"), ("beta", "submit_failed"),
    ]
    assert await ledger_kinds(db, user_id) == ["payment", "charge", "refund"]
    assert await balance_checked(db, user_id) == 10


async def test_duplicate_concurrent_webhooks_from_provider(api, db, providers, funded_user):
    await providers.configure("alpha", duplicates=5, duplicates_concurrent=True)
    user_id = await funded_user(10)
    task_id = (await generate(api, user_id, "image-fast")).json()["task_id"]

    task = await wait_task(api, task_id, finished)

    assert task["status"] == "completed"
    assert len(task["attempts"]) == 1
    assert await ledger_kinds(db, user_id) == ["payment", "charge"]
