import asyncpg
import pytest


async def test_every_balance_equals_its_ledger(db):
    broken = await db.fetch(
        "SELECT u.id, u.balance, coalesce(sum(l.delta), 0) AS total "
        "FROM users u LEFT JOIN ledger l ON l.user_id = u.id "
        "GROUP BY u.id HAVING u.balance <> coalesce(sum(l.delta), 0)"
    )
    assert broken == []


async def test_no_task_is_both_completed_and_refunded(db):
    both = await db.fetch(
        "SELECT t.id FROM tasks t JOIN ledger l ON l.op_key = 'refund:' || t.id WHERE t.status <> 'failed'"
    )
    assert both == []


async def test_balance_cannot_change_without_ledger(db, funded_user):
    user_id = await funded_user(5)
    with pytest.raises(asyncpg.RaiseError, match="only through ledger"):
        await db.execute("UPDATE users SET balance = balance + 100 WHERE id = $1", user_id)
    with pytest.raises(asyncpg.RaiseError, match="append-only"):
        await db.execute("UPDATE ledger SET delta = 1000 WHERE user_id = $1", user_id)
