import asyncpg

from .engine import Conflict

CREDIT_STATUS = "succeeded"


async def apply_payment(
    conn: asyncpg.Connection, *, payment_id: str, user_id: int, tokens: int, status: str
) -> str:
    """зачислить оплату ровно один раз, сколько бы раз и как одновременно она ни пришла"""
    async with conn.transaction():
        # регистрации нет, юзера создаёт первая оплата
        await conn.execute("INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)
        await conn.execute(
            "INSERT INTO payments (payment_id, user_id, tokens, status) VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (payment_id) DO NOTHING",
            payment_id, user_id, tokens, status,
        )
        # все доставки одной оплаты встают тут в очередь, каждая видит что закоммитила предыдущая
        payment = await conn.fetchrow("SELECT * FROM payments WHERE payment_id = $1 FOR UPDATE", payment_id)

        if payment["user_id"] != user_id or payment["tokens"] != tokens:
            raise Conflict(f"payment {payment_id} was already seen with a different user or amount")
        if payment["credited_at"] is not None:
            return "duplicate"
        if status != CREDIT_STATUS:
            await conn.execute(
                "UPDATE payments SET status = $2, updated_at = now() WHERE payment_id = $1", payment_id, status
            )
            return "recorded"

        await conn.execute(
            "UPDATE payments SET status = $2, credited_at = now(), updated_at = now() WHERE payment_id = $1",
            payment_id, status,
        )
        await conn.execute(
            "INSERT INTO ledger (user_id, delta, kind, op_key, payment_id) VALUES ($1, $2, 'payment', $3, $4)",
            user_id, tokens, f"payment:{payment_id}", payment_id,
        )
        return "credited"
